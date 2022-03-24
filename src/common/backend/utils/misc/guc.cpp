/* --------------------------------------------------------------------
 * guc.c
 *
 * Support for grand unified configuration schema, including SET
 * command, configuration file, and command line options.
 * See src/backend/utils/misc/README for more information.
 *
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * IDENTIFICATION
 * src/backend/utils/misc/guc.c
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
#ifdef PGXC
#include "access/gtm.h"
#include "pgxc/pgxc.h"
#endif
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/dfs/dfs_insert.h"
#ifdef ENABLE_WHITEBOX
#include "access/ustore/knl_whitebox_test.h"
#endif
#include "gs_bbox.h"
#include "catalog/namespace.h"
#include "catalog/pgxc_group.h"
#include "catalog/storage_gtt.h"
#include "commands/async.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "commands/copy.h"
#endif
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
#ifdef PGXC
#include "commands/tablecmds.h"
#include "nodes/nodes.h"
#include "optimizer/pgxcship.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "optimizer/pgxcplan.h"
#include "pgxc/poolmgr.h"
#include "pgxc/nodemgr.h"
#include "utils/lsyscache.h"
#endif
#include "access/multi_redo_settings.h"
#include "catalog/pg_authid.h"
#include "commands/user.h"
#include "flock.h"
#include "gaussdb_version.h"
#include "utils/hll.h"
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
#include "postmaster/bgworker.h"
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
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/utils/constant_def.h"
#endif
#include "utils/acl.h"
#include "utils/anls_opt.h"
#include "utils/atomic.h"
#include "utils/be_module.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/distribute_test.h"
#include "utils/segment_test.h"
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
#include "utils/guc_sql.h"
#include "utils/guc_storage.h"
#include "utils/guc_security.h"
#include "utils/guc_memory.h"
#include "utils/guc_network.h"
#include "utils/guc_resource.h"

#ifndef PG_KRB_SRVTAB
#define PG_KRB_SRVTAB ""
#endif
#ifndef PG_KRB_SRVNAM
#define PG_KRB_SRVNAM ""
#endif

#define CONFIG_FILENAME "postgresql.conf"
#define CONFIG_LOCK_FILE "postgresql.conf.lock"
#ifdef ENABLE_MOT
#define MOT_CONFIG_FILENAME "mot.conf"
#endif
#define HBA_FILENAME "pg_hba.conf"
#define IDENT_FILENAME "pg_ident.conf"
#define INVALID_LINES_IDX (int)(~0)
#define MAX_PARAM_LEN 1024
#define WRITE_CONFIG_LOCK_LEN (1024 * 1024)

#ifdef EXEC_BACKEND
#define CONFIG_EXEC_PARAMS "global/config_exec_params"
#define CONFIG_EXEC_PARAMS_NEW "global/config_exec_params.new"
#endif

/* upper limit for GUC variables measured in kilobytes of memory */
/* note that various places assume the byte size fits in a "long" variable */
#if SIZEOF_SIZE_T > 4 && SIZEOF_LONG > 4
#define MAX_KILOBYTES INT_MAX
#else
#define MAX_KILOBYTES (INT_MAX / 1024)
#endif

#ifdef ENABLE_UT
#define static
#endif

#define KB_PER_MB 1024
#define KB_PER_GB (1024 * 1024)

#define MS_PER_S 1000
#define S_PER_MIN 60
#define MS_PER_MIN (1000 * 60)
#define MIN_PER_H 60
#define S_PER_H (60 * 60)
#define MS_PER_H (1000 * 60 * 60)
#define MIN_PER_D (60 * 24)
#define S_PER_D (60 * 60 * 24)
#define MS_PER_D (1000 * 60 * 60 * 24)
#define H_PER_D 24

extern volatile bool most_available_sync;
extern void SetThreadLocalGUC(knl_session_context* session);
THR_LOCAL int comm_ackchk_time;

THR_LOCAL GucContext currentGucContext;

const char* sync_guc_variable_namelist[] = {"work_mem",
    "query_mem",
    "ustore_attr",
    "ssl_renegotiation_limit",
    "temp_buffers",
    "maintenance_work_mem",
    "max_stack_depth",
    "temp_file_limit",
    "vacuum_cost_delay",
    "vacuum_cost_page_hit",
    "vacuum_cost_page_miss",
    "vacuum_cost_page_dirty",
    "vacuum_cost_limit",
    "effective_io_concurrency",
    "synchronous_commit",
    "commit_delay",
    "commit_siblings",
    "client_min_messages",
    "log_min_messages",
    "log_min_error_statement",
    "log_min_duration_statement",
    "logging_module",
    "analysis_options",
    "plog_merge_age",
    "application_name",
    "connection_info",
    "cgroup_name",
    "memory_detail_tracking",
    "debug_print_parse",
    "debug_print_rewritten",
    "debug_print_plan",
    "debug_pretty_print",
    "log_connections",
    "log_disconnections",
    "log_duration",
    "log_error_verbosity",
    "log_lock_waits",
    "log_statement",
    "log_temp_files",
    "track_activities",
    "enable_instr_track_wait",
    "enable_instr_rt_percentile",
    "instr_rt_percentile_interval",
    "enable_wdr_snapshot",
    "wdr_snapshot_interval",
    "wdr_snapshot_retention_days",
    "wdr_snapshot_query_timeout",
    "asp_sample_num",
    "asp_sample_interval",
    "asp_flush_rate",
    "asp_retention_days",
    "enable_asp",
    "track_counts",
    "track_io_timing",
    "track_functions",
    "update_process_title",
    "log_statement_stats",
    "log_parser_stats",
    "log_planner_stats",
    "log_executor_stats",
    "search_path",
    "percentile",
    "default_tablespace",
    "temp_tablespaces",
    "check_function_bodies",
    "default_transaction_isolation",
    "default_transaction_read_only",
    "default_transaction_deferrable",
    "session_replication_role",
    "session_respool",
    "statement_timeout",
    "vacuum_freeze_table_age",
    "vacuum_freeze_min_age",
    "bytea_output",
    "xmlbinary",
    "xmloption",
    "DateStyle",
    "IntervalStyle",
    "TimeZone",
    "timezone_abbreviations",
    "extra_float_digits",
    "client_encoding",
    "lc_messages",
    "lc_monetary",
    "lc_numeric",
    "lc_time",
    "default_text_search_config",
    "dynamic_library_path",
    "gin_fuzzy_search_limit",
    "local_preload_libraries",
    "deadlock_timeout",
    "array_nulls",
    "backslash_quote",
    "default_with_oids",
    "escape_string_warning",
    "lo_compat_privileges",
    "quote_all_identifiers",
    "sql_inheritance",
    "standard_conforming_strings",
    "synchronize_seqscans",
    "transform_null_equals",
    "exit_on_error",
#ifdef ENABLE_MULTIPLE_NODES
    "gtm_backup_barrier",
#endif
    "xc_maintenance_mode",
    "enable_hdfs_predicate_pushdown",
    "enable_hadoop_env",
    "behavior_compat_options",
    "enable_valuepartition_pruning",
    "enable_constraint_optimization",
    "enable_bloom_filter",
    "cstore_insert_mode",
    "enable_delta_store",
    "enable_codegen",
    "enable_codegen_print",
    "codegen_cost_threshold",
    "codegen_strategy",
    "max_query_retry_times",
    "convert_string_to_digit",
#ifdef ENABLE_MULTIPLE_NODES
    "agg_redistribute_enhancement",
#endif
    "sql_compatibility",
    "hashagg_table_size",
    "max_loaded_cudesc",
    "partition_mem_batch",
    "partition_max_cache_size",
    "memory_tracking_mode",
    "enable_early_free",
    "cstore_backwrite_quantity",
    "cstore_backwrite_max_threshold",
    "prefetch_quantity",
    "backwrite_quantity",
    "cstore_prefetch_quantity",
    "enable_fast_allocate",
    "enable_adio_debug",
    "enable_adio_function",
    "fast_extend_file_size",
    "enable_global_stats",
    "enable_hypo_index",
    "td_compatible_truncation",
    "ngram_punctuation_ignore",
    "ngram_grapsymbol_ignore",
    "ngram_gram_size",
    "enable_orc_cache",
#ifdef ENABLE_MULTIPLE_NODES
    "enable_cluster_resize",
    "gds_debug_mod",
#endif
    "enable_compress_spill",
    "resource_track_level",
    "fault_mon_timeout",
    "trace_sort",
    "ignore_checksum_failure",
    "wal_log_hints",
#ifdef ENABLE_MULTIPLE_NODES
    "enable_parallel_ddl",
    "max_cn_temp_file_size",
#endif
    "cn_send_buffer_size",
    "enable_compress_hll",
    "hll_default_log2m",
    "hll_default_log2explicit",
    "hll_default_log2sparse",
    "hll_duplicate_check",
    "hll_default_regwidth",
    "hll_default_sparseon",
    "hll_default_expthresh",
    "hll_max_sparse",
    "enable_sonic_optspill",
    "enable_sonic_hashjoin",
    "enable_sonic_hashagg",
#ifdef ENABLE_MULTIPLE_NODES
    "enable_stream_recursive",
#endif
    "data_sync_retry",
#ifdef ENABLE_QUNIT
    "qunit_case_number",
#endif
    "instr_unique_sql_count",
    "enable_instr_cpu_timer",
    "instr_unique_sql_track_type",
    "enable_incremental_catchup",
    "wait_dummy_time",
    "max_recursive_times",
    "sql_use_spacelimit",
    "default_limit_rows",
    "sql_beta_feature",
#ifndef ENABLE_MULTIPLE_NODES
    "plsql_show_all_error",
    "uppercase_attribute_name",
#endif 
    "track_stmt_session_slot",
    "track_stmt_stat_level",
    "track_stmt_details_size"
    };

static void set_config_sourcefile(const char* name, char* sourcefile, int sourceline);
static bool call_bool_check_hook(struct config_bool* conf, bool* newval, void** extra, GucSource source, int elevel);
static bool call_int_check_hook(struct config_int* conf, int* newval, void** extra, GucSource source, int elevel);
static bool call_int64_check_hook(struct config_int64* conf, int64* newval, void** extra, GucSource source, int elevel);
static bool call_real_check_hook(struct config_real* conf, double* newval, void** extra, GucSource source, int elevel);
static bool call_string_check_hook(
    struct config_string* conf, char** newval, void** extra, GucSource source, int elevel);
static bool call_enum_check_hook(struct config_enum* conf, int* newval, void** extra, GucSource source, int elevel);
static bool check_log_destination(char** newval, void** extra, GucSource source);
static void assign_log_destination(const char* newval, void* extra);

static void assign_syslog_facility(int newval, void* extra);
static void assign_syslog_ident(const char* newval, void* extra);
static void assign_session_replication_role(int newval, void* extra);
static bool check_client_min_messages(int* newval, void** extra, GucSource source);
static bool check_debug_assertions(bool* newval, void** extra, GucSource source);
#ifdef USE_BONJOUR
static bool check_bonjour(bool* newval, void** extra, GucSource source);
#endif

static bool check_gpc_syscache_threshold(bool* newval, void** extra, GucSource source);
static bool check_stage_log_stats(bool* newval, void** extra, GucSource source);
static bool check_log_stats(bool* newval, void** extra, GucSource source);
static void assign_thread_working_version_num(int newval, void* extra);
static bool check_pgxc_maintenance_mode(bool* newval, void** extra, GucSource source);

#ifdef LIBCOMM_SPEED_TEST_ENABLE
static void assign_comm_test_thread_num(int newval, void* extra);
static void assign_comm_test_msg_len(int newval, void* extra);
static void assign_comm_test_send_sleep(int newval, void* extra);
static void assign_comm_test_send_once(int newval, void* extra);
static void assign_comm_test_recv_sleep(int newval, void* extra);
static void assign_comm_test_recv_once(int newval, void* extra);
#endif
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
static void assign_comm_fault_injection(int newval, void* extra);
#endif

bool check_canonical_path(char** newval, void** extra, GucSource source);
bool check_directory(char** newval, void** extra, GucSource source);
static bool check_log_filename(char** newval, void** extra, GucSource source);
static bool check_perf_log(char** newval, void** extra, GucSource source);
static bool check_timezone_abbreviations(char** newval, void** extra, GucSource source);
static void assign_timezone_abbreviations(const char* newval, void* extra);
static void pg_timezone_abbrev_initialize(void);
static void assign_tcp_keepalives_idle(int newval, void *extra);
static void assign_tcp_keepalives_interval(int newval, void *extra);
static void assign_tcp_keepalives_count(int newval, void *extra);
static const char* show_tcp_keepalives_idle(void);
static const char* show_tcp_keepalives_interval(void);
static const char* show_tcp_keepalives_count(void);
static bool check_effective_io_concurrency(int* newval, void** extra, GucSource source);
static void assign_effective_io_concurrency(int newval, void* extra);
static void assign_pgstat_temp_directory(const char* newval, void* extra);
static bool check_application_name(char** newval, void** extra, GucSource source);
static void assign_application_name(const char* newval, void* extra);
static const char* show_log_file_mode(void);
static char* config_enum_get_options(
    struct config_enum* record, const char* prefix, const char* suffix, const char* separator);
static bool validate_conf_option(struct config_generic* record, const char *name, const char *value,
    GucSource source, int elevel, bool freemem, void *newval, void **newextra);

/* Database Security: Support password complexity */
bool check_double_parameter(double* newval, void** extra, GucSource source);
static void check_setrole_permission(const char* rolename, char* passwd, bool IsSetRole);
static bool verify_setrole_passwd(const char* rolename, char* passwd, bool IsSetRole);
bool CheckReplChannel(const char* ChannelInfo);
static bool isOptLineCommented(char* optLine);
static bool isMatchOptionName(char* optLine, const char* paraName,
    int paraLength, int* paraOffset, int* valueLength, int* valueOffset, bool ignore_case);

bool logging_module_check(char** newval, void** extra, GucSource source);
void logging_module_guc_assign(const char* newval, void* extra);

/* Inplace Upgrade GUC hooks */
static bool check_is_upgrade(bool* newval, void** extra, GucSource source);
static void assign_is_inplace_upgrade(const bool newval, void* extra);
bool transparent_encrypt_kms_url_region_check(char** newval, void** extra, GucSource source);

/* SQL DFx Options : Support different sql dfx option */
static const char* analysis_options_guc_show(void);
static bool analysis_options_check(char** newval, void** extra, GucSource source);
static void analysis_options_guc_assign(const char* newval, void* extra);
static void assign_instr_unique_sql_count(int newval, void* extra);
static bool validate_conf_bool(struct config_generic *record, const char *name, const char *value, GucSource source,
                          int elevel, bool freemem, void *newvalue, void **newextra);
static bool validate_conf_int(struct config_generic *record, const char *name, const char *value, GucSource source,
                          int elevel, bool freemem, void *newvalue, void **newextra);
static bool validate_conf_int64(struct config_generic *record, const char *name, const char *value, GucSource source,
                          int elevel, bool freemem, void *newvalue, void **newextra);
static bool validate_conf_real(struct config_generic *record, const char *name, const char *value, GucSource source,
                          int elevel, bool freemem, void *newvalue, void **newextra);
static bool validate_conf_string(struct config_generic *record, const char *value, GucSource source,
                          int elevel, bool freemem, void *newvalue, void **newextra);
static bool validate_conf_enum(struct config_generic *record, const char *name, const char *value, GucSource source,
                          int elevel, bool freemem, void *newvalue, void **newextra);

#ifndef ENABLE_MULTIPLE_NODES
static void CheckAndGetAlterSystemSetParam(AlterSystemStmt* altersysstmt,
    char** outer_name, char** outer_value, struct config_generic** outer_record);
static void FinishAlterSystemSet(GucContext context);
static void ConfFileNameCat(char* ConfFileName, char* ConfTmpFileName,
    char* ConfTmpBakFileName, char* ConfLockFileName);
static void WriteAlterSystemSetGucFile(char* ConfFileName, char** opt_lines, ConfFileLock* filelock);
static char** LockAndReadConfFile(char* ConfFileName, char* ConfTmpFileName, char* ConfLockFileName,
    ConfFileLock* filelock);
#endif
inline void scape_space(char **pp)
{
    while (isspace((unsigned char)*(*pp))) {
        (*pp)++;
    }
}

#ifdef ENABLE_MULTIPLE_NODES
static void assign_gtm_rw_timeout(int newval, void* extra);
static bool check_gtmhostconninfo(char** newval, void** extra, GucSource source);
static GTM_HOST_IP* ParseGtmHostConnInfo(const char* ConnInfoList, int* InfoLength);
static void assign_gtmhostconninfo0(const char* newval, void* extra);
static void assign_gtmhostconninfo1(const char* newval, void* extra);
static void assign_gtmhostconninfo2(const char* newval, void* extra);
static void assign_gtmhostconninfo3(const char* newval, void* extra);
static void assign_gtmhostconninfo4(const char* newval, void* extra);
static void assign_gtmhostconninfo5(const char* newval, void* extra);
static void assign_gtmhostconninfo6(const char* newval, void* extra);
static void assign_gtmhostconninfo7(const char* newval, void* extra);
static bool check_compaciton_strategy(char** newval, void** extra, GucSource source);
static bool ts_compaction_guc_filter(const char* ptoken, const int min_num, const int max_num);
#endif

static void count_variables_num(GucNodeType nodetype,
    int &num_single_vars_out, int &num_both_vars_out, int &num_distribute_vars_out);
static void InitConfigureNamesBool();
static void InitConfigureNamesInt();
static void InitConfigureNamesInt64();
static void InitConfigureNamesReal();
static void InitConfigureNamesString();
static void InitConfigureNamesEnum();

/*
 * isMatchOptionName - Check wether the option name is in the configure file.
 *
 * Params:
 * @optLine: Line in the configure file.
 * @paraName: Paramater name.
 * @paraLength: Paramater length.
 * @paraOffset: Paramater offset int the optLine.
 * @valueLength: Value length.
 * @valueOffset: Value offset int the optLine.
 *
 * Returns:
 * True, iff the option name is in the configure file; else false.
 */
static bool isMatchOptionName(char* optLine, const char* paraName,
    int paraLength, int* paraOffset, int* valueLength, int* valueOffset, bool ignore_case)
{
    char* p = NULL;
    char* q = NULL;
    char* tmp = NULL;

    p = optLine;

    /* Skip all the blanks at the begin of the optLine */
    scape_space(&p);

    if ('#' == *p) {
        p++;
    }

    /* Skip all the blanks after '#' and before the paraName */
    scape_space(&p);

    if (ignore_case && strncasecmp(p, paraName, paraLength) != 0) {
        return false;
    } else if (!ignore_case && strncmp(p, paraName, paraLength) != 0) {
        return false;
    }

    if (paraOffset != NULL) {
        *paraOffset = p - optLine;
    }
    p += paraLength;

    scape_space(&p);

    /* If not '=', this optLine's format is wrong in configure file */
    if (*p != '=') {
        return false;
    }

    p++;

    /* Skip all the blanks after '=' and before the value */
    scape_space(&p);
    q = p + 1;
    tmp = q;

    while (*q && !('\n' == *q || '#' == *q)) {
        if (!isspace((unsigned char)*q)) {
            /* End of string */
            if ('\'' == *q) {
                tmp = ++q;
                break;
            } else {
                tmp = ++q;
            }
        } else {
            /*
             * If paraName is a string, the ' ' is considered to
             * be part of the string.
             */
            (*p == '\'') ? (tmp = ++q) : q++;
        }
    }

    if (valueOffset != NULL) {
        *valueOffset = p - optLine;
    }

    if (valueLength != NULL) {
        *valueLength = (NULL == tmp) ? 0 : (tmp - p);
    }

    return true;
}

/*
 * isOptLineCommented - Check wether the option line is commented with '#'.
 *
 * Params:
 * @optLine: Line in the configure file.
 *
 * Returns:
 * True, iff the option line starts with '#'; else false.
 */
static bool isOptLineCommented(char* optLine)
{
    char* tmp = NULL;

    if (NULL == optLine) {
        return false;
    }

    tmp = optLine;

    /* Skip all the blanks at the begin of the optLine */
    while (isspace((unsigned char)*tmp)) {
        tmp++;
    }

    if ('#' == *tmp) {
        return true;
    }

    return false;
}

/*
 * Options for enum values defined in this module.
 *
 * NOTE! Option values may not contain double quotes!
 */

static const struct config_enum_entry bytea_output_options[] = {
    {"escape", BYTEA_OUTPUT_ESCAPE, false}, {"hex", BYTEA_OUTPUT_HEX, false}, {NULL, 0, false}};

/*
 * We have different sets for client and server message level options because
 * they sort slightly different (see "log" level)
 */
static const struct config_enum_entry client_message_level_options[] = {{"debug", DEBUG2, false},
    {"debug5", DEBUG5, false},
    {"debug4", DEBUG4, false},
    {"debug3", DEBUG3, false},
    {"debug2", DEBUG2, false},
    {"debug1", DEBUG1, false},
    {"log", LOG, false},
    {"info", INFO, false},
    {"notice", NOTICE, false},
    {"warning", WARNING, false},
    {"error", ERROR, false},
    {"fatal", FATAL, false},
    {"panic", PANIC, false},
    {NULL, 0, false}};

static const struct config_enum_entry server_message_level_options[] = {{"debug", DEBUG2, false},
    {"debug5", DEBUG5, false},
    {"debug4", DEBUG4, false},
    {"debug3", DEBUG3, false},
    {"debug2", DEBUG2, false},
    {"debug1", DEBUG1, false},
    {"info", INFO, false},
    {"notice", NOTICE, false},
    {"warning", WARNING, false},
    {"error", ERROR, false},
    {"log", LOG, false},
    {"fatal", FATAL, false},
    {"panic", PANIC, false},
    {NULL, 0, false}};

static const struct config_enum_entry intervalstyle_options[] = {{"postgres", INTSTYLE_POSTGRES, false},
    {"postgres_verbose", INTSTYLE_POSTGRES_VERBOSE, false},
    {"sql_standard", INTSTYLE_SQL_STANDARD, false},
    {"iso_8601", INTSTYLE_ISO_8601, false},
    {g_interStyleVal.name, INTSTYLE_A, false},
    {NULL, 0, false}};

static const struct config_enum_entry log_error_verbosity_options[] = {{"terse", PGERROR_TERSE, false},
    {"default", PGERROR_DEFAULT, false},
    {"verbose", PGERROR_VERBOSE, false},
    {NULL, 0, false}};

static const struct config_enum_entry log_statement_options[] = {{"none", LOGSTMT_NONE, false},
    {"ddl", LOGSTMT_DDL, false},
    {"mod", LOGSTMT_MOD, false},
    {"all", LOGSTMT_ALL, false},
    {NULL, 0, false}};

static const struct config_enum_entry isolation_level_options[] = {{"serializable", XACT_SERIALIZABLE, false},
    {"repeatable read", XACT_REPEATABLE_READ, false},
    {"read committed", XACT_READ_COMMITTED, false},
    {"read uncommitted", XACT_READ_UNCOMMITTED, false},
    {NULL, 0}};

static const struct config_enum_entry session_replication_role_options[] = {
    {"origin", SESSION_REPLICATION_ROLE_ORIGIN, false},
    {"replica", SESSION_REPLICATION_ROLE_REPLICA, false},
    {"local", SESSION_REPLICATION_ROLE_LOCAL, false},
    {NULL, 0, false}};

static const struct config_enum_entry syslog_facility_options[] = {
#ifdef HAVE_SYSLOG
    {"local0", LOG_LOCAL0, false},
    {"local1", LOG_LOCAL1, false},
    {"local2", LOG_LOCAL2, false},
    {"local3", LOG_LOCAL3, false},
    {"local4", LOG_LOCAL4, false},
    {"local5", LOG_LOCAL5, false},
    {"local6", LOG_LOCAL6, false},
    {"local7", LOG_LOCAL7, false},
#else
    {"none", 0, false},
#endif
    {NULL, 0}};

static const struct config_enum_entry track_function_options[] = {
    {"none", TRACK_FUNC_OFF, false}, {"pl", TRACK_FUNC_PL, false}, {"all", TRACK_FUNC_ALL, false}, {NULL, 0, false}};

static const struct config_enum_entry xmlbinary_options[] = {
    {"base64", XMLBINARY_BASE64, false}, {"hex", XMLBINARY_HEX, false}, {NULL, 0, false}};

static const struct config_enum_entry xmloption_options[] = {
    {"content", XMLOPTION_CONTENT, false}, {"document", XMLOPTION_DOCUMENT, false}, {NULL, 0, false}};

/*
 * Define remote connection types for PGXC
 */
static const struct config_enum_entry pgxc_conn_types[] = {
    {"application", REMOTE_CONN_APP, false},
    {"coordinator", REMOTE_CONN_COORD, false},
    {"datanode", REMOTE_CONN_DATANODE, false},
    {"gtm", REMOTE_CONN_GTM, false},
    {"gtmproxy", REMOTE_CONN_GTM_PROXY, false},
    {"internaltool", REMOTE_CONN_INTERNAL_TOOL, false},
    {"gtmtool", REMOTE_CONN_GTM_TOOL, false},
    {NULL, 0, false}};

static const struct config_enum_entry unique_sql_track_option[] = {
    {"top", UNIQUE_SQL_TRACK_TOP, false}, {"all", UNIQUE_SQL_TRACK_ALL, true}, {NULL, 0, false}};

#ifndef ENABLE_MULTIPLE_NODES
/*
 * Synchronization strategy for configuration files between host and standby.
 */
static const struct config_enum_entry sync_config_strategy_options[] = {
    {"all_node", ALL_NODE, true},
    {"only_sync_node", ONLY_SYNC_NODE, true},
    {"none_node", NONE_NODE, true},
    {NULL, 0, false}
};
#endif

static const struct config_enum_entry cluster_run_mode_options[] = {
    {"cluster_primary", RUN_MODE_PRIMARY, false}, 
    {"cluster_standby", RUN_MODE_STANDBY, false},
    {NULL, 0, false}};

/*
 * GUC option variables that are exported from this module
 */
#ifdef USE_ASSERT_CHECKING
THR_LOCAL bool assert_enabled = true;
#else
THR_LOCAL bool assert_enabled = false;
#endif

THR_LOCAL int log_min_messages = WARNING;
THR_LOCAL int client_min_messages = NOTICE;

THR_LOCAL bool force_backtrace_messages = false;

/*
 * Displayable names for context types (enum GucContext)
 *
 * Note: these strings are deliberately not localized.
 */
const char* const GucContext_Names[] = {
    /* PGC_INTERNAL */ "internal",
    /* PGC_POSTMASTER */ "postmaster",
    /* PGC_SIGHUP */ "sighup",
    /* PGC_BACKEND */ "backend",
    /* PGC_SUSET */ "superuser",
    /* PGC_USERSET */ "user"};

/*
 * Displayable names for source types (enum GucSource)
 *
 * Note: these strings are deliberately not localized.
 */
const char* const GucSource_Names[] = {
    /* PGC_S_DEFAULT */ "default",
    /* PGC_S_DYNAMIC_DEFAULT */ "default",
    /* PGC_S_ENV_VAR */ "environment variable",
    /* PGC_S_FILE */ "configuration file",
    /* PGC_S_ARGV */ "command line",
    /* PGC_S_DATABASE */ "database",
    /* PGC_S_USER */ "user",
    /* PGC_S_DATABASE_USER */ "database user",
    /* PGC_S_CLIENT */ "client",
    /* PGC_S_OVERRIDE */ "override",
    /* PGC_S_INTERACTIVE */ "interactive",
    /* PGC_S_TEST */ "test",
    /* PGC_S_SESSION */ "session"};

/*
 * Displayable names for the groupings defined in enum config_group
 */
const char* const config_group_names[] = {
    /* UNGROUPED */
    gettext_noop("Ungrouped"),
    /* FILE_LOCATIONS */
    gettext_noop("File Locations"),
    /* CONN_AUTH */
    gettext_noop("Connections and Authentication"),
    /* CONN_AUTH_SETTINGS */
    gettext_noop("Connections and Authentication / Connection Settings"),
    /* CONN_AUTH_SECURITY */
    gettext_noop("Connections and Authentication / Security and Authentication"),
    /* RESOURCES */
    gettext_noop("Resource Usage"),
    /* RESOURCES_MEM */
    gettext_noop("Resource Usage / Memory"),
    /* RESOURCES_DISK */
    gettext_noop("Resource Usage / Disk"),
    /* RESOURCES_KERNEL */
    gettext_noop("Resource Usage / Kernel Resources"),
    /* RESOURCES_VACUUM_DELAY */
    gettext_noop("Resource Usage / Cost-Based Vacuum Delay"),
    /* RESOURCES_BGWRITER */
    gettext_noop("Resource Usage / Background Writer"),
    /* RESOURCES_ASYNCHRONOUS */
    gettext_noop("Resource Usage / Asynchronous Behavior"),
    /* RESOURCES_RECOVERY */
    gettext_noop("Resource usage / Recovery"),
    /* RESOURCES_WORKLOAD */
    gettext_noop("Resource usage / Workload Scheduling"),
    /* WAL */
    gettext_noop("Write-Ahead Log"),
    /* WAL_SETTINGS */
    gettext_noop("Write-Ahead Log / Settings"),
    /* WAL_CHECKPOINTS */
    gettext_noop("Write-Ahead Log / Checkpoints"),
    /* WAL_ARCHIVING */
    gettext_noop("Write-Ahead Log / Archiving"),
    /* REPLICATION */
    gettext_noop("Replication"),
    /* REPLICATION_SENDING */
    gettext_noop("Replication / Sending Servers"),
    /* REPLICATION_MASTER */
    gettext_noop("Replication / Master Server"),
    /* REPLICATION_STANDBY */
    gettext_noop("Replication / Standby Servers"),
    /* REPLICATION_PAXOS */
    gettext_noop("Replication / Paxos"),
    /* QUERY_TUNING */
    gettext_noop("Query Tuning"),
    /* QUERY_TUNING_METHOD */
    gettext_noop("Query Tuning / Planner Method Configuration"),
    /* QUERY_TUNING_COST */
    gettext_noop("Query Tuning / Planner Cost Constants"),
    /* QUERY_TUNING_GEQO */
    gettext_noop("Query Tuning / Genetic Query Optimizer"),
    /* QUERY_TUNING_OTHER */
    gettext_noop("Query Tuning / Other Planner Options"),
    /* LOGGING */
    gettext_noop("Reporting and Logging"),
    /* LOGGING_WHERE */
    gettext_noop("Reporting and Logging / Where to Log"),
    /* LOGGING_WHEN */
    gettext_noop("Reporting and Logging / When to Log"),
    /* LOGGING_WHAT */
    gettext_noop("Reporting and Logging / What to Log"),
    /* AUDIT_OPTIONS */
    gettext_noop("Audit Options"),
    /* TRANSPARENT_DATA_ENCRYPTION */
    gettext_noop("Transparent Data Encryption group"),
    /* STATS */
    gettext_noop("Statistics"),
    /* STATS_MONITORING */
    gettext_noop("Statistics / Monitoring"),
    /* STATS_COLLECTOR */
    gettext_noop("Statistics / Query and Index Statistics Collector"),
    /* STREAMING */
    gettext_noop("Streaming"),
    /* AUTOVACUUM */
    gettext_noop("Autovacuum"),
    /* JOB_ENABLE */
    gettext_noop("Job Schedule"),
    /* CLIENT_CONN */
    gettext_noop("Client Connection Defaults"),
    /* CLIENT_CONN_STATEMENT */
    gettext_noop("Client Connection Defaults / Statement Behavior"),
    /* CLIENT_CONN_LOCALE */
    gettext_noop("Client Connection Defaults / Locale and Formatting"),
    /* CLIENT_CONN_OTHER */
    gettext_noop("Client Connection Defaults / Other Defaults"),
    /* LOCK_MANAGEMENT */
    gettext_noop("Lock Management"),
    /* COMPAT_OPTIONS */
    gettext_noop("Version and Platform Compatibility"),
    /* COMPAT_OPTIONS_PREVIOUS */
    gettext_noop("Version and Platform Compatibility / Previous openGauss Versions"),
    /* COMPAT_OPTIONS_CLIENT */
    gettext_noop("Version and Platform Compatibility / Other Platforms and Clients"),
    /* ERROR_HANDLING */
    gettext_noop("Error Handling"),
    /* PRESET_OPTIONS */
    gettext_noop("Preset Options"),
    /* CUSTOM_OPTIONS */
    gettext_noop("Customized Options"),
    /* TEXT_SEARCH */
    gettext_noop("Text Search Parser Options"),
    /* DEVELOPER_OPTIONS */
    gettext_noop("Developer Options"),
    /* UPGRADE_OPTIONS */
    gettext_noop("Upgrade Options"),
    /* INSTRUMENTS_OPTIONS */
    gettext_noop("Instruments Options"),
    gettext_noop("Column Encryption"),
    gettext_noop("Compress Options"),
#ifdef PGXC
    /* DATA_NODES */
    gettext_noop("Datanodes and Connection Pooling"),
    /* GTM */
    gettext_noop("GTM Connection"),
    /* COORDINATORS */
    gettext_noop("Coordinator Options"),
    /* XC_HOUSEKEEPING_OPTIONS */
    gettext_noop("XC Housekeeping Options"),
#endif
    /* help_config wants this array to be null-terminated */
    NULL};

/*
 * Displayable names for GUC variable types (enum config_type)
 *
 * Note: these strings are deliberately not localized.
 */
const char* const config_type_names[] = {
    /* PGC_BOOL */ "bool",
    /* PGC_INT */ "integer",
    /* PGC_INT64 */ "int64",
    /* PGC_REAL */ "real",
    /* PGC_STRING */ "string",
    /* PGC_ENUM */ "enum"};

/*
 * Contents of GUC tables
 *
 * See src/backend/utils/misc/README for design notes.
 *
 * TO ADD AN OPTION:
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

/******** option records follow ********/

static void InitUStoreAttr();

static void InitCommonConfigureNames()
{
    InitConfigureNamesBool();
    InitConfigureNamesInt();
    InitConfigureNamesInt64();
    InitConfigureNamesReal();
    InitConfigureNamesString();
    InitConfigureNamesEnum();

    return;
}
static void InitConfigureNamesBool()
{
    struct config_bool localConfigureNamesBool[] = {
#ifndef ENABLE_MULTIPLE_NODES
        {{"enable_auto_clean_unique_sql",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Enable auto clean unique sql entry when the UniqueSQL hash table is full."),
            NULL},
            &g_instance.attr.attr_common.enable_auto_clean_unique_sql,
            false,
            NULL,
            NULL,
            NULL},
#endif
        {{"enable_wdr_snapshot",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Enable wdr snapshot"),
            NULL},
            &u_sess->attr.attr_common.enable_wdr_snapshot,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_asp",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Enable active session profile"),
            NULL},
            &u_sess->attr.attr_common.enable_asp,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_stmt_track",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Enable full/slow sql feature"), NULL},
            &u_sess->attr.attr_common.enable_stmt_track,
            true,
            NULL,
            NULL,
            NULL},
        {{"track_stmt_parameter",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Enable to track the parameter of statements"), NULL},
            &u_sess->attr.attr_common.track_stmt_parameter,
            false,
            NULL,
            NULL,
            NULL},
        {{"support_extended_features",
            PGC_POSTMASTER,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Enables unofficial supported extended features."),
            NULL},
            &g_instance.attr.attr_common.support_extended_features,
            false,
            NULL,
            NULL,
            NULL},
        {{"lastval_supported",
            PGC_POSTMASTER,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Enable functionality of lastval() function."),
            NULL},
            &g_instance.attr.attr_common.lastval_supported,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_beta_features",
            PGC_USERSET,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Enable features that ever supported in former version ."),
            NULL},
            &u_sess->attr.attr_common.enable_beta_features,
            false,
            NULL,
            NULL,
            NULL},
        /* Not for general use --- used by SET SESSION AUTHORIZATION */
        {{"is_sysadmin",
            PGC_INTERNAL,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Shows whether the current user is a system admin."),
            NULL,
            GUC_REPORT | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_common.session_auth_is_superuser,
            false,
            NULL,
            NULL,
            NULL},
#ifdef USE_BONJOUR
        {{"bonjour",
            PGC_POSTMASTER,
            NODE_ALL,
            CONN_AUTH_SETTINGS,
            gettext_noop("Enables advertising the server via Bonjour."),
            NULL},
            &g_instance.attr.attr_common.enable_bonjour,
            false,
            check_bonjour,
            NULL,
            NULL},
#endif
        {{"ignore_checksum_failure",
            PGC_SUSET,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Continues processing after a checksum failure."),
            gettext_noop("Detection of a checksum failure normally causes openGauss to "
                         "report an error, aborting the current transaction. Setting "
                         "ignore_checksum_failure to true causes the system to ignore the failure "
                         "(but still report a warning), and continue processing. This "
                         "behavior could cause crashes or other serious problems. Only "
                         "has an effect if checksums are enabled."),
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_common.ignore_checksum_failure,
            false,
            NULL,
            NULL,
            NULL},
        {{"log_checkpoints",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Logs each checkpoint."),
            NULL},
            &u_sess->attr.attr_common.log_checkpoints,
            false,
            NULL,
            NULL,
            NULL},
        {{"log_disconnections",
            PGC_BACKEND,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Logs end of a session, including duration."),
            NULL},
            &u_sess->attr.attr_common.Log_disconnections,
            false,
            NULL,
            NULL,
            NULL},
        {{"debug_assertions",
            PGC_USERSET,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Turns on various assertion checks."),
            gettext_noop("This is a debugging aid."),
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_common.assert_enabled,
#ifdef USE_ASSERT_CHECKING
            true,
#else
            false,
#endif
            check_debug_assertions,
            NULL,
            NULL},
        /*
         * security requirements: ordinary users can not set this parameter,  promoting the setting level to PGC_SUSET.
         */
        {{"exit_on_error",
            PGC_SUSET,
            NODE_ALL,
            ERROR_HANDLING_OPTIONS,
            gettext_noop("Terminates session on any error."),
            NULL},
            &u_sess->attr.attr_common.ExitOnAnyError,
            false,
            NULL,
            NULL,
            NULL},
        // omit untranslatable char error
        {{"omit_encoding_error",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Omits encoding convert error."),
            NULL},
            &u_sess->attr.attr_common.omit_encoding_error,
            false,
            NULL,
            NULL,
            NULL},
        {{"log_parser_stats",
            PGC_SUSET,
            NODE_ALL,
            STATS_MONITORING,
            gettext_noop("Writes parser performance statistics to the server log."),
            NULL},
            &u_sess->attr.attr_common.log_parser_stats,
            false,
            check_stage_log_stats,
            NULL,
            NULL},
        {{"log_planner_stats",
            PGC_SUSET,
            NODE_ALL,
            STATS_MONITORING,
            gettext_noop("Writes planner performance statistics to the server log."),
            NULL},
            &u_sess->attr.attr_common.log_planner_stats,
            false,
            check_stage_log_stats,
            NULL,
            NULL},
        {{"log_executor_stats",
            PGC_SUSET,
            NODE_ALL,
            STATS_MONITORING,
            gettext_noop("Writes executor performance statistics to the server log."),
            NULL},
            &u_sess->attr.attr_common.log_executor_stats,
            false,
            check_stage_log_stats,
            NULL,
            NULL},
        {{"log_statement_stats",
            PGC_SUSET,
            NODE_ALL,
            STATS_MONITORING,
            gettext_noop("Writes cumulative performance statistics to the server log."),
            NULL},
            &u_sess->attr.attr_common.log_statement_stats,
            false,
            check_log_stats,
            NULL,
            NULL},
        {{"track_activities",
            PGC_SUSET,
            NODE_ALL,
            STATS_COLLECTOR,
            gettext_noop("Collects information about executing commands."),
            gettext_noop("Enables the collection of information on the current "
                         "executing command of each session, along with "
                         "the time at which that command began execution.")},
            &u_sess->attr.attr_common.pgstat_track_activities,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_instr_track_wait",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Collects information about wait status."),
            NULL},
            &u_sess->attr.attr_common.enable_instr_track_wait,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_slow_query_log",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Write slow query log."),
            NULL},
            &u_sess->attr.attr_common.enable_slow_query_log,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_instr_rt_percentile",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Calculate percentile info of sql responstime."),
            NULL},
            &u_sess->attr.attr_common.enable_instr_rt_percentile,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_instr_cpu_timer",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Enables instruments cpu timer functionality."),
            NULL},
            &u_sess->attr.attr_common.enable_instr_cpu_timer,
            true,
            NULL,
            NULL,
            NULL},
        {{"track_counts",
            PGC_SUSET,
            NODE_ALL,
            STATS_COLLECTOR,
            gettext_noop("Collects statistics on database activity."),
            NULL},
            &u_sess->attr.attr_common.pgstat_track_counts,
            true,
            NULL,
            NULL,
            NULL},
        {{"track_sql_count",
            PGC_SUSET,
            NODE_ALL,
            STATS_COLLECTOR,
            gettext_noop("Collects query info on database activity."),
            NULL},
            &u_sess->attr.attr_common.pgstat_track_sql_count,
            true,
            NULL,
            NULL,
            NULL},
        {{"track_io_timing",
            PGC_SUSET,
            NODE_ALL,
            STATS_COLLECTOR,
            gettext_noop("Collects timing statistics for database I/O activity."),
            NULL},
            &u_sess->attr.attr_common.track_io_timing,
            false,
            NULL,
            NULL,
            NULL},

        {{"update_process_title",
            PGC_INTERNAL,
            NODE_ALL,
            STATS_COLLECTOR,
            gettext_noop("Updates the process title to show the active SQL command."),
            gettext_noop(
                "Enables updating of the process title every time a new SQL command is received by the server.")},
            &u_sess->attr.attr_common.update_process_title,
            false,
            NULL,
            NULL,
            NULL},
        {{"cache_connection",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES,
            gettext_noop("pooler cache connection"),
            NULL},
            &u_sess->attr.attr_common.pooler_cache_connection,
            true,
            NULL,
            NULL,
            NULL},

        {{"trace_notify",
            PGC_USERSET,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Generates debugging output for LISTEN and NOTIFY."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_common.Trace_notify,
            false,
            NULL,
            NULL,
            NULL},
        {{"log_hostname",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Logs the host name in the connection logs."),
            gettext_noop("By default, connection logs only show the IP address "
                         "of the connecting host. If you want them to show the host name you "
                         "can turn this on, but depending on your host name resolution "
                         "setup it might impose a non-negligible performance penalty.")},
            &u_sess->attr.attr_common.log_hostname,
            false,
            NULL,
            NULL,
            NULL},
        {{"transaction_read_only",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets the current transaction's read-only status."),
            NULL,
            GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_common.XactReadOnly,
            false,
            check_transaction_read_only,
            NULL,
            NULL},
        {{"logging_collector",
            PGC_POSTMASTER,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Starts a subprocess to capture stderr output and/or csvlogs into log files."),
            NULL},
            &g_instance.attr.attr_common.Logging_collector,
            false,
            NULL,
            NULL,
            NULL},
        {{"log_truncate_on_rotation",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Truncates existing log files of same name during log rotation."),
            NULL},
            &u_sess->attr.attr_common.Log_truncate_on_rotation,
            false,
            NULL,
            NULL,
            NULL},

#ifdef TRACE_SORT
        {{"trace_sort",
            PGC_USERSET,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Emits information about resource usage in sorting."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_common.trace_sort,
            false,
            NULL,
            NULL,
            NULL},
#endif
        {{"integer_datetimes",
            PGC_INTERNAL,
            NODE_ALL,
            PRESET_OPTIONS,
            gettext_noop("Datetimes are integer based."),
            NULL,
            GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_common.integer_datetimes,
#ifdef HAVE_INT64_TIMESTAMP
            true,
#else
            false,
#endif
            NULL,
            NULL,
            NULL},
        {{"archive_mode",
            PGC_SIGHUP,
            NODE_ALL,
            WAL_ARCHIVING,
            gettext_noop("Allows archiving of WAL files using archive_command."),
            NULL},
            &u_sess->attr.attr_common.XLogArchiveMode,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_alarm",
            PGC_POSTMASTER,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Enables alarm or not."),
            NULL},
            &g_instance.attr.attr_common.enable_alarm,
            false,
            NULL,
            NULL,
            NULL},
        {{"allow_system_table_mods",
            PGC_POSTMASTER,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Allows modifications of the structure of system tables."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_common.allowSystemTableMods,
            false,
            NULL,
            NULL,
            NULL},

        {{"IsInplaceUpgrade",
            PGC_SUSET,
            NODE_ALL,
            UPGRADE_OPTIONS,
            gettext_noop("Enable/disable inplace upgrade mode."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.IsInplaceUpgrade,
            false,
            check_is_upgrade,
            assign_is_inplace_upgrade,
            NULL},
        {{"allow_concurrent_tuple_update",
            PGC_USERSET,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Allows concurrent tuple update."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_common.allow_concurrent_tuple_update,
            true,
            NULL,
            NULL,
            NULL},

        {{"ignore_system_indexes",
            PGC_BACKEND,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Disables reading from system indexes."),
            gettext_noop("It does not prevent updating the indexes, so it is safe "
                         "to use.  The worst consequence is slowness."),
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_common.IgnoreSystemIndexes,
            false,
            NULL,
            NULL,
            NULL},
        {{"xc_maintenance_mode",
            PGC_SUSET,
            NODE_ALL,
            XC_HOUSEKEEPING_OPTIONS,
            gettext_noop("Turns on XC maintenance mode."),
            gettext_noop("Can set ON by SET command by system admin.")},
            &u_sess->attr.attr_common.xc_maintenance_mode,
            false,
            check_pgxc_maintenance_mode,
            NULL,
            NULL},
        {{"support_batch_bind",
            PGC_SIGHUP,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Sets to use batch bind-execute for PBE."),
            NULL},
            &u_sess->attr.attr_common.support_batch_bind,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_bbox_dump",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Enables bbox_handler to create core dump. "),
            NULL},
            &u_sess->attr.attr_common.enable_bbox_dump,
            true,
            NULL,
            assign_bbox_coredump,
            NULL},

        {{"enable_ffic_log",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Enables First Failure Info Capture. "),
            NULL},
            &g_instance.attr.attr_common.enable_ffic_log,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_thread_pool",
            PGC_POSTMASTER,
            NODE_ALL,
            CLIENT_CONN,
            gettext_noop("enable to use thread pool. "),
            NULL},
            &g_instance.attr.attr_common.enable_thread_pool,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_global_plancache",
            PGC_POSTMASTER,
            NODE_ALL,
            CLIENT_CONN,
            gettext_noop("enable to use global plan cache. "),
            NULL},
            &g_instance.attr.attr_common.enable_global_plancache,
            false,
            check_gpc_syscache_threshold,
            NULL,
            NULL},
#ifdef ENABLE_MULTIPLE_NODES
        {{"enable_gpc_grayrelease_mode",
            PGC_SIGHUP,
            NODE_ALL,
            CLIENT_CONN,
            gettext_noop("in distribute cluster, enable grayly change guc enable_global_plancache in three stage."),
            NULL},
            &u_sess->attr.attr_common.enable_gpc_grayrelease_mode,
            false,
            NULL,
            NULL,
            NULL},
#endif
        {{"enable_global_syscache",
            PGC_POSTMASTER,
            NODE_ALL,
            CLIENT_CONN,
            gettext_noop("enable to use global system cache. "), NULL},
            &g_instance.attr.attr_common.enable_global_syscache,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_router",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            CLIENT_CONN,
            gettext_noop("enable to use router."),
            NULL},
            &u_sess->attr.attr_common.enable_router,
            false,
            NULL,
            NULL,
            NULL},
        {{"data_sync_retry",
            PGC_POSTMASTER,
            NODE_ALL,
            ERROR_HANDLING_OPTIONS,
            gettext_noop("Whether to continue running after a failure to sync data files."),
            },
            &g_instance.attr.attr_common.data_sync_retry,
            false,
            NULL,
            NULL,
            NULL},
        {{"check_implicit_conversions",
            PGC_USERSET,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("check whether there is an implicit conversion on index column"),
            NULL},
            &u_sess->attr.attr_common.check_implicit_conversions_for_indexcol,
            false,
            NULL,
            NULL,
            NULL},
        {{"persistent_datanode_connections",
            PGC_BACKEND,
            NODE_DISTRIBUTE,
            DEVELOPER_OPTIONS,
            gettext_noop("Session never releases acquired connections."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_common.PersistentConnections,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_redistribute",
            PGC_SUSET,
            NODE_DISTRIBUTE,
            XC_HOUSEKEEPING_OPTIONS,
            gettext_noop("Enables redistribute at nodes mis-match."),
            NULL},
            &u_sess->attr.attr_common.enable_redistribute,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_tsdb", 
            PGC_POSTMASTER, 
            NODE_DISTRIBUTE,
            TSDB, 
            gettext_noop("Enables control tsdb feature."), 
            NULL},
            &g_instance.attr.attr_common.enable_tsdb,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_ts_compaction", 
            PGC_SIGHUP, 
            NODE_DISTRIBUTE,
            TSDB, 
            gettext_noop("Enables timeseries compaction feature."), 
            NULL},
            &u_sess->attr.attr_common.enable_ts_compaction,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_ts_kvmerge",
            PGC_SIGHUP,
	        NODE_DISTRIBUTE,
	        TSDB,
	        gettext_noop("Enables timeseries kvmerge feature."),
	        NULL},
            &u_sess->attr.attr_common.enable_ts_kvmerge,
	        false,
	        NULL,
	        NULL,
	        NULL},
        {{"enable_ts_outorder",
            PGC_SIGHUP,
	        NODE_DISTRIBUTE,
	        TSDB,
	        gettext_noop("Enables timeseries ourorder feature."),
	        NULL},
            &u_sess->attr.attr_common.enable_ts_outorder,
	        false,
	        NULL,
	        NULL,
	        NULL},        
        {{"ts_adaptive_threads",
            PGC_SIGHUP, 
            NODE_DISTRIBUTE,
            TSDB, 
            gettext_noop("Enables compaction give a adaptive number of consumers."), 
            NULL},
            &u_sess->attr.attr_common.ts_adaptive_threads,
            false,
            NULL,
            NULL,
            NULL},
        {{
            "reserve_space_for_nullable_atts",
            PGC_USERSET,
            NODE_SINGLENODE,
            QUERY_TUNING,
            gettext_noop("Enable reserve space for nullable attributes, only applicable to ustore."),
            NULL
            },
            &u_sess->attr.attr_storage.reserve_space_for_nullable_atts,
            true,
            NULL,
            NULL,
            NULL
        },
        {{"enable_default_ustore_table",
            PGC_USERSET,
            NODE_SINGLENODE,
            QUERY_TUNING_METHOD,
            gettext_noop("Creates all user-defined tables with orientation inplace"),
            NULL},
            &u_sess->attr.attr_sql.enable_default_ustore_table,
            false,
            NULL,
            NULL,
            NULL
        },
        {{"enable_ustore",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            QUERY_TUNING_METHOD,
            gettext_noop("Enable ustore storage engine"),
            NULL},
            &g_instance.attr.attr_storage.enable_ustore,
            true,
            NULL,
            NULL,
            NULL,
            NULL
        },
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
    u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_COMMON] =
        (struct config_bool*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_COMMON], bytes, localConfigureNamesBool, bytes);
    securec_check_ss(rc, "\0", "\0");
}

#ifdef ENABLE_QUNIT

/*
 * This function is used to store an integer for every thread. we can use __thread to do this, but when we use it,
 * GDB complains that it can not find thread local variables sometimes. So, we use this function to bind the integer to
 * a thread.
 *
 * The main idea is that we store an integer on the thread's stack. Actually, the value is stored outside the stack,
 * but next to the stack. So the value will not be overwritten when gaussdb is running.
 *
 * When a thread is started up, we set its stack's boundary to addresses ALIGNED with 16MB. So, no matter how much stack
 * space a thread uses, we can find its stack's boundary easily. Just do this: (rsp/16MB + 1)*16MB. We store the value
 * here.
 *
 * In Linux kernel, this method is used to implement current_thread_info(), please refer to the Linux kernel source code
 * for more information.
 *
 *                         ____
 *         (K+1)*16MB  -> |____|  <- (rsp/16MB + 1)*16MB points here, we store the value here
 *                        |    |
 *                        |    |  <- rsp may point here
 *                        |    |
 *                        |    |
 *            K*16MB  ->  |____|
 *
 *          [K*16MB, (K+1)*16MB) is used by the stack. [(K+1)*16MB, (K+1)*16MB)+4] is not used by the stack, so we store
 *          the value at [(K+1)*16MB, (K+1)*16MB)+4].
 *
 * When we start a new thread, we allocate a space from K*16MB to (K+1)*16MB+4, and then [K*16MB, (K+1)*16MB) is
 * used for the stack of the thread, [(K+1)*16MB, (K+1)*16MB)+4] is used for storing the integer bound to the stack.
 * So, the stack's space and the integer's space are not overlapped.
 */
void set_qunit_case_number_hook(int newval, void* extra)
{
    if (!IsUnderPostmaster)
        return;
    size_t size = DEFUALT_STACK_SIZE * 1024L;
    Assert(!(size & (size - 1)));
    long rsp;
    __asm__("movq %%rsp, %0" : "=r"(rsp) :);
    rsp = rsp / size + 1;
    rsp = rsp * size;
    *(int*)rsp = newval;
}
#endif

static void InitConfigureNamesInt()
{
    struct config_int localConfigureNamesInt[] = {
        {{"archive_timeout",
            PGC_SIGHUP,
            NODE_ALL,
            WAL_ARCHIVING,
            gettext_noop("Forces a switch to the next xlog file if a "
                         "new file has not been started within N seconds."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_common.XLogArchiveTimeout,
            0,
            0,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},
        {{"alarm_report_interval",
            PGC_SIGHUP,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Sets the interval time between two alarm report."),
            NULL},
            &u_sess->attr.attr_common.AlarmReportInterval,
            10,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"log_file_mode",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the file permissions for log files."),
            gettext_noop("The parameter value is expected "
                         "to be a numeric mode specification in the form "
                         "accepted by the chmod and umask system calls. "
                         "(To use the customary octal format the number must "
                         "start with a 0 (zero).)")},
            &u_sess->attr.attr_common.Log_file_mode,
            0600,
            0000,
            0777,
            NULL,
            NULL,
            show_log_file_mode},
        {{"bbox_dump_count",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets the maximum number of core dump created by bbox_handler."),
            gettext_noop("This value is set to 8 as default. "),
            },
            &u_sess->attr.attr_common.bbox_dump_count,
            8,
            1,
            20,
            NULL,
            NULL,
            NULL},
        /*
         * We use the hopefully-safely-small value of 100kB as the compiled-in
         * default for max_stack_depth.  InitializeGUCOptions will increase it if
         * possible, depending on the actual platform-specific stack limit.
         */
        {{"max_stack_depth",
            PGC_SUSET,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the maximum stack depth, in kilobytes."),
            NULL,
            GUC_UNIT_KB},
            &u_sess->attr.attr_common.max_stack_depth,
            100,
            100,
            MAX_KILOBYTES,
            check_max_stack_depth,
            assign_max_stack_depth,
            NULL},
        {{"max_files_per_process",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_KERNEL,
            gettext_noop("Sets the maximum number of simultaneously open files for each server process."),
            NULL},
            &g_instance.attr.attr_common.max_files_per_process,
            1000,
            25,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"max_query_retry_times",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets the maximum sql retry times."),
            gettext_noop("A value of 0 turns off the retry."),
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_common.max_query_retry_times,
            0,
            0,
            20,
            NULL,
            NULL,
            NULL},
        {{"statement_timeout",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets the maximum allowed duration of any statement."),
            gettext_noop("A value of 0 turns off the timeout."),
            GUC_UNIT_MS},
            &u_sess->attr.attr_common.StatementTimeout,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"session_timeout",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Set the maximum allowed duration of any unused session."),
            gettext_noop("A value of 0 turns off the timeout."),
            GUC_UNIT_S},
            &u_sess->attr.attr_common.SessionTimeout,
            600,
            0,
            MAX_SESSION_TIMEOUT,
            NULL,
            NULL,
            NULL},
        {{"track_thread_wait_status_interval",
            PGC_SUSET,
            NODE_ALL,
            STATS_COLLECTOR,
            gettext_noop("Sets the interval for collecting thread status in pgstat thread, in minute"),
            NULL,
            GUC_UNIT_MIN},
            &u_sess->attr.attr_common.pgstat_collect_thread_status_interval,
            30,
            0,
            24 * 60,
            NULL,
            NULL,
            NULL},

        {{"instr_rt_percentile_interval",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Sets the interval for calculating percentile in pgstat thread, in seconds"),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_common.instr_rt_percentile_interval,
            10,
            0,
            60 * 60,
            NULL,
            NULL,
            NULL},

        {{"wdr_snapshot_interval",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Sets the interval for wdr snapshot in snapshot thread, in min"),
            NULL,
            GUC_UNIT_MIN},
            &u_sess->attr.attr_common.wdr_snapshot_interval,
            60,
            10,
            60,
            NULL,
            NULL,
            NULL},

        {{"wdr_snapshot_retention_days",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Sets the max time span for wdr snapshot, in seconds"),
            NULL},
            &u_sess->attr.attr_common.wdr_snapshot_retention_days,
            8,
            1,
            8,
            NULL,
            NULL,
            NULL},

        {{"wdr_snapshot_query_timeout",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Sets the timeout for wdr snapshot query, in seconds"),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_common.wdr_snapshot_query_timeout,
            100,
            100,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"asp_sample_num",
            PGC_POSTMASTER,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Sets the active session profile max sample nums in buff"),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.asp_sample_num,
            100000,
#ifdef ENABLE_MULTIPLE_NODES
            10000,
#else
            10,
#endif
            100000,
            NULL,
            NULL,
            NULL},

        {{"asp_sample_interval",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Sets the active session profile max sample nums in buff"),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_common.asp_sample_interval,
            1,
            1,
            10,
            NULL,
            NULL,
            NULL},

        {{"asp_flush_rate",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("every Nth sample to disk, MOD(sample_id, N) = 0 will flush to dist"),
            NULL},
            &u_sess->attr.attr_common.asp_flush_rate,
            10,
            1,
            10,
            NULL,
            NULL,
            NULL},

        {{"asp_retention_days",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("set max retention days for pg_asp"),
            NULL},
            &u_sess->attr.attr_common.asp_retention_days,
            2,
            1,
            7,
            NULL,
            NULL,
            NULL},
        {{"extra_float_digits",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Sets the number of digits displayed for floating-point values."),
            gettext_noop("This affects real, double precision, and geometric data types. "
                         "The parameter value is added to the standard number of digits "
                         "(FLT_DIG or DBL_DIG as appropriate).")},
            &u_sess->attr.attr_common.extra_float_digits,
            0,
            -15,
            3,
            NULL,
            NULL,
            NULL},
        {{"effective_io_concurrency",
#ifdef USE_PREFETCH
            PGC_USERSET,
#else
            PGC_INTERNAL,
#endif
            NODE_ALL,
            RESOURCES_ASYNCHRONOUS,
            gettext_noop("Number of simultaneous requests that can be handled efficiently by the disk subsystem."),
            gettext_noop("For RAID arrays, this should be approximately the number of drive spindles in the array.")},
            &u_sess->attr.attr_common.effective_io_concurrency,
#ifdef USE_PREFETCH
            1,
            0,
            1000,
#else
            0,
            0,
            0,
#endif
            check_effective_io_concurrency,
            assign_effective_io_concurrency,
            NULL},

        {{"backend_flush_after",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_ASYNCHRONOUS,
            gettext_noop("Number of pages after which previously performed writes are flushed to disk."),
            NULL,
            GUC_UNIT_BLOCKS},
            &u_sess->attr.attr_common.backend_flush_after,
            DEFAULT_BACKEND_FLUSH_AFTER,
            0,
            WRITEBACK_MAX_PENDING_FLUSHES,
            NULL,
            NULL,
            NULL},

        {{"log_rotation_age",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Automatic log file rotation will occur after N minutes."),
            NULL,
            GUC_UNIT_MIN},
            &u_sess->attr.attr_common.Log_RotationAge,
            HOURS_PER_DAY * MINS_PER_HOUR,
            0,
            INT_MAX / SECS_PER_MINUTE,
            NULL,
            NULL,
            NULL},

        {{"log_rotation_size",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Automatic log file rotation will occur after N kilobytes."),
            NULL,
            GUC_UNIT_KB},
            &u_sess->attr.attr_common.Log_RotationSize,
            10 * 1024,
            0,
            INT_MAX / 1024,
            NULL,
            NULL,
            NULL},

        {{"max_function_args",
            PGC_INTERNAL,
            NODE_ALL,
            PRESET_OPTIONS,
            gettext_noop("Shows the maximum number of function arguments."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_common.max_function_args,
            FUNC_MAX_ARGS,
            FUNC_MAX_ARGS,
            FUNC_MAX_ARGS,
            NULL,
            NULL,
            NULL},
        {{"max_user_defined_exception",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("GUC parameter of max_user_defined_exception."),
            NULL},
            &u_sess->attr.attr_common.max_user_defined_exception,
            1000,
            1000,
            1000,
            NULL,
            NULL,
            NULL},
        {{"tcp_keepalives_idle",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_OTHER,
            gettext_noop("Time between issuing TCP keepalives."),
            gettext_noop("A value of 0 uses the system default."),
            GUC_UNIT_S},
            &u_sess->attr.attr_common.tcp_keepalives_idle,
            60,
            0,
            3600,
            NULL,
            assign_tcp_keepalives_idle,
            show_tcp_keepalives_idle},

        {{"tcp_keepalives_interval",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_OTHER,
            gettext_noop("Time between TCP keepalive retransmits."),
            gettext_noop("A value of 0 uses the system default."),
            GUC_UNIT_S},
            &u_sess->attr.attr_common.tcp_keepalives_interval,
            30,
            0,
            180,
            NULL,
            assign_tcp_keepalives_interval,
            show_tcp_keepalives_interval},
        {{"tcp_keepalives_count",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_OTHER,
            gettext_noop("Maximum number of TCP keepalive retransmits."),
            gettext_noop("This controls the number of consecutive keepalive retransmits that can be "
                         "lost before a connection is considered dead. A value of 0 uses the "
                         "system default."),
            },
            &u_sess->attr.attr_common.tcp_keepalives_count,
            20,
            0,
            100,
            NULL,
            assign_tcp_keepalives_count,
            show_tcp_keepalives_count},
        {{"gin_fuzzy_search_limit",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_OTHER,
            gettext_noop("Sets the maximum allowed result for exact search by GIN."),
            NULL,
            0},
            &u_sess->attr.attr_common.GinFuzzySearchLimit,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        /* Can't be set in postgresql.conf */
        {{"server_version_num",
            PGC_INTERNAL,
            NODE_ALL,
            PRESET_OPTIONS,
            gettext_noop("Shows the server version as an integer."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_common.server_version_num,
            PG_VERSION_NUM,
            PG_VERSION_NUM,
            PG_VERSION_NUM,
            NULL,
            NULL,
            NULL},

        /* Can't be set in postgresql.conf */
        {{"backend_version",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            PRESET_OPTIONS,
            gettext_noop("Set and show the backend version as an integer."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_common.backend_version,
            0,
            0,
            100000,
            NULL,
            assign_thread_working_version_num,
            NULL},

        {{"log_temp_files",
            PGC_SUSET,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Logs the use of temporary files larger than this number of kilobytes."),
            gettext_noop("Zero logs all files. The default is -1 (turning this feature off)."),
            GUC_UNIT_KB},
            &u_sess->attr.attr_common.log_temp_files,
            -1,
            -1,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"track_activity_query_size",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the size reserved for pg_stat_activity.query, in bytes."),
            NULL,
            },
            &g_instance.attr.attr_common.pgstat_track_activity_query_size,
            1024,
            100,
            102400,
            NULL,
            NULL,
            NULL},
        {{"ngram_gram_size",
            PGC_USERSET,
            NODE_ALL,
            TEXT_SEARCH,
            gettext_noop("N-value for N-gram parser"),
            NULL},
            &u_sess->attr.attr_common.ngram_gram_size,
            2,
            1,
            4,
            NULL,
            NULL,
            NULL},
        {{"fault_mon_timeout",
            PGC_SIGHUP,
            NODE_ALL,
            STATS_MONITORING,
            gettext_noop("how many miniutes to monitor lwlock. 0 will disable that"),
            NULL,
            GUC_UNIT_MIN}, /* minute */
            &u_sess->attr.attr_common.fault_mon_timeout,
            5,
            0,
            (24 * 60), /* 1 min ~ 1 day, 0 will disable this thread starting */
            NULL,
            NULL,
            NULL},
        {{"max_changes_in_memory",
            PGC_POSTMASTER,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("how many memory a transaction can use in reorderbuffer."),
            NULL},
            &g_instance.attr.attr_common.max_changes_in_memory,
            4096,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"max_cached_tuplebufs",
            PGC_POSTMASTER,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("how many memory reorderbuffer can use."),
            NULL},
            &g_instance.attr.attr_common.max_cached_tuplebufs,
            8192,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"datanode_heartbeat_interval",
            PGC_SIGHUP,
            NODE_ALL,
            WAL_CHECKPOINTS,
            gettext_noop("Sets the heartbeat interval of the standby nodes."),
            gettext_noop(
                "The value is best configured less than half of the wal_receiver_timeout and wal_sender_timeout."),
            GUC_UNIT_MS},
            &u_sess->attr.attr_common.dn_heartbeat_interval,
            1000,
            1000,
            60 * 1000,
            NULL,
            NULL,
            NULL},

        {{"upgrade_mode",
            PGC_SIGHUP,
            NODE_ALL,
            UPGRADE_OPTIONS,
            gettext_noop("Indicate the upgrade mode: inplace upgrade mode, grey upgrade mode or not in upgrade."),
            NULL,
            0},
            &u_sess->attr.attr_common.upgrade_mode,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"instr_unique_sql_count",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Sets the number of entries collected in gs_instr_unique_sql."),
            NULL},
            &u_sess->attr.attr_common.instr_unique_sql_count,
            100,
            0,
            INT_MAX,
            NULL,
            assign_instr_unique_sql_count,
            NULL},
        {{"track_stmt_session_slot",
            PGC_SIGHUP,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Sets the number of entries collected for full sql/slow sql in each session."),
            NULL},
            &u_sess->attr.attr_common.track_stmt_session_slot,
            1000,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
#ifdef ENABLE_QUNIT
        {{"qunit_case_number",
            PGC_USERSET,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Sets the qunit_case_number."),
            gettext_noop("Only QUnit framework uses this guc parameter.")},
            &u_sess->utils_cxt.qunit_case_number,
            0,
            0,
            INT_MAX,
            NULL,
            set_qunit_case_number_hook,
            NULL},
#endif

        {{"gpc_clean_timeout",
            PGC_SIGHUP,
            NODE_ALL,
            CLIENT_CONN,
            gettext_noop("Set the maximum allowed duration of any unused global plancache."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_common.gpc_clean_timeout,
            30 * 60,      /* 30min */
            5 * 60,       /* 5min */
            24 * 60 * 60, /* 24h */
            NULL,
            NULL,
            NULL},

        {{"transaction_sync_naptime",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            STATS_MONITORING,
            gettext_noop("Sets the timeout to call gs_clean when sync transaction with GTM."),
            gettext_noop("A value of 0 turns off the timeout."),
            GUC_UNIT_S},
            &u_sess->attr.attr_common.transaction_sync_naptime,
            30,
            0,
            INT_MAX / 1000,
            NULL,
            NULL,
            NULL},
        {{"transaction_sync_timeout",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            STATS_MONITORING,
            gettext_noop("Sets the timeout when sync every transaction with GTM to avoid deadlock sync."),
            gettext_noop("A value of 0 turns off the timeout. Please make sure it larger than gs_clean_timeout."),
            GUC_UNIT_S},
            &u_sess->attr.attr_common.transaction_sync_timeout,
            600,
            0,
            INT_MAX / 1000,
            NULL,
            NULL,
            NULL},
        {{"gtm_port",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Port of GTM."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.GtmHostPortArray[0],
            6666,
            1,
            65535,
            NULL,
            NULL,
            NULL},

        {{"gtm_port1",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Port of GTM."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.GtmHostPortArray[1],
            6665,
            1,
            65535,
            NULL,
            NULL,
            NULL},

        {{"gtm_port2",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Port of GTM."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.GtmHostPortArray[2],
            6666,
            1,
            65535,
            NULL,
            NULL,
            NULL},

        {{"gtm_port3",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Port of GTM."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.GtmHostPortArray[3],
            6666,
            1,
            65535,
            NULL,
            NULL,
            NULL},

        {{"gtm_port4",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Port of GTM."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.GtmHostPortArray[4],
            6666,
            1,
            65535,
            NULL,
            NULL,
            NULL},

        {{"gtm_port5",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Port of GTM."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.GtmHostPortArray[5],
            6666,
            1,
            65535,
            NULL,
            NULL,
            NULL},

        {{"gtm_port6",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Port of GTM."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.GtmHostPortArray[6],
            6666,
            1,
            65535,
            NULL,
            NULL,
            NULL},

        {{"gtm_port7",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Port of GTM."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.GtmHostPortArray[7],
            6666,
            1,
            65535,
            NULL,
            NULL,
            NULL},
        {{"max_datanodes",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            DATA_NODES,
            gettext_noop("Maximum number of Datanodes in the cluster."),
            gettext_noop("It is not possible to create more Datanodes in the cluster than "
                         "this maximum number.")},
            &g_instance.attr.attr_common.MaxDataNodes,
            64,
            2,
            65535,
            NULL,
            NULL,
            NULL},
        {{"session_sequence_cache",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets current session sequence cache."),
            NULL},
            &u_sess->attr.attr_common.session_sequence_cache,
            10,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"gtm_connect_timeout",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Sets the max wait time in seconds to connect GTM."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_common.gtm_connect_timeout,
            2,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
#ifdef ENABLE_MULTIPLE_NODES
        {{"gtm_rw_timeout",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Sets the max time in seconds to wait for a reponse from GTM."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_common.gtm_rw_timeout,
            60,
            1,
            INT_MAX,
            NULL,
            assign_gtm_rw_timeout,
            NULL},
#endif
        {{"max_datanode_for_plan",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            UNGROUPED,
            gettext_noop("The max number of DN datanodes to show plan, 0 means just CN plan."),
            NULL},
            &u_sess->attr.attr_common.max_datanode_for_plan,
            0,
            0,
            MAX_DN_NODE_NUM,
            NULL,
            NULL,
            NULL},
        {{"ts_consumer_workers", 
            PGC_SIGHUP, 
            NODE_DISTRIBUTE,
            TSDB, 
            gettext_noop("Enables compaction consumers feature."), 
            NULL},
            &u_sess->attr.attr_common.ts_consumer_workers,
            3,
            1,
            100,
            NULL,
            NULL,
            NULL},
        {{"ts_valid_partition",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            TSDB,
            gettext_noop("Number of partition producer and delete search."),
            NULL},
            &u_sess->attr.attr_common.ts_valid_partition,
            2,
            1,
            30,
            NULL,
            NULL,
            NULL},
        {{"ts_cudesc_threshold", 
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            TSDB,
            gettext_noop("If the number of cudesc tuples is larger than "\
            "ts_cudesc_threshold, cudesc index will be created by compaction threads."),
            NULL},
            &u_sess->attr.attr_common.ts_cudesc_threshold,
            0,
            0,
            10000000,
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
    u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_COMMON] =
        (struct config_int*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_COMMON], bytes, localConfigureNamesInt, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitConfigureNamesReal()
{
    struct config_real localConfigureNamesReal[] = {
        {{"connection_alarm_rate",
            PGC_SIGHUP,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Reports alarm if connection rate overload."),
            NULL},
            &u_sess->attr.attr_common.ConnectionAlarmRate,
            0.9,
            0.0,
            1.0,
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
            0.0,
            0.0,
            0.0,
            NULL,
            NULL,
            NULL}
    };

    Size bytes = sizeof(localConfigureNamesReal);
    u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_COMMON] =
        (struct config_real*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_COMMON], bytes, localConfigureNamesReal, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitConfigureNamesInt64()
{
    struct config_int64 localConfigureNamesInt64[] = {
        {{"track_stmt_details_size",
            PGC_USERSET,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("the maximum bytes of statement details to be gathered."),
            NULL},
            &u_sess->attr.attr_common.track_stmt_details_size,
            INT64CONST(4096),
            INT64CONST(0),
            INT64CONST(100000000),
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

    Size bytes = sizeof(localConfigureNamesInt64);
    u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_COMMON] =
        (struct config_int64*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_COMMON], bytes,
        localConfigureNamesInt64, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitConfigureNamesString()
{
    struct config_string localConfigureNamesString[] = {
        {{"client_encoding",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Sets the client's character set encoding."),
            NULL,
            GUC_IS_NAME | GUC_REPORT},
            &u_sess->attr.attr_common.client_encoding_string,
            "SQL_ASCII",
            check_client_encoding,
            assign_client_encoding,
            NULL},
        {{"log_line_prefix",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Controls information prefixed to each log line."),
            gettext_noop("If blank, no prefix is used.")},
            &u_sess->attr.attr_common.Log_line_prefix,
            "",
            NULL,
            NULL,
            NULL},
        {{"log_timezone",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Sets the time zone to use in log messages."),
            NULL},
            &u_sess->attr.attr_common.log_timezone_string,
            "GMT",
            check_log_timezone,
            assign_log_timezone,
            show_log_timezone},

        {{"DateStyle",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Sets the display format for date and time values."),
            gettext_noop("Also controls interpretation of ambiguous "
                        "date inputs."),
            GUC_LIST_INPUT | GUC_REPORT},
            &u_sess->attr.attr_common.datestyle_string,
            "ISO, MDY",
            check_datestyle,
            assign_datestyle,
            NULL},
        {{"dynamic_library_path",
            PGC_SUSET,
            NODE_ALL,
            CLIENT_CONN_OTHER,
            gettext_noop("Sets the path for dynamically loadable modules."),
            gettext_noop("If a dynamically loadable module needs to be opened and "
                        "the specified name does not have a directory component (i.e., the "
                        "name does not contain a slash), the system will search this path for "
                        "the specified file."),
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.Dynamic_library_path,
            "$libdir",
            NULL,
            NULL,
            NULL},
#ifdef USE_BONJOUR
        {{"bonjour_name",
            PGC_POSTMASTER,
            NODE_ALL,
            CONN_AUTH_SETTINGS,
            gettext_noop("Sets the Bonjour service name."),
            NULL},
            &g_instance.attr.attr_common.bonjour_name,
            "",
            NULL,
            NULL,
            NULL},
#endif
        /* See main.c about why defaults for LC_foo are not all alike */
        {{"lc_collate",
            PGC_INTERNAL,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Shows the collation order locale."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_common.locale_collate,
            "C",
            NULL,
            NULL,
            NULL},

        {{"lc_ctype",
            PGC_INTERNAL,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Shows the character classification and case conversion locale."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_common.locale_ctype,
            "C",
            NULL,
            NULL,
            NULL},

        {{"lc_messages",
            PGC_SUSET,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Sets the language in which messages are displayed."),
            NULL},
            &u_sess->attr.attr_common.locale_messages,
            "",
            check_locale_messages,
            assign_locale_messages,
            NULL},

        {{"lc_monetary",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Sets the locale for formatting monetary amounts."),
            NULL},
            &u_sess->attr.attr_common.locale_monetary,
            "C",
            check_locale_monetary,
            assign_locale_monetary,
            NULL},

        {{"lc_numeric",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Sets the locale for formatting numbers."),
            NULL},
            &u_sess->attr.attr_common.locale_numeric,
            "C",
            check_locale_numeric,
            assign_locale_numeric,
            NULL},

        {{"lc_time",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Sets the locale for formatting date and time values."),
            NULL},
            &u_sess->attr.attr_common.locale_time,
            "C",
            check_locale_time,
            assign_locale_time,
            NULL},

        {{"shared_preload_libraries",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_KERNEL,
            gettext_noop("Lists shared libraries to preload into server."),
            NULL,
            GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.shared_preload_libraries_string,
            "security_plugin",
            NULL,
            NULL,
            NULL},
        {{"thread_pool_attr",
            PGC_POSTMASTER,
            NODE_ALL,
            CLIENT_CONN,
            gettext_noop("Spare Cpu that can not be used in thread pool."),
            NULL,
            GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.thread_pool_attr,
            "16, 2, (nobind)",
            NULL,
            NULL,
            NULL},
            
         {{"thread_pool_stream_attr",
            PGC_POSTMASTER,
            NODE_ALL,
            CLIENT_CONN,
            gettext_noop("Spare Cpu that can not be used in thread pool stream."),
            NULL,
            GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.thread_pool_stream_attr,
            "16, 0.2, 2, (nobind)",
            NULL,
            NULL,
            NULL},

        {{"comm_proxy_attr",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            CLIENT_CONN,
            gettext_noop("Sets comm_proxy attribute in comm_proxy mode."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.comm_proxy_attr,
            "none",
            NULL,
            NULL,
            NULL},

        {{"track_stmt_retention_time",
            PGC_SIGHUP,
            NODE_ALL,
            CLIENT_CONN,
            gettext_noop("The longest retention time of full SQL and slow query in statement_ history"),
            NULL,
            GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.track_stmt_retention_time,
            "3600,604800",
            check_statement_retention_time,
            assign_statement_retention_time,
            NULL},

        {{"router",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            CLIENT_CONN,
            gettext_noop("set send node router for sql before unrouter."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_common.router_att,
            "",
            check_router_attr,
            assign_router_attr,
            NULL},

        {{"local_preload_libraries",
            PGC_BACKEND,
            NODE_ALL,
            CLIENT_CONN_OTHER,
            gettext_noop("Lists shared libraries to preload into each backend."),
            NULL,
            GUC_LIST_INPUT | GUC_LIST_QUOTE},
            &u_sess->attr.attr_common.local_preload_libraries_string,
            "",
            NULL,
            NULL,
            NULL},
        /* Can't be set in postgresql.conf */
        {{"current_logic_cluster",
            PGC_INTERNAL,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Shows current logic cluster."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_common.current_logic_cluster_name,
            NULL,
            NULL,
            NULL,
            show_lcgroup_name},
        {{"search_path",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets the schema search order for names that are not schema-qualified."),
            NULL,
            GUC_LIST_INPUT | GUC_LIST_QUOTE},
            &u_sess->attr.attr_common.namespace_search_path,
            "\"$user\",public",
            check_search_path,
            assign_search_path,
            NULL},

        {{"percentile",
            PGC_INTERNAL,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Sets the percentile of sql responstime that DBA want to know."),
            NULL,
            GUC_LIST_INPUT | GUC_LIST_QUOTE},
            &u_sess->attr.attr_common.percentile_values,
            "80,95",
            check_percentile,
            NULL,
            NULL},
        {{"nls_timestamp_format",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("defines the default timestamp format to use with the TO_TIMESTAMP functions."),
            NULL,
            },
            &u_sess->attr.attr_common.nls_timestamp_format_string,
            "DD-Mon-YYYY HH:MI:SS.FF AM",
            NULL,
            NULL,
            NULL},

        {{"current_schema",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets the schema search order for names that are not schema-qualified."),
            NULL,
            GUC_LIST_INPUT | GUC_LIST_QUOTE},
            &u_sess->attr.attr_common.namespace_current_schema,
            "\"$user\",public",
            check_search_path,
            assign_search_path,
            NULL},
        /* Can't be set in postgresql.conf */
        {{"server_encoding",
            PGC_INTERNAL,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Sets the server (database) character set encoding."),
            NULL,
            GUC_IS_NAME | GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_common.server_encoding_string,
            "SQL_ASCII",
            NULL,
            NULL,
            NULL},
        /* Can't be set in postgresql.conf */
        {{"server_version",
            PGC_INTERNAL,
            NODE_ALL,
            PRESET_OPTIONS,
            gettext_noop("Shows the server version."),
            NULL,
            GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_common.server_version_string,
            PG_VERSION,
            NULL,
            NULL,
            NULL},
        /* Not for general use --- used by SET ROLE */
        {{"role",
            PGC_USERSET,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Sets the current role."),
            NULL,
            GUC_IS_NAME | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE |
                GUC_NOT_WHILE_SEC_REST},
            &u_sess->attr.attr_common.role_string,
            "none",
            check_role,
            assign_role,
            show_role},
        /* Not for general use --- used by SET SESSION AUTHORIZATION */
        {{"session_authorization",
            PGC_USERSET,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Sets the session user name."),
            NULL,
            GUC_IS_NAME | GUC_REPORT | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE |
                GUC_DISALLOW_IN_FILE | GUC_NOT_WHILE_SEC_REST},
            &u_sess->attr.attr_common.session_authorization_string,
            NULL,
            check_session_authorization,
            assign_session_authorization,
            NULL},

        {{"log_destination",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the destination for server log output."),
            gettext_noop("Valid values are combinations of \"stderr\", "
                        "\"syslog\", \"csvlog\", and \"eventlog\", "
                        "depending on the platform."),
            GUC_LIST_INPUT},
            &u_sess->attr.attr_common.log_destination_string,
            "stderr",
            check_log_destination,
            assign_log_destination,
            NULL},
        {{"log_directory",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the destination directory for log files."),
            gettext_noop("Can be specified as relative to the data directory "
                        "or as absolute path."),
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.Log_directory,
            "pg_log",
            check_directory,
            NULL,
            NULL},
        {{"asp_log_directory",
            PGC_POSTMASTER,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the destination directory for asp log files."),
            gettext_noop("Can be specified as relative to the data directory "
                        "or as absolute path."),
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.asp_log_directory,
            NULL,
            check_directory,
            NULL,
            NULL},
        {{"query_log_directory",
            PGC_POSTMASTER,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the destination directory for slow query log files."),
            gettext_noop("Can be specified as relative to the data directory "
                        "or as absolute path."),
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.query_log_directory,
            NULL,
            check_directory,
            NULL,
            NULL},
        {{"perf_directory",
            PGC_POSTMASTER,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the destination directory for perf json files."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.Perf_directory,
            NULL,
            check_directory,
            NULL,
            NULL},
        {{"explain_dna_file",
            PGC_USERSET,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the destination file for explain performance data."),
            gettext_noop("Must be specified as an absolute directory + .csv filename."),
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_common.Perf_log,
            NULL,
            check_perf_log,
            NULL,
            NULL},
        {{"log_filename",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the file name pattern for log files."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.Log_filename,
            "postgresql-%Y-%m-%d_%H%M%S.log",
            check_log_filename,
            NULL,
            NULL},
        {{"query_log_file",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the file name pattern for slow query log files."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.query_log_file,
            "slow_query_log-%Y-%m-%d_%H%M%S.log",
            NULL,
            NULL,
            NULL},

        {{"asp_flush_mode",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the active session profile flush mode:file/table/all."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.asp_flush_mode,
            "table",
            check_asp_flush_mode,
            NULL,
            NULL},

        {{"asp_log_filename",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the file name pattern for asp data files."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.asp_log_filename,
            "asp-%Y-%m-%d_%H%M%S.log",
            NULL,
            NULL,
            NULL},

        {{"bbox_dump_path",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the path of core dump created by bbox_handler."),
            NULL},
            &u_sess->attr.attr_common.bbox_dump_path,
            "",
            check_bbox_corepath,
            assign_bbox_corepath,
            show_bbox_dump_path},

        {{"alarm_component",
            PGC_POSTMASTER,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Sets the component for alarm function."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.Alarm_component,
            "/opt/snas/bin/snas_cm_cmd",
            NULL,
            NULL,
            NULL},

        {{"syslog_ident",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the program name used to identify openGauss "
                        "messages in syslog."),
            NULL},
            &u_sess->attr.attr_common.syslog_ident_str,
            "postgres",
            NULL,
            assign_syslog_ident,
            NULL},

        {{"event_source",
            PGC_POSTMASTER,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the application name used to identify "
                        "openGauss messages in the event log."),
            NULL},
            &g_instance.attr.attr_common.event_source,
            "PostgreSQL",
            NULL,
            NULL,
            NULL},

        {{"TimeZone",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Sets the time zone for displaying and interpreting time stamps."),
            NULL,
            GUC_REPORT},
            &u_sess->attr.attr_common.timezone_string,
            "GMT",
            check_timezone,
            assign_timezone,
            show_timezone},
        {{"timezone_abbreviations",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Selects a file of time zone abbreviations."),
            NULL},
            &u_sess->attr.attr_common.timezone_abbreviations_string,
            NULL,
            check_timezone_abbreviations,
            assign_timezone_abbreviations,
            NULL},
        {{"data_directory",
            PGC_POSTMASTER,
            NODE_ALL,
            FILE_LOCATIONS,
            gettext_noop("Sets the server's data directory."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.data_directory,
            NULL,
            NULL,
            NULL,
            NULL},

        {{"config_file",
            PGC_POSTMASTER,
            NODE_ALL,
            FILE_LOCATIONS,
            gettext_noop("Sets the server's main configuration file."),
            NULL,
            GUC_DISALLOW_IN_FILE | GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.ConfigFileName,
            NULL,
            NULL,
            NULL,
            NULL},

#ifdef ENABLE_MOT
        {{"mot_config_file",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            FILE_LOCATIONS,
            gettext_noop("Sets mot main configuration file."),
            NULL,
            GUC_DISALLOW_IN_FILE | GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.MOTConfigFileName,
            NULL,
            NULL,
            NULL,
            NULL},
#endif

        {{"hba_file",
            PGC_POSTMASTER,
            NODE_ALL,
            FILE_LOCATIONS,
            gettext_noop("Sets the server's \"hba\" configuration file."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.HbaFileName,
            NULL,
            NULL,
            NULL,
            NULL},

        {{"ident_file",
            PGC_POSTMASTER,
            NODE_ALL,
            FILE_LOCATIONS,
            gettext_noop("Sets the server's \"ident\" configuration file."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.IdentFileName,
            NULL,
            NULL,
            NULL,
            NULL},

        {{"external_pid_file",
            PGC_POSTMASTER,
            NODE_ALL,
            FILE_LOCATIONS,
            gettext_noop("Writes the postmaster PID to the specified file."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.external_pid_file,
            NULL,
            check_canonical_path,
            NULL,
            NULL},
        {{"stats_temp_directory",
            PGC_SIGHUP,
            NODE_ALL,
            STATS_COLLECTOR,
            gettext_noop("Writes temporary statistics files to the specified directory."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.pgstat_temp_directory,
            "pg_stat_tmp",
            check_canonical_path,
            assign_pgstat_temp_directory,
            NULL},
        {{"default_text_search_config",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Sets default text search configuration."),
            NULL},
            &u_sess->attr.attr_common.TSCurrentConfig,
            "pg_catalog.simple",
            check_TSCurrentConfig,
            assign_TSCurrentConfig,
            NULL},
        {{"pgxc_node_name",
            PGC_POSTMASTER,
            NODE_ALL,
            GTM,
            gettext_noop("The Coordinator or Datanode name."),
            NULL,
            GUC_NO_RESET_ALL | GUC_IS_NAME},
            &g_instance.attr.attr_common.PGXCNodeName,
            "",
            NULL,
            NULL,
            NULL},
#ifdef ENABLE_DISTRIBUTE_TEST
        {{"distribute_test_param",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            DEVELOPER_OPTIONS,
            gettext_noop("Sets the parameters of distributed test framework."),
            NULL,
            GUC_LIST_INPUT | GUC_REPORT | GUC_NO_SHOW_ALL},
            &u_sess->attr.attr_common.test_param_str,
            "-1, default, default",
            check_distribute_test_param,
            assign_distribute_test_param,
            NULL},
#endif
#ifdef ENABLE_SEGMENT_TEST
        {{"segment_test_param",
            PGC_USERSET,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Sets the parameters of segment test framework."),
            NULL,
            GUC_LIST_INPUT | GUC_REPORT | GUC_NO_SHOW_ALL},
            &u_sess->attr.attr_common.seg_test_param_str,
            "-1, default, default",
            check_segment_test_param,
            assign_segment_test_param,
            NULL},
#endif
        {{"application_name",
            PGC_USERSET,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Sets the application name to be reported in statistics and logs."),
            NULL,
            GUC_IS_NAME | GUC_REPORT | GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_common.application_name,
            "",
            check_application_name,
            assign_application_name,
            NULL},
        /* analysis options for dfx */
        {{"analysis_options",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("enable/disable sql dfx option."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_common.analysis_options,
            "off(ALL)",
            analysis_options_check,
            analysis_options_guc_assign,
            analysis_options_guc_show},
        {{"transparent_encrypt_kms_url",
            PGC_POSTMASTER,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("The URL to get transparent encryption key."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_NO_RESET_ALL},
            &g_instance.attr.attr_common.transparent_encrypt_kms_url,
            "",
            transparent_encrypt_kms_url_region_check,
            NULL,
            NULL},
        {{"numa_distribute_mode",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES,
            gettext_noop("Sets the NUMA node distribution mode."),
            NULL},
            &g_instance.attr.attr_common.numa_distribute_mode,
            "none",
            check_numa_distribute_mode,
            NULL,
            NULL},

        {{"bbox_blanklist_items",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("List of names of bbox blanklist items."),
            NULL,
            GUC_LIST_INPUT},
            &g_instance.attr.attr_common.bbox_blacklist_items,
            "",
            check_bbox_blacklist,
            assign_bbox_blacklist,
            show_bbox_blacklist},

        {{"track_stmt_stat_level",
            PGC_USERSET,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("specify which level statement's statistics to be gathered."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_common.track_stmt_stat_level,
            "OFF,L0",
            check_statement_stat_level,
            assign_statement_stat_level,
            NULL},
#ifdef ENABLE_MULTIPLE_NODES
        /* Can't be set in postgresql.conf */
        {{"node_group_mode",
            PGC_INTERNAL,
            NODE_DISTRIBUTE,
            UNGROUPED,
            gettext_noop("Shows node group mode."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_common.node_group_mode,
            "node group",
            NULL,
            NULL,
            show_nodegroup_mode},
        {{"gtm_host",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Host name or address of GTM"),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.GtmHostInfoStringArray[0],
            "localhost",
            check_gtmhostconninfo,
            assign_gtmhostconninfo0,
            NULL},
        {{"gtm_host1",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Host name or address of GTM"),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.GtmHostInfoStringArray[1],
            "localhost",
            check_gtmhostconninfo,
            assign_gtmhostconninfo1,
            NULL},

        {{"gtm_host2",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Host name or address of GTM"),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.GtmHostInfoStringArray[2],
            "",
            check_gtmhostconninfo,
            assign_gtmhostconninfo2,
            NULL},

        {{"gtm_host3",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Host name or address of GTM"),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.GtmHostInfoStringArray[3],
            "",
            check_gtmhostconninfo,
            assign_gtmhostconninfo3,
            NULL},

        {{"gtm_host4",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Host name or address of GTM"),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.GtmHostInfoStringArray[4],
            "",
            check_gtmhostconninfo,
            assign_gtmhostconninfo4,
            NULL},

        {{"gtm_host5",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Host name or address of GTM"),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.GtmHostInfoStringArray[5],
            "",
            check_gtmhostconninfo,
            assign_gtmhostconninfo5,
            NULL},
        {{"gtm_host6",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Host name or address of GTM"),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.GtmHostInfoStringArray[6],
            "",
            check_gtmhostconninfo,
            assign_gtmhostconninfo6,
            NULL},

        {{"gtm_host7",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Host name or address of GTM"),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.GtmHostInfoStringArray[7],
            "",
            check_gtmhostconninfo,
            assign_gtmhostconninfo7,
            NULL},
        {{"ts_compaction_strategy",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            TSDB,
            gettext_noop("TSDB producer's Strategy for sending task"),
            NULL,
            GUC_LIST_INPUT | GUC_LIST_QUOTE},
            &u_sess->attr.attr_common.ts_compaction_strategy,
            "3,6,6,12,0", 
            check_compaciton_strategy,
            NULL},
#endif
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
    u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_COMMON] =
        (struct config_string*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_COMMON], bytes,
        localConfigureNamesString, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitConfigureNamesEnum()
{
    struct config_enum localConfigureNamesEnum[] = {
#ifndef ENABLE_MULTIPLE_NODES
        {{"sync_config_strategy",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            WAL_SETTINGS,
            gettext_noop("Synchronization strategy for configuration files between host and standby."),
            NULL},
            &g_instance.attr.attr_common.sync_config_strategy,
            ALL_NODE,
            sync_config_strategy_options,
            NULL,
            NULL,
            NULL},
#endif
        {{"bytea_output",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets the output format for bytea."),
            NULL},
            &u_sess->attr.attr_common.bytea_output,
            BYTEA_OUTPUT_HEX,
            bytea_output_options,
            NULL,
            NULL,
            NULL},

        {{"client_min_messages",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets the message levels that are sent to the client."),
            gettext_noop("Each level includes all the levels that follow it. The later"
                         " the level, the fewer messages are sent.")},
            &u_sess->attr.attr_common.client_min_messages,
            NOTICE,
            client_message_level_options,
            check_client_min_messages,
            NULL,
            NULL},
        {{"default_transaction_isolation",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets the transaction isolation level of each new transaction."),
            NULL},
            &u_sess->attr.attr_common.DefaultXactIsoLevel,
            XACT_READ_COMMITTED,
            isolation_level_options,
            NULL,
            NULL,
            NULL},

        {{"IntervalStyle",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_LOCALE,
            gettext_noop("Sets the display format for interval values."),
            NULL,
            GUC_REPORT},
            &u_sess->attr.attr_common.IntervalStyle,
            INTSTYLE_POSTGRES,
            intervalstyle_options,
            NULL,
            NULL,
            NULL},
        {{"log_error_verbosity",
            PGC_SUSET,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Sets the verbosity of logged messages."),
            NULL},
            &u_sess->attr.attr_common.Log_error_verbosity,
            PGERROR_DEFAULT,
            log_error_verbosity_options,
            NULL,
            NULL,
            NULL},

        {{"log_min_messages",
            PGC_SUSET,
            NODE_ALL,
            LOGGING_WHEN,
            gettext_noop("Sets the message levels that are logged."),
            gettext_noop("Each level includes all the levels that follow it. The later"
                         " the level, the fewer messages are sent.")},
            &u_sess->attr.attr_common.log_min_messages,
            WARNING,
            server_message_level_options,
            NULL,
            NULL,
            NULL},

        {{"backtrace_min_messages",
            PGC_SUSET,
            NODE_ALL,
            LOGGING_WHEN,
            gettext_noop("Sets the message levels for print backtrace that are logged."),
            gettext_noop("Each level includes all the levels that follow it. The later"
                         " the level, the fewer messages are sent.")},
            &u_sess->attr.attr_common.backtrace_min_messages,
#ifdef USE_ASSERT_CHECKING
            ERROR,
#else
            PANIC,
#endif
            server_message_level_options,
            NULL,
            NULL,
            NULL},

        {{"log_min_error_statement",
            PGC_SUSET,
            NODE_ALL,
            LOGGING_WHEN,
            gettext_noop("Causes all statements generating error at or above this level to be logged."),
            gettext_noop("Each level includes all the levels that follow it. The later"
                         " the level, the fewer messages are sent.")},
            &u_sess->attr.attr_common.log_min_error_statement,
            ERROR,
            server_message_level_options,
            NULL,
            NULL,
            NULL},
        {{"log_statement",
            PGC_SUSET,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Sets the type of statements logged."),
            NULL},
            &u_sess->attr.attr_common.log_statement,
            LOGSTMT_NONE,
            log_statement_options,
            NULL,
            NULL,
            NULL},
        {{"syslog_facility",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHERE,
            gettext_noop("Sets the syslog \"facility\" to be used when syslog enabled."),
            NULL},
            &u_sess->attr.attr_common.syslog_facility,
#ifdef HAVE_SYSLOG
            LOG_LOCAL0,
#else
            0,
#endif
            syslog_facility_options,
            NULL,
            assign_syslog_facility,
            NULL},

        {{"session_replication_role",
            PGC_SUSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets the session's behavior for triggers and rewrite rules."),
            NULL},
            &u_sess->attr.attr_common.SessionReplicationRole,
            SESSION_REPLICATION_ROLE_ORIGIN,
            session_replication_role_options,
            NULL,
            assign_session_replication_role,
            NULL},
        {{"trace_recovery_messages",
            PGC_SIGHUP,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Enables logging of recovery-related debugging information."),
            gettext_noop("Each level includes all the levels that follow it. The later"
                         " the level, the fewer messages are sent.")},
            &u_sess->attr.attr_common.trace_recovery_messages,

            /*
             * client_message_level_options allows too many values, really, but
             * it's not worth having a separate options array for this.
             */
            LOG,
            client_message_level_options,
            NULL,
            NULL,
            NULL},

        {{"track_functions",
            PGC_SUSET,
            NODE_ALL,
            STATS_COLLECTOR,
            gettext_noop("Collects function-level statistics on database activity."),
            NULL},
            &u_sess->attr.attr_common.pgstat_track_functions,
            TRACK_FUNC_OFF,
            track_function_options,
            NULL,
            NULL,
            NULL},
        {{"xmlbinary",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets how binary values are to be encoded in XML."),
            NULL},
            &u_sess->attr.attr_common.xmlbinary,
            XMLBINARY_BASE64,
            xmlbinary_options,
            NULL,
            NULL,
            NULL},

        {{"xmloption",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets whether XML data in implicit parsing and serialization "
                         "operations is to be considered as documents or content fragments."),
            NULL},
            &u_sess->attr.attr_common.xmloption,
            XMLOPTION_CONTENT,
            xmloption_options,
            NULL,
            NULL,
            NULL},
        {{"remotetype",
            PGC_BACKEND,
            NODE_ALL,
            CONN_AUTH,
            gettext_noop("Sets the type of openGauss remote connection"),
            NULL},
            &u_sess->attr.attr_common.remoteConnType,
            REMOTE_CONN_APP,
            pgxc_conn_types,
            NULL,
            NULL,
            NULL},
        {{"instr_unique_sql_track_type",
            PGC_INTERNAL,
            NODE_ALL,
            INSTRUMENTS_OPTIONS,
            gettext_noop("unique sql track type"),
            NULL},
            &u_sess->attr.attr_common.unique_sql_track_type,
            UNIQUE_SQL_TRACK_TOP,
            unique_sql_track_option,
            NULL,
            NULL,
            NULL},
        {{"cluster_run_mode",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            PRESET_OPTIONS,
            gettext_noop("Sets the type of shared storage cluster."),
            NULL},
            &g_instance.attr.attr_common.cluster_run_mode,
            RUN_MODE_PRIMARY,
            cluster_run_mode_options,
            NULL,
            NULL,
            NULL},
        {{"stream_cluster_run_mode",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            PRESET_OPTIONS,
            gettext_noop("Sets the type of streaming cluster."),
            NULL},
            &g_instance.attr.attr_common.stream_cluster_run_mode,
            RUN_MODE_PRIMARY,
            cluster_run_mode_options,
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
    u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_COMMON] =
        (struct config_enum*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_COMMON], bytes, localConfigureNamesEnum, bytes);
    securec_check_ss(rc, "\0", "\0");
}

/******** end of options list ********/

/*
 * To allow continued support of obsolete names for GUC variables, we apply
 * the following mappings to any unrecognized name.  Note that an old name
 * should be mapped to a new one only if the new variable has very similar
 * semantics to the old.
 */
static const char* const map_old_guc_names[] = {"sort_mem", "work_mem", "vacuum_mem", "maintenance_work_mem", NULL};

static int guc_var_compare(const void* a, const void* b);
static int guc_name_compare(const char* namea, const char* nameb);
static void InitializeGUCOptionsFromEnvironment(void);
static void InitializeOneGUCOption(struct config_generic* gconf);
static void push_old_value(struct config_generic* gconf, GucAction action);
static void ReportGUCOption(struct config_generic* record);
static void reapply_stacked_values(struct config_generic* variable, struct config_string* pHolder, GucStack* stack,
    const char* curvalue, GucContext curscontext, GucSource cursource);
static void ShowGUCConfigOption(const char* name, DestReceiver* dest);
static void ShowAllGUCConfig(const char* likename, DestReceiver* dest);
static char* _ShowOption(struct config_generic* record, bool use_units);
static bool validate_option_array_item(const char* name, const char* value, bool skipIfNoPermissions);
#ifndef ENABLE_MULTIPLE_NODES
static void replace_config_value(char** optlines, char* name, char* value, config_type vartype);
#endif

/*
 * Some infrastructure for checking malloc/strdup/realloc calls
 */
static void* guc_malloc(int elevel, size_t size)
{
    void* data = NULL;

    /* Avoid unportable behavior of malloc(0) */
    if (size == 0)
        size = 1;

#ifdef FRONTEND
    data = malloc(size);
#else
    data = MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), size);
#endif

    if (data == NULL)
        ereport(elevel, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

    return data;
}

static void* guc_realloc(int elevel, void* old, size_t size)
{
    void* data = NULL;

    /* Avoid unportable behavior of realloc(NULL, 0) */
    if (old == NULL && size == 0)
        size = 1;

#ifdef FRONTEND
    data = realloc(old, size);
#else
    data = repalloc(old, size);
#endif

    if (data == NULL)
        ereport(elevel, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

    return data;
}

static char* guc_strdup(int elevel, const char* src)
{
    char* data = NULL;

#ifdef FRONTEND
    data = strdup(src);
#else
    data = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), src);
#endif

    if (data == NULL)
        ereport(elevel, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

    return data;
}

/*
 * Detect whether strval is referenced anywhere in a GUC string item
 */
static bool string_field_used(struct config_string* conf, char* strval)
{
    GucStack* stack = NULL;

    if (strval == *(conf->variable) || strval == conf->reset_val || strval == conf->boot_val)
        return true;

    for (stack = conf->gen.stack; stack; stack = stack->prev) {
        if (strval == stack->prior.val.stringval || strval == stack->masked.val.stringval)
            return true;
    }

    return false;
}

/*
 * Support for assigning to a field of a string GUC item.  Free the prior
 * value if it's not referenced anywhere else in the item (including stacked
 * states).
 */
static void set_string_field(struct config_string* conf, char** field, char* newval)
{
    char* oldval = *field;

    /* Do the assignment */
    *field = newval;

    /* Free old value if it's not NULL and isn't referenced anymore */
    if (oldval && !string_field_used(conf, oldval)) {
        pfree(oldval);
        oldval = NULL;
    }
}

/*
 * Detect whether an "extra" struct is referenced anywhere in a GUC item
 */
static bool extra_field_used(struct config_generic* gconf, void* extra)
{
    GucStack* stack = NULL;

    if (extra == gconf->extra)
        return true;

    switch (gconf->vartype) {
        case PGC_BOOL:
            if (extra == ((struct config_bool*)gconf)->reset_extra)
                return true;

            break;

        case PGC_INT:
            if (extra == ((struct config_int*)gconf)->reset_extra)
                return true;

            break;

        case PGC_INT64:
            if (extra == ((struct config_int64*)gconf)->reset_extra)
                return true;
            break;

        case PGC_REAL:
            if (extra == ((struct config_real*)gconf)->reset_extra)
                return true;

            break;

        case PGC_STRING:
            if (extra == ((struct config_string*)gconf)->reset_extra)
                return true;

            break;

        case PGC_ENUM:
            if (extra == ((struct config_enum*)gconf)->reset_extra)
                return true;

            break;
        default:
            break;
    }

    for (stack = gconf->stack; stack; stack = stack->prev) {
        if (extra == stack->prior.extra || extra == stack->masked.extra)
            return true;
    }

    return false;
}

/*
 * Support for assigning to an "extra" field of a GUC item.  Free the prior
 * value if it's not referenced anywhere else in the item (including stacked
 * states).
 */
static void set_extra_field(struct config_generic* gconf, void** field, void* newval)
{
    void* oldval = *field;

    /* Do the assignment */
    *field = newval;

    /* Free old value if it's not NULL and isn't referenced anymore */
    if (oldval && !extra_field_used(gconf, oldval))
        pfree(oldval);
}

/*
 * Support for copying a variable's active value into a stack entry.
 * The "extra" field associated with the active value is copied, too.
 *
 * NB: be sure stringval and extra fields of a new stack entry are
 * initialized to NULL before this is used, else we'll try to free() them.
 */
static void set_stack_value(struct config_generic* gconf, config_var_value* val)
{
    switch (gconf->vartype) {
        case PGC_BOOL:
            val->val.boolval = *((struct config_bool*)gconf)->variable;
            break;

        case PGC_INT:
            val->val.intval = *((struct config_int*)gconf)->variable;
            break;

        case PGC_INT64:
            val->val.int64val = *((struct config_int64*)gconf)->variable;
            break;

        case PGC_REAL:
            val->val.realval = *((struct config_real*)gconf)->variable;
            break;

        case PGC_STRING:
            set_string_field(
                (struct config_string*)gconf, &(val->val.stringval), *((struct config_string*)gconf)->variable);
            break;

        case PGC_ENUM:
            val->val.enumval = *((struct config_enum*)gconf)->variable;
            break;
        default:
            break;
    }

    set_extra_field(gconf, &(val->extra), gconf->extra);
}

/*
 * Support for discarding a no-longer-needed value in a stack entry.
 * The "extra" field associated with the stack entry is cleared, too.
 */
static void discard_stack_value(struct config_generic* gconf, config_var_value* val)
{
    switch (gconf->vartype) {
        case PGC_BOOL:
        case PGC_INT:
        case PGC_INT64:
        case PGC_REAL:
        case PGC_ENUM:
            /* no need to do anything */
            break;

        case PGC_STRING:
            set_string_field((struct config_string*)gconf, &(val->val.stringval), NULL);
            break;
        default:
            break;
    }

    set_extra_field(gconf, &(val->extra), NULL);
}

/*
 * Fetch the sorted array pointer (exported for help_config.c's use ONLY)
 */
struct config_generic** get_guc_variables(void)
{
    return u_sess->guc_variables;
}

#ifndef ENABLE_MULTIPLE_NODES
static void InitSingleNodeUnsupportGuc()
{
    /* for Bool Guc Variables */
    u_sess->attr.attr_sql.enable_remotejoin = true;
    u_sess->attr.attr_sql.enable_fast_query_shipping = true;
    u_sess->attr.attr_sql.enable_remotegroup = true;
    u_sess->attr.attr_sql.enable_remotesort = true;
    u_sess->attr.attr_sql.enable_remotelimit = true;
    u_sess->attr.attr_sql.gtm_backup_barrier = false;
    u_sess->attr.attr_sql.enable_stream_operator = true;
    u_sess->attr.attr_sql.enable_unshipping_log = false;
    u_sess->attr.attr_sql.enable_stream_concurrent_update = true;
    u_sess->attr.attr_sql.enable_stream_recursive = true;
    u_sess->attr.attr_sql.enable_random_datanode = true;
    u_sess->attr.attr_sql.enable_fstream = false;
    u_sess->attr.attr_common.PersistentConnections = false;
    u_sess->attr.attr_network.PoolerForceReuse = false;
    u_sess->attr.attr_network.comm_client_bind = false;
    u_sess->attr.attr_common.enable_redistribute = false;
    u_sess->attr.attr_sql.enable_cluster_resize = false;
    u_sess->attr.attr_resource.bypass_workload_manager = false;
    u_sess->attr.attr_resource.enable_control_group = true;
    u_sess->attr.attr_resource.enable_cgroup_switch = false;
    u_sess->attr.attr_resource.enable_verify_statements = true;
    u_sess->attr.attr_resource.enable_force_memory_control = false;
    u_sess->attr.attr_resource.enable_dywlm_adjust = true;
    u_sess->attr.attr_resource.enable_hotkeys_collection = false;
    u_sess->attr.attr_resource.enable_reaper_backend = true;
    u_sess->attr.attr_resource.enable_transaction_parctl = true;
    u_sess->attr.attr_sql.agg_redistribute_enhancement = false;
    u_sess->attr.attr_sql.enable_agg_pushdown_for_cooperation_analysis = true;
    u_sess->attr.attr_common.update_process_title = false;
    u_sess->attr.attr_sql.enable_parallel_ddl = true;
    u_sess->attr.attr_sql.enable_nodegroup_debug = false;
    u_sess->attr.attr_sql.enable_dngather = false;
    u_sess->attr.attr_sql.enable_light_proxy = true;
    u_sess->attr.attr_sql.enable_trigger_shipping = true;
    u_sess->attr.attr_common.enable_router = false;

    /* for Int Guc Variables */
    u_sess->attr.attr_common.session_sequence_cache = 10;
    u_sess->attr.attr_resource.max_active_statements = -1;
    u_sess->attr.attr_resource.dynamic_memory_quota = 80;
    u_sess->attr.attr_network.PoolerTimeout = 600;
    u_sess->attr.attr_network.PoolerConnectTimeout = 60;
    u_sess->attr.attr_network.PoolerCancelTimeout = 15;
    u_sess->attr.attr_sql.best_agg_plan = 0;
    u_sess->attr.attr_network.comm_max_datanode = 256;
    u_sess->attr.attr_common.gtm_connect_timeout = 2;
    u_sess->attr.attr_storage.gtm_connect_retries = 30;
    u_sess->attr.attr_common.gtm_rw_timeout = 60;
    u_sess->attr.attr_storage.gtm_conn_check_interval = 10;
    u_sess->attr.attr_common.max_datanode_for_plan = 0;
    u_sess->attr.attr_common.max_datanode_for_plan = 0;
    u_sess->attr.attr_common.transaction_sync_naptime = 30;
    u_sess->attr.attr_common.transaction_sync_timeout = 600;
    u_sess->attr.attr_storage.default_index_kind = DEFAULT_INDEX_KIND_GLOBAL;
    u_sess->attr.attr_sql.application_type = NOT_PERFECT_SHARDING_TYPE;
    /* for Double Guc Variables */
    u_sess->attr.attr_sql.stream_multiple = DEFAULT_STREAM_MULTIPLE;
    u_sess->attr.attr_sql.max_cn_temp_file_size = 5 * 1024 * 1024;
    u_sess->attr.attr_sql.dngather_min_rows = 500.0;


    /* for String Guc Variables */
    u_sess->attr.attr_common.node_group_mode = (char *)"node group";
    u_sess->attr.attr_common.GtmHostInfoStringArray[0] = (char *)"localhost";
    u_sess->attr.attr_common.GtmHostInfoStringArray[1] = (char *)"localhost";
    u_sess->attr.attr_common.GtmHostInfoStringArray[2] = (char *)"";
    u_sess->attr.attr_common.GtmHostInfoStringArray[3] = (char *)"";
    u_sess->attr.attr_common.GtmHostInfoStringArray[4] = (char *)"";
    u_sess->attr.attr_common.GtmHostInfoStringArray[5] = (char *)"";
    u_sess->attr.attr_common.GtmHostInfoStringArray[6] = (char *)"";
    u_sess->attr.attr_common.GtmHostInfoStringArray[7] = (char *)"";
    u_sess->attr.attr_sql.expected_computing_nodegroup = CNG_OPTION_QUERY;
    u_sess->attr.attr_sql.default_storage_nodegroup = INSTALLATION_MODE;

    if (!IsUnderPostmaster) {
        g_instance.attr.attr_network.PoolerStatelessReuse = false;
        g_instance.attr.attr_storage.enable_gtm_free = true;
        g_instance.attr.attr_resource.enable_dynamic_workload = false;
        g_instance.attr.attr_sql.enable_acceleration_cluster_wlm = false;
        g_instance.attr.attr_resource.enable_backend_control = true;
        g_instance.attr.attr_resource.enable_vacuum_control = true;
        g_instance.attr.attr_resource.enable_perm_space = true;
        g_instance.attr.attr_storage.comm_cn_dn_logic_conn = false;
        g_instance.attr.attr_network.MaxPoolSize = 400;
        g_instance.attr.attr_network.PoolerPort = 6667;
        g_instance.attr.attr_common.GtmHostPortArray[0] = 6666;
        g_instance.attr.attr_common.GtmHostPortArray[1] = 6665;
        g_instance.attr.attr_common.GtmHostPortArray[2] = 6666;
        g_instance.attr.attr_common.GtmHostPortArray[3] = 6666;
        g_instance.attr.attr_common.GtmHostPortArray[4] = 6666;
        g_instance.attr.attr_common.GtmHostPortArray[5] = 6666;
        g_instance.attr.attr_common.GtmHostPortArray[6] = 6666;
        g_instance.attr.attr_common.GtmHostPortArray[7] = 6666;
        g_instance.attr.attr_common.MaxDataNodes = 64;
        g_instance.attr.attr_network.MaxCoords = 1024;
        g_instance.attr.attr_storage.gtm_option = GTMOPTION_GTMFREE;
        g_instance.attr.attr_network.comm_max_stream = 1024;
    }

    return;
}
#endif

/*
 * Build the sorted array.	This is split out so that it could be
 * re-executed after startup (eg, we could allow loadable modules to
 * add vars, and then we'd need to re-sort).
 */
void build_guc_get_variables_num(enum guc_attr_strategy stragety,
    int &num_single_vars_out, int &num_both_vars_out, int &num_distribute_vars_out)
{
    int i;

    /* Bind thread local GUC variables to their names */
    if (stragety == GUC_ATTR_COMMON) {
        InitCommonConfigureNames();
    } else if (stragety == GUC_ATTR_SQL) {
        InitSqlConfigureNames();
    } else if (stragety == GUC_ATTR_STORAGE) {
        InitStorageConfigureNames();
    } else if (stragety == GUC_ATTR_SECURITY) {
        InitSecurityConfigureNames();
    } else if (stragety == GUC_ATTR_NETWORK) {
        InitNetworkConfigureNames();
    } else if (stragety == GUC_ATTR_MEMORY) {
        InitMemoryConfigureNames();
    } else if (stragety == GUC_ATTR_RESOURCE) {
        InitResourceConfigureNames();
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION), errmsg("unexpected stragety element: %d", stragety)));
    }

    InitUStoreAttr();

    for (i = 0; u_sess->utils_cxt.ConfigureNamesBool[stragety][i].gen.name; i++) {
        struct config_bool* conf = &u_sess->utils_cxt.ConfigureNamesBool[stragety][i];

        /* Rather than requiring vartype to be filled in by hand, do this: */
        conf->gen.vartype = PGC_BOOL;
        count_variables_num(conf->gen.nodetype, num_single_vars_out, num_both_vars_out, num_distribute_vars_out);
    }

    for (i = 0; u_sess->utils_cxt.ConfigureNamesInt[stragety][i].gen.name; i++) {
        struct config_int* conf = &u_sess->utils_cxt.ConfigureNamesInt[stragety][i];

        conf->gen.vartype = PGC_INT;
        count_variables_num(conf->gen.nodetype, num_single_vars_out, num_both_vars_out, num_distribute_vars_out);
    }

    for (i = 0; u_sess->utils_cxt.ConfigureNamesReal[stragety][i].gen.name; i++) {
        struct config_real* conf = &u_sess->utils_cxt.ConfigureNamesReal[stragety][i];

        conf->gen.vartype = PGC_REAL;
        count_variables_num(conf->gen.nodetype, num_single_vars_out, num_both_vars_out, num_distribute_vars_out);
    }

    for (i = 0; u_sess->utils_cxt.ConfigureNamesString[stragety][i].gen.name; i++) {
        struct config_string* conf = &u_sess->utils_cxt.ConfigureNamesString[stragety][i];

        conf->gen.vartype = PGC_STRING;
        count_variables_num(conf->gen.nodetype, num_single_vars_out, num_both_vars_out, num_distribute_vars_out);
    }
    
    for (i = 0; u_sess->utils_cxt.ConfigureNamesInt64[stragety][i].gen.name; i++) {
        struct config_int64* conf = &u_sess->utils_cxt.ConfigureNamesInt64[stragety][i];

        conf->gen.vartype = PGC_INT64;
        count_variables_num(conf->gen.nodetype, num_single_vars_out, num_both_vars_out, num_distribute_vars_out);
    }

    for (i = 0; u_sess->utils_cxt.ConfigureNamesEnum[stragety][i].gen.name; i++) {
        struct config_enum* conf = &u_sess->utils_cxt.ConfigureNamesEnum[stragety][i];

        conf->gen.vartype = PGC_ENUM;
        count_variables_num(conf->gen.nodetype, num_single_vars_out, num_both_vars_out, num_distribute_vars_out);
    }

    return;
}

static void count_variables_num(GucNodeType nodetype,
    int &num_single_vars_out, int &num_both_vars_out, int &num_distribute_vars_out)
{
    if (nodetype == NODE_ALL) {
        num_both_vars_out++;
    } else if (nodetype == NODE_SINGLENODE) {
        num_single_vars_out++;
    } else if (nodetype == NODE_DISTRIBUTE) {
        num_distribute_vars_out++;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION), errmsg("unexpected node type: %d", nodetype)));
    }

    return;
}

void build_guc_variables_internal_bytype(enum guc_attr_strategy stragety, struct config_generic** guc_vars,
    int &num_vars, GucNodeType specific_type)
{
    int i;

    for (i = 0; u_sess->utils_cxt.ConfigureNamesBool[stragety][i].gen.name; i++) {
        struct config_generic* gen = &u_sess->utils_cxt.ConfigureNamesBool[stragety][i].gen;
        if ((gen->nodetype == NODE_ALL) || (gen->nodetype == specific_type)) {
            guc_vars[num_vars++] = gen;
        }
    }

    for (i = 0; u_sess->utils_cxt.ConfigureNamesInt[stragety][i].gen.name; i++) {
        struct config_generic* gen = &u_sess->utils_cxt.ConfigureNamesInt[stragety][i].gen;
        if ((gen->nodetype == NODE_ALL) || (gen->nodetype == specific_type)) {
            guc_vars[num_vars++] = gen;
        }
    }

    for (i = 0; u_sess->utils_cxt.ConfigureNamesReal[stragety][i].gen.name; i++) {
        struct config_generic* gen = &u_sess->utils_cxt.ConfigureNamesReal[stragety][i].gen;
        if ((gen->nodetype == NODE_ALL) || (gen->nodetype == specific_type)) {
            guc_vars[num_vars++] = gen;
        }
    }

    for (i = 0; u_sess->utils_cxt.ConfigureNamesString[stragety][i].gen.name; i++) {
        struct config_generic* gen = &u_sess->utils_cxt.ConfigureNamesString[stragety][i].gen;
        if ((gen->nodetype == NODE_ALL) || (gen->nodetype == specific_type)) {
            guc_vars[num_vars++] = gen;
        }
    }

    for (i = 0; u_sess->utils_cxt.ConfigureNamesInt64[stragety][i].gen.name; i++) {
        struct config_generic* gen = &u_sess->utils_cxt.ConfigureNamesInt64[stragety][i].gen;
        if ((gen->nodetype == NODE_ALL) || (gen->nodetype == specific_type)) {
            guc_vars[num_vars++] = gen;
        }
    }

    for (i = 0; u_sess->utils_cxt.ConfigureNamesEnum[stragety][i].gen.name; i++) {
        struct config_generic* gen = &u_sess->utils_cxt.ConfigureNamesEnum[stragety][i].gen;
        if ((gen->nodetype == NODE_ALL) || (gen->nodetype == specific_type)) {
            guc_vars[num_vars++] = gen;
        }
    }

    return;
}

void build_guc_variables_internal(struct config_generic** guc_vars)
{
    int stragety;
    bool flag = false;
    int num_vars = 0;

#ifdef ENABLE_MULTIPLE_NODES
    flag = true;
#endif
    for (stragety = 0; stragety < MAX_GUC_ATTR; stragety++) {
        GucNodeType nodetype;
        if (flag) {
            nodetype = NODE_DISTRIBUTE;
        } else {
            nodetype = NODE_SINGLENODE;
        }
        build_guc_variables_internal_bytype(guc_attr_strategy(stragety), guc_vars, num_vars, nodetype);
    }

    return;
}

void build_guc_variables(void)
{
    struct config_generic** guc_vars;
    int single_vars_num = 0;
    int multi_vars_num = 0;
    int size_vars = 0;
    int num_vars = 0;
    int both_vars_num = 0;

    build_guc_get_variables_num(GUC_ATTR_SQL, single_vars_num, both_vars_num, multi_vars_num);
    build_guc_get_variables_num(GUC_ATTR_STORAGE, single_vars_num, both_vars_num, multi_vars_num);
    build_guc_get_variables_num(GUC_ATTR_SECURITY, single_vars_num, both_vars_num, multi_vars_num);
    build_guc_get_variables_num(GUC_ATTR_NETWORK, single_vars_num, both_vars_num, multi_vars_num);
    build_guc_get_variables_num(GUC_ATTR_MEMORY, single_vars_num, both_vars_num, multi_vars_num);
    build_guc_get_variables_num(GUC_ATTR_RESOURCE, single_vars_num, both_vars_num, multi_vars_num);
    build_guc_get_variables_num(GUC_ATTR_COMMON, single_vars_num, both_vars_num, multi_vars_num);

#ifdef ENABLE_MULTIPLE_NODES
    num_vars = both_vars_num + multi_vars_num;
#else
    num_vars = single_vars_num + both_vars_num;
#endif
    /*
     * Create table with 20% slack
     */
    size_vars = num_vars + num_vars / 4;
    guc_vars = (struct config_generic**)guc_malloc(FATAL, size_vars * sizeof(struct config_generic*));

    build_guc_variables_internal(guc_vars);

    if (u_sess->guc_variables)
        pfree(u_sess->guc_variables);

    u_sess->guc_variables = guc_vars;
    u_sess->num_guc_variables = num_vars;
    u_sess->utils_cxt.size_guc_variables = size_vars;
    qsort((void*)u_sess->guc_variables, u_sess->num_guc_variables, sizeof(struct config_generic*), guc_var_compare);
    return;
}

/*
 * Add a new GUC variable to the list of known variables. The
 * list is expanded if needed.
 */
static bool add_guc_variable(struct config_generic* var, int elevel)
{
    if (u_sess->num_guc_variables + 1 >= u_sess->utils_cxt.size_guc_variables) {
        /*
         * Increase the vector by 25%
         */
        int size_vars = u_sess->utils_cxt.size_guc_variables + u_sess->utils_cxt.size_guc_variables / 4;
        struct config_generic** guc_vars;

        if (size_vars == 0) {
            size_vars = 100;
            guc_vars = (struct config_generic**)guc_malloc(elevel, size_vars * sizeof(struct config_generic*));
        } else {
            guc_vars = (struct config_generic**)guc_realloc(
                elevel, u_sess->guc_variables, size_vars * sizeof(struct config_generic*));
        }

        if (guc_vars == NULL)
            return false; /* out of memory */

        u_sess->guc_variables = guc_vars;
        u_sess->utils_cxt.size_guc_variables = size_vars;
    }

    u_sess->guc_variables[u_sess->num_guc_variables++] = var;
    qsort((void*)u_sess->guc_variables, u_sess->num_guc_variables, sizeof(struct config_generic*), guc_var_compare);
    return true;
}

/*
 * Create and add a placeholder variable for a custom variable name.
 */
static struct config_generic* add_placeholder_variable(const char* name, int elevel)
{
    size_t sz = sizeof(struct config_string) + sizeof(char*);
    struct config_string* var = NULL;
    struct config_generic* gen = NULL;

    var = (struct config_string*)guc_malloc(elevel, sz);

    if (var == NULL)
        return NULL;

    errno_t rc = memset_s(var, sz, 0, sz);
    securec_check(rc, "\0", "\0");

    gen = &var->gen;

    gen->name = guc_strdup(elevel, name);

    if (gen->name == NULL) {
        pfree(var);
        return NULL;
    }

    gen->context = PGC_USERSET;
    gen->group = CUSTOM_OPTIONS;
    gen->short_desc = "GUC placeholder variable";
    gen->flags = GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_CUSTOM_PLACEHOLDER;
    gen->vartype = PGC_STRING;

    /*
     * The char* is allocated at the end of the struct since we have no
     * 'static' place to point to.	Note that the current value, as well as
     * the boot and reset values, start out NULL.
     */
    var->variable = (char**)(var + 1);

    if (!add_guc_variable((struct config_generic*)var, elevel)) {
        pfree((void*)gen->name);
        pfree(var);
        return NULL;
    }

    return gen;
}

/*
 * Look up option NAME.  If it exists, return a pointer to its record,
 * else return NULL.  If create_placeholders is TRUE, we'll create a
 * placeholder record for a valid-looking custom variable name.
 */
static struct config_generic* find_option(const char* name, bool create_placeholders, int elevel)
{
    const char** key = &name;
    struct config_generic** res;
    int i;

    Assert(name);

    /*
     * By equating const char ** with struct config_generic *, we are assuming
     * the name field is first in config_generic.
     */
    res = (struct config_generic**)bsearch((void*)&key,
        (void*)u_sess->guc_variables,
        u_sess->num_guc_variables,
        sizeof(struct config_generic*),
        guc_var_compare);

    if (res != NULL)
        return *res;

    /*
     * See if the name is an obsolete name for a variable.	We assume that the
     * set of supported old names is short enough that a brute-force search is
     * the best way.
     */
    for (i = 0; map_old_guc_names[i] != NULL; i += 2) {
        if (guc_name_compare(name, map_old_guc_names[i]) == 0)
            return find_option(map_old_guc_names[i + 1], false, elevel);
    }

    if (create_placeholders) {
        /*
         * Check if the name is qualified, and if so, add a placeholder.
         */
        if (strchr(name, GUC_QUALIFIER_SEPARATOR) != NULL)
            return add_placeholder_variable(name, elevel);
    }

    /* Unknown name */
    return NULL;
}

/*
 * comparator for qsorting and bsearching guc_variables array
 */
static int guc_var_compare(const void* a, const void* b)
{
    const struct config_generic* confa = *(struct config_generic* const*)a;
    const struct config_generic* confb = *(struct config_generic* const*)b;

    return guc_name_compare(confa->name, confb->name);
}

/*
 * the bare comparison function for GUC names
 */
static int guc_name_compare(const char* namea, const char* nameb)
{
    /*
     * The temptation to use strcasecmp() here must be resisted, because the
     * array ordering has to remain stable across setlocale() calls. So, build
     * our own with a simple ASCII-only downcasing.
     */
    while (*namea && *nameb) {
        char cha = *namea++;
        char chb = *nameb++;

        if (cha >= 'A' && cha <= 'Z')
            cha += 'a' - 'A';

        if (chb >= 'A' && chb <= 'Z')
            chb += 'a' - 'A';

        if (cha != chb)
            return cha - chb;
    }

    if (*namea)
        return 1; /* a is longer */

    if (*nameb)
        return -1; /* b is longer */

    return 0;
}

/*
 * Initiaize Postmaster level GUC options during postmaster proc.
 *
 * Note that some guc can't get right value because of some global var not init,
 * so need this function to init postmaster level guc.
 */
void InitializePostmasterGUC()
{
#ifdef ENABLE_MULTIPLE_NODES
    (void)check_enable_gtm_free(&g_instance.attr.attr_storage.enable_gtm_free, NULL, PGC_S_DEFAULT);
#else
    g_instance.attr.attr_storage.enable_gtm_free = true;
#endif
    g_instance.attr.attr_network.PoolerPort = g_instance.attr.attr_network.PostPortNumber + 1;
}

/*
 * Initialize GUC options during program startup.
 *
 * Note that we cannot read the config file yet, since we have not yet
 * processed command-line switches.
 */
void InitializeGUCOptions(void)
{
    int i;

    /*
     * Before log_line_prefix could possibly receive a nonempty setting, make
     * sure that timezone processing is minimally alive (see elog.c).
     */
    pg_timezone_initialize();

    /*
     * Build sorted array of all GUC variables.
     */
    build_guc_variables();

    /*
     * Load all variables with their compiled-in defaults, and initialize
     * status fields as needed.
     */
    GucContext oldContext = currentGucContext;
    currentGucContext = PGC_POSTMASTER;
    for (i = 0; i < u_sess->num_guc_variables; i++) {
        InitializeOneGUCOption(u_sess->guc_variables[i]);
    }
    currentGucContext = oldContext;

#ifndef ENABLE_MULTIPLE_NODES
    InitSingleNodeUnsupportGuc();
#endif

    u_sess->utils_cxt.guc_dirty = false;

    u_sess->utils_cxt.reporting_enabled = false;

    /*
     * Prevent any attempt to override the transaction modes from
     * non-interactive sources.
     */
    SetConfigOption("transaction_isolation", "default", PGC_POSTMASTER, PGC_S_OVERRIDE);
    SetConfigOption("transaction_read_only", "no", PGC_POSTMASTER, PGC_S_OVERRIDE);
    SetConfigOption("transaction_deferrable", "no", PGC_POSTMASTER, PGC_S_OVERRIDE);

    /*
     * For historical reasons, some GUC parameters can receive defaults from
     * environment variables.  Process those settings.
     */
    InitializeGUCOptionsFromEnvironment();
}

/*
 * Assign any GUC values that can come from the server's environment.
 *
 * This is called from InitializeGUCOptions, and also from ProcessConfigFile
 * to deal with the possibility that a setting has been removed from
 * postgresql.conf and should now get a value from the environment.
 * (The latter is a kludge that should probably go away someday; if so,
 * fold this back into InitializeGUCOptions.)
 */
static void InitializeGUCOptionsFromEnvironment(void)
{
    char* env = NULL;
    long stack_rlimit;

    env = gs_getenv_r("PGPORT");

    if (env != NULL) {
        check_backend_env(env);
        SetConfigOption("port", env, PGC_POSTMASTER, PGC_S_ENV_VAR);
    }

    env = gs_getenv_r("PGDATESTYLE");

    if (env != NULL) {
        check_backend_env(env);
        SetConfigOption("datestyle", env, PGC_POSTMASTER, PGC_S_ENV_VAR);
    }

    env = gs_getenv_r("PGCLIENTENCODING");

    if (env != NULL) {
        check_backend_env(env);
        SetConfigOption("client_encoding", env, PGC_POSTMASTER, PGC_S_ENV_VAR);
    }

    /*
     * rlimit isn't exactly an "environment variable", but it behaves about
     * the same.  If we can identify the platform stack depth rlimit, increase
     * default stack depth setting up to whatever is safe (but at most 2MB).
     */
    stack_rlimit = get_stack_depth_rlimit();

    if (stack_rlimit > 0) {
        long new_limit = (stack_rlimit - STACK_DEPTH_SLOP) / 1024L;

        if (new_limit > 100) {
            char limbuf[16];

            new_limit = Min(new_limit, 2048);
            int rcs = snprintf_s(limbuf, sizeof(limbuf), sizeof(limbuf) - 1, "%ld", new_limit);
            securec_check_ss(rcs, "\0", "\0");
            SetConfigOption("max_stack_depth", limbuf, PGC_POSTMASTER, PGC_S_ENV_VAR);
        }
    }
}

/*
 * Initialize one GUC option variable to its compiled-in default.
 *
 * Note: the reason for calling check_hooks is not that we think the boot_val
 * might fail, but that the hooks might wish to compute an "extra" struct.
 */

static void InitializeOneGUCOption(struct config_generic* gconf)
{
    gconf->status = 0;
    gconf->source = PGC_S_DEFAULT;
    gconf->reset_source = PGC_S_DEFAULT;
    gconf->scontext = PGC_INTERNAL;
    gconf->reset_scontext = PGC_INTERNAL;
    gconf->stack = NULL;
    gconf->extra = NULL;
    gconf->sourcefile = NULL;
    gconf->sourceline = 0;

    switch (gconf->vartype) {
        case PGC_BOOL: {
            struct config_bool* conf = (struct config_bool*)gconf;
            bool newval = conf->boot_val;
            void* extra = NULL;

            if (!call_bool_check_hook(conf, &newval, &extra, PGC_S_DEFAULT, LOG))
                ereport(FATAL, (errmsg("failed to initialize %s to %d", conf->gen.name, (int)newval)));

            if (conf->assign_hook)
                (*conf->assign_hook)(newval, extra);

            if (!IsUnderPostmaster || gconf->context != PGC_POSTMASTER) {
                *conf->variable = conf->reset_val = newval;
            }
            conf->gen.extra = conf->reset_extra = extra;
            break;
        }

        case PGC_INT: {
            struct config_int* conf = (struct config_int*)gconf;
            int newval = conf->boot_val;
            void* extra = NULL;

            Assert(newval >= conf->min);
            Assert(newval <= conf->max);

            if (!call_int_check_hook(conf, &newval, &extra, PGC_S_DEFAULT, LOG))
                ereport(FATAL, (errmsg("failed to initialize %s to %d", conf->gen.name, newval)));

            if (conf->assign_hook)
                (*conf->assign_hook)(newval, extra);

            if (!IsUnderPostmaster || gconf->context != PGC_POSTMASTER) {
                *conf->variable = conf->reset_val = newval;
            }
            conf->gen.extra = conf->reset_extra = extra;
            break;
        }

        case PGC_INT64: {
            struct config_int64* conf = (struct config_int64*)gconf;
            int64 newval = conf->boot_val;
            void* extra = NULL;

            Assert(newval >= conf->min);
            Assert(newval <= conf->max);
            if (!call_int64_check_hook(conf, &newval, &extra, PGC_S_DEFAULT, LOG))
                ereport(FATAL, (errmsg("failed to initialize %s to " INT64_FORMAT, conf->gen.name, newval)));
            if (conf->assign_hook)
                (*conf->assign_hook)(newval, extra);

            if (!IsUnderPostmaster || gconf->context != PGC_POSTMASTER) {
                *conf->variable = conf->reset_val = newval;
            }
            conf->gen.extra = conf->reset_extra = extra;
            break;
        }

        case PGC_REAL: {
            struct config_real* conf = (struct config_real*)gconf;
            double newval = conf->boot_val;
            void* extra = NULL;

            Assert(newval >= conf->min);
            Assert(newval <= conf->max);

            if (!call_real_check_hook(conf, &newval, &extra, PGC_S_DEFAULT, LOG))
                ereport(FATAL, (errmsg("failed to initialize %s to %g", conf->gen.name, newval)));

            if (conf->assign_hook)
                (*conf->assign_hook)(newval, extra);

            if (!IsUnderPostmaster || gconf->context != PGC_POSTMASTER) {
                *conf->variable = conf->reset_val = newval;
            }
            conf->gen.extra = conf->reset_extra = extra;
            break;
        }

        case PGC_STRING: {
            struct config_string* conf = (struct config_string*)gconf;
            char* newval = NULL;
            void* extra = NULL;

            /* non-NULL boot_val must always get strdup'd */
            if (conf->boot_val != NULL)
                newval = guc_strdup(FATAL, conf->boot_val);
            else
                newval = NULL;

            if (!call_string_check_hook(conf, &newval, &extra, PGC_S_DEFAULT, LOG)) {
                pfree_ext(newval);
#ifdef ENABLE_UT
                return;
#else
                ereport(FATAL, (errmsg("failed to initialize %s to \"%s\"", conf->gen.name, newval ? newval : "")));
#endif
            }
            if (conf->assign_hook)
                (*conf->assign_hook)(newval, extra);

            if (!IsUnderPostmaster || gconf->context != PGC_POSTMASTER) {
                *conf->variable = conf->reset_val = newval;
            }
            conf->gen.extra = conf->reset_extra = extra;
            break;
        }

        case PGC_ENUM: {
            struct config_enum* conf = (struct config_enum*)gconf;
            int newval = conf->boot_val;
            void* extra = NULL;

            if (!call_enum_check_hook(conf, &newval, &extra, PGC_S_DEFAULT, LOG))
                ereport(FATAL, (errmsg("failed to initialize %s to %d", conf->gen.name, newval)));

            if (conf->assign_hook)
                (*conf->assign_hook)(newval, extra);

            if (!IsUnderPostmaster || gconf->context != PGC_POSTMASTER) {
                *conf->variable = conf->reset_val = newval;
            }
            conf->gen.extra = conf->reset_extra = extra;
            break;
        }
        default:
            break;
    }
}

/*initialize guc variables which need to be sended to stream threads*/
void init_sync_guc_variables()
{
    int i;
    struct config_generic** res;
    int sync_guc_number = lengthof(sync_guc_variable_namelist);

    u_sess->utils_cxt.sync_guc_variables =
        (struct config_generic**)palloc(sizeof(struct config_generic*) * sync_guc_number);

    for (i = 0; i < sync_guc_number; i++) {
        const char** key = &sync_guc_variable_namelist[i];
        res = (struct config_generic**)bsearch((void*)&key,
            (void*)u_sess->guc_variables,
            u_sess->num_guc_variables,
            sizeof(struct config_generic*),
            guc_var_compare);
        Assert(res);
        u_sess->utils_cxt.sync_guc_variables[i] = *res;
    }
}

/*repair the guc variables in stream thread*/
void repair_guc_variables()
{
    int i;
    struct config_generic** res;

    int sync_guc_number = lengthof(sync_guc_variable_namelist);

    for (i = 0; i < sync_guc_number; i++) {
        const char** key = &sync_guc_variable_namelist[i];
        res = (struct config_generic**)bsearch((void*)&key,
            (void*)u_sess->guc_variables,
            u_sess->num_guc_variables,
            sizeof(struct config_generic*),
            guc_var_compare);
        Assert(res);
        switch ((*res)->vartype) {
            case PGC_BOOL: {
                struct config_bool* destination = (struct config_bool*)(*res);
                struct config_bool* source = (struct config_bool*)u_sess->utils_cxt.sync_guc_variables[i];
                bool newval = false;
                void* newextra = NULL;

                if (*destination->variable == *source->variable)
                    break;

                newval = *source->variable;
                if (destination->check_hook) {
                    if (!call_bool_check_hook(destination, &newval, &newextra, destination->gen.source, LOG)) {
                        ereport(LOG,
                            (errmsg("Failed to initialize %s to %s for stream thread",
                                destination->gen.name,
                                newval ? "true" : "false")));
                        break;
                    }
                }

                if (destination->assign_hook)
                    (*destination->assign_hook)(newval, newextra);

                *destination->variable = *source->variable;
                set_extra_field(&destination->gen, &destination->gen.extra, newextra);
                break;
            }
            case PGC_INT: {
                struct config_int* destination = (struct config_int*)(*res);
                struct config_int* source = (struct config_int*)u_sess->utils_cxt.sync_guc_variables[i];
                int newval;
                void* newextra = NULL;

                if (*destination->variable == *source->variable)
                    break;

                newval = *source->variable;

                if (destination->check_hook) {
                    if (!call_int_check_hook(destination, &newval, &newextra, destination->gen.source, LOG)) {
                        ereport(LOG,
                            (errmsg("Failed to initialize %s to %d for stream thread", destination->gen.name, newval)));
                        break;
                    }
                }

                if (destination->assign_hook)
                    (*destination->assign_hook)(newval, newextra);

                *destination->variable = *source->variable;
                set_extra_field(&destination->gen, &destination->gen.extra, newextra);
                break;
            }
            case PGC_INT64: {
                struct config_int64* destination = (struct config_int64*)(*res);
                struct config_int64* source = (struct config_int64*)u_sess->utils_cxt.sync_guc_variables[i];
                int64 newval;
                void* newextra = NULL;

                if (*destination->variable == *source->variable)
                    break;

                newval = *source->variable;

                if (destination->check_hook) {
                    if (!call_int64_check_hook(destination, &newval, &newextra, destination->gen.source, LOG)) {
                        ereport(LOG,
                            (errmsg("Failed to initialize %s to " INT64_FORMAT " for stream thread",
                                destination->gen.name,
                                newval)));
                        break;
                    }
                }

                if (destination->assign_hook)
                    (*destination->assign_hook)(newval, newextra);

                *destination->variable = *source->variable;
                set_extra_field(&destination->gen, &destination->gen.extra, newextra);
                break;
            }
            case PGC_REAL: {
                struct config_real* destination = (struct config_real*)(*res);
                struct config_real* source = (struct config_real*)u_sess->utils_cxt.sync_guc_variables[i];
                double newval;
                void* newextra = NULL;

                if (*destination->variable == *source->variable)
                    break;

                newval = *source->variable;

                if (destination->check_hook) {
                    if (!call_real_check_hook(destination, &newval, &newextra, destination->gen.source, LOG)) {
                        ereport(LOG,
                            (errmsg("Failed to initialize %s to %g for stream thread", destination->gen.name, newval)));
                        break;
                    }
                }

                if (destination->assign_hook)
                    (*destination->assign_hook)(newval, newextra);

                *destination->variable = *source->variable;
                set_extra_field(&destination->gen, &destination->gen.extra, newextra);
                break;
            }
            case PGC_STRING: {
                struct config_string* destination = (struct config_string*)(*res);
                struct config_string* source = (struct config_string*)u_sess->utils_cxt.sync_guc_variables[i];
                char* newval = NULL;
                void* newextra = NULL;

                if (strlen(*destination->variable) == strlen(*source->variable) &&
                    0 == strncmp(*destination->variable, *source->variable, strlen(*destination->variable)))
                    break;

                newval = guc_strdup(FATAL, *source->variable);

                if (destination->check_hook) {
                    if (!call_string_check_hook(destination, &newval, &newextra, destination->gen.source, LOG)) {
                        ereport(LOG,
                            (errmsg("Failed to initialize %s to \"%s\" for stream thread",
                                destination->gen.name,
                                newval ? newval : "")));
                        pfree_ext(newval);
                        break;
                    }
                }

                if (destination->assign_hook)
                    (*destination->assign_hook)(newval, newextra);

                set_string_field(destination, destination->variable, newval);
                set_extra_field(&destination->gen, &destination->gen.extra, newextra);
                break;
            }
            case PGC_ENUM: {
                struct config_enum* destination = (struct config_enum*)(*res);
                struct config_enum* source = (struct config_enum*)u_sess->utils_cxt.sync_guc_variables[i];
                int newval;
                void* newextra = NULL;

                if (*destination->variable == *source->variable)
                    break;

                newval = *source->variable;

                if (destination->check_hook) {
                    if (!call_enum_check_hook(destination, &newval, &newextra, destination->gen.source, LOG)) {
                        ereport(LOG,
                            (errmsg("Failed to initialize %s to %d for stream thread", destination->gen.name, newval)));
                        break;
                    }
                }

                if (destination->assign_hook)
                    (*destination->assign_hook)(newval, newextra);

                *destination->variable = *source->variable;
                set_extra_field(&destination->gen, &destination->gen.extra, newextra);
                break;
            }
            default:
                break;
        }
    }

    SetThreadLocalGUC(u_sess);
}
/*
 * Select the configuration files and data directory to be used, and
 * do the initial read of postgresql.conf.
 *
 * This is called after processing command-line switches.
 *		userDoption is the -D switch value if any (NULL if unspecified).
 *		progname is just for use in error messages.
 *
 * Returns true on success; on failure, prints a suitable error message
 * to stderr and returns false.
 */
bool SelectConfigFiles(const char* userDoption, const char* progname)
{
    char* configdir = NULL;
    char* fname = NULL;
    struct stat stat_buf;
#ifdef ENABLE_MOT
    char* motfname = NULL;
#endif
    char* TempConfigPath = NULL;
    char TempConfigFile[MAXPGPATH] = {0};
    char TempConfigFileName[MAXPGPATH] = {0};
    int rc;

    /* configdir is -D option, or $PGDATA if no -D */
    if (userDoption != NULL)
        configdir = make_absolute_path(userDoption);
    else {
        char* pgdata = gs_getenv_r("PGDATA");
        if (backend_env_valid(pgdata, "PGDATA") == false) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Incorrect backend environment variable $PGDATA"),
                errdetail("Please refer to the backend instance log for the detail")));
        }
        configdir = make_absolute_path(pgdata);
    }
    /*
     * Find the configuration file: if config_file was specified on the
     * command line, use it, else use configdir/postgresql.conf.  In any case
     * ensure the result is an absolute path, so that it will be interpreted
     * the same way by future backends.
     */
    if (g_instance.attr.attr_common.ConfigFileName)
        fname = make_absolute_path(g_instance.attr.attr_common.ConfigFileName);
    else if (configdir != NULL) {
        fname = (char*)guc_malloc(FATAL, strlen(configdir) + strlen(CONFIG_FILENAME) + 2);
        rc = snprintf_s(fname,
            strlen(configdir) + strlen(CONFIG_FILENAME) + 2,
            strlen(configdir) + strlen(CONFIG_FILENAME) + 1,
            "%s/%s",
            configdir,
            CONFIG_FILENAME);
        securec_check_ss(rc, configdir, "\0");
    } else {
        write_stderr("%s does not know where to find the server configuration file.\n"
                     "You must specify the --config-file or -D invocation "
                     "option or set the PGDATA environment variable.\n",
            progname);
        return false;
    }

    /*
     * Set the g_instance.attr.attr_common.ConfigFileName GUC variable to its final value, ensuring that
     * it can't be overridden later.
     */
    SetConfigOption("config_file", fname, PGC_POSTMASTER, PGC_S_OVERRIDE);
    pfree(fname);

    /*
     * Now read the config file for the first time.
     */
    if (g_instance.attr.attr_common.ConfigFileName != NULL &&
        stat(g_instance.attr.attr_common.ConfigFileName, &stat_buf) != 0) {
        int str_len = strlen(g_instance.attr.attr_common.ConfigFileName);
        rc = strncpy_s(
            TempConfigFile, MAXPGPATH, g_instance.attr.attr_common.ConfigFileName, Min(str_len, MAXPGPATH - 1));
        securec_check(rc, configdir, "\0");
        if (str_len > MAXPGPATH - 1) {
            ereport(WARNING, (errmsg("strncpy_s failed")));
        }
        TempConfigPath = TempConfigFile;
        get_parent_directory(TempConfigPath);
        rc = snprintf_s(TempConfigFileName, MAXPGPATH, MAXPGPATH - 1, "%s/%s", TempConfigPath, CONFIG_BAK_FILENAME);
        securec_check_ss(rc, configdir, "\0");
        if (lstat(TempConfigFileName, &stat_buf) != 0) {
            write_stderr("%s cannot access the server configuration file \"%s\": %s\n",
                progname,
                g_instance.attr.attr_common.ConfigFileName,
                gs_strerror(errno));
            pfree(configdir);
            return false;
        } else {
            char **optLines = read_guc_file(TempConfigFileName);
            if (optLines == NULL) {
                ereport(LOG, (errmsg("the config file has no data,please check it.")));
                pfree(configdir);
                return false;
            }
            ErrCode ret = write_guc_file(g_instance.attr.attr_common.ConfigFileName, optLines);
            release_opt_lines(optLines);
            optLines = NULL;
            if (ret == CODE_OK) {
                ereport(LOG,
                    (errmsg("the config file %s recover success.", g_instance.attr.attr_common.ConfigFileName)));
            } else {
                write_stderr("%s: recover file %s to %s failed \n", progname, TempConfigFileName,
                    g_instance.attr.attr_common.ConfigFileName);
                pfree(configdir);
                return false;
            }
        }
    }

    ProcessConfigFile(PGC_POSTMASTER);
    most_available_sync = (volatile bool)u_sess->attr.attr_storage.guc_most_available_sync;

    /*
     * If the data_directory GUC variable has been set, use that as t_thrd.proc_cxt.DataDir;
     * otherwise use configdir if set; else punt.
     *
     * Note: SetDataDir will copy and absolute-ize its argument, so we don't
     * have to.
     */
    if (g_instance.attr.attr_common.data_directory)
        SetDataDir(g_instance.attr.attr_common.data_directory);
    else if (configdir != NULL)
        SetDataDir(configdir);
    else {
        write_stderr("%s does not know where to find the database system data.\n"
                     "This can be specified as \"data_directory\" in \"%s\", "
                     "or by the -D invocation option, or by the "
                     "PGDATA environment variable.\n",
            progname,
            g_instance.attr.attr_common.ConfigFileName);
        return false;
    }

    /*
     * Reflect the final t_thrd.proc_cxt.DataDir value back into the data_directory GUC var.
     * (If you are wondering why we don't just make them a single variable,
     * it's because the EXEC_BACKEND case needs t_thrd.proc_cxt.DataDir to be transmitted to
     * child backends specially.  XXX is that still true?  Given that we now
     * chdir to t_thrd.proc_cxt.DataDir, EXEC_BACKEND can read the config file without knowing
     * t_thrd.proc_cxt.DataDir in advance.)
     */
    SetConfigOption("data_directory", t_thrd.proc_cxt.DataDir, PGC_POSTMASTER, PGC_S_OVERRIDE);

#ifdef ENABLE_MOT
    if (g_instance.attr.attr_common.MOTConfigFileName)
        motfname = make_absolute_path(g_instance.attr.attr_common.MOTConfigFileName);
    else if (configdir) {
        motfname = (char*)guc_malloc(FATAL, strlen(configdir) + strlen(MOT_CONFIG_FILENAME) + 2);
        rc = snprintf_s(motfname,
            strlen(configdir) + strlen(MOT_CONFIG_FILENAME) + 2,
            strlen(configdir) + strlen(MOT_CONFIG_FILENAME) + 1,
            "%s/%s",
            configdir,
            MOT_CONFIG_FILENAME);
        securec_check_ss(rc, configdir, "\0");
    }

    SetConfigOption("mot_config_file", motfname, PGC_POSTMASTER, PGC_S_OVERRIDE);
    selfpfree(motfname);
#endif

    /*
     * If timezone_abbreviations wasn't set in the configuration file, install
     * the default value.  We do it this way because we can't safely install a
     * "real" value until my_exec_path is set, which may not have happened
     * when InitializeGUCOptions runs, so the bootstrap default value cannot
     * be the real desired default.
     */
    pg_timezone_abbrev_initialize();

    /*
     * Figure out where pg_hba.conf is, and make sure the path is absolute.
     */
    if (g_instance.attr.attr_common.HbaFileName)
        fname = make_absolute_path(g_instance.attr.attr_common.HbaFileName);
    else if (configdir != NULL) {
        fname = (char*)guc_malloc(FATAL, strlen(configdir) + strlen(HBA_FILENAME) + 2);
        rc = snprintf_s(fname,
            strlen(configdir) + strlen(HBA_FILENAME) + 2,
            strlen(configdir) + strlen(HBA_FILENAME) + 1,
            "%s/%s",
            configdir,
            HBA_FILENAME);
        securec_check_ss(rc, configdir, "\0");
    } else {
        write_stderr("%s does not know where to find the \"hba\" configuration file.\n"
                     "This can be specified as \"hba_file\" in \"%s\", "
                     "or by the -D invocation option, or by the "
                     "PGDATA environment variable.\n",
            progname,
            g_instance.attr.attr_common.ConfigFileName);
        return false;
    }

    SetConfigOption("hba_file", fname, PGC_POSTMASTER, PGC_S_OVERRIDE);
    pfree(fname);

    /*
     * Likewise for pg_ident.conf.
     */
    if (g_instance.attr.attr_common.IdentFileName)
        fname = make_absolute_path(g_instance.attr.attr_common.IdentFileName);
    else if (configdir != NULL) {
        fname = (char*)guc_malloc(FATAL, strlen(configdir) + strlen(IDENT_FILENAME) + 2);
        rc = snprintf_s(fname,
            strlen(configdir) + strlen(IDENT_FILENAME) + 2,
            strlen(configdir) + strlen(IDENT_FILENAME) + 1,
            "%s/%s",
            configdir,
            IDENT_FILENAME);
        securec_check_ss(rc, configdir, "\0");
    } else {
        write_stderr("%s does not know where to find the \"ident\" configuration file.\n"
                     "This can be specified as \"ident_file\" in \"%s\", "
                     "or by the -D invocation option, or by the "
                     "PGDATA environment variable.\n",
            progname,
            g_instance.attr.attr_common.ConfigFileName);
        return false;
    }

    SetConfigOption("ident_file", fname, PGC_POSTMASTER, PGC_S_OVERRIDE);
    pfree(fname);

    pfree(configdir);

    // shared_buffers is 32MB for dummyStandby
    //
    if (dummyStandbyMode)
        g_instance.attr.attr_storage.NBuffers = (int)(32 * 1024 * 1024 / BLCKSZ);

    return true;
}

/*
 * Reset all options to their saved default values (implements RESET ALL)
 */
bool check_options_need_reset(struct config_generic* gconf)
{
    /* Don't reset non-SET-able values */
    if (gconf->context != PGC_SUSET && gconf->context != PGC_USERSET)
        return false;

    /* Don't reset if special exclusion from RESET ALL */
    if (gconf->flags & GUC_NO_RESET_ALL)
        return false;
    
    /* No need to reset if wasn't SET */
    if (gconf->source <= PGC_S_OVERRIDE)
        return false;

    return true;
}
void ResetAllOptions(void)
{
    int i;

    for (i = 0; i < u_sess->num_guc_variables; i++) {
        struct config_generic* gconf = u_sess->guc_variables[i];

        if (!check_options_need_reset(gconf)) {
            continue;
        }
        /* Save old value to support transaction abort */
        push_old_value(gconf, GUC_ACTION_SET);

        switch (gconf->vartype) {
            case PGC_BOOL: {
                struct config_bool* conf = (struct config_bool*)gconf;

                if (conf->assign_hook)
                    (*conf->assign_hook)(conf->reset_val, conf->reset_extra);

                *conf->variable = conf->reset_val;
                set_extra_field(&conf->gen, &conf->gen.extra, conf->reset_extra);
                break;
            }

            case PGC_INT: {
                struct config_int* conf = (struct config_int*)gconf;

                if (conf->assign_hook)
                    (*conf->assign_hook)(conf->reset_val, conf->reset_extra);

                *conf->variable = conf->reset_val;
                set_extra_field(&conf->gen, &conf->gen.extra, conf->reset_extra);
                break;
            }

            case PGC_INT64: {
                struct config_int64* conf = (struct config_int64*)gconf;

                if (conf->assign_hook)
                    (*conf->assign_hook)(conf->reset_val, conf->reset_extra);
                *conf->variable = conf->reset_val;
                set_extra_field(&conf->gen, &conf->gen.extra, conf->reset_extra);
                break;
            }

            case PGC_REAL: {
                struct config_real* conf = (struct config_real*)gconf;

                if (conf->assign_hook)
                    (*conf->assign_hook)(conf->reset_val, conf->reset_extra);

                *conf->variable = conf->reset_val;
                set_extra_field(&conf->gen, &conf->gen.extra, conf->reset_extra);
                break;
            }

            case PGC_STRING: {
                struct config_string* conf = (struct config_string*)gconf;

                if (conf->assign_hook)
                    (*conf->assign_hook)(conf->reset_val, conf->reset_extra);

                set_string_field(conf, conf->variable, conf->reset_val);
                set_extra_field(&conf->gen, &conf->gen.extra, conf->reset_extra);
                break;
            }

            case PGC_ENUM: {
                struct config_enum* conf = (struct config_enum*)gconf;

                if (conf->assign_hook)
                    (*conf->assign_hook)(conf->reset_val, conf->reset_extra);

                *conf->variable = conf->reset_val;
                set_extra_field(&conf->gen, &conf->gen.extra, conf->reset_extra);
                break;
            }
            default:
                break;
        }

        gconf->source = gconf->reset_source;
        gconf->scontext = gconf->reset_scontext;

        if (gconf->flags & GUC_REPORT)
            ReportGUCOption(gconf);
    }

    if (IS_PGXC_COORDINATOR) {
        PoolAgent* agent = (PoolAgent*)u_sess->pgxc_cxt.poolHandle;
        if (agent == NULL) {
            return;
        }
        agent_reset_session(agent, (bool)POOL_CMD_LOCAL_SET);
    }
}

/*
 * push_old_value
 *		Push previous state during transactional assignment to a GUC variable.
 */
static void push_old_value(struct config_generic* gconf, GucAction action)
{
    GucStack* stack = NULL;

    /* If we're not inside a nest level, do nothing */
    if (u_sess->utils_cxt.GUCNestLevel == 0)
        return;

    /* Do we already have a stack entry of the current nest level? */
    stack = gconf->stack;

    if (stack != NULL && stack->nest_level >= u_sess->utils_cxt.GUCNestLevel) {
        switch (action) {
            case GUC_ACTION_SET:

                /* SET overrides any prior action at same nest level */
                if (stack->state == GUC_SET_LOCAL) {
                    /* must discard old masked value */
                    discard_stack_value(gconf, &stack->masked);
                }

                stack->state = GUC_SET;
                break;

            case GUC_ACTION_LOCAL:
                if (stack->state == GUC_SET) {
                    /* SET followed by SET LOCAL, remember SET's value */
                    stack->masked_scontext = gconf->scontext;
                    set_stack_value(gconf, &stack->masked);
                    stack->state = GUC_SET_LOCAL;
                }

                /* in all other cases, no change to stack entry */
                break;

            case GUC_ACTION_SAVE:
                /* Could only have a prior SAVE of same variable */
                Assert(stack->state == GUC_SAVE);
                break;
            default:
                break;
        }

        Assert(u_sess->utils_cxt.guc_dirty); /* must be set already */
        return;
    }

    /*
     * Push a new stack entry
     *
     * We keep all the stack entries in u_sess->top_transaction_mem_cxt for simplicity.
     */
    stack = (GucStack*)MemoryContextAllocZero(u_sess->top_transaction_mem_cxt, sizeof(GucStack));

    stack->prev = gconf->stack;
    stack->nest_level = u_sess->utils_cxt.GUCNestLevel;

    switch (action) {
        case GUC_ACTION_SET:
            stack->state = GUC_SET;
            break;

        case GUC_ACTION_LOCAL:
            stack->state = GUC_LOCAL;
            break;

        case GUC_ACTION_SAVE:
            stack->state = GUC_SAVE;
            break;
        default:
            break;
    }

    stack->source = gconf->source;
    stack->scontext = gconf->scontext;
    set_stack_value(gconf, &stack->prior);

    gconf->stack = stack;

    /* Ensure we remember to pop at end of xact */
    u_sess->utils_cxt.guc_dirty = true;
}

/*
 * Do GUC processing at main transaction start.
 */
void AtStart_GUC(void)
{
    /*
     * The nest level should be 0 between transactions; if it isn't, somebody
     * didn't call AtEOXact_GUC, or called it with the wrong nestLevel.  We
     * throw a warning but make no other effort to clean up.
     */
    if (u_sess->utils_cxt.GUCNestLevel != 0)
        ereport(WARNING, (errmsg("GUC nest level = %d at transaction start", u_sess->utils_cxt.GUCNestLevel)));

    u_sess->utils_cxt.GUCNestLevel = 1;
    reset_set_message(false);
}

/*
 * Enter a new nesting level for GUC values.  This is called at subtransaction
 * start, and when entering a function that has proconfig settings, and in
 * some other places where we want to set GUC variables transiently.
 * NOTE we must not risk error here, else subtransaction start will be unhappy.
 */
int NewGUCNestLevel(void)
{
    return ++u_sess->utils_cxt.GUCNestLevel;
}

char* GetGucName(const char *command, char *targeGucName)
{
    unsigned int rpos, lpos, i, j, commandLen, gucNums;

    if (command == NULL || targeGucName == NULL) {
        return NULL;
    }

    commandLen = (unsigned int)strlen(command);
    lpos = rpos = gucNums = 0;

    /* Find the name location. */
    for (i = 1; i < commandLen; i++) {
        if (command[i] == '=') {
            gucNums++;
        }

        if (gucNums == 0) {
            if (command[i - 1] == ' ' && command[i] != ' ') {
                lpos = i;
            }

            if (command[i - 1] != ' ' && command[i] == ' ') {
                rpos = i - 1;
            }
        }
    }

    if (gucNums == 0 || gucNums > 1 || lpos >= rpos || (rpos - lpos + 1 > MAX_PARAM_LEN)) {
        return NULL;
    }

    /* fill name */
    for (i = lpos, j = 0; i <= rpos; i++) {
        targeGucName[j++] = command[i];
    }

    targeGucName[j] = '\0';

    MODULE_LOG_TRACE(MOD_COMM_PARAM,
        "get pooler session params: command[%s], params name[%s]", command, targeGucName);

    return targeGucName;
}

/*
 * Do GUC processing at transaction or subtransaction commit or abort, or
 * when exiting a function that has proconfig settings, or when undoing a
 * transient assignment to some GUC variables.	(The name is thus a bit of
 * a misnomer; perhaps it should be ExitGUCNestLevel or some such.)
 * During abort, we discard all GUC settings that were applied at nesting
 * levels >= nestLevel.  nestLevel == 1 corresponds to the main transaction.
 */
void AtEOXact_GUC(bool isCommit, int nestLevel)
{
    bool still_dirty = false;
    int i;

    /*
     * Note: it's possible to get here with GUCNestLevel == nestLevel-1 during
     * abort, if there is a failure during transaction start before
     * AtStart_GUC is called.
     */
    Assert(nestLevel > 0 && (nestLevel <= u_sess->utils_cxt.GUCNestLevel ||
                                (nestLevel == u_sess->utils_cxt.GUCNestLevel + 1 && !isCommit)));

    /* Quick exit if nothing's changed in this transaction */
    if (!u_sess->utils_cxt.guc_dirty) {
        u_sess->utils_cxt.GUCNestLevel = nestLevel - 1;
        return;
    }

    for (i = 0; i < u_sess->num_guc_variables; i++) {
        struct config_generic* gconf = u_sess->guc_variables[i];
        GucStack* stack = NULL;

        /*
         * Process and pop each stack entry within the nest level. To simplify
         * fmgr_security_definer() and other places that use GUC_ACTION_SAVE,
         * we allow failure exit from code that uses a local nest level to be
         * recovered at the surrounding transaction or subtransaction abort;
         * so there could be more than one stack entry to pop.
         */
        while ((stack = gconf->stack) != NULL && stack->nest_level >= nestLevel) {
            GucStack* prev = stack->prev;
            bool restorePrior = false;
            bool restoreMasked = false;
            bool changed = false;

            /*
             * In this next bit, if we don't set either restorePrior or
             * restoreMasked, we must "discard" any unwanted fields of the
             * stack entries to avoid leaking memory.  If we do set one of
             * those flags, unused fields will be cleaned up after restoring.
             */
            if (!isCommit) /* if abort, always restore prior value */
                restorePrior = true;
            else if (stack->state == GUC_SAVE)
                restorePrior = true;
            else if (stack->nest_level == 1) {
                /* transaction commit */
                if (stack->state == GUC_SET_LOCAL)
                    restoreMasked = true;
                else if (stack->state == GUC_SET) {
                    /* we keep the current active value */
                    discard_stack_value(gconf, &stack->prior);
                } else /* must be GUC_LOCAL */
                    restorePrior = true;
            } else if (prev == NULL || prev->nest_level < stack->nest_level - 1) {
                /* decrement entry's level and do not pop it */
                stack->nest_level--;
                continue;
            } else {
                /*
                 * We have to merge this stack entry into prev. See README for
                 * discussion of this bit.
                 */
                switch (stack->state) {
                    case GUC_SAVE:
                        Assert(false); /* can't get here */

                    case GUC_SET:
                        /* next level always becomes SET */
                        discard_stack_value(gconf, &stack->prior);

                        if (prev->state == GUC_SET_LOCAL)
                            discard_stack_value(gconf, &prev->masked);

                        prev->state = GUC_SET;
                        break;

                    case GUC_LOCAL:
                        if (prev->state == GUC_SET) {
                            /* LOCAL migrates down */
                            prev->masked_scontext = stack->scontext;
                            prev->masked = stack->prior;
                            prev->state = GUC_SET_LOCAL;
                        } else {
                            /* else just forget this stack level */
                            discard_stack_value(gconf, &stack->prior);
                        }

                        break;

                    case GUC_SET_LOCAL:
                        /* prior state at this level no longer wanted */
                        discard_stack_value(gconf, &stack->prior);
                        /* copy down the masked state */
                        prev->masked_scontext = stack->masked_scontext;

                        if (prev->state == GUC_SET_LOCAL)
                            discard_stack_value(gconf, &prev->masked);

                        prev->masked = stack->masked;
                        prev->state = GUC_SET_LOCAL;
                        break;
                    default:
                        break;
                }
            }

            changed = false;

            if (restorePrior || restoreMasked) {
                /* Perform appropriate restoration of the stacked value */
                config_var_value newvalue;
                GucSource newsource;
                GucContext newscontext;

                if (restoreMasked) {
                    newvalue = stack->masked;
                    newsource = PGC_S_SESSION;
                    newscontext = stack->masked_scontext;
                } else {
                    newvalue = stack->prior;
                    newsource = stack->source;
                    newscontext = stack->scontext;
                }

                switch (gconf->vartype) {
                    case PGC_BOOL: {
                        struct config_bool* conf = (struct config_bool*)gconf;
                        bool newval = newvalue.val.boolval;
                        void* newextra = newvalue.extra;

                        if (*conf->variable != newval || conf->gen.extra != newextra) {
                            if (conf->assign_hook)
                                (*conf->assign_hook)(newval, newextra);

                            *conf->variable = newval;
                            set_extra_field(&conf->gen, &conf->gen.extra, newextra);
                            changed = true;
                        }

                        break;
                    }

                    case PGC_INT: {
                        struct config_int* conf = (struct config_int*)gconf;
                        int newval = newvalue.val.intval;
                        void* newextra = newvalue.extra;

                        if (*conf->variable != newval || conf->gen.extra != newextra) {
                            if (conf->assign_hook)
                                (*conf->assign_hook)(newval, newextra);

                            *conf->variable = newval;
                            set_extra_field(&conf->gen, &conf->gen.extra, newextra);
                            changed = true;
                        }

                        break;
                    }

                    case PGC_INT64: {
                        struct config_int64* conf = (struct config_int64*)gconf;
                        int64 newval = newvalue.val.int64val;
                        void* newextra = newvalue.extra;

                        if (*conf->variable != newval || conf->gen.extra != newextra) {
                            if (conf->assign_hook)
                                (*conf->assign_hook)(newval, newextra);
                            *conf->variable = newval;
                            set_extra_field(&conf->gen, &conf->gen.extra, newextra);
                            changed = true;
                        }
                        break;
                    }

                    case PGC_REAL: {
                        struct config_real* conf = (struct config_real*)gconf;
                        double newval = newvalue.val.realval;
                        void* newextra = newvalue.extra;

                        if (*conf->variable != newval || conf->gen.extra != newextra) {
                            if (conf->assign_hook)
                                (*conf->assign_hook)(newval, newextra);

                            *conf->variable = newval;
                            set_extra_field(&conf->gen, &conf->gen.extra, newextra);
                            changed = true;
                        }

                        break;
                    }

                    case PGC_STRING: {
                        struct config_string* conf = (struct config_string*)gconf;
                        char* newval = newvalue.val.stringval;
                        void* newextra = newvalue.extra;

                        if (*conf->variable != newval || conf->gen.extra != newextra) {
                            if (conf->assign_hook)
                                (*conf->assign_hook)(newval, newextra);

                            set_string_field(conf, conf->variable, newval);
                            set_extra_field(&conf->gen, &conf->gen.extra, newextra);
                            changed = true;
                        }

                        /*
                         * Release stacked values if not used anymore. We
                         * could use discard_stack_value() here, but since
                         * we have type-specific code anyway, might as
                         * well inline it.
                         */
                        set_string_field(conf, &stack->prior.val.stringval, NULL);
                        set_string_field(conf, &stack->masked.val.stringval, NULL);
                        break;
                    }

                    case PGC_ENUM: {
                        struct config_enum* conf = (struct config_enum*)gconf;
                        int newval = newvalue.val.enumval;
                        void* newextra = newvalue.extra;

                        if (*conf->variable != newval || conf->gen.extra != newextra) {
                            if (conf->assign_hook)
                                (*conf->assign_hook)(newval, newextra);

                            *conf->variable = newval;
                            set_extra_field(&conf->gen, &conf->gen.extra, newextra);
                            changed = true;
                        }

                        break;
                    }
                    default:
                        break;
                }

                /*
                 * Release stacked extra values if not used anymore.
                 */
                set_extra_field(gconf, &(stack->prior.extra), NULL);
                set_extra_field(gconf, &(stack->masked.extra), NULL);

                /* And restore source information */
                gconf->source = newsource;
                gconf->scontext = newscontext;
            }

            /* Finish popping the state stack */
            gconf->stack = prev;
            pfree(stack);

            /* Report new value if we changed it */
            if (changed && (gconf->flags & GUC_REPORT))
                ReportGUCOption(gconf);
        } /* end of stack-popping loop */

        if (stack != NULL)
            still_dirty = true;
    }

    /* If there are no remaining stack entries, we can reset guc_dirty */
    u_sess->utils_cxt.guc_dirty = still_dirty;

    /* Update nesting level */
    u_sess->utils_cxt.GUCNestLevel = nestLevel - 1;

    /*
     * Send set command which is in transactionBlock to pool
     *
     * During inplace upgrade, we don't need to set agent and DNs'
     * GUC at prepare or commit. These connections will always be closed.
     */
    if (isCommit && nestLevel == 1 && !u_sess->attr.attr_common.IsInplaceUpgrade) {
        const char* setStr = get_set_string();

        if (setStr != NULL) {
            char tmpName[MAX_PARAM_LEN + 1] = {0};
            const char *gucName = GetGucName(setStr, tmpName);
            int rs = PoolManagerSetCommand(POOL_CMD_GLOBAL_SET, setStr, gucName);
            if (rs < 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Communication failure, failed to send set commands to pool in transaction commit.")));
            }
        }
        /* reset input_set_message  when current transaction is in top level and isCommit is true.*/
        reset_set_message(isCommit);
    }

    SetThreadLocalGUC(u_sess);
}

/*
 * Start up automatic reporting of changes to variables marked GUC_REPORT.
 * This is executed at completion of backend startup.
 */
void BeginReportingGUCOptions(void)
{
    int i;

    /*
     * Don't do anything unless talking to an interactive frontend of protocol
     * 3.0 or later.
     */
    if (t_thrd.postgres_cxt.whereToSendOutput != DestRemote || PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
        return;

    u_sess->utils_cxt.reporting_enabled = true;

    /* Transmit initial values of interesting variables */
    for (i = 0; i < u_sess->num_guc_variables; i++) {
        struct config_generic* conf = u_sess->guc_variables[i];

        if (conf->flags & GUC_REPORT)
            ReportGUCOption(conf);
    }
}

/*
 * ReportGUCOption: if appropriate, transmit option value to frontend
 */
static void ReportGUCOption(struct config_generic* record)
{
    if (u_sess->utils_cxt.reporting_enabled && (record->flags & GUC_REPORT)) {
        char* val = _ShowOption(record, false);
        StringInfoData msgbuf;

        pq_beginmessage(&msgbuf, 'S');
        pq_sendstring(&msgbuf, record->name);
        pq_sendstring(&msgbuf, val);
        pq_endmessage(&msgbuf);

        pfree(val);
    }
}

/*
 * Try to parse value as an integer.  The accepted formats are the
 * usual decimal, octal, or hexadecimal formats, optionally followed by
 * a unit name if "flags" indicates a unit is allowed.
 *
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 * If not okay and hintmsg is not NULL, *hintmsg is set to a suitable
 *	HINT message, or NULL if no hint provided.
 */
bool parse_int(const char* value, int* result, int flags, const char** hintmsg)
{
    int64 val;
    char* endptr = NULL;

    /* To suppress compiler warnings, always set output params */
    if (result != NULL)
        *result = 0;

    if (hintmsg != NULL)
        *hintmsg = NULL;

    /* We assume here that int64 is at least as wide as long */
    errno = 0;
    val = strtol(value, &endptr, 0);

    if (endptr == value)
        return false; /* no HINT for integer syntax error */

    if (errno == ERANGE) {
        if (hintmsg != NULL)
            *hintmsg = gettext_noop("Value exceeds integer range.");

        return false;
    }

    /* allow whitespace between integer and unit */
    while (isspace((unsigned char)*endptr))
        endptr++;

    /* Handle possible unit conversion before check integer overflow */
    if (*endptr != '\0') {
        /*
         * Note: the multiple-switch coding technique here is a bit tedious,
         * but seems necessary to avoid intermediate-value overflows.
         */
        if (flags & GUC_UNIT_MEMORY) {
            val = (int64)MemoryUnitConvert(&endptr, val, flags, hintmsg);
        } else if (flags & GUC_UNIT_TIME) {
            val = (int64)TimeUnitConvert(&endptr, val, flags, hintmsg);
        }

        /* allow whitespace after unit */
        while (isspace((unsigned char)*endptr))
            endptr++;

        if (*endptr != '\0')
            return false; /* appropriate hint, if any, already set */
    }

    /* Check for integer overflow */
    if (val != (int64)((int32)val)) {
        if (hintmsg != NULL)
            *hintmsg = gettext_noop("Value exceeds integer range.");

        return false;
    }

    if (result != NULL)
        *result = (int)val;

    return true;
}

/*
 * Try to parse value as an 64-bit integer.  The accepted format is
 * decimal number.
 *
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 * If not okay and hintmsg is not NULL, *hintmsg is set to a suitable
 *	HINT message, or NULL if no hint provided.
 */
bool parse_int64(const char* value, int64* result, const char** hintmsg)
{
    int64 val;
    char* endptr = NULL;

    /* To suppress compiler warnings, always set output params */
    if (result != NULL) {
        *result = 0;
    }
    if (hintmsg != NULL) {
        *hintmsg = NULL;
    }

    /* We assume here that int64 is at least as wide as long */
    errno = 0;
#ifdef _MSC_VER /* MSVC only */
    val = _strtoi64(value, &endptr, 10);
#elif defined(HAVE_STRTOLL) && SIZEOF_LONG < 8
    val = strtoll(value, &endptr, 10);
#else
    val = strtol(value, &endptr, 10);
#endif

    if (endptr == value || *endptr != '\0') {
        return false; /* no HINT for integer syntax error */
    }

    if (errno == ERANGE) {
        if (hintmsg != NULL) {
            *hintmsg = gettext_noop("Value exceeds 64-bit integer range.");
        }
        return false;
    }

    if (result != NULL) {
        *result = val;
    }
    return true;
}

/*
 * Try to parse value as a floating point number in the usual format.
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 */
bool parse_real(const char* value, double* result, int flags, const char** hintmsg)
{
    double val;
    char* endptr = NULL;

    if (result != NULL)
        *result = 0; /* suppress compiler warning */

    errno = 0;
    val = strtod(value, &endptr);

    if (endptr == value || errno == ERANGE)
        return false;
    /* reject NaN (infinities will fail range checks later) */

    if (isnan(val)) {
        return false;
    }

    /* allow whitespace after number */
    while (isspace((unsigned char)*endptr))
        endptr++;

    if (*endptr != '\0') {
        /*
         * Note: the multiple-switch coding technique here is a bit tedious,
         * but seems necessary to avoid intermediate-value overflows.
         */
        if (flags & GUC_UNIT_MEMORY) {
            val = MemoryUnitConvert(&endptr, val, flags, hintmsg);
        } else if (flags & GUC_UNIT_TIME) {
            val = TimeUnitConvert(&endptr, val, flags, hintmsg);
        }

        /* allow whitespace after unit */
        while (isspace((unsigned char)*endptr)) {
            endptr++;
        }

        if (*endptr != '\0') {
            return false; /* appropriate hint, if any, already set */
        }
    }

    if (result != NULL) {
        *result = val;
    }

    return true;
}

/*
 * The reference is used because auto-increment of formal parameters does not change the value of the actual parameter.
 * As a result, an error is reported.
 */
double MemoryUnitConvert(char** endptr, double value, int flags, const char** hintmsg)
{
    double val = value;
    const int size_2byte = 2;

    /* Set hint for use if no match or trailing garbage */
    if (hintmsg != NULL) {
        *hintmsg = gettext_noop("Valid units for this parameter are \"kB\", \"MB\", and \"GB\".");
    }

#if BLCKSZ < 1024 || BLCKSZ > (1024 * 1024)
#error BLCKSZ must be between 1KB and 1MB
#endif
#if XLOG_BLCKSZ < 1024 || XLOG_BLCKSZ > (1024 * 1024)
#error XLOG_BLCKSZ must be between 1KB and 1MB
#endif

    if (strncmp(*endptr, "kB", 2) == 0) {
        *endptr += size_2byte;

        switch (flags & GUC_UNIT_MEMORY) {
            case GUC_UNIT_BLOCKS:
                val /= (double)(BLCKSZ / 1024);
                break;

            case GUC_UNIT_XBLOCKS:
                val /= (double)(XLOG_BLCKSZ / 1024);
                break;
            default:
                break;
        }
    } else if (strncmp(*endptr, "MB", 2) == 0) {
        *endptr += size_2byte;

        switch (flags & GUC_UNIT_MEMORY) {
            case GUC_UNIT_KB:
                val *= KB_PER_MB;
                break;

            case GUC_UNIT_BLOCKS:
                val *= KB_PER_MB / ((double)BLCKSZ / 1024);
                break;

            case GUC_UNIT_XBLOCKS:
                val *= KB_PER_MB / ((double)XLOG_BLCKSZ / 1024);
                break;
            default:
                break;
        }
    } else if (strncmp(*endptr, "GB", 2) == 0) {
        *endptr += size_2byte;

        switch (flags & GUC_UNIT_MEMORY) {
            case GUC_UNIT_KB:
                val *= KB_PER_GB;
                break;

            case GUC_UNIT_BLOCKS:
                val *= KB_PER_GB / ((double)BLCKSZ / 1024);
                break;

            case GUC_UNIT_XBLOCKS:
                val *= KB_PER_GB / ((double)XLOG_BLCKSZ / 1024);
                break;
            default:
                break;
        }
    }
    return val;
}

double TimeUnitConvert(char** endptr, double value, int flags, const char** hintmsg)
{
    double val = value;
    const int size_1byte = 1;
    const int size_2byte = 2;
    const int size_3byte = 3;

    /* Set hint for use if no match or trailing garbage */
    if (hintmsg != NULL) {
        *hintmsg = gettext_noop("Valid units for this parameter are \"ms\", \"s\", \"min\", \"h\", and \"d\".");
    }
    if (strncmp(*endptr, "ms", 2) == 0) {
        *endptr += size_2byte;

        switch (flags & GUC_UNIT_TIME) {
            case GUC_UNIT_S:
                val /= MS_PER_S;
                break;
            case GUC_UNIT_MIN:
                val /= MS_PER_MIN;
                break;
            case GUC_UNIT_HOUR:
                val /= MS_PER_H;
                break;
            case GUC_UNIT_DAY:
                val /= MS_PER_D;
                break;
            default:
                break;
        }
    } else if (strncmp(*endptr, "s", 1) == 0) {
        *endptr += size_1byte;
        switch (flags & GUC_UNIT_TIME) {
            case GUC_UNIT_MS:
                val *= MS_PER_S;
                break;

            case GUC_UNIT_MIN:
                val /= S_PER_MIN;
                break;
            case GUC_UNIT_HOUR:
                val /= S_PER_H;
                break;
            case GUC_UNIT_DAY:
                val /= S_PER_D;
                break;
            default:
                break;
        }
    } else if (strncmp(*endptr, "min", 3) == 0) {
        *endptr += size_3byte;
        switch (flags & GUC_UNIT_TIME) {
            case GUC_UNIT_MS:
                val *= MS_PER_MIN;
                break;

            case GUC_UNIT_S:
                val *= S_PER_MIN;
                break;
            case GUC_UNIT_HOUR:
                val /= MIN_PER_H;
                break;
            case GUC_UNIT_DAY:
                val /= MIN_PER_D;
                break;
            default:
                break;
        }
    } else if (strncmp(*endptr, "h", 1) == 0) {
        *endptr += size_1byte;
        switch (flags & GUC_UNIT_TIME) {
            case GUC_UNIT_MS:
                val *= MS_PER_H;
                break;

            case GUC_UNIT_S:
                val *= S_PER_H;
                break;

            case GUC_UNIT_MIN:
                val *= MIN_PER_H;
                break;
            case GUC_UNIT_DAY:
                val /= H_PER_D;
                break;
            default:
                break;
        }
    } else if (strncmp(*endptr, "d", 1) == 0) {
        *endptr += size_1byte;
        switch (flags & GUC_UNIT_TIME) {
            case GUC_UNIT_MS:
                val *= MS_PER_D;
                break;

            case GUC_UNIT_S:
                val *= S_PER_D;
                break;

            case GUC_UNIT_MIN:
                val *= MIN_PER_D;
                break;
            case GUC_UNIT_HOUR:
                val *= H_PER_D;
            default:
                break;
        }
    }
    return val;
}

/*
 * @Description: Get guc string value according to val.
 * @in record: Enum configure struct.
 * @in val: Guc value.
 * @return: Guc string values.
 */
static const char* config_enum_map_lookup_by_value(struct config_enum* record, int val)
{
    StringInfoData buf;
    initStringInfo(&buf);

    bool is_first = true;
    const struct config_enum_entry* entry = NULL;

    for (entry = record->options; entry && entry->name; entry++) {
        if (entry->val & val) {
            if (is_first) {
                appendStringInfo(&buf, "%s", entry->name);
                is_first = false;
            } else {
                appendStringInfo(&buf, ", %s", entry->name);
            }
        }
    }

    if (buf.len == 0) {
        appendStringInfoString(&buf, "none");
    }

    return buf.data;
}

/*
 * Lookup the name for an enum option with the selected value.
 * Should only ever be called with known-valid values, so throws
 * an elog(ERROR) if the enum option is not found.
 *
 * The returned string is a pointer to static THR_LOCAL data and not
 * allocated for modification.
 */
const char* config_enum_lookup_by_value(struct config_enum* record, int val)
{
    if (pg_strncasecmp(record->gen.name, "rewrite_rule", sizeof("rewrite_rule")) == 0 ||
        pg_strncasecmp(record->gen.name, "sql_beta_feature", sizeof("sql_beta_feature")) == 0) {
        return config_enum_map_lookup_by_value(record, val);
    } else {
        const struct config_enum_entry* entry = NULL;

        for (entry = record->options; entry && entry->name; entry++) {
            if (entry->val == val)
                return entry->name;
        }
    }

    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("could not find enum option %d for %s", val, record->gen.name)));
    return NULL; /* silence compiler */
}

/*
 * @Description: Set bit map to guc.
 * @in record: Enum config.
 * @in newval: New values.
 * @out retval: Guc param.
 */
static bool config_enum_map_lookup_by_name(struct config_enum* record, const char* newval, int* retval)
{
    char* val = pstrdup(newval);

    List* valList = NIL;

    if (!SplitIdentifierString(val, ',', &valList)) {
        pfree(val);
        list_free(valList);
        return false;
    }

    int set_value = 0;

    const struct config_enum_entry* entry = NULL;

    ListCell* lc = NULL;
    foreach (lc, valList) {
        char* valItem = (char*)lfirst(lc);

        for (entry = record->options; entry && entry->name; entry++) {
            if (pg_strcasecmp(valItem, entry->name) == 0) {
                set_value |= entry->val;
                break;
            }
        }

        if (entry != NULL && entry->name == NULL) {
            return false;
        }
    }

    pfree(val);
    list_free(valList);

    *retval = set_value;
    return true;
}

/*
 * Lookup the value for an enum option with the selected name
 * (case-insensitive).
 * If the enum option is found, sets the retval value and returns
 * true. If it's not found, return FALSE and retval is set to 0.
 */
bool config_enum_lookup_by_name(struct config_enum* record, const char* value, int* retval)
{
    if (pg_strncasecmp(record->gen.name, "rewrite_rule", sizeof("rewrite_rule")) == 0 ||
        pg_strncasecmp(record->gen.name, "sql_beta_feature", sizeof("sql_beta_feature")) == 0) {
        return config_enum_map_lookup_by_name(record, value, retval);
    } else {
        const struct config_enum_entry* entry = NULL;

        for (entry = record->options; entry && entry->name; entry++) {
            if (pg_strcasecmp(value, entry->name) == 0) {
                *retval = entry->val;
                return TRUE;
            }
        }
    }

    *retval = 0;
    return FALSE;
}

/*
 * Return a list of all available options for an enum, excluding
 * hidden ones, separated by the given separator.
 * If prefix is non-NULL, it is added before the first enum value.
 * If suffix is non-NULL, it is added to the end of the string.
 */
static char* config_enum_get_options(
    struct config_enum* record, const char* prefix, const char* suffix, const char* separator)
{
    const struct config_enum_entry* entry = NULL;
    StringInfoData retstr;
    int seplen;

    initStringInfo(&retstr);
    appendStringInfoString(&retstr, prefix);

    seplen = strlen(separator);

    for (entry = record->options; entry && entry->name; entry++) {
        if (!entry->hidden) {
            appendStringInfoString(&retstr, entry->name);
            appendBinaryStringInfo(&retstr, separator, seplen);
        }
    }

    /*
     * All the entries may have been hidden, leaving the string empty if no
     * prefix was given. This indicates a broken GUC setup, since there is no
     * use for an enum without any values, so we just check to make sure we
     * don't write to invalid memory instead of actually trying to do
     * something smart with it.
     */
    if (retstr.len >= seplen) {
        /* Replace final separator */
        retstr.data[retstr.len - seplen] = '\0';
        retstr.len -= seplen;
    }

    appendStringInfoString(&retstr, suffix);

    return retstr.data;
}

/*
 * Validates configuration parameter and value, by calling check hook functions
 * depending on record's vartype. It validates if the parameter
 * value given is in range of expected predefined value for that parameter.
 *
 * freemem - true indicates memory for newval and newextra will be
 *          freed in this function, false indicates it will be freed
 *          by caller.
 * Return value:
 * true : the value is valid
 * false: the name or value is invalid
 */
static bool validate_conf_option(struct config_generic *record, const char *name, const char *value, GucSource source,
                          int elevel, bool freemem, void *newvalue, void **newextra)
{
    Assert(value != NULL);

    /*
     * Validate the value for the passed record, to ensure it is in expected range.
     */
    switch (record->vartype) {
        case PGC_BOOL: {
            return validate_conf_bool(record, name, value, source, elevel, freemem, newvalue, newextra);
        }

        case PGC_INT: {
            return validate_conf_int(record, name, value, source, elevel, freemem, newvalue, newextra);
        }

        case PGC_INT64: {
            return validate_conf_int64(record, name, value, source, elevel, freemem, newvalue, newextra);
        }

        case PGC_REAL: {
            return validate_conf_real(record, name, value, source, elevel, freemem, newvalue, newextra);
        }

        case PGC_STRING: {
            return validate_conf_string(record, value, source, elevel, freemem, newvalue, newextra);
        }

        case PGC_ENUM: {
            return validate_conf_enum(record, name, value, source, elevel, freemem, newvalue, newextra);
        }

        default: {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_ATTRIBUTE),
                     errmsg("unknow guc variable type: %d", record->vartype)));
        }
        break;
    }

    return true;
}

static bool validate_conf_bool(struct config_generic *record, const char *name, const char *value, GucSource source,
                          int elevel, bool freemem, void *newvalue, void **newextra)
{
    struct config_bool *conf = (struct config_bool *) record;
    bool tmpnewval;
    bool* newval = (newvalue == NULL ? &tmpnewval : (bool*)newvalue);

    if (!parse_bool(value, newval)) {
        ereport(elevel,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("parameter \"%s\" requires a Boolean value", name)));
        return false;
    }

    if (!call_bool_check_hook(conf, newval, newextra, source, elevel)) {
        return false;
    }

    if (*newextra && freemem) {
        pfree(*newextra);
        *newextra = NULL;
    }

    return true;
}

static bool validate_conf_int(struct config_generic *record, const char *name, const char *value, GucSource source,
                          int elevel, bool freemem, void *newvalue, void **newextra)
{
    struct config_int *conf = (struct config_int *) record;
    int  tmpnewval;
    int* newval = (newvalue == NULL ? &tmpnewval : (int*)newvalue);
    const char *hintmsg = NULL;

    if (!parse_int(value, newval, conf->gen.flags, &hintmsg)) {
        ereport(elevel,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid value for parameter \"%s\": \"%s\"",
                 name, value), hintmsg ? errhint("%s", _(hintmsg)) : 0));
        return false;
    }

    if (*newval < conf->min || *newval > conf->max) {
        ereport(elevel,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)",
                        *newval, name, conf->min, conf->max)));
        return false;
    }

    if (!call_int_check_hook(conf, newval, newextra, source, elevel)) {
        return false;
    }

    if (*newextra && freemem) {
        pfree(*newextra);
        *newextra = NULL;
    }

    return true;
}

static bool validate_conf_int64(struct config_generic *record, const char *name, const char *value, GucSource source,
                          int elevel, bool freemem, void *newvalue, void **newextra)
{
    struct config_int64* conf = (struct config_int64*)record;
    int64  tmpnewval;
    int64* newval = (newvalue == NULL ? &tmpnewval : (int64*)newvalue);
    const char* hintmsg = NULL;

    if (!parse_int64(value, newval, &hintmsg)) {
        ereport(elevel,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("parameter \"%s\" requires a numeric value", name)));
        return false;
    } 

    if (*newval < conf->min || *newval > conf->max) {
        ereport(elevel,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%ld is outside the valid range for parameter \"%s\" (%ld .. %ld)", 
                        *newval, name, conf->min, conf->max)));
        return false;
    }

    if (!call_int64_check_hook(conf, newval, newextra, source, elevel)) {
        return false;
    }

    if (*newextra && freemem) {
        pfree(newextra);
        *newextra = NULL;
    }

    return true;
}

static bool validate_conf_real(struct config_generic *record, const char *name, const char *value, GucSource source,
                          int elevel, bool freemem, void *newvalue, void **newextra)
{
    struct config_real *conf = (struct config_real *) record;
    double  tmpnewval;
    double* newval = (newvalue == NULL ? &tmpnewval : (double*)newvalue);
    const char *hintmsg = NULL;

    if (!parse_real(value, newval, conf->gen.flags, &hintmsg)) {
        ereport(elevel,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("parameter \"%s\" requires a numeric value", name)));
        return false;
    }

    if (*newval < conf->min || *newval > conf->max) {
        ereport(elevel,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%g is outside the valid range for parameter \"%s\" (%g .. %g)",
                        *newval, name, conf->min, conf->max)));
        return false;
    }

    if (!call_real_check_hook(conf, newval, newextra, source, elevel))
        return false;

    if (*newextra && freemem) {
        pfree(*newextra);
        *newextra = NULL;
    }

    return true;
}

static bool validate_conf_string(struct config_generic *record, const char *value, GucSource source,
                          int elevel, bool freemem, void *newvalue, void **newextra)
{
    struct config_string *conf = (struct config_string *) record;
    char*  tmpnewval = NULL;
    char** newval = newvalue == NULL ? &tmpnewval : (char**)newvalue;

    pfree_ext(*newval);
    /* The value passed by the caller could be transient, so we always strdup it. */
    *newval = guc_strdup(elevel, value);
    if (*newval == NULL) {
        return false;
    }

    /* The only built-in "parsing" check we have is to apply truncation if GUC_IS_NAME. */
    if (conf->gen.flags & GUC_IS_NAME) {
        truncate_identifier(*newval, strlen(*newval), true);
    }

    if (!call_string_check_hook(conf, newval, newextra, source, elevel)) {
        if (*newval != NULL) {
            pfree(*newval);
            *newval = NULL;
        }
        return false;
    }

    /* Free the malloc'd data if any */
    if (freemem) {
        if (*newval != NULL) {
            pfree(*newval);
            *newval = NULL;
        }
        if (*newextra != NULL) {
            pfree(*newextra);
            *newextra = NULL;
        }
    }

    return true;
}

static bool validate_conf_enum(struct config_generic *record, const char *name, const char *value, GucSource source,
                          int elevel, bool freemem, void *newvalue, void **newextra)
{
    struct config_enum *conf = (struct config_enum *) record;
    int  tmpnewval;
    int* newval = (newvalue == NULL ? &tmpnewval : (int*)newvalue);

    if (!config_enum_lookup_by_name(conf, value, newval)) {
        char* hintmsg;

        hintmsg = config_enum_get_options(conf, "Available values: ", ".", ", ");

        ereport(elevel,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid value for parameter \"%s\": \"%s\"", name, value),
                 hintmsg ? errhint("%s", _(hintmsg)) : 0));

        if (hintmsg != NULL) {
            pfree(hintmsg);
            hintmsg = NULL;
        }
        return false;
    }
    if (!call_enum_check_hook(conf, newval, newextra, source, LOG))
        return false;

    if (*newextra && freemem) {
        pfree(*newextra);
        *newextra = NULL;
    }

    return true;
}

void SetThreadLocalGUC(knl_session_context* session)
{
    default_statistics_target = session->attr.attr_sql.default_statistics_target;
    session_timezone = session->attr.attr_common.session_timezone;
    log_timezone = session->attr.attr_common.log_timezone;
    client_min_messages = session->attr.attr_common.client_min_messages;
    log_min_messages = session->attr.attr_common.log_min_messages;
    enable_alarm = g_instance.attr.attr_common.enable_alarm;
    Alarm_component = g_instance.attr.attr_common.Alarm_component;
    tcp_link_addr = g_instance.attr.attr_network.tcp_link_addr;
    assert_enabled = session->attr.attr_common.assert_enabled;
    AlarmReportInterval = session->attr.attr_common.AlarmReportInterval;
    xmloption = session->attr.attr_common.xmloption;
    comm_client_bind = session->attr.attr_network.comm_client_bind;
    comm_ackchk_time = session->attr.attr_network.comm_ackchk_time;
}

/*
 * Find if there is a namespace other than pg_temp_xxx in the value to be set.
 * If no one, return. Otherwise set them to search_path.
 * Discard pg_temp in newval because
 * 1) it's implicit in search_path and will be added the forefront of the path.
 * 2) gsql will execute the query below and set the result to search_path.
 *    select n.nspname||','||s.setting from pg_namespace n, pg_settings s
 *      where n.oid = pg_my_temp_schema() and s.name='search_path';
 *    The result should be pg_temp, contents in search_path
 * Here is the example of setting twice from gsql(current search_path is public).
 * 1) gsql execute the query and get result pg_temp, public.
 * 2) gsql set search_path = pg_temp, public
 * 3) gsql execute the query and get result pg_temp, pg_temp, public
 * 4) gsql set search_path = pg_temp, pg_temp, public
 * From the example above, we find pg_temp will be added multiple times and
 * exceed the string's max length.
 */
static char* getSchemaNameList(const char* value)
{
    /*
     * The reason of checking pg_temp_ rather than pg_temp above is
     * pg_temp is the general valid temp namespace but we cannot find oid
     * from it, so don't process it here.
     * gsql set temp search_path pg_temp_xxx not pg_temp.
     *
     * strings in namelist are already strip quote.
     * No need to consider quote_all_identifiers problem.
     */
    List *namelist = NIL;
    char* valueCopy = pstrdup(value);
    SplitIdentifierString((char *)valueCopy, ',', &namelist);

    /*
     * If current CN receives query
     * 'set search_path = pg_temp_xxx' from other CN,
     * don't process temp namespaces which don't belong
     * the current CN.
     */
    if (!IS_PGXC_COORDINATOR || !IsConnFromCoord()) {
       SetTempFromSearchPath(namelist);
    }

    int cntSearchItem = 0; /* number of namespaces other than pg_temp */
    StringInfoData buf;
    initStringInfo(&buf);
    ListCell *item = NULL;
    foreach(item, namelist) {
        char *searchPathName = (char *)lfirst(item);
        if (!isTempNamespaceName(searchPathName)) {
            if (cntSearchItem > 0) {
                appendStringInfo(&buf, ", ");
            }
            cntSearchItem++;
            /* search_path needs to consider adding quote */
            appendStringInfoString(&buf, quote_identifier(searchPathName));
        }
    }

    if (cntSearchItem == 0) {
        pfree_ext(buf.data);
        return NULL;
    } else {
        return buf.data;
    }

}

/*
 * Sets option `name' to given value.
 *
 * The value should be a string, which will be parsed and converted to
 * the appropriate data type.  The context and source parameters indicate
 * in which context this function is being called, so that it can apply the
 * access restrictions properly.
 *
 * If value is NULL, set the option to its default value (normally the
 * reset_val, but if source == PGC_S_DEFAULT we instead use the boot_val).
 *
 * action indicates whether to set the value globally in the session, locally
 * to the current top transaction, or just for the duration of a function call.
 *
 * If changeVal is false then don't really set the option but do all
 * the checks to see if it would work.
 *
 * elevel should normally be passed as zero, allowing this function to make
 * its standard choice of ereport level.  However some callers need to be
 * able to override that choice; they should pass the ereport level to use.
 *
 * Return value:
 *	+1: the value is valid and was successfully applied.
 *	0:	the name or value is invalid (but see below).
 *	-1: the value was not applied because of context, priority, or changeVal.
 *
 * If there is an error (non-existing option, invalid value) then an
 * ereport(ERROR) is thrown *unless* this is called for a source for which
 * we don't want an ERROR (currently, those are defaults, the config file,
 * and per-database or per-user settings, as well as callers who specify
 * a less-than-ERROR elevel).  In those cases we write a suitable error
 * message via ereport() and return 0.
 *
 * See also SetConfigOption for an external interface.
 */
int set_config_option(const char* name, const char* value, GucContext context, GucSource source, GucAction action,
    bool changeVal, int elevel, bool isReload)
{
    struct config_generic* record = NULL;
    bool prohibitValueChange = false;
    bool makeDefault = false;

#ifdef PGXC
    /*
     * Current GucContest value is needed to check if xc_maintenance_mode parameter
     * is specified in valid contests.   It is allowed only by SET command or
     * libpq connect parameters so that setting this ON is just temporary.
     */
    currentGucContext = context;
#endif

    if (elevel == 0) {
        if (source == PGC_S_DEFAULT || source == PGC_S_FILE) {
            /*
             * To avoid cluttering the log, only the postmaster bleats loudly
             * about problems with the config file.
             */
            elevel = IsUnderPostmaster ? DEBUG3 : LOG;
        } else if (source == PGC_S_DATABASE || source == PGC_S_USER || source == PGC_S_DATABASE_USER)
            elevel = WARNING;
        else
            elevel = ERROR;
    }

    record = find_option(name, true, elevel);

    if (record == NULL) {
        ereport(
            elevel, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("unrecognized configuration parameter \"%s\"", name)));
        return 0;
    }

    if (strcmp(name, "enable_cluster_resize") == 0) {
        ereport(LOG, (errmsg("Set parameter \"%s\": \"%s\"", name, value)));
    }

    if (strcmp(name, "application_name") == 0 && context == PGC_SIGHUP) {
        return 0;
    }

    /*
     * Check if the option can be set at this time. See guc.h for the precise
     * rules.
     */
    switch (record->context) {
        case PGC_INTERNAL:
            if (context != PGC_INTERNAL) {
                /*Add for upgrade*/
                if (strncmp(name, "sql_compatibility", sizeof("sql_compatibility")) == 0) {
                    ereport(WARNING,
                        (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                            errmsg("parameter \"%s\" cannot be changed. Compatibility can only be specified when "
                                   "creating database",
                                name)));
                    return 1;
                } else {
                    ereport(elevel,
                        (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                            errmsg("parameter \"%s\" cannot be changed", name)));
                }
                return 0;
            }

            break;

        case PGC_POSTMASTER:
            if (context == PGC_SIGHUP) {
                /*
                 * We are re-reading a PGC_POSTMASTER variable from
                 * postgresql.conf.  We can't change the setting, so we should
                 * give a warning if the DBA tries to change it.  However,
                 * because of variant formats, canonicalization by check
                 * hooks, etc, we can't just compare the given string directly
                 * to what's stored.  Set a flag to check below after we have
                 * the final storable value.
                 */
                prohibitValueChange = true;
            } else if (context != PGC_POSTMASTER) {
                ereport(elevel,
                    (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                        errmsg("parameter \"%s\" cannot be changed without restarting the server", name)));
                return 0;
            }

            break;

        case PGC_SIGHUP:
            if (context != PGC_SIGHUP && context != PGC_POSTMASTER) {
                ereport(elevel,
                    (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                        errmsg("parameter \"%s\" cannot be changed now", name)));
                return 0;
            }

            /*
             * Hmm, the idea of the SIGHUP context is "ought to be global, but
             * can be changed after postmaster start". But there's nothing
             * that prevents a crafty administrator from sending SIGHUP
             * signals to individual backends only.
             */
            break;

        case PGC_BACKEND:
            if (context == PGC_SIGHUP) {
                /*
                 * If a PGC_BACKEND parameter is changed in the config file,
                 * we want to accept the new value in the postmaster (whence
                 * it will propagate to subsequently-started backends), but
                 * ignore it in existing backends.	This is a tad klugy, but
                 * necessary because we don't re-read the config file during
                 * backend start.
                 */
                if (IsUnderPostmaster && !isReload)
                    return -1;
            } else if (context != PGC_POSTMASTER && context != PGC_BACKEND && source != PGC_S_CLIENT) {
                ereport(elevel,
                    (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                        errmsg("parameter \"%s\" cannot be set after connection start", name)));
                return 0;
            }

            break;

        case PGC_SUSET:
            if (context == PGC_USERSET || context == PGC_BACKEND) {
                ereport(elevel,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("permission denied to set parameter \"%s\"", name)));
                return 0;
            }
            break;

        case PGC_USERSET:
            /* always okay */
            break;
        default:
            break;
    }

    /*
     * Disallow changing GUC_NOT_WHILE_SEC_REST values if we are inside a
     * security restriction context.  We can reject this regardless of the GUC
     * context or source, mainly because sources that it might be reasonable
     * to override for won't be seen while inside a function.
     *
     * Note: variables marked GUC_NOT_WHILE_SEC_REST should usually be marked
     * GUC_NO_RESET_ALL as well, because ResetAllOptions() doesn't check this.
     * An exception might be made if the reset value is assumed to be "safe".
     *
     * Note: this flag is currently used for "session_authorization" and
     * "role".	We need to prohibit changing these inside a local userid
     * context because when we exit it, GUC won't be notified, leaving things
     * out of sync.  (This could be fixed by forcing a new GUC nesting level,
     * but that would change behavior in possibly-undesirable ways.)  Also, we
     * prohibit changing these in a security-restricted operation because
     * otherwise RESET could be used to regain the session user's privileges.
     */
    if (record->flags & GUC_NOT_WHILE_SEC_REST) {
        if (InLocalUserIdChange()) {
            /*
             * Phrasing of this error message is historical, but it's the most
             * common case.
             */
            ereport(elevel,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("cannot set parameter \"%s\" within security-definer function", name)));
            return 0;
        }

        if (InSecurityRestrictedOperation()) {
            ereport(elevel,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("cannot set parameter \"%s\" within security-restricted operation", name)));
            return 0;
        }
    }

    /*
     * Should we set reset/stacked values?	(If so, the behavior is not
     * transactional.)	This is done either when we get a default value from
     * the database's/user's/client's default settings or when we reset a
     * value to its default.
     */
    makeDefault = changeVal && (source <= PGC_S_OVERRIDE) && ((value != NULL) || source == PGC_S_DEFAULT);

    /*
     * Ignore attempted set if overridden by previously processed setting.
     * However, if changeVal is false then plow ahead anyway since we are
     * trying to find out if the value is potentially good, not actually use
     * it. Also keep going if makeDefault is true, since we may want to set
     * the reset/stacked values even if we can't set the variable itself.
     */
    if (record->source > source) {
        if (changeVal && !makeDefault) {
            ereport(DEBUG3, (errmsg("\"%s\": setting ignored because previous source is higher priority", name)));
            return -1;
        }

        changeVal = false;
    }

    /*
     * Evaluate value and set variable.
     */
    switch (record->vartype) {
        case PGC_BOOL: {
            struct config_bool* conf = (struct config_bool*)record;
            bool newval = false;
            void* newextra = NULL;

            if (value != NULL) {
                if (!validate_conf_option(record, name, value, source, elevel, false, &newval, &newextra)) {
                    return 0;
                }
            } else if (source == PGC_S_DEFAULT) {
                newval = conf->boot_val;

                if (!call_bool_check_hook(conf, &newval, &newextra, source, elevel))
                    return 0;
            } else {
                newval = conf->reset_val;
                newextra = conf->reset_extra;
                source = conf->gen.reset_source;
                context = conf->gen.reset_scontext;
            }

            if (prohibitValueChange) {
                if (*conf->variable != newval) {
                    ereport(elevel,
                        (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                            errmsg("parameter \"%s\" cannot be changed without restarting the server", name)));
                    return 0;
                }

                return -1;
            }

            if (changeVal) {
                /* Save old value to support transaction abort */
                if (!makeDefault)
                    push_old_value(&conf->gen, action);

                if (conf->assign_hook) {
                    if (strcasecmp(record->name, "enable_save_datachanged_timestamp") == 0) {
                        bool set_op = false;
                        if (value != NULL && source == PGC_S_SESSION)
                            set_op = true;
                        (*conf->assign_hook)(newval, &set_op);
                    } else
                        (*conf->assign_hook)(newval, newextra);
                }

                if (!IsUnderPostmaster || conf->gen.context != PGC_POSTMASTER) {
                    *conf->variable = newval;
                }
                set_extra_field(&conf->gen, &conf->gen.extra, newextra);
                conf->gen.source = source;
                conf->gen.scontext = context;
            }

            if (makeDefault) {
                GucStack* stack = NULL;

                if (conf->gen.reset_source <= source) {
                    conf->reset_val = newval;
                    set_extra_field(&conf->gen, &conf->reset_extra, newextra);
                    conf->gen.reset_source = source;
                    conf->gen.reset_scontext = context;
                }

                for (stack = conf->gen.stack; stack; stack = stack->prev) {
                    if (stack->source <= source) {
                        stack->prior.val.boolval = newval;
                        set_extra_field(&conf->gen, &stack->prior.extra, newextra);
                        stack->source = source;
                        stack->scontext = context;
                    }
                }
            }

            /* Perhaps we didn't install newextra anywhere */
            if (newextra && !extra_field_used(&conf->gen, newextra))
                pfree(newextra);

            break;
        }

        case PGC_INT: {
            struct config_int* conf = (struct config_int*)record;
            int newval;
            void* newextra = NULL;

            if (value != NULL) {
                if (!validate_conf_option(record, name, value, source, elevel, false, &newval, &newextra)) {
                    return 0;
                }
            } else if (source == PGC_S_DEFAULT) {
                newval = conf->boot_val;

                if (!call_int_check_hook(conf, &newval, &newextra, source, elevel))
                    return 0;
            } else {
                newval = conf->reset_val;
                newextra = conf->reset_extra;
                source = conf->gen.reset_source;
                context = conf->gen.reset_scontext;
            }

            if (prohibitValueChange) {
                if (*conf->variable != newval) {
                    ereport(elevel,
                        (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                            errmsg("parameter \"%s\" cannot be changed without restarting the server", name)));
                    return 0;
                }

                return -1;
            }

            if (changeVal) {
                /* Save old value to support transaction abort */
                if (!makeDefault)
                    push_old_value(&conf->gen, action);

                u_sess->utils_cxt.guc_new_value = &newval;

                if (conf->assign_hook)
                    (*conf->assign_hook)(newval, newextra);

                if (!IsUnderPostmaster || conf->gen.context != PGC_POSTMASTER) {
                    *conf->variable = newval;
                }
                set_extra_field(&conf->gen, &conf->gen.extra, newextra);
                conf->gen.source = source;
                conf->gen.scontext = context;
                u_sess->utils_cxt.guc_new_value = NULL;
            }

            if (makeDefault) {
                GucStack* stack = NULL;

                if (conf->gen.reset_source <= source) {
                    conf->reset_val = newval;
                    set_extra_field(&conf->gen, &conf->reset_extra, newextra);
                    conf->gen.reset_source = source;
                    conf->gen.reset_scontext = context;
                }

                for (stack = conf->gen.stack; stack; stack = stack->prev) {
                    if (stack->source <= source) {
                        stack->prior.val.intval = newval;
                        set_extra_field(&conf->gen, &stack->prior.extra, newextra);
                        stack->source = source;
                        stack->scontext = context;
                    }
                }
            }

            /* Perhaps we didn't install newextra anywhere */
            if (newextra && !extra_field_used(&conf->gen, newextra))
                pfree(newextra);

            break;
        }

        case PGC_INT64: {
            struct config_int64* conf = (struct config_int64*)record;
            int64 newval;
            void* newextra = NULL;

            if (value != NULL) {
                if (!validate_conf_option(record, name, value, source, elevel, false, &newval, &newextra)) {
                    return 0;
                }
            } else if (source == PGC_S_DEFAULT) {
                newval = conf->boot_val;
                if (!call_int64_check_hook(conf, &newval, &newextra, source, elevel))
                    return 0;
            } else {
                newval = conf->reset_val;
                newextra = conf->reset_extra;
                source = conf->gen.reset_source;
                context = conf->gen.reset_scontext;
            }

            if (prohibitValueChange) {
                if (*conf->variable != newval) {
                    ereport(elevel,
                        (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                            errmsg("parameter \"%s\" cannot be changed without restarting the server", name)));
                    return 0;
                }
                return -1;
            }

            if (changeVal) {
                /* Save old value to support transaction abort */
                if (!makeDefault)
                    push_old_value(&conf->gen, action);

                if (conf->assign_hook)
                    (*conf->assign_hook)(newval, newextra);

                if (!IsUnderPostmaster || conf->gen.context != PGC_POSTMASTER) {
                    *conf->variable = newval;
                }
                set_extra_field(&conf->gen, &conf->gen.extra, newextra);
                conf->gen.source = source;
                conf->gen.scontext = context;
            }
            if (makeDefault) {
                GucStack* stack = NULL;

                if (conf->gen.reset_source <= source) {
                    conf->reset_val = newval;
                    set_extra_field(&conf->gen, &conf->reset_extra, newextra);
                    conf->gen.reset_source = source;
                    conf->gen.reset_scontext = context;
                }
                for (stack = conf->gen.stack; stack; stack = stack->prev) {
                    if (stack->source <= source) {
                        stack->prior.val.intval = newval;
                        set_extra_field(&conf->gen, &stack->prior.extra, newextra);
                        stack->source = source;
                        stack->scontext = context;
                    }
                }
            }

            /* Perhaps we didn't install newextra anywhere */
            if (newextra && !extra_field_used(&conf->gen, newextra)) {
                pfree(newextra);
                newextra = NULL;
            }
            break;
        }

        case PGC_REAL: {
            struct config_real* conf = (struct config_real*)record;
            double newval;
            void* newextra = NULL;

            if (value != NULL) {
                if (!validate_conf_option(record, name, value, source, elevel, false, &newval, &newextra)) {
                    return 0;
                }
            } else if (source == PGC_S_DEFAULT) {
                newval = conf->boot_val;

                if (!call_real_check_hook(conf, &newval, &newextra, source, elevel))
                    return 0;
            } else {
                newval = conf->reset_val;
                newextra = conf->reset_extra;
                source = conf->gen.reset_source;
                context = conf->gen.reset_scontext;
            }

            if (prohibitValueChange) {
                if (*conf->variable != newval) {
                    ereport(elevel,
                        (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                            errmsg("parameter \"%s\" cannot be changed without restarting the server", name)));
                    return 0;
                }

                return -1;
            }

            if (changeVal) {
                /* Save old value to support transaction abort */
                if (!makeDefault)
                    push_old_value(&conf->gen, action);

                if (conf->assign_hook)
                    (*conf->assign_hook)(newval, newextra);

                if (!IsUnderPostmaster || conf->gen.context != PGC_POSTMASTER) {
                    *conf->variable = newval;
                }
                set_extra_field(&conf->gen, &conf->gen.extra, newextra);
                conf->gen.source = source;
                conf->gen.scontext = context;
            }

            if (makeDefault) {
                GucStack* stack = NULL;

                if (conf->gen.reset_source <= source) {
                    conf->reset_val = newval;
                    set_extra_field(&conf->gen, &conf->reset_extra, newextra);
                    conf->gen.reset_source = source;
                    conf->gen.reset_scontext = context;
                }

                for (stack = conf->gen.stack; stack; stack = stack->prev) {
                    if (stack->source <= source) {
                        stack->prior.val.realval = newval;
                        set_extra_field(&conf->gen, &stack->prior.extra, newextra);
                        stack->source = source;
                        stack->scontext = context;
                    }
                }
            }

            /* Perhaps we didn't install newextra anywhere */
            if (newextra && !extra_field_used(&conf->gen, newextra))
                pfree(newextra);

            break;
        }

        case PGC_STRING: {
            struct config_string* conf = (struct config_string*)record;
            char* newval = NULL;
            void* newextra = NULL;
            bool is_reset_val = false;
            if (u_sess->catalog_cxt.overrideStack &&
                (strcasecmp(name, SEARCH_PATH_GUC_NAME) == 0 || strcasecmp(name, CURRENT_SCHEMA_GUC_NAME) == 0)) {
                /*
                 * set search_path or current_schema inside stored procedure is invalid,
                 * return directly.
                 */
                OverrideStackEntry* entry = NULL;
                entry = (OverrideStackEntry*)linitial(u_sess->catalog_cxt.overrideStack);
                if (entry->inProcedure)
                    return -1;
            }

            if (value != NULL) {
                /*
                 * The value passed by the caller could be transient, so we always strdup it.
                 * At the same time, we need to make sure newval is correctly initialized and ready
                 * for validation below.
                 */
                newval = guc_strdup(elevel, value);

                if (newval == NULL) {
                    return 0;
                }

                if (!validate_conf_option(record, name, value, source, elevel, false, &newval, &newextra)) {
                    pfree_ext(newval);
                    return 0;
                }

                /*
                 * when \parallel in gsql is on, we use "set search_path to pg_temp_XXX"
                 * to tell new started parallel openGauss which temp namespace should use.
                 * but we should not clean the temp namespace when parallel openGauss
                 * quiting, so a flag deleteTempOnQuiting is set to prevent clean
                 * temp namespace when openGauss quiting.
                 */
                if ((strcasecmp(name, "search_path") == 0 || strcasecmp(name, "current_schema") == 0) &&
                    (isTempNamespaceName(value) || isTempNamespaceNameWithQuote(value))) {

                    char* schemaNameList = getSchemaNameList(value);
                    pfree_ext(newval);  /* this is guaranteed to be set or freed, we need to make room for strdup */
                    if (schemaNameList == NULL) {
                        return 1;
                    } else {
                        newval = guc_strdup(elevel, schemaNameList);
                        pfree(schemaNameList);
                    }
                }
            } else if (source == PGC_S_DEFAULT) {
                /* non-NULL boot_val must always get strdup'd */
                if (conf->boot_val != NULL) {
                    newval = guc_strdup(elevel, conf->boot_val);

                    if (newval == NULL)
                        return 0;
                } else
                    newval = NULL;

                if (!call_string_check_hook(conf, &newval, &newextra, source, elevel)) {
                    pfree(newval);
                    return 0;
                }
            } else {
                /*
                 * strdup not needed, since reset_val is already under
                 * guc.c's control
                 */
                newval = conf->reset_val;
                newextra = conf->reset_extra;
                source = conf->gen.reset_source;
                context = conf->gen.reset_scontext;
                is_reset_val = true;
            }

            if (prohibitValueChange) {
                if (!is_reset_val) {
                    pfree_ext(newval);
                }
                /* newval shouldn't be NULL, so we're a bit sloppy here */
                if (*conf->variable == NULL || newval == NULL || strcmp(*conf->variable, newval) != 0) {
                    ereport(elevel,
                        (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                            errmsg("parameter \"%s\" cannot be changed without restarting the server", name)));
                    return 0;
                }

                return -1;
            }

            if (changeVal) {
                /* Save old value to support transaction abort */
                if (!makeDefault)
                    push_old_value(&conf->gen, action);

                if (conf->assign_hook)
                    (*conf->assign_hook)(newval, newextra);

                if (!IsUnderPostmaster || conf->gen.context != PGC_POSTMASTER) {
                    set_string_field(conf, conf->variable, newval);
                }
                set_extra_field(&conf->gen, &conf->gen.extra, newextra);
                conf->gen.source = source;
                conf->gen.scontext = context;
            }

            if (makeDefault) {
                GucStack* stack = NULL;

                if (conf->gen.reset_source <= source) {
                    set_string_field(conf, &conf->reset_val, newval);
                    set_extra_field(&conf->gen, &conf->reset_extra, newextra);
                    conf->gen.reset_source = source;
                    conf->gen.reset_scontext = context;
                }

                for (stack = conf->gen.stack; stack; stack = stack->prev) {
                    if (stack->source <= source) {
                        set_string_field(conf, &stack->prior.val.stringval, newval);
                        set_extra_field(&conf->gen, &stack->prior.extra, newextra);
                        stack->source = source;
                        stack->scontext = context;
                    }
                }
            }

            /* Perhaps we didn't install newval anywhere */
            if (newval && !string_field_used(conf, newval))
                pfree(newval);

            /* Perhaps we didn't install newextra anywhere */
            if (newextra && !extra_field_used(&conf->gen, newextra))
                pfree(newextra);

            break;
        }

        case PGC_ENUM: {
            struct config_enum* conf = (struct config_enum*)record;
            int newval;
            void* newextra = NULL;

            if (value != NULL) {
                if (!validate_conf_option(record, name, value, source, elevel, false, &newval, &newextra)) {
                    return 0;
                }
            } else if (source == PGC_S_DEFAULT) {
                newval = conf->boot_val;

                if (!call_enum_check_hook(conf, &newval, &newextra, source, elevel))
                    return 0;
            } else {
                newval = conf->reset_val;
                newextra = conf->reset_extra;
                source = conf->gen.reset_source;
                context = conf->gen.reset_scontext;
            }

            if (prohibitValueChange) {
                if (*conf->variable != newval) {
                    ereport(elevel,
                        (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                            errmsg("parameter \"%s\" cannot be changed without restarting the server", name)));
                    return 0;
                }

                return -1;
            }

            if (changeVal) {
                /* Save old value to support transaction abort */
                if (!makeDefault)
                    push_old_value(&conf->gen, action);

                if (conf->assign_hook)
                    (*conf->assign_hook)(newval, newextra);

                if (!IsUnderPostmaster || conf->gen.context != PGC_POSTMASTER) {
                    *conf->variable = newval;
                }
                set_extra_field(&conf->gen, &conf->gen.extra, newextra);
                conf->gen.source = source;
                conf->gen.scontext = context;
            }

            if (makeDefault) {
                GucStack* stack = NULL;

                if (conf->gen.reset_source <= source) {
                    conf->reset_val = newval;
                    set_extra_field(&conf->gen, &conf->reset_extra, newextra);
                    conf->gen.reset_source = source;
                    conf->gen.reset_scontext = context;
                }

                for (stack = conf->gen.stack; stack; stack = stack->prev) {
                    if (stack->source <= source) {
                        stack->prior.val.enumval = newval;
                        set_extra_field(&conf->gen, &stack->prior.extra, newextra);
                        stack->source = source;
                        stack->scontext = context;
                    }
                }
            }

            /* Perhaps we didn't install newextra anywhere */
            if (newextra && !extra_field_used(&conf->gen, newextra))
                pfree(newextra);

            break;
        }
        default:
            break;
    }

    if (changeVal && (record->flags & GUC_REPORT))
        ReportGUCOption(record);

    SetThreadLocalGUC(u_sess);

    return changeVal ? 1 : -1;
}

/*
 * Set the fields for source file and line number the setting came from.
 */
static void set_config_sourcefile(const char* name, char* sourcefile, int sourceline)
{
    struct config_generic* record = NULL;
    int elevel;

    /*
     * To avoid cluttering the log, only the postmaster bleats loudly about
     * problems with the config file.
     */
    elevel = IsUnderPostmaster ? DEBUG3 : LOG;

    record = find_option(name, true, elevel);

    /* should not happen */
    if (record == NULL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("unrecognized configuration parameter \"%s\"", name)));

    sourcefile = guc_strdup(elevel, sourcefile);

    if (record->sourcefile)
        pfree(record->sourcefile);

    record->sourcefile = sourcefile;
    record->sourceline = sourceline;
}

/*
 * Set a config option to the given value.
 *
 * See also set_config_option; this is just the wrapper to be called from
 * outside GUC.  (This function should be used when possible, because its API
 * is more stable than set_config_option's.)
 *
 * Note: there is no support here for setting source file/line, as it
 * is currently not needed.
 */
void SetConfigOption(const char* name, const char* value, GucContext context, GucSource source)
{
    (void)set_config_option(name, value, context, source, GUC_ACTION_SET, true, 0);
}

/*
 * Fetch the current value of the option `name', as a string.
 *
 * If the option doesn't exist, return NULL if missing_ok is true (NOTE that
 * this cannot be distinguished from a string variable with a NULL value!),
 * otherwise throw an ereport and don't return.
 *
 * If restrict_superuser is true, we also enforce that only superusers can
 * see GUC_SUPERUSER_ONLY variables.  This should only be passed as true
 * in user-driven calls.
 *
 * The string is *not* allocated for modification and is really only
 * valid until the next call to configuration related functions.
 */
const char* GetConfigOption(const char* name, bool missing_ok, bool restrict_superuser)
{
    struct config_generic* record = NULL;
    char* buffer = t_thrd.buf_cxt.config_opt_buf;
    int size = sizeof(t_thrd.buf_cxt.config_opt_buf);
    int rc = 0;

    record = find_option(name, false, ERROR);

    if (record == NULL) {
        if (missing_ok)
            return NULL;

        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("unrecognized configuration parameter \"%s\"", name)));
    }

    if (restrict_superuser && (record->flags & GUC_SUPERUSER_ONLY) && !superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to examine \"%s\"", name)));


    switch (record->vartype) {
        case PGC_BOOL:
            return *((struct config_bool*)record)->variable ? "on" : "off";

        case PGC_INT:
            rc = snprintf_s(buffer, size, size - 1, "%d", *((struct config_int*)record)->variable);
            securec_check_ss(rc, "\0", "\0");
            return buffer;

        case PGC_INT64:
            rc = snprintf_s(buffer, size, size - 1, INT64_FORMAT, *((struct config_int64*)record)->variable);
            securec_check_ss(rc, "\0", "\0");
            return buffer;

        case PGC_REAL:
            rc = snprintf_s(buffer, size, size - 1, "%g", *((struct config_real*)record)->variable);
            securec_check_ss(rc, "\0", "\0");
            return buffer;

        case PGC_STRING:
            return *((struct config_string*)record)->variable;

        case PGC_ENUM:
            return config_enum_lookup_by_value((struct config_enum*)record, *((struct config_enum*)record)->variable);
        default:
            break;
    }

    return NULL;
}

/*
 * Get the RESET value associated with the given option.
 *
 * Note: this is not re-entrant, due to use of static THR_LOCAL result buffer;
 * not to mention that a string variable could have its reset_val changed.
 * Beware of assuming the result value is good for very long.
 */
const char* GetConfigOptionResetString(const char* name)
{
    struct config_generic* record = NULL;
    char* buffer = t_thrd.buf_cxt.config_opt_reset_buf;
    int size = sizeof(t_thrd.buf_cxt.config_opt_reset_buf);
    int rc = 0;

    record = find_option(name, false, ERROR);

    if (record == NULL)
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("unrecognized configuration parameter \"%s\"", name)));
    if ((record->flags & GUC_SUPERUSER_ONLY) && !superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to examine \"%s\"", name)));

    switch (record->vartype) {
        case PGC_BOOL:
            return ((struct config_bool*)record)->reset_val ? "on" : "off";

        case PGC_INT:
            rc = snprintf_s(buffer, size, size - 1, "%d", ((struct config_int*)record)->reset_val);
            securec_check_ss(rc, "\0", "\0");
            return buffer;

        case PGC_INT64:
            rc = snprintf_s(buffer, size, size - 1, INT64_FORMAT, ((struct config_int64*)record)->reset_val);
            securec_check_ss(rc, "\0", "\0");
            return buffer;

        case PGC_REAL:
            rc = snprintf_s(buffer, size, size - 1, "%g", ((struct config_real*)record)->reset_val);
            securec_check_ss(rc, "\0", "\0");
            return buffer;

        case PGC_STRING:
            return ((struct config_string*)record)->reset_val;

        case PGC_ENUM:
            return config_enum_lookup_by_value((struct config_enum*)record, ((struct config_enum*)record)->reset_val);
    }

    return NULL;
}

/*
 * flatten_set_variable_args
 *		Given a parsenode List as emitted by the grammar for SET,
 *		convert to the flat string representation used by GUC.
 *
 * We need to be told the name of the variable the args are for, because
 * the flattening rules vary (ugh).
 *
 * The result is NULL if args is NIL (ie, SET ... TO DEFAULT), otherwise
 * a palloc'd string.
 */
static char* flatten_set_variable_args(const char* name, List* args)
{
    struct config_generic* record = NULL;
    unsigned int flags;
    StringInfoData buf;
    ListCell* l = NULL;

    /* Fast path if just DEFAULT */
    if (args == NIL)
        return NULL;

    /*
     * Get flags for the variable; if it's not known, use default flags.
     * (Caller might throw error later, but not our business to do so here.)
     */
    record = find_option(name, false, WARNING);

    if (record != NULL)
        flags = (unsigned int)record->flags;
    else
        flags = 0;

    /* Complain if list input and non-list variable */
    if ((flags & GUC_LIST_INPUT) == 0 && list_length(args) != 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("SET %s takes only one argument", name)));

    initStringInfo(&buf);

    /*
     * Each list member may be a plain A_Const node, or an A_Const within a
     * TypeCast; the latter case is supported only for ConstInterval arguments
     * (for SET TIME ZONE).
     */
    foreach (l, args) {
        Node* arg = (Node*)lfirst(l);
        char* val = NULL;
        TypeName* typname = NULL;
        A_Const* con = NULL;

        if (l != list_head(args))
            appendStringInfo(&buf, ", ");

        if (IsA(arg, TypeCast)) {
            TypeCast* tc = (TypeCast*)arg;

            arg = tc->arg;
            typname = tc->typname;
        }

        if (!IsA(arg, A_Const))
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("unrecognized node type: %d", (int)nodeTag(arg))));

        con = (A_Const*)arg;

        switch (nodeTag(&con->val)) {
            case T_Integer:
                appendStringInfo(&buf, "%ld", intVal(&con->val));
                break;

            case T_Float:
                /* represented as a string, so just copy it */
                appendStringInfoString(&buf, strVal(&con->val));
                break;

            case T_String:
            case T_Null:
                if (nodeTag(&con->val) == T_String)
                    val = strVal(&con->val);
                else
                    val = "";

                if (typname != NULL) {
                    /*
                     * Must be a ConstInterval argument for TIME ZONE. Coerce
                     * to interval and back to normalize the value and account
                     * for any typmod.
                     */
                    Oid typoid;
                    int32 typmod;
                    Datum interval;
                    char* intervalout = NULL;

                    typenameTypeIdAndMod(NULL, typname, &typoid, &typmod);
                    Assert(typoid == INTERVALOID);

                    interval = DirectFunctionCall3(
                        interval_in, CStringGetDatum(val), ObjectIdGetDatum(InvalidOid), Int32GetDatum(typmod));

                    intervalout = DatumGetCString(DirectFunctionCall1(interval_out, interval));
                    appendStringInfo(&buf, "INTERVAL '%s'", intervalout);
                } else {
                    /*
                     * Plain string literal or identifier.	For quote mode,
                     * quote it if it's not a vanilla identifier.
                     */
                    if (flags & GUC_LIST_QUOTE)
                        appendStringInfoString(&buf, quote_identifier(val));
                    else
                        appendStringInfoString(&buf, val);
                }

                break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("unrecognized node type: %d", (int)nodeTag(&con->val))));
                break;
        }
    }

    return buf.data;
}

#ifndef ENABLE_MULTIPLE_NODES
/*
 * This function takes list of all configuration parameters in
 * postgresql.conf and parameter to be updated as input arguments and
 * replace the updated configuration parameter value in a list. If the
 * parameter to be updated is new then it is appended to the list of
 * parameters.
 */
static void replace_config_value(char** optlines, char* name, char* value, config_type vartype)
{
    Assert(optlines != NULL);

    int index = 0;
    int optvalue_off = 0;
    int optvalue_len = 0;
    char* newline = (char*)pg_malloc(MAX_PARAM_LEN);
    int rc = 0;

    switch (vartype) {
        case PGC_BOOL:
        case PGC_INT:
        case PGC_INT64:
        case PGC_REAL:
        case PGC_ENUM:
            rc = snprintf_s(newline, MAX_PARAM_LEN, MAX_PARAM_LEN - 1, "%s = %s\n", name, value);
            break;
        case PGC_STRING:
            rc = snprintf_s(newline, MAX_PARAM_LEN, MAX_PARAM_LEN - 1, "%s = '%s'\n", name, value);
            break;
    }
    securec_check_ss(rc, "\0", "\0");

    index = find_guc_option(optlines, name, NULL, NULL, &optvalue_off, &optvalue_len, true);

    /* add or replace */
    if (index == INVALID_LINES_IDX) {
        /* find the first NULL point reserved. */
        for (index = 0; optlines[index] != NULL; index++);

    } else {
        /* copy comment to newline and pfree old line */
        rc = strncpy_s(newline + rc - 1, MAX_PARAM_LEN - rc,
                       optlines[index] + optvalue_off + optvalue_len, MAX_PARAM_LEN - rc - 1);
        securec_check(rc, "\0", "\0");
        if (optlines[index] != NULL) {
            pfree(optlines[index]);
            optlines[index] = NULL;
        }

    }

    optlines[index] = newline;
}

/*
 * Only the administrator can modify the GUC.
 * gs_guc can only modify GUC locally, and Alter System Set is a supplement.
 * But in the case of remote connection we cannot change some param,
 * otherwise, there will be some security risks.
 */
static void CheckAlterSystemSetPrivilege(const char* name)
{
    if (GetUserId() == BOOTSTRAP_SUPERUSERID) {
        return;
    } else if (superuser()) {
        /* do nothing here, check black list later. */
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg("Permission denied, must be sysadmin to execute ALTER SYSTEM SET command."))));
    }

    static char* blackList[] = {
        "audit_copy_exec", "audit_data_format","audit_database_process", "audit_directory", "audit_dml_state",
        "audit_dml_state_select", "audit_enabled", "audit_file_remain_threshold", "audit_file_remain_time",
        "audit_function_exec", "audit_grant_revoke", "audit_login_logout", "audit_resource_policy",
        "audit_rotation_interval", "audit_rotation_size", "audit_set_parameter", "audit_space_limit",
        "audit_system_object", "audit_user_locked", "audit_user_violation",
        "asp_log_directory", "config_file", "data_directory", "enable_access_server_directory",
        "enable_copy_server_files", "external_pid_file", "hba_file", "ident_file", "log_directory", "perf_directory",
        "query_log_directory", "ssl_ca_file", "ssl_cert_file", "ssl_crl_file", "ssl_key_file", "stats_temp_directory",
        "unix_socket_directory", "unix_socket_group", "unix_socket_permissions",
        "krb_caseins_users", "krb_server_keyfile", "krb_srvname", "allow_system_table_mods", "enableSeparationOfDuty",
        "modify_initial_password", "password_encryption_type", "password_policy", "audit_xid_info",
        NULL
    };
    for (int i = 0; blackList[i] != NULL; i++) {
        if (pg_strcasecmp(name, blackList[i]) == 0) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    (errmsg("GUC:%s could only be set by initial user.", name))));
        }
    }
}

/*
 * Persist the configuration parameter value.
 *
 * This function takes all previous configuration parameters
 * set by ALTER SYSTEM command and the currently set ones
 * and write them all to the automatic configuration file.
 *
 * The configuration parameters are written to a temporary
 * file then renamed to the final name. The template for the
 * temporary file is postgresql.auto.conf.temp.
 *
 * An LWLock is used to serialize writing to the same file.
 *
 * In case of an error, we leave the original automatic
 * configuration file (postgresql.auto.conf) intact.
 */
static void CheckAndGetAlterSystemSetParam(AlterSystemStmt* altersysstmt,
    char** outer_name, char** outer_value, struct config_generic** outer_record)
{
    void* newextra = NULL;
    char* value = NULL;
    char* name = NULL;
    struct config_generic *record = NULL;

    CheckAlterSystemSetPrivilege(altersysstmt->setstmt->name);

    /*
     * Validate the name and arguments [value1, value2 ... ].
     */
    name = altersysstmt->setstmt->name;
    switch (altersysstmt->setstmt->kind) {
        case VAR_SET_VALUE:
            value = ExtractSetVariableArgs(altersysstmt->setstmt);
            break;
        case VAR_SET_DEFAULT:
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                 (errmsg("ALTER SYSTEM SET does not support 'set to default'."))));
        default:
            elog(ERROR, "unrecognized ALTER SYSTEM SET stmt type: %d",
                 altersysstmt->setstmt->kind);
    }

    record = find_option(name, false, LOG);
    if (record == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("unrecognized configuration parameter \"%s\"", name)));

    if ((record->context != PGC_POSTMASTER && record->context != PGC_SIGHUP && record->context != PGC_BACKEND) ||
        record->flags & GUC_DISALLOW_IN_FILE)
        ereport(ERROR,
            (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
             errmsg("unsupport parameter: %s\n"
                    "ALTER SYSTEM SET only support POSTMASTER-level, SIGHUP-level and BACKEND-level guc variable,\n"
                    "and it must be allowed to set in postgresql.conf.", name)));

    if (!validate_conf_option(record, name, value, PGC_S_FILE, ERROR, true, NULL, &newextra)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid value for parameter \"%s\": \"%s\"", name, value)));
    }

    *outer_name = name;
    *outer_value = value;
    *outer_record = record;
}

/*
 * rename ConfTmpBakFileName to ConfFileName.

 * send signal to postmaster to reload postgresql.conf, POSTMASTER-level variable
 * takes effect only after restarting the database, but we still need to reload so
 * that we can send the changes to standby node.
 *
 * finally print a notice message.
 */
static void FinishAlterSystemSet(GucContext context)
{
    if (gs_signal_send(PostmasterPid, SIGHUP)) {
        ereport(WARNING, (errmsg("Failed to send signal to postmaster to reload guc config file.")));
        return;
    }

    switch (context) {
        case PGC_POSTMASTER:
            ereport(NOTICE,
                    (errmsg("please restart the database for the POSTMASTER level parameter to take effect.")));
            break;
        case PGC_BACKEND:
            ereport(NOTICE,
                    (errmsg("please reconnect the database for the BACKEND level parameter to take effect.")));
            break;
        default:
            break;
    }
}

static void ConfFileNameCat(char* ConfFileName, char* ConfTmpFileName,
    char* ConfTmpBakFileName, char* ConfLockFileName)
{
    errno_t rc;

    rc = snprintf_s(ConfFileName, MAXPGPATH, MAXPGPATH - 1, "%s/%s", t_thrd.proc_cxt.DataDir, "postgresql.conf");
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(ConfTmpFileName, MAXPGPATH, MAXPGPATH - 1,
                    "%s/%s", t_thrd.proc_cxt.DataDir, "postgresql.conf.bak");
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(ConfLockFileName, MAXPGPATH, MAXPGPATH - 1,
                    "%s/%s", t_thrd.proc_cxt.DataDir, "postgresql.conf.lock");
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(ConfTmpBakFileName, MAXPGPATH, MAXPGPATH - 1,
                    "%s/%s", t_thrd.proc_cxt.DataDir, "postgresql.conf.bak_bak");
    securec_check_ss(rc, "\0", "\0");

}

static void WriteAlterSystemSetGucFile(char* ConfFileName, char** opt_lines, ConfFileLock* filelock)
{
    ErrCode ret;

    ret = write_guc_file(ConfFileName, opt_lines);
    release_opt_lines(opt_lines);
    opt_lines = NULL;
    if (ret != CODE_OK) {
        release_file_lock(filelock);
        ereport(ERROR,
                (errcode(ERRCODE_FILE_WRITE_FAILED),
                 errmsg("write %s failed: %s.", ConfFileName, gs_strerror(ret))));
    }
}

static char** LockAndReadConfFile(char* ConfFileName, char* ConfTmpFileName, char* ConfLockFileName,
    ConfFileLock* filelock)
{
    struct stat st;
    char** opt_lines = NULL;
    char* file = NULL;

    if (stat(ConfFileName, &st) == 0) {
        file = ConfFileName;
    } else if (stat(ConfTmpFileName, &st) == 0) {
        file = ConfTmpFileName;
    }

    if (file != NULL && S_ISREG(st.st_mode) && get_file_lock(file, filelock) == CODE_OK) {
        opt_lines = read_guc_file(file);
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_FILE_READ_FAILED), errmsg("Can not open configure file.")));
    }

    if (opt_lines == NULL) {
        release_file_lock(filelock);
        ereport(ERROR, (errcode(ERRCODE_FILE_READ_FAILED), errmsg("Read configure file falied.")));
    }

    return opt_lines;
}

/*
 * Persist the configuration parameter value.
 *
 * This function takes all previous configuration parameters
 * set by ALTER SYSTEM command and the currently set ones
 * and write them all to the automatic configuration file.
 *
 * The configuration parameters are written to a temporary
 * file then renamed to the final name. The template for the
 * temporary file is postgresql.conf.bak
 *
 * An LWLock is used to serialize writing to the same file.
 *
 * In case of an error, we leave the original automatic
 * configuration file (postgresql.conf.bak) intact.
 */
void AlterSystemSetConfigFile(AlterSystemStmt * altersysstmt)
{
    char*  name = NULL;
    char*  value = NULL;
    char   ConfFileName[MAXPGPATH];
    char   ConfTmpFileName[MAXPGPATH];
    char   ConfTmpBakFileName[MAXPGPATH];
    char   ConfLockFileName[MAXPGPATH];
    char** opt_lines = NULL;
    struct config_generic *record = NULL;

    ConfFileLock filelock = {NULL, 0};

    CheckAndGetAlterSystemSetParam(altersysstmt, &name, &value, &record);

    ConfFileNameCat(ConfFileName, ConfTmpFileName, ConfTmpBakFileName, ConfLockFileName);

    /*
     * one backend is allowed to operate on postgresql.conf.bak file, to
     * ensure that we need to update the contents of the file with
     * ConfFileLock. To ensure crash safety, first the contents are written to
     * temporary file and then rename it to postgresql.auto.conf. In case
     * there exists a temp file from previous crash, that can be reused.
     */
    opt_lines = LockAndReadConfFile(ConfFileName, ConfTmpFileName, ConfLockFileName, &filelock);

    /*
     * replace with new value if the configuration parameter already
     * exists OR add it as a new cofiguration parameter in the file.
     * and we should leave postgresql.conf.bak like gs_guc.
     */
    replace_config_value(opt_lines, name, value, record->vartype);
    WriteAlterSystemSetGucFile(ConfTmpFileName, opt_lines, &filelock);
    opt_lines = read_guc_file(ConfTmpFileName);
    WriteAlterSystemSetGucFile(ConfTmpBakFileName, opt_lines, &filelock);
    opt_lines = NULL;

    /*
     * As the rename is atomic operation, if any problem occurs after this
     * at max it can loose the parameters set by last ALTER SYSTEM SET
     * command.
     */
    if (rename(ConfTmpBakFileName, ConfFileName) < 0) {
        release_file_lock(&filelock);
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not rename file \"%s\" to \"%s\"", ConfTmpFileName, ConfFileName)));
    }

    release_file_lock(&filelock);

    FinishAlterSystemSet(record->context);
}
#endif

/*
 * SET command
 */
void ExecSetVariableStmt(VariableSetStmt* stmt)
{
    GucAction action = stmt->is_local ? GUC_ACTION_LOCAL : GUC_ACTION_SET;

    A_Const* arg = NULL;
    char* role = NULL;
    char* passwd = NULL;
    ListCell* phead = NULL;

    switch (stmt->kind) {
        case VAR_SET_VALUE:
        case VAR_SET_CURRENT:

            (void)set_config_option(stmt->name,
                ExtractSetVariableArgs(stmt),
                ((superuser() || (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) ?
                    PGC_SUSET : PGC_USERSET),
                PGC_S_SESSION,
                action,
                true,
                0);

            // switch the tablespace to the one with the same name as schema
            // when we switch the current schema with search_path
            if (!pg_strcasecmp(stmt->name, "search_path") || !pg_strcasecmp(stmt->name, "current_schema")) {
                char* var = NULL;
                char* spcname = NULL;

                if (!pg_strcasecmp(stmt->name, "search_path")) {
                    var = "current_schema";
                } else {
                    var = "search_path";
                }

                (void)set_config_option(var,
                    ExtractSetVariableArgs(stmt),
                    ((superuser() || (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) ?
                        PGC_SUSET : PGC_USERSET),
                    PGC_S_SESSION,
                    action,
                    true,
                    0);
                /* must not be null */
                if (!PointerIsValid(stmt->args)) {
                    break;
                }

                /* get parameter as string */
                spcname = ExtractSetVariableArgs(stmt);

                if (spcname != NULL) {
                    ListCell *item = NULL;
                    bool hasAccessFirstItem = false;
                    foreach(item, stmt->args) {
                        if (!hasAccessFirstItem) {
                            hasAccessFirstItem = true;
                            continue;
                        }

                        A_Const *con = castNode(A_Const, lfirst(item));
                        if (!IsA(&con->val, String)) {
                            continue;
                        }
                        char *val = strVal(&con->val);
                        if (IS_TEMP_NAMESPACE(val) || IS_CATALOG_NAMESPACE(val)) {
                            ereport(WARNING,
                                   (errmsg("It is invalid to set pg_temp or pg_catalog behind other schemas "
                                           "in search path explicitly. The priority order is pg_temp, pg_catalog"
                                           " and other schemas.")));
                            break;
                        }
                    }
                }

                if (SUPPORT_BIND_TABLESPACE) {
                    /* must be one item */
                    if (1 != stmt->args->length) {
                        break;
                    }

                    /* get the tablespace id that has the same name as parameter */
                    if (OidIsValid(get_tablespace_oid(spcname, true))) {
                        (void)set_config_option("default_tablespace",
                            spcname,
                            ((superuser() || (isOperatoradmin(GetUserId()) &&
                                u_sess->attr.attr_security.operation_mode)) ? PGC_SUSET : PGC_USERSET),
                            PGC_S_SESSION,
                            action,
                            true,
                            0);
                    }
                }
            }
            break;

        case VAR_SET_MULTI:

            /*
             * Special-case SQL syntaxes.  The TRANSACTION and SESSION
             * CHARACTERISTICS cases effectively set more than one variable
             * per statement.  TRANSACTION SNAPSHOT only takes one argument,
             * but we put it here anyway since it's a special case and not
             * related to any GUC variable.
             */
            if (strcmp(stmt->name, "TRANSACTION") == 0) {
                ListCell* head = NULL;

                foreach (head, stmt->args) {
                    DefElem* item = (DefElem*)lfirst(head);

                    if (strcmp(item->defname, "transaction_isolation") == 0)
                        SetPGVariable("transaction_isolation", list_make1(item->arg), stmt->is_local);
                    else if (strcmp(item->defname, "transaction_read_only") == 0)
                        SetPGVariable("transaction_read_only", list_make1(item->arg), stmt->is_local);
                    else if (strcmp(item->defname, "transaction_deferrable") == 0)
                        SetPGVariable("transaction_deferrable", list_make1(item->arg), stmt->is_local);
                    else
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OPERATION),
                                errmsg("unexpected SET TRANSACTION element: %s", item->defname)));
                }
            } else if (strcmp(stmt->name, "SESSION CHARACTERISTICS") == 0) {
                ListCell* head = NULL;

                foreach (head, stmt->args) {
                    DefElem* item = (DefElem*)lfirst(head);

                    if (strcmp(item->defname, "transaction_isolation") == 0)
                        SetPGVariable("default_transaction_isolation", list_make1(item->arg), stmt->is_local);
                    else if (strcmp(item->defname, "transaction_read_only") == 0)
                        SetPGVariable("default_transaction_read_only", list_make1(item->arg), stmt->is_local);
                    else if (strcmp(item->defname, "transaction_deferrable") == 0)
                        SetPGVariable("default_transaction_deferrable", list_make1(item->arg), stmt->is_local);
                    else
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OPERATION),
                                errmsg("unexpected SET SESSION element: %s", item->defname)));
                }
            } else if (strcmp(stmt->name, "TRANSACTION SNAPSHOT") == 0) {
                A_Const* con = (A_Const*)linitial(stmt->args);

                if (stmt->is_local)
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("SET LOCAL TRANSACTION SNAPSHOT is not implemented")));

                Assert(IsA(con, A_Const));
                Assert(nodeTag(&con->val) == T_String || (nodeTag(&con->val) == T_Null && strVal(&con->val) != NULL));
                ImportSnapshot(strVal(&con->val));
            } else
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OPERATION), errmsg("unexpected SET MULTI element: %s", stmt->name)));

            break;

        case VAR_SET_ROLEPWD:
            if ((NULL == stmt->name) ||
                ((strcmp(stmt->name, "role") != 0) && pg_strcasecmp(stmt->name, "session_authorization") != 0)) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("unexpected SET name: %s", stmt->name)));
                break;
            }

            /* OK, Get role and pwd from arguments */
            role = NULL;
            passwd = NULL;
            phead = NULL;
            phead = list_head(stmt->args);

            if (phead != NULL) {
                /* role */
                arg = (A_Const*)linitial(stmt->args);

                if (NULL == arg) {
                    ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("role name in the set command is empty.")));
                    break;
                }

                /* check the existence of role and verify the password */
                role = strVal(&arg->val);

                if (lnext(phead)) {
                    arg = (A_Const*)lsecond(stmt->args);

                    if (NULL != arg) {
                        passwd = strVal(&arg->val);
                    }
                }
                if (pg_strcasecmp(stmt->name, "session_authorization") == 0) {
                    check_setrole_permission(role, passwd, false);
                    if (!verify_setrole_passwd(role, passwd, false)) {
                        str_reset(passwd);
                        ereport(ERROR,
                            (errcode(ERRCODE_SYSTEM_ERROR),
                                errmsg("verify set session_authorization and passwd failed.")));
                        break;
                    }
                } else {
                    check_setrole_permission(role, passwd, true);
                    if (!verify_setrole_passwd(role, passwd, true)) {
                        str_reset(passwd);
                        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("verify set role and passwd failed.")));
                        break;
                    }
                }

                /* passwd is sensitive info, it should be cleaned when it's useless */
                str_reset(passwd);
            }

            (void)set_config_option(stmt->name, role,
                ((superuser() || (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) ?
                    PGC_SUSET : PGC_USERSET), PGC_S_SESSION, action, true, 0);
            break;

        case VAR_SET_DEFAULT:
        case VAR_RESET:
            (void)set_config_option(stmt->name, NULL,
                ((superuser() || (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) ?
                    PGC_SUSET : PGC_USERSET), PGC_S_SESSION, action, true, 0);
            if (!pg_strcasecmp(stmt->name, "search_path") || !pg_strcasecmp(stmt->name, "current_schema")) {
                char* var = NULL;

                if (!pg_strcasecmp(stmt->name, "search_path")) {
                    var = "current_schema";
                } else {
                    var = "search_path";
                }

                (void)set_config_option(var, NULL,
                    ((superuser() || (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) ?
                        PGC_SUSET : PGC_USERSET), PGC_S_SESSION, action, true, 0);
            }
            if (IS_PGXC_COORDINATOR && pg_strcasecmp(stmt->name, "session_authorization") == 0) {
                /* cmd:
                 * RESET SESSION AUTHORIZATION;
                 * this cmd will reset SESSION_USER and CURRENT_USER
                 * we need delete params "role" if exist
                 */
                (void)delete_pooler_session_params("role");
                (void)delete_pooler_session_params("session_authorization");
            }

            break;

        case VAR_RESET_ALL:
            ResetAllOptions();
            break;
        default:
            break;
    }
}

/*
 * Get the value to assign for a VariableSetStmt, or NULL if it's RESET.
 * The result is palloc'd.
 *
 * This is exported for use by actions such as ALTER ROLE SET.
 */
char* ExtractSetVariableArgs(VariableSetStmt* stmt)
{
    switch (stmt->kind) {
        case VAR_SET_VALUE:
            return flatten_set_variable_args(stmt->name, stmt->args);

        case VAR_SET_CURRENT:
            return GetConfigOptionByName(stmt->name, NULL);

        default:
            return NULL;
    }
}

/*
 * SetPGVariable - SET command exported as an easily-C-callable function.
 *
 * This provides access to SET TO value, as well as SET TO DEFAULT (expressed
 * by passing args == NIL), but not SET FROM CURRENT functionality.
 */
void SetPGVariable(const char* name, List* args, bool is_local)
{
    char* argstring = flatten_set_variable_args(name, args);

    /* Note SET DEFAULT (argstring == NULL) is equivalent to RESET */
    (void)set_config_option(name, argstring,
        ((superuser() || (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) ?
            PGC_SUSET : PGC_USERSET), PGC_S_SESSION, is_local ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0);
}

/*
 * SET command wrapped as a SQL callable function.
 */
Datum set_config_by_name(PG_FUNCTION_ARGS)
{
    char* name = NULL;
    char* value = NULL;
    char* new_value = NULL;
    bool is_local = false;

    if (PG_ARGISNULL(0))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("SET requires parameter name")));

    /* Get the GUC variable name */
    name = TextDatumGetCString(PG_GETARG_DATUM(0));

    /* Get the desired value or set to NULL for a reset request */
    if (PG_ARGISNULL(1))
        value = NULL;
    else
        value = TextDatumGetCString(PG_GETARG_DATUM(1));

    /*
     * Get the desired state of is_local. Default to false if provided value
     * is NULL
     */
    if (PG_ARGISNULL(2))
        is_local = false;
    else
        is_local = PG_GETARG_BOOL(2);

    /* Note SET DEFAULT (argstring == NULL) is equivalent to RESET */
    (void)set_config_option(name,
        value,
        ((superuser() || (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) ?
            PGC_SUSET : PGC_USERSET),
        PGC_S_SESSION,
        is_local ? GUC_ACTION_LOCAL : GUC_ACTION_SET,
        true,
        0);

    /* get the new current value */
    new_value = GetConfigOptionByName(name, NULL);

#ifdef PGXC

    /*
     * Convert this to SET statement and pass it to pooler.
     * If command is local and we are not in a transaction block do NOT
     * send this query to backend nodes, it is just bypassed by the backend.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && (!is_local || IsTransactionBlock())) {
        PoolCommandType poolcmdType = (is_local ? POOL_CMD_LOCAL_SET : POOL_CMD_GLOBAL_SET);
        StringInfoData poolcmd;

        initStringInfo(&poolcmd);
        appendStringInfo(&poolcmd, "SET %s %s TO %s", (is_local ? "LOCAL" : ""), name, (value ? value : "DEFAULT"));

        // Add retry query error code ERRCODE_SET_QUERY for error "ERROR SET query".
        if (PoolManagerSetCommand(poolcmdType, poolcmd.data) < 0)
            ereport(ERROR, (errcode(ERRCODE_SET_QUERY), errmsg("openGauss: ERROR SET query")));
    }

#endif

    /* Convert return string to text */
    PG_RETURN_TEXT_P(cstring_to_text(new_value));
}

/*
 * Common code for DefineCustomXXXVariable subroutines: allocate the
 * new variable's config struct and fill in generic fields.
 */
static struct config_generic* init_custom_variable(const char* name, const char* short_desc, const char* long_desc,
    GucContext context, int flags, enum config_type type, size_t sz)
{
    struct config_generic* gen = NULL;
    errno_t rc = 0;

    /*
     * Only allow custom PGC_POSTMASTER variables to be created during shared
     * library preload; any later than that, we can't ensure that the value
     * doesn't change after startup.  This is a fatal elog if it happens; just
     * erroring out isn't safe because we don't know what the calling loadable
     * module might already have hooked into.
     */
    if (context == PGC_POSTMASTER && !u_sess->misc_cxt.process_shared_preload_libraries_in_progress)
        ereport(FATAL, (errmsg("cannot create PGC_POSTMASTER variables after startup")));

    gen = (struct config_generic*)guc_malloc(ERROR, sz);
    rc = memset_s(gen, sz, 0, sz);
    securec_check(rc, "\0", "\0");

    gen->name = guc_strdup(ERROR, name);
    gen->context = context;
    gen->group = CUSTOM_OPTIONS;
    gen->short_desc = short_desc;
    gen->long_desc = long_desc;
    gen->flags = flags;
    gen->vartype = type;

    return gen;
}

/*
 * Common code for DefineCustomXXXVariable subroutines: insert the new
 * variable into the GUC variable array, replacing any placeholder.
 */
static void define_custom_variable(struct config_generic* variable)
{
    const char* name = variable->name;
    const char** nameAddr = &name;
    struct config_string* pHolder = NULL;
    struct config_generic** res;

    /*
     * See if there's a placeholder by the same name.
     */
    res = (struct config_generic**)bsearch((void*)&nameAddr,
        (void*)u_sess->guc_variables,
        u_sess->num_guc_variables,
        sizeof(struct config_generic*),
        guc_var_compare);

    if (res == NULL) {
        /*
         * No placeholder to replace, so we can just add it ... but first,
         * make sure it's initialized to its default value.
         */
        InitializeOneGUCOption(variable);
        add_guc_variable(variable, ERROR);
        return;
    }

    /*
     * This better be a placeholder
     */
    if (((unsigned int)(*res)->flags & GUC_CUSTOM_PLACEHOLDER) == 0)
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("attempt to redefine parameter \"%s\"", name)));

    Assert((*res)->vartype == PGC_STRING);
    pHolder = (struct config_string*)(*res);

    /*
     * First, set the variable to its default value.  We must do this even
     * though we intend to immediately apply a new value, since it's possible
     * that the new value is invalid.
     */
    InitializeOneGUCOption(variable);

    /*
     * Replace the placeholder. We aren't changing the name, so no re-sorting
     * is necessary
     */
    *res = variable;

    /*
     * Assign the string value(s) stored in the placeholder to the real
     * variable.  Essentially, we need to duplicate all the active and stacked
     * values, but with appropriate validation and datatype adjustment.
     *
     * If an assignment fails, we report a WARNING and keep going.	We don't
     * want to throw ERROR for bad values, because it'd bollix the add-on
     * module that's presumably halfway through getting loaded.  In such cases
     * the default or previous state will become active instead.
     */
    /* First, apply the reset value if any */
    if (pHolder->reset_val)
        (void)set_config_option(name,
            pHolder->reset_val,
            pHolder->gen.reset_scontext,
            pHolder->gen.reset_source,
            GUC_ACTION_SET,
            true,
            WARNING);

    /* That should not have resulted in stacking anything */
    Assert(variable->stack == NULL);

    /* Now, apply current and stacked values, in the order they were stacked */
    reapply_stacked_values(
        variable, pHolder, pHolder->gen.stack, *(pHolder->variable), pHolder->gen.scontext, pHolder->gen.source);

    /* Also copy over any saved source-location information */
    if (pHolder->gen.sourcefile)
        set_config_sourcefile(name, pHolder->gen.sourcefile, pHolder->gen.sourceline);

    /*
     * Free up as much as we conveniently can of the placeholder structure.
     * (This neglects any stack items, so it's possible for some memory to be
     * leaked.	Since this can only happen once per session per variable, it
     * doesn't seem worth spending much code on.)
     */
    set_string_field(pHolder, pHolder->variable, NULL);
    set_string_field(pHolder, &pHolder->reset_val, NULL);

    pfree(pHolder);
}

/*
 * Recursive subroutine for define_custom_variable: reapply non-reset values
 *
 * We recurse so that the values are applied in the same order as originally.
 * At each recursion level, apply the upper-level value (passed in) in the
 * fashion implied by the stack entry.
 */
static void reapply_stacked_values(struct config_generic* variable, struct config_string* pHolder, GucStack* stack,
    const char* curvalue, GucContext curscontext, GucSource cursource)
{
    const char* name = variable->name;
    GucStack* oldvarstack = variable->stack;

    if (stack != NULL) {
        /* First, recurse, so that stack items are processed bottom to top */
        reapply_stacked_values(
            variable, pHolder, stack->prev, stack->prior.val.stringval, stack->scontext, stack->source);

        /* See how to apply the passed-in value */
        switch (stack->state) {
            case GUC_SAVE:
                (void)set_config_option(name, curvalue, curscontext, cursource, GUC_ACTION_SAVE, true, WARNING);
                break;

            case GUC_SET:
                (void)set_config_option(name, curvalue, curscontext, cursource, GUC_ACTION_SET, true, WARNING);
                break;

            case GUC_LOCAL:
                (void)set_config_option(name, curvalue, curscontext, cursource, GUC_ACTION_LOCAL, true, WARNING);
                break;

            case GUC_SET_LOCAL:
                /* first, apply the masked value as SET */
                (void)set_config_option(name,
                    stack->masked.val.stringval,
                    stack->masked_scontext,
                    PGC_S_SESSION,
                    GUC_ACTION_SET,
                    true,
                    WARNING);
                /* then apply the current value as LOCAL */
                (void)set_config_option(name, curvalue, curscontext, cursource, GUC_ACTION_LOCAL, true, WARNING);
                break;
            default:
                break;
        }

        /* If we successfully made a stack entry, adjust its nest level */
        if (variable->stack && variable->stack != oldvarstack)
            variable->stack->nest_level = stack->nest_level;
    } else {
        /*
         * We are at the end of the stack.	If the active/previous value is
         * different from the reset value, it must represent a previously
         * committed session value.  Apply it, and then drop the stack entry
         * that set_config_option will have created under the impression that
         * this is to be just a transactional assignment.  (We leak the stack
         * entry.)
         */
        if (curvalue != pHolder->reset_val || curscontext != pHolder->gen.reset_scontext ||
            cursource != pHolder->gen.reset_source) {
            (void)set_config_option(name, curvalue, curscontext, cursource, GUC_ACTION_SET, true, WARNING);
            variable->stack = NULL;
        }
    }
}

void DefineCustomBoolVariable(const char* name, const char* short_desc, const char* long_desc, bool* valueAddr,
    bool bootValue, GucContext context, int flags, GucBoolCheckHook check_hook, GucBoolAssignHook assign_hook,
    GucShowHook show_hook)
{
    struct config_bool* var = NULL;

    var = (struct config_bool*)init_custom_variable(
        name, short_desc, long_desc, context, flags, PGC_BOOL, sizeof(struct config_bool));
    var->variable = valueAddr;
    var->boot_val = bootValue;
    var->reset_val = bootValue;
    var->check_hook = check_hook;
    var->assign_hook = assign_hook;
    var->show_hook = show_hook;
    define_custom_variable(&var->gen);
}

void DefineCustomIntVariable(const char* name, const char* short_desc, const char* long_desc, int* valueAddr,
    int bootValue, int minValue, int maxValue, GucContext context, int flags, GucIntCheckHook check_hook,
    GucIntAssignHook assign_hook, GucShowHook show_hook)
{
    struct config_int* var = NULL;

    var = (struct config_int*)init_custom_variable(
        name, short_desc, long_desc, context, flags, PGC_INT, sizeof(struct config_int));
    var->variable = valueAddr;
    var->boot_val = bootValue;
    var->reset_val = bootValue;
    var->min = minValue;
    var->max = maxValue;
    var->check_hook = check_hook;
    var->assign_hook = assign_hook;
    var->show_hook = show_hook;
    define_custom_variable(&var->gen);
}

void DefineCustomInt64Variable(const char* name, const char* short_desc, const char* long_desc, int64* valueAddr,
    int64 bootValue, int64 minValue, int64 maxValue, GucContext context, int flags, GucInt64CheckHook check_hook,
    GucInt64AssignHook assign_hook, GucShowHook show_hook)
{
    struct config_int64* var = NULL;

    var = (struct config_int64*)init_custom_variable(
        name, short_desc, long_desc, context, flags, PGC_INT64, sizeof(struct config_int64));
    var->variable = valueAddr;
    var->boot_val = bootValue;
    var->reset_val = bootValue;
    var->min = minValue;
    var->max = maxValue;
    var->check_hook = check_hook;
    var->assign_hook = assign_hook;
    var->show_hook = show_hook;
    define_custom_variable(&var->gen);
}

void DefineCustomRealVariable(const char* name, const char* short_desc, const char* long_desc, double* valueAddr,
    double bootValue, double minValue, double maxValue, GucContext context, int flags, GucRealCheckHook check_hook,
    GucRealAssignHook assign_hook, GucShowHook show_hook)
{
    struct config_real* var = NULL;

    var = (struct config_real*)init_custom_variable(
        name, short_desc, long_desc, context, flags, PGC_REAL, sizeof(struct config_real));
    var->variable = valueAddr;
    var->boot_val = bootValue;
    var->reset_val = bootValue;
    var->min = minValue;
    var->max = maxValue;
    var->check_hook = check_hook;
    var->assign_hook = assign_hook;
    var->show_hook = show_hook;
    define_custom_variable(&var->gen);
}

void DefineCustomStringVariable(const char* name, const char* short_desc, const char* long_desc, char** valueAddr,
    const char* bootValue, GucContext context, int flags, GucStringCheckHook check_hook,
    GucStringAssignHook assign_hook, GucShowHook show_hook)
{
    struct config_string* var = NULL;

    var = (struct config_string*)init_custom_variable(
        name, short_desc, long_desc, context, flags, PGC_STRING, sizeof(struct config_string));
    var->variable = valueAddr;
    var->boot_val = bootValue;
    var->check_hook = check_hook;
    var->assign_hook = assign_hook;
    var->show_hook = show_hook;
    define_custom_variable(&var->gen);
}

void DefineCustomEnumVariable(const char* name, const char* short_desc, const char* long_desc, int* valueAddr,
    int bootValue, const struct config_enum_entry* options, GucContext context, int flags, GucEnumCheckHook check_hook,
    GucEnumAssignHook assign_hook, GucShowHook show_hook)
{
    struct config_enum* var = NULL;

    var = (struct config_enum*)init_custom_variable(
        name, short_desc, long_desc, context, flags, PGC_ENUM, sizeof(struct config_enum));
    var->variable = valueAddr;
    var->boot_val = bootValue;
    var->reset_val = bootValue;
    var->options = options;
    var->check_hook = check_hook;
    var->assign_hook = assign_hook;
    var->show_hook = show_hook;
    define_custom_variable(&var->gen);
}

void EmitWarningsOnPlaceholders(const char* className)
{
    int classLen = strlen(className);
    int i;

    for (i = 0; i < u_sess->num_guc_variables; i++) {
        struct config_generic* var = u_sess->guc_variables[i];

        if ((var->flags & GUC_CUSTOM_PLACEHOLDER) != 0 && strncmp(className, var->name, classLen) == 0 &&
            var->name[classLen] == GUC_QUALIFIER_SEPARATOR) {
            ereport(WARNING,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("unrecognized configuration parameter \"%s\"", var->name)));
        }
    }
}

/*
 * SHOW command
 */
void GetPGVariable(const char* name, const char* likename, DestReceiver* dest)
{
    if (guc_name_compare(name, "all") == 0) {
        ShowAllGUCConfig(likename, dest);
    } else {
        ShowGUCConfigOption(name, dest);
    }
}

TupleDesc GetPGVariableResultDesc(const char* name)
{
    TupleDesc tupdesc;

    if (guc_name_compare(name, "all") == 0) {
        /* need a tuple descriptor representing three TEXT columns */
        tupdesc = CreateTemplateTupleDesc(3, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "setting", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "description", TEXTOID, -1, 0);
    } else {
        const char* varname = NULL;

        /* Get the canonical spelling of name */
        (void)GetConfigOptionByName(name, &varname);

        /* need a tuple descriptor representing a single TEXT column */
        tupdesc = CreateTemplateTupleDesc(1, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, varname, TEXTOID, -1, 0);
    }

    return tupdesc;
}

/*
 * SHOW command
 */
static void ShowGUCConfigOption(const char* name, DestReceiver* dest)
{
    TupOutputState* tstate = NULL;
    TupleDesc tupdesc;
    const char* varname = NULL;
    char* value = NULL;

    /* Get the value and canonical spelling of name */
    value = GetConfigOptionByName(name, &varname);

    /* need a tuple descriptor representing a single TEXT column */
    tupdesc = CreateTemplateTupleDesc(1, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, varname, TEXTOID, -1, 0);

    /* prepare for projection of tuples */
    tstate = begin_tup_output_tupdesc(dest, tupdesc);

    /* Send it */
    do_text_output_oneline(tstate, value);

    end_tup_output(tstate);
}

/*
 * SHOW ALL command
 */
static void ShowAllGUCConfig(const char* likename, DestReceiver* dest)
{
    bool am_superuser = superuser();
    int i;
    TupOutputState* tstate = NULL;
    TupleDesc tupdesc;
    Datum values[3];
    bool isnull[3] = {false, false, false};

    /* need a tuple descriptor representing three TEXT columns */
    tupdesc = CreateTemplateTupleDesc(3, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "setting", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "description", TEXTOID, -1, 0);

    /* prepare for projection of tuples */
    tstate = begin_tup_output_tupdesc(dest, tupdesc);

    for (i = 0; i < u_sess->num_guc_variables; i++) {
        struct config_generic* conf = u_sess->guc_variables[i];
        char* setting = NULL;
        unsigned int flags = (unsigned int)conf->flags;

        if ((flags & GUC_NO_SHOW_ALL) || ((flags & GUC_SUPERUSER_ONLY) && !am_superuser))
            continue;

        if (likename != NULL && strstr((char*)conf->name, likename) == NULL) {
            continue;
        }

        /* assign to the values array */
        values[0] = PointerGetDatum(cstring_to_text(conf->name));

        setting = _ShowOption(conf, true);

        if (setting != NULL) {
            values[1] = PointerGetDatum(cstring_to_text(setting));
            isnull[1] = false;
        } else {
            values[1] = PointerGetDatum(NULL);
            isnull[1] = true;
        }

        values[2] = PointerGetDatum(cstring_to_text(conf->short_desc));

        /* send it to dest */
        do_tup_output(tstate, values, 3, isnull, 3);

        /* clean up */
        pfree(DatumGetPointer(values[0]));

        if (setting != NULL) {
            pfree(setting);
            pfree(DatumGetPointer(values[1]));
        }

        pfree(DatumGetPointer(values[2]));
    }

    end_tup_output(tstate);
}

/*
 * Return GUC variable value by name; optionally return canonical
 * form of name.  Return value is palloc'd.
 */
char* GetConfigOptionByName(const char* name, const char** varname)
{
    struct config_generic* record = NULL;

    record = find_option(name, false, ERROR);

    if (record == NULL)
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("unrecognized configuration parameter \"%s\"", name)));

    if ((record->flags & GUC_SUPERUSER_ONLY) && !superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to examine \"%s\"", name)));

    if (varname != NULL)
        *varname = record->name;

    return _ShowOption(record, true);
}

/*
 * Return GUC variable value by variable number; optionally return canonical
 * form of name.  Return value is palloc'd.
 */
void GetConfigOptionByNum(int varnum, const char** values, bool* noshow)
{
    char buffer[256];
    struct config_generic* conf = NULL;
    int rc = 0;

    /* check requested variable number valid */
    Assert((varnum >= 0) && (varnum < u_sess->num_guc_variables));

    conf = u_sess->guc_variables[varnum];

    if (noshow != NULL) {
        if ((conf->flags & GUC_NO_SHOW_ALL) ||
            ((conf->flags & GUC_SUPERUSER_ONLY) && !superuser()))
            *noshow = true;
        else
            *noshow = false;
    }

    /* first get the generic attributes */

    /* name */
    values[0] = conf->name;

    /* setting : use _ShowOption in order to avoid duplicating the logic */
    values[1] = _ShowOption(conf, false);

    /* unit */
    if (conf->vartype == PGC_INT || conf->vartype == PGC_REAL) {
        char buf[8];

        switch (conf->flags & (GUC_UNIT_MEMORY | GUC_UNIT_TIME)) {
            case GUC_UNIT_KB:
                values[2] = "kB";
                break;

            case GUC_UNIT_BLOCKS:
                rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%dkB", BLCKSZ / 1024);
                securec_check_ss(rc, "\0", "\0");
                values[2] = pstrdup(buf);
                break;

            case GUC_UNIT_XBLOCKS:
                rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%dkB", XLOG_BLCKSZ / 1024);
                securec_check_ss(rc, "\0", "\0");
                values[2] = pstrdup(buf);
                break;

            case GUC_UNIT_MS:
                values[2] = "ms";
                break;

            case GUC_UNIT_S:
                values[2] = "s";
                break;

            case GUC_UNIT_MIN:
                values[2] = "min";
                break;

            case GUC_UNIT_HOUR:
                values[2] = "hour";
                break;

            case GUC_UNIT_DAY:
                values[2] = "d";
                break;

            default:
                values[2] = "";
                break;
        }
    } else
        values[2] = NULL;

    /* group */
    values[3] = config_group_names[conf->group];

    /* short_desc */
    values[4] = conf->short_desc;

    /* extra_desc */
    values[5] = conf->long_desc;

    /* context */
    values[6] = GucContext_Names[conf->context];

    /* vartype */
    values[7] = config_type_names[conf->vartype];

    /* source */
    values[8] = GucSource_Names[conf->source];

    /* now get the type specifc attributes */
    switch (conf->vartype) {
        case PGC_BOOL: {
            struct config_bool* lconf = (struct config_bool*)conf;

            /* min_val */
            values[9] = NULL;

            /* max_val */
            values[10] = NULL;

            /* enumvals */
            values[11] = NULL;

            /* boot_val */
            values[12] = pstrdup(lconf->boot_val ? "on" : "off");

            /* reset_val */
            values[13] = pstrdup(lconf->reset_val ? "on" : "off");
        } break;

        case PGC_INT: {
            struct config_int* lconf = (struct config_int*)conf;

            /* min_val */
            rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, "%d", lconf->min);
            securec_check_ss(rc, "\0", "\0");
            values[9] = pstrdup(buffer);

            /* max_val */
            rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, "%d", lconf->max);
            securec_check_ss(rc, "\0", "\0");
            values[10] = pstrdup(buffer);

            /* enumvals */
            values[11] = NULL;

            /* boot_val */
            rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, "%d", lconf->boot_val);
            securec_check_ss(rc, "\0", "\0");
            values[12] = pstrdup(buffer);

            /* reset_val */
            rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, "%d", lconf->reset_val);
            securec_check_ss(rc, "\0", "\0");
            values[13] = pstrdup(buffer);
        } break;

        case PGC_INT64: {
            struct config_int64* lconf = (struct config_int64*)conf;

            /* min_val */
            rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, INT64_FORMAT, lconf->min);
            securec_check_ss(rc, "\0", "\0");
            values[9] = pstrdup(buffer);

            /* max_val */
            rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, INT64_FORMAT, lconf->max);
            securec_check_ss(rc, "\0", "\0");
            values[10] = pstrdup(buffer);

            /* enumvals */
            values[11] = NULL;

            /* boot_val */
            rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, INT64_FORMAT, lconf->boot_val);
            securec_check_ss(rc, "\0", "\0");
            values[12] = pstrdup(buffer);

            /* reset_val */
            rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, INT64_FORMAT, lconf->reset_val);
            securec_check_ss(rc, "\0", "\0");
            values[13] = pstrdup(buffer);
        } break;

        case PGC_REAL: {
            struct config_real* lconf = (struct config_real*)conf;

            /* min_val */
            rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, "%g", lconf->min);
            securec_check_ss(rc, "\0", "\0");
            values[9] = pstrdup(buffer);

            /* max_val */
            rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, "%g", lconf->max);
            securec_check_ss(rc, "\0", "\0");
            values[10] = pstrdup(buffer);

            /* enumvals */
            values[11] = NULL;

            /* boot_val */
            rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, "%g", lconf->boot_val);
            securec_check_ss(rc, "\0", "\0");
            values[12] = pstrdup(buffer);

            /* reset_val */
            rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, "%g", lconf->reset_val);
            securec_check_ss(rc, "\0", "\0");
            values[13] = pstrdup(buffer);
        } break;

        case PGC_STRING: {
            struct config_string* lconf = (struct config_string*)conf;

            /* min_val */
            values[9] = NULL;

            /* max_val */
            values[10] = NULL;

            /* enumvals */
            values[11] = NULL;

            /* boot_val */
            if (lconf->boot_val == NULL)
                values[12] = NULL;
            else
                values[12] = pstrdup(lconf->boot_val);

            /* reset_val */
            if (lconf->reset_val == NULL)
                values[13] = NULL;
            else
                values[13] = pstrdup(lconf->reset_val);
        } break;

        case PGC_ENUM: {
            struct config_enum* lconf = (struct config_enum*)conf;

            /* min_val */
            values[9] = NULL;

            /* max_val */
            values[10] = NULL;

            /* enumvals */

            /*
             * NOTE! enumvals with double quotes in them are not
             * supported!
             */
            values[11] = config_enum_get_options((struct config_enum*)conf, "{\"", "\"}", "\",\"");

            /* boot_val */
            values[12] = pstrdup(config_enum_lookup_by_value(lconf, lconf->boot_val));

            /* reset_val */
            values[13] = pstrdup(config_enum_lookup_by_value(lconf, lconf->reset_val));
        } break;

        default: {
            /*
             * should never get here, but in case we do, set 'em to NULL
             */

            /* min_val */
            values[9] = NULL;

            /* max_val */
            values[10] = NULL;

            /* enumvals */
            values[11] = NULL;

            /* boot_val */
            values[12] = NULL;

            /* reset_val */
            values[13] = NULL;
        } break;
    }

    /*
     * If the setting came from a config file, set the source location. For
     * security reasons, we don't show source file/line number for
     * non-superusers.
     */
    if (conf->source == PGC_S_FILE && (superuser() ||
        (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))) {
        values[14] = conf->sourcefile;
        rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, "%d", conf->sourceline);
        securec_check_ss(rc, "\0", "\0");
        values[15] = pstrdup(buffer);
    } else {
        values[14] = NULL;
        values[15] = NULL;
    }
}

/*
 * Return the total number of GUC variables
 */
int GetNumConfigOptions(void)
{
    return u_sess->num_guc_variables;
}

/*
 * show_config_by_name - equiv to SHOW X command but implemented as
 * a function.
 */
Datum show_config_by_name(PG_FUNCTION_ARGS)
{
    char* varname = NULL;
    char* varval = NULL;

    /* Get the GUC variable name */
    varname = TextDatumGetCString(PG_GETARG_DATUM(0));

    /* Get the value */
    varval = GetConfigOptionByName(varname, NULL);

    /* Convert to text */
    PG_RETURN_TEXT_P(cstring_to_text(varval));
}

/*
 * show_all_settings - equiv to SHOW ALL command but implemented as
 * a Table Function.
 */
#define NUM_PG_SETTINGS_ATTS 16

Datum show_all_settings(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    TupleDesc tupdesc;
    int call_cntr;
    int max_calls;
    AttInMetadata* attinmeta = NULL;
    MemoryContext oldcontext;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /*
         * need a tuple descriptor representing NUM_PG_SETTINGS_ATTS columns
         * of the appropriate types
         */
        tupdesc = CreateTemplateTupleDesc(NUM_PG_SETTINGS_ATTS, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "setting", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "unit", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "category", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "short_desc", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "extra_desc", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "context", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "vartype", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "source", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)10, "min_val", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)11, "max_val", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)12, "enumvals", TEXTARRAYOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)13, "boot_val", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)14, "reset_val", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)15, "sourcefile", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)16, "sourceline", INT4OID, -1, 0);

        /*
         * Generate attribute metadata needed later to produce tuples from raw
         * C strings
         */
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;

        /* total number of tuples to be returned */
        funcctx->max_calls = GetNumConfigOptions();

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    attinmeta = funcctx->attinmeta;

    if (call_cntr < max_calls) /* do when there is more left to send */
    {
        char* values[NUM_PG_SETTINGS_ATTS];
        bool noshow = false;
        HeapTuple tuple;
        Datum result;

        /*
         * Get the next visible GUC variable name and value
         */
        do {
            GetConfigOptionByNum(call_cntr, (const char**)values, &noshow);

            if (noshow) {
                /* bump the counter and get the next config setting */
                call_cntr = ++funcctx->call_cntr;

                /* make sure we haven't gone too far now */
                if (call_cntr >= max_calls)
                    SRF_RETURN_DONE(funcctx);
            }
        } while (noshow);

        /* build a tuple */
        tuple = BuildTupleFromCStrings(attinmeta, values);

        /* make the tuple into a datum */
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
    }
}

static char* _ShowOption(struct config_generic* record, bool use_units)
{
    char buffer[256];
    const char* val = NULL;
    int rc = 0;

    switch (record->vartype) {
        case PGC_BOOL: {
            struct config_bool* conf = (struct config_bool*)record;

            if (conf->show_hook)
                val = (*conf->show_hook)();
            else
                val = *conf->variable ? "on" : "off";
        } break;

        case PGC_INT: {
            struct config_int* conf = (struct config_int*)record;

            if (conf->show_hook)
                val = (*conf->show_hook)();
            else {
                /*
                 * Use int64 arithmetic to avoid overflows in units
                 * conversion.
                 */
                int64 result = *conf->variable;
                const char* unit = NULL;
                int flags = record->flags;

                if (use_units && result > 0 && (record->flags & GUC_UNIT_MEMORY)) {
                    switch (flags & GUC_UNIT_MEMORY) {
                        case GUC_UNIT_BLOCKS:
                            result *= (double)BLCKSZ / 1024;
                            break;

                        case GUC_UNIT_XBLOCKS:
                            result *= (double)XLOG_BLCKSZ / 1024;
                            break;
                        default:
                            break;
                    }

                    if (result % KB_PER_GB == 0) {
                        result /= KB_PER_GB;
                        unit = "GB";
                    } else if (result % KB_PER_MB == 0) {
                        result /= KB_PER_MB;
                        unit = "MB";
                    } else {
                        unit = "kB";
                    }
                } else if (use_units && result > 0 && (record->flags & GUC_UNIT_TIME)) {
                    switch (flags & GUC_UNIT_TIME) {
                        case GUC_UNIT_S:
                            result *= MS_PER_S;
                            break;

                        case GUC_UNIT_MIN:
                            result *= MS_PER_MIN;
                            break;
                        case GUC_UNIT_HOUR:
                            result *= MS_PER_H;
                            break;
                        case GUC_UNIT_DAY:
                            result *= MS_PER_D;
                            break;
                        default:
                            break;
                    }

                    if (result % MS_PER_D == 0) {
                        result /= MS_PER_D;
                        unit = "d";
                    } else if (result % MS_PER_H == 0) {
                        result /= MS_PER_H;
                        unit = "h";
                    } else if (result % MS_PER_MIN == 0) {
                        result /= MS_PER_MIN;
                        unit = "min";
                    } else if (result % MS_PER_S == 0) {
                        result /= MS_PER_S;
                        unit = "s";
                    } else {
                        unit = "ms";
                    }
                } else
                    unit = "";

                rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, INT64_FORMAT "%s", result, unit);
                securec_check_ss(rc, "\0", "\0");
                val = buffer;
            }
        } break;

        case PGC_INT64: {
            struct config_int64* conf = (struct config_int64*)record;

            if (conf->show_hook)
                val = (*conf->show_hook)();
            else {
                rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, INT64_FORMAT, *conf->variable);
                securec_check_ss(rc, "\0", "\0");
                val = buffer;
            }
        } break;

        case PGC_REAL: {
            struct config_real* conf = (struct config_real*)record;

            if (conf->show_hook) {
                val = (*conf->show_hook)();
            } else {
                double result = *conf->variable;
                const char* unit = NULL;
                int flags = record->flags;

                if (use_units && result > 0 && (record->flags & GUC_UNIT_MEMORY)) {
                    switch (flags & GUC_UNIT_MEMORY) {
                        case GUC_UNIT_BLOCKS:
                            result *= (double)BLCKSZ / 1024;
                            break;
                        case GUC_UNIT_XBLOCKS:
                            result *= (double)XLOG_BLCKSZ / 1024;
                            break;
                        default:
                            break;
                    }

                    if (result / KB_PER_GB >= 1.0) {
                        result /= KB_PER_GB;
                        unit = "GB";
                    } else if (result / KB_PER_MB >= 1.0) {
                        result /= KB_PER_MB;
                        unit = "MB";
                    } else {
                        unit = "kB";
                    }
                } else if (use_units && result > 0 && (record->flags & GUC_UNIT_TIME)) {
                    switch (flags & GUC_UNIT_TIME) {
                        case GUC_UNIT_S:
                            result *= MS_PER_S;
                            break;
                        case GUC_UNIT_MIN:
                            result *= MS_PER_MIN;
                            break;
                        case GUC_UNIT_HOUR:
                            result *= MS_PER_H;
                            break;
                        case GUC_UNIT_DAY:
                            result *= MS_PER_D;
                            break;
                        default:
                            break;
                    }

                    if (result / MS_PER_D >= 1.0) {
                        result /= MS_PER_D;
                        unit = "d";
                    } else if (result / MS_PER_H >= 1.0) {
                        result /= MS_PER_H;
                        unit = "h";
                    } else if (result / MS_PER_MIN >= 1.0) {
                        result /= MS_PER_MIN;
                        unit = "min";
                    } else if (result / MS_PER_S >= 1.0) {
                        result /= MS_PER_S;
                        unit = "s";
                    } else {
                        unit = "ms";
                    }
                } else {
                    unit = "";
                }

                rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, "%g%s", result, unit);
                securec_check_ss(rc, "\0", "\0");
                val = buffer;
            }
        } break;

        case PGC_STRING: {
            struct config_string* conf = (struct config_string*)record;

            if (conf->show_hook)
                val = (*conf->show_hook)();
            else if (*conf->variable && **conf->variable)
                val = *conf->variable;
            else
                val = "";
        } break;

        case PGC_ENUM: {
            struct config_enum* conf = (struct config_enum*)record;

            if (conf->show_hook)
                val = (*conf->show_hook)();
            else
                val = config_enum_lookup_by_value(conf, *conf->variable);
        } break;

        default:
            /* just to keep compiler quiet */
            val = "???";
            break;
    }

    return pstrdup(val);
}

#ifdef EXEC_BACKEND

inline static void write_fp(FILE* fp, config_generic* gconf, int elevel)
{
    if (fwrite(&gconf->sourceline, 1, sizeof(gconf->sourceline), fp) != sizeof(gconf->sourceline)) {
        ereport(elevel, (errcode(ERRCODE_FILE_WRITE_FAILED),
            errmsg("write nondefault variables's sourceline failed")));
    }
    if (fwrite(&gconf->source, 1, sizeof(gconf->source), fp) != sizeof(gconf->source)) {
        ereport(elevel, (errcode(ERRCODE_FILE_WRITE_FAILED),
            errmsg("write nondefault variables's source failed")));
    }
    if (fwrite(&gconf->scontext, 1, sizeof(gconf->scontext), fp) != sizeof(gconf->scontext)) {
        ereport(elevel, (errcode(ERRCODE_FILE_WRITE_FAILED),
            errmsg("write nondefault variables's scontext failed")));
    }
}

/*
 *	These routines dump out all non-default GUC options into a binary
 *	file that is read by all exec'ed backends.  The format is:
 *
 *		variable name, string, null terminated
 *		variable value, string, null terminated
 *		variable sourcefile, string, null terminated (empty if none)
 *		variable sourceline, integer
 *		variable source, integer
 *		variable scontext, integer
 */
static void write_one_nondefault_variable(FILE* fp, struct config_generic* gconf, int elevel)
{
    if (gconf->source == PGC_S_DEFAULT)
        return;

    fprintf(fp, "%s", gconf->name);
    fputc(0, fp);

    switch (gconf->vartype) {
        case PGC_BOOL: {
            struct config_bool* conf = (struct config_bool*)gconf;

            if (*conf->variable)
                fprintf(fp, "true");
            else
                fprintf(fp, "false");
        } break;

        case PGC_INT: {
            struct config_int* conf = (struct config_int*)gconf;

            fprintf(fp, "%d", *conf->variable);
        } break;

        case PGC_INT64: {
            struct config_int64* conf = (struct config_int64*)gconf;

            fprintf(fp, INT64_FORMAT, *conf->variable);
        } break;

        case PGC_REAL: {
            struct config_real* conf = (struct config_real*)gconf;

            fprintf(fp, "%.17g", *conf->variable);
        } break;

        case PGC_STRING: {
            struct config_string* conf = (struct config_string*)gconf;

            fprintf(fp, "%s", *conf->variable);
        } break;

        case PGC_ENUM: {
            struct config_enum* conf = (struct config_enum*)gconf;

            fprintf(fp, "%s", config_enum_lookup_by_value(conf, *conf->variable));
        } break;
        default:
            break;
    }

    fputc(0, fp);

    if (gconf->sourcefile)
        fprintf(fp, "%s", gconf->sourcefile);

    fputc(0, fp);

    write_fp(fp, gconf, elevel);
}

void write_nondefault_variables(GucContext context)
{
    int elevel;
    FILE* fp = NULL;
    int i;

    Assert(context == PGC_POSTMASTER || context == PGC_SIGHUP);

    elevel = (context == PGC_SIGHUP) ? LOG : ERROR;

    /*
     * Open file
     */
    fp = AllocateFile(CONFIG_EXEC_PARAMS_NEW, "w");

    if (fp == NULL) {
        ereport(
            elevel, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", CONFIG_EXEC_PARAMS_NEW)));
        return;
    }

    for (i = 0; i < u_sess->num_guc_variables; i++) {
        write_one_nondefault_variable(fp, u_sess->guc_variables[i], elevel);
    }

    /*
     * make sure the content has been flush to disk.
     * we need not check if fp == NULL, it has been check before.
     */
    fflush(fp);

    if (FreeFile(fp)) {
        ereport(
            elevel, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", CONFIG_EXEC_PARAMS_NEW)));
        return;
    }

    /*
     * Put new file in place.  This could delay on Win32, but we don't hold
     * any exclusive locks.
     */
    rename(CONFIG_EXEC_PARAMS_NEW, CONFIG_EXEC_PARAMS);
}

/*
 *	Read string, including null byte from file
 *
 *	Return NULL on EOF and nothing read
 */
static char* read_string_with_null(FILE* fp)
{
    int i = 0, ch, maxlen = 256;
    char* str = NULL;

    do {
        if ((ch = fgetc(fp)) == EOF) {
            if (i == 0)
                return NULL;
            else
                ereport(FATAL, (errmsg("invalid format of exec config params file")));
        }

        if (i == 0)
            str = (char*)guc_malloc(FATAL, maxlen);
        else if (i == maxlen)
            str = (char*)guc_realloc(FATAL, str, maxlen *= 2);

        str[i++] = ch;
    } while (ch != 0);

    return str;
}

/*
 *	This routine loads a previous postmaster dump of its non-default
 *	settings.
 */
void read_nondefault_variables(void)
{
    FILE* fp = NULL;
    char *varname = NULL, *varvalue = NULL, *varsourcefile = NULL;
    int varsourceline;
    GucSource varsource;
    GucContext varscontext;
    int retry_times = 0;

    do {
        /*
         * Open file
         */
        fp = AllocateFile(CONFIG_EXEC_PARAMS, "r");

        if (fp == NULL) {
            retry_times++;
        }

    } while (fp == NULL && retry_times <= 1000);

    if (fp == NULL) {
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not read from file \"%s\": %m", CONFIG_EXEC_PARAMS)));
        return;
    }

    for (;;) {
        struct config_generic* record = NULL;

        if ((varname = read_string_with_null(fp)) == NULL)
            break;

        if ((record = find_option(varname, true, FATAL)) == NULL)
            ereport(FATAL, (errmsg("failed to locate variable \"%s\" in exec config params file", varname)));

        if ((varvalue = read_string_with_null(fp)) == NULL)
            ereport(FATAL, (errmsg("invalid format of exec config params file")));

        if ((varsourcefile = read_string_with_null(fp)) == NULL)
            ereport(FATAL, (errmsg("invalid format of exec config params file")));

        if (fread(&varsourceline, 1, sizeof(varsourceline), fp) != sizeof(varsourceline))
            ereport(FATAL, (errmsg("invalid format of exec config params file")));

        if (fread(&varsource, 1, sizeof(varsource), fp) != sizeof(varsource))
            ereport(FATAL, (errmsg("invalid format of exec config params file")));

        if (fread(&varscontext, 1, sizeof(varscontext), fp) != sizeof(varscontext))
            ereport(FATAL, (errmsg("invalid format of exec config params file")));

        (void)set_config_option(varname, varvalue, varscontext, varsource, GUC_ACTION_SET, true, 0, true);

        if (varsourcefile[0])
            set_config_sourcefile(varname, varsourcefile, varsourceline);

        pfree(varname);
        pfree(varvalue);
        pfree(varsourcefile);
    }

    FreeFile(fp);

    // shared_buffers is 32MB for dummyStandby
    //
    if (dummyStandbyMode)
        g_instance.attr.attr_storage.NBuffers = (int)(32 * 1024 * 1024 / BLCKSZ);
}
#endif /* EXEC_BACKEND */

/*
 * A little "long argument" simulation, although not quite GNU
 * compliant. Takes a string of the form "some-option=some value" and
 * returns name = "some_option" and value = "some value" in malloc'ed
 * storage. Note that '-' is converted to '_' in the option name. If
 * there is no '=' in the input string then value will be NULL.
 */
void ParseLongOption(const char* string, char** name, char** value)
{
    size_t equal_pos;
    char* cp = NULL;

    AssertArg(string);
    AssertArg(name);
    AssertArg(value);

    equal_pos = strcspn(string, "=");
    if (string[equal_pos] == '=') {
        *name = (char*)guc_malloc(FATAL, equal_pos + 1);
        errno_t rc = strncpy_s(*name, equal_pos + 1, string, equal_pos);
        securec_check_ss(rc, "\0", "\0");
        *value = guc_strdup(FATAL, &string[equal_pos + 1]);
    } else {
        /* no equal sign in string */
        *name = guc_strdup(FATAL, string);
        *value = NULL;
    }

    for (cp = *name; *cp; cp++)
        if (*cp == '-')
            *cp = '_';
}

/*
 * Handle options fetched from pg_db_role_setting.setconfig,
 * pg_proc.proconfig, etc.	Caller must specify proper context/source/action.
 *
 * The array parameter must be an array of TEXT (it must not be NULL).
 */
void ProcessGUCArray(ArrayType* array, GucContext context, GucSource source, GucAction action)
{
    int i;

    Assert(array != NULL);
    Assert(ARR_ELEMTYPE(array) == TEXTOID);
    Assert(ARR_NDIM(array) == 1);
    Assert(ARR_LBOUND(array)[0] == 1);

    for (i = 1; i <= ARR_DIMS(array)[0]; i++) {
        Datum d;
        bool isnull = false;
        char* s = NULL;
        char* name = NULL;
        char* value = NULL;

        d = array_ref(array,
            1,
            &i,
            -1 /* varlenarray */,
            -1 /* TEXT's typlen */,
            false /* TEXT's typbyval */,
            'i' /* TEXT's typalign */,
            &isnull);

        if (isnull != false)
            continue;

        s = TextDatumGetCString(d);

        ParseLongOption(s, &name, &value);

        if (value == NULL) {
            ereport(
                WARNING, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("could not parse setting for parameter \"%s\"", name)));
            pfree(name);
            continue;
        }

        (void)set_config_option(name, value, context, source, action, true, 0);

        pfree(name);

        if (value != NULL)
            selfpfree(value);

        pfree(s);
    }
}

/*
 * Add an entry to an option array.  The array parameter may be NULL
 * to indicate the current table entry is NULL.
 */
ArrayType* GUCArrayAdd(ArrayType* array, const char* name, const char* value)
{
    struct config_generic* record = NULL;
    Datum datum;
    char* newval = NULL;
    ArrayType* a = NULL;
    int rcs = 0;

    Assert(name);
    Assert(value);

    /* test if the option is valid and we're allowed to set it */
    (void)validate_option_array_item(name, value, false);

    /* normalize name (converts obsolete GUC names to modern spellings) */
    record = find_option(name, false, WARNING);

    if (record != NULL)
        name = record->name;

    /* build new item for array */
    newval = (char*)palloc(strlen(name) + 1 + strlen(value) + 1);
    rcs = snprintf_s(
        newval, strlen(name) + 1 + strlen(value) + 1, strlen(name) + 1 + strlen(value), "%s=%s", name, value);
    securec_check_ss(rcs, "\0", "\0");
    datum = CStringGetTextDatum(newval);

    if (array != NULL) {
        int index;
        bool isnull = false;
        int i;

        Assert(ARR_ELEMTYPE(array) == TEXTOID);
        Assert(ARR_NDIM(array) == 1);
        Assert(ARR_LBOUND(array)[0] == 1);

        index = ARR_DIMS(array)[0] + 1; /* add after end */

        for (i = 1; i <= ARR_DIMS(array)[0]; i++) {
            Datum d;
            char* current = NULL;

            d = array_ref(array,
                1,
                &i,
                -1 /* varlenarray */,
                -1 /* TEXT's typlen */,
                false /* TEXT's typbyval */,
                'i' /* TEXT's typalign */,
                &isnull);

            if (isnull)
                continue;

            current = TextDatumGetCString(d);

            /* check for match up through and including '=' */
            if (strncmp(current, newval, strlen(name) + 1) == 0) {
                index = i;
                break;
            }
        }

        a = array_set(array,
            1,
            &index,
            datum,
            false,
            -1 /* varlena array */,
            -1 /* TEXT's typlen */,
            false /* TEXT's typbyval */,
            'i' /* TEXT's typalign */);
    } else
        a = construct_array(&datum, 1, TEXTOID, -1, false, 'i');

    return a;
}

/*
 * Delete an entry from an option array.  The array parameter may be NULL
 * to indicate the current table entry is NULL.  Also, if the return value
 * is NULL then a null should be stored.
 */
ArrayType* GUCArrayDelete(ArrayType* array, const char* name)
{
    struct config_generic* record = NULL;
    ArrayType* newarray = NULL;
    int i;
    int index;

    Assert(name);

    /* test if the option is valid and we're allowed to set it */
    (void)validate_option_array_item(name, NULL, false);

    /* normalize name (converts obsolete GUC names to modern spellings) */
    record = find_option(name, false, WARNING);

    if (record != NULL)
        name = record->name;

    /* if array is currently null, then surely nothing to delete */
    if (array == NULL)
        return NULL;

    newarray = NULL;
    index = 1;

    for (i = 1; i <= ARR_DIMS(array)[0]; i++) {
        Datum d;
        char* val = NULL;
        bool isnull = false;

        d = array_ref(array,
            1,
            &i,
            -1 /* varlenarray */,
            -1 /* TEXT's typlen */,
            false /* TEXT's typbyval */,
            'i' /* TEXT's typalign */,
            &isnull);

        if (isnull)
            continue;

        val = TextDatumGetCString(d);

        /* ignore entry if it's what we want to delete */
        if (strncmp(val, name, strlen(name)) == 0 && val[strlen(name)] == '=')
            continue;

        /* else add it to the output array */
        if (newarray != NULL)
            newarray = array_set(newarray,
                1,
                &index,
                d,
                false,
                -1 /* varlenarray */,
                -1 /* TEXT's typlen */,
                false /* TEXT's typbyval */,
                'i' /* TEXT's typalign */);
        else
            newarray = construct_array(&d, 1, TEXTOID, -1, false, 'i');

        index++;
    }

    return newarray;
}

/*
 * Given a GUC array, delete all settings from it that our permission
 * level allows: if superuser, delete them all; if regular user, only
 * those that are PGC_USERSET
 */
ArrayType* GUCArrayReset(ArrayType* array)
{
    ArrayType* newarray = NULL;
    int i;
    int index;

    /* if array is currently null, nothing to do */
    if (array == NULL)
        return NULL;

    /* if we're superuser, we can delete everything, so just do it */
    if ((superuser() || (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)))
        return NULL;

    for (i = 1, index = 1; i <= ARR_DIMS(array)[0]; i++) {
        Datum d;
        char* val = NULL;
        char* eqsgn = NULL;
        bool isnull = false;

        d = array_ref(array,
            1,
            &i,
            -1 /* varlenarray */,
            -1 /* TEXT's typlen */,
            false /* TEXT's typbyval */,
            'i' /* TEXT's typalign */,
            &isnull);

        if (isnull)
            continue;

        val = TextDatumGetCString(d);

        eqsgn = strchr(val, '=');
        if (eqsgn != NULL) {
            *eqsgn = '\0';
        }

        /* skip if we have permission to delete it */
        if (validate_option_array_item(val, NULL, true))
            continue;

        /* else add it to the output array */
        if (newarray != NULL)
            newarray = array_set(newarray,
                1,
                &index,
                d,
                false,
                -1 /* varlenarray */,
                -1 /* TEXT's typlen */,
                false /* TEXT's typbyval */,
                'i' /* TEXT's typalign */);
        else
            newarray = construct_array(&d, 1, TEXTOID, -1, false, 'i');

        index++;
        pfree(val);
    }

    return newarray;
}

/*
 * Validate a proposed option setting for GUCArrayAdd/Delete/Reset.
 *
 * name is the option name.  value is the proposed value for the Add case,
 * or NULL for the Delete/Reset cases.	If skipIfNoPermissions is true, it's
 * not an error to have no permissions to set the option.
 *
 * Returns TRUE if OK, FALSE if skipIfNoPermissions is true and user does not
 * have permission to change this option (all other error cases result in an
 * error being thrown).
 */
static bool validate_option_array_item(const char* name, const char* value, bool skipIfNoPermissions)

{
    struct config_generic* gconf = NULL;

    /*
     * There are three cases to consider:
     *
     * name is a known GUC variable.  Check the value normally, check
     * permissions normally (ie, allow if variable is USERSET, or if it's
     * SUSET and user is superuser).
     *
     * name is not known, but exists or can be created as a placeholder (i.e.,
     * it has a prefixed name).  We allow this case if you're a superuser,
     * otherwise not.  Superusers are assumed to know what they're doing. We
     * can't allow it for other users, because when the placeholder is
     * resolved it might turn out to be a SUSET variable;
     * define_custom_variable assumes we checked that.
     *
     * name is not known and can't be created as a placeholder.  Throw error,
     * unless skipIfNoPermissions is true, in which case return FALSE.
     */
    gconf = find_option(name, true, WARNING);

    if (gconf == NULL) {
        /* not known, failed to make a placeholder */
        if (skipIfNoPermissions)
            return false;

        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("unrecognized configuration parameter \"%s\"", name)));
    }

    if (gconf->flags & GUC_CUSTOM_PLACEHOLDER) {
        /*
         * We cannot do any meaningful check on the value, so only permissions
         * are useful to check.
         */
        if ((superuser() || (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)))
            return true;

        if (skipIfNoPermissions)
            return false;

        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("permission denied to set parameter \"%s\"", name)));
    }

    /* manual permissions check so we can avoid an error being thrown */
    if (gconf->context == PGC_USERSET)
        /* ok */;
    else if (gconf->context == PGC_SUSET && (superuser() ||
        (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)))
        /* ok */;
    else if (skipIfNoPermissions)
        return false;

    /* if a permissions error should be thrown, let set_config_option do it */

    /* test for permissions and valid option value */
    (void)set_config_option(name, value,
        (superuser() || (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) ?
            PGC_SUSET : PGC_USERSET, PGC_S_TEST, GUC_ACTION_SET, false, 0);

    return true;
}

/*
 * Called by check_hooks that want to override the normal
 * ERRCODE_INVALID_PARAMETER_VALUE SQLSTATE for check hook failures.
 *
 * Note that GUC_check_errmsg() etc are just macros that result in a direct
 * assignment to the associated variables.	That is ugly, but forced by the
 * limitations of C's macro mechanisms.
 */
void GUC_check_errcode(int sqlerrcode)
{
    u_sess->utils_cxt.GUC_check_errcode_value = sqlerrcode;
}

/*
 * Convenience functions to manage calling a variable's check_hook.
 * These mostly take care of the protocol for letting check hooks supply
 * portions of the error report on failure.
 */

static bool call_bool_check_hook(struct config_bool* conf, bool* newval, void** extra, GucSource source, int elevel)
{
    /* Quick success if no hook */
    if (!conf->check_hook)
        return true;

    /* Reset variables that might be set by hook */
    u_sess->utils_cxt.GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    u_sess->utils_cxt.GUC_check_errmsg_string = NULL;
    u_sess->utils_cxt.GUC_check_errdetail_string = NULL;
    u_sess->utils_cxt.GUC_check_errhint_string = NULL;

    if (!(*conf->check_hook)(newval, extra, source)) {
        ereport(elevel,
            (errcode(u_sess->utils_cxt.GUC_check_errcode_value),
                errmodule(MOD_GUC),
                u_sess->utils_cxt.GUC_check_errmsg_string
                    ? errmsg_internal("%s", u_sess->utils_cxt.GUC_check_errmsg_string)
                    : errmsg("invalid value for parameter \"%s\": %d", conf->gen.name, (int)*newval),
                u_sess->utils_cxt.GUC_check_errdetail_string
                    ? errdetail_internal("%s", u_sess->utils_cxt.GUC_check_errdetail_string)
                    : 0,
                u_sess->utils_cxt.GUC_check_errhint_string ? errhint("%s", u_sess->utils_cxt.GUC_check_errhint_string)
                                                           : 0));
        /* Flush any strings created in ErrorContext */
        FlushErrorState();
        return false;
    }

    return true;
}

static bool call_int_check_hook(struct config_int* conf, int* newval, void** extra, GucSource source, int elevel)
{
    /* Quick success if no hook */
    if (!conf->check_hook)
        return true;

    /* Reset variables that might be set by hook */
    u_sess->utils_cxt.GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    u_sess->utils_cxt.GUC_check_errmsg_string = NULL;
    u_sess->utils_cxt.GUC_check_errdetail_string = NULL;
    u_sess->utils_cxt.GUC_check_errhint_string = NULL;

    if (!(*conf->check_hook)(newval, extra, source)) {
        ereport(elevel,
            (errcode(u_sess->utils_cxt.GUC_check_errcode_value),
                errmodule(MOD_GUC),
                u_sess->utils_cxt.GUC_check_errmsg_string
                    ? errmsg_internal("%s", u_sess->utils_cxt.GUC_check_errmsg_string)
                    : errmsg("invalid value for parameter \"%s\": %d", conf->gen.name, *newval),
                u_sess->utils_cxt.GUC_check_errdetail_string
                    ? errdetail_internal("%s", u_sess->utils_cxt.GUC_check_errdetail_string)
                    : 0,
                u_sess->utils_cxt.GUC_check_errhint_string ? errhint("%s", u_sess->utils_cxt.GUC_check_errhint_string)
                                                           : 0));
        /* Flush any strings created in ErrorContext */
        FlushErrorState();
        return false;
    }

    return true;
}

static bool call_int64_check_hook(struct config_int64* conf, int64* newval, void** extra, GucSource source, int elevel)
{
    /* Quick success if no hook */
    if (!conf->check_hook)
        return true;

    /* Reset variables that might be set by hook */
    u_sess->utils_cxt.GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    u_sess->utils_cxt.GUC_check_errmsg_string = NULL;
    u_sess->utils_cxt.GUC_check_errdetail_string = NULL;
    u_sess->utils_cxt.GUC_check_errhint_string = NULL;

    if (!(*conf->check_hook)(newval, extra, source)) {
        ereport(elevel,
            (errcode(u_sess->utils_cxt.GUC_check_errcode_value),
                u_sess->utils_cxt.GUC_check_errmsg_string
                    ? errmsg_internal("%s", u_sess->utils_cxt.GUC_check_errmsg_string)
                    : errmsg("invalid value for parameter \"%s\": " INT64_FORMAT, conf->gen.name, *newval),
                u_sess->utils_cxt.GUC_check_errdetail_string
                    ? errdetail_internal("%s", u_sess->utils_cxt.GUC_check_errdetail_string)
                    : 0,
                u_sess->utils_cxt.GUC_check_errhint_string ? errhint("%s", u_sess->utils_cxt.GUC_check_errhint_string)
                                                           : 0));
        /* Flush any strings created in ErrorContext */
        FlushErrorState();
        return false;
    }

    return true;
}

static bool call_real_check_hook(struct config_real* conf, double* newval, void** extra, GucSource source, int elevel)
{
    /* Quick success if no hook */
    if (!conf->check_hook)
        return true;

    /* Reset variables that might be set by hook */
    u_sess->utils_cxt.GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    u_sess->utils_cxt.GUC_check_errmsg_string = NULL;
    u_sess->utils_cxt.GUC_check_errdetail_string = NULL;
    u_sess->utils_cxt.GUC_check_errhint_string = NULL;

    if (!(*conf->check_hook)(newval, extra, source)) {
        ereport(elevel,
            (errcode(u_sess->utils_cxt.GUC_check_errcode_value),
                errmodule(MOD_GUC),
                u_sess->utils_cxt.GUC_check_errmsg_string
                    ? errmsg_internal("%s", u_sess->utils_cxt.GUC_check_errmsg_string)
                    : errmsg("invalid value for parameter \"%s\": %g", conf->gen.name, *newval),
                u_sess->utils_cxt.GUC_check_errdetail_string
                    ? errdetail_internal("%s", u_sess->utils_cxt.GUC_check_errdetail_string)
                    : 0,
                u_sess->utils_cxt.GUC_check_errhint_string ? errhint("%s", u_sess->utils_cxt.GUC_check_errhint_string)
                                                           : 0));
        /* Flush any strings created in ErrorContext */
        FlushErrorState();
        return false;
    }

    return true;
}

static bool call_string_check_hook(
    struct config_string* conf, char** newval, void** extra, GucSource source, int elevel)
{
    /* Quick success if no hook */
    if (!conf->check_hook)
        return true;

    /* Reset variables that might be set by hook */
    u_sess->utils_cxt.GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    u_sess->utils_cxt.GUC_check_errmsg_string = NULL;
    u_sess->utils_cxt.GUC_check_errdetail_string = NULL;
    u_sess->utils_cxt.GUC_check_errhint_string = NULL;

    if (!(*conf->check_hook)(newval, extra, source)) {
        ereport(elevel,
            (errcode(u_sess->utils_cxt.GUC_check_errcode_value),
                errmodule(MOD_GUC),
                u_sess->utils_cxt.GUC_check_errmsg_string
                    ? errmsg_internal("%s", u_sess->utils_cxt.GUC_check_errmsg_string)
                    : errmsg("invalid value for parameter \"%s\": \"%s\"", conf->gen.name, *newval ? *newval : ""),
                u_sess->utils_cxt.GUC_check_errdetail_string
                    ? errdetail_internal("%s", u_sess->utils_cxt.GUC_check_errdetail_string)
                    : 0,
                u_sess->utils_cxt.GUC_check_errhint_string ? errhint("%s", u_sess->utils_cxt.GUC_check_errhint_string)
                                                           : 0));
        /* Flush any strings created in ErrorContext */
        FlushErrorState();
        return false;
    }

    return true;
}

static bool call_enum_check_hook(struct config_enum* conf, int* newval, void** extra, GucSource source, int elevel)
{
    /* Quick success if no hook */
    if (!conf->check_hook)
        return true;

    /* Reset variables that might be set by hook */
    u_sess->utils_cxt.GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    u_sess->utils_cxt.GUC_check_errmsg_string = NULL;
    u_sess->utils_cxt.GUC_check_errdetail_string = NULL;
    u_sess->utils_cxt.GUC_check_errhint_string = NULL;

    if (!(*conf->check_hook)(newval, extra, source)) {
        ereport(elevel,
            (errcode(u_sess->utils_cxt.GUC_check_errcode_value),
                errmodule(MOD_GUC),
                u_sess->utils_cxt.GUC_check_errmsg_string
                    ? errmsg_internal("%s", u_sess->utils_cxt.GUC_check_errmsg_string)
                    : errmsg("invalid value for parameter \"%s\": \"%s\"",
                          conf->gen.name,
                          config_enum_lookup_by_value(conf, *newval)),
                u_sess->utils_cxt.GUC_check_errdetail_string
                    ? errdetail_internal("%s", u_sess->utils_cxt.GUC_check_errdetail_string)
                    : 0,
                u_sess->utils_cxt.GUC_check_errhint_string ? errhint("%s", u_sess->utils_cxt.GUC_check_errhint_string)
                                                           : 0));
        /* Flush any strings created in ErrorContext */
        FlushErrorState();
        return false;
    }

    return true;
}

/*
 * check_hook, assign_hook and show_hook subroutines
 */

static bool check_log_destination(char** newval, void** extra, GucSource source)
{
    char* rawstring = NULL;
    List* elemlist = NIL;
    ListCell* l = NULL;
    unsigned int newlogdest = 0;
    unsigned int* myextra = NULL;

    /* Need a modifiable copy of string */
    rawstring = pstrdup(*newval);

    /* Parse string into list of identifiers */
    if (!SplitIdentifierString(rawstring, ',', &elemlist)) {
        /* syntax error in list */
        GUC_check_errdetail("List syntax is invalid.");
        pfree(rawstring);
        list_free(elemlist);
        return false;
    }

    foreach (l, elemlist) {
        char* tok = (char*)lfirst(l);

        if (pg_strcasecmp(tok, "stderr") == 0)
            newlogdest |= LOG_DESTINATION_STDERR;
        else if (pg_strcasecmp(tok, "csvlog") == 0)
            newlogdest |= LOG_DESTINATION_CSVLOG;

#ifdef HAVE_SYSLOG
        else if (pg_strcasecmp(tok, "syslog") == 0)
            newlogdest |= LOG_DESTINATION_SYSLOG;

#endif
#ifdef WIN32
        else if (pg_strcasecmp(tok, "eventlog") == 0)
            newlogdest |= LOG_DESTINATION_EVENTLOG;

#endif
        else {
            GUC_check_errdetail("Unrecognized key word: \"%s\".", tok);
            pfree(rawstring);
            list_free(elemlist);
            return false;
        }
    }

    pfree(rawstring);
    list_free(elemlist);

    myextra = (unsigned int*)guc_malloc(ERROR, sizeof(unsigned int));
    *myextra = newlogdest;
    *extra = (void*)myextra;

    return true;
}

static void assign_log_destination(const char* newval, void* extra)
{
    t_thrd.log_cxt.Log_destination = *((int*)extra);
}

static void assign_syslog_facility(int newval, void* extra)
{
#ifdef HAVE_SYSLOG
    set_syslog_parameters(
        u_sess->attr.attr_common.syslog_ident_str ? u_sess->attr.attr_common.syslog_ident_str : "postgres", newval);
#endif
    /* Without syslog support, just ignore it */
}

static void assign_syslog_ident(const char* newval, void* extra)
{
#ifdef HAVE_SYSLOG
    set_syslog_parameters(newval, u_sess->attr.attr_common.syslog_facility);
#endif
    /* Without syslog support, it will always be set to "none", so ignore */
}

static void assign_session_replication_role(int newval, void* extra)
{
    /*
     * Must flush the plan cache when changing replication role; but don't
     * flush unnecessarily.
     */
    if (u_sess->attr.attr_common.SessionReplicationRole != newval)
        ResetPlanCache();
}

static bool check_client_min_messages(int* newval, void** extra, GucSource source)
{
    /*
     * We disallow setting client_min_messages above ERROR, because not
     * sending an ErrorResponse message for an error breaks the FE/BE
     * protocol.  However, for backwards compatibility, we still accept FATAL
     * or PANIC as input values, and then adjust here.
     */
    if (*newval > ERROR)
        *newval = ERROR;
    return true;
}

static bool check_debug_assertions(bool* newval, void** extra, GucSource source)
{
#ifndef USE_ASSERT_CHECKING

    if (*newval) {
        GUC_check_errmsg("assertion checking is not supported by this build");
        return false;
    }

#endif
    return true;
}

#ifdef USE_BONJOUR
static bool check_bonjour(bool* newval, void** extra, GucSource source)
{
    // if not supported bonjour, must report error
    if (*newval) {
        GUC_check_errmsg("Bonjour is not supported by this build");
        return false;
    }

    return true;
}
#endif


static bool check_gpc_syscache_threshold(bool* newval, void** extra, GucSource source)
{
    if (*newval) {
        /* in case local_syscache_threshold is too low cause gpc does not take effect, we set local_syscache_threshold
           at least 16MB if gpc is on. */
        if (g_instance.attr.attr_memory.local_syscache_threshold < 16 * 1024) {
            ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                          errmsg("set local_syscache_threshold from %dMB to 16MB in case gpc is on but not valid.",
                                 g_instance.attr.attr_memory.local_syscache_threshold / 1024)));
            g_instance.attr.attr_memory.local_syscache_threshold = 16 * 1024;
        }
    }
    return true;
}

static bool check_stage_log_stats(bool* newval, void** extra, GucSource source)
{
    if (*newval && u_sess->attr.attr_common.log_statement_stats) {
        GUC_check_errdetail("Cannot enable parameter when \"log_statement_stats\" is true.");
        return false;
    }

    return true;
}

static bool check_log_stats(bool* newval, void** extra, GucSource source)
{
    if (*newval && (u_sess->attr.attr_common.log_parser_stats || u_sess->attr.attr_common.log_planner_stats ||
                       u_sess->attr.attr_common.log_executor_stats)) {
        GUC_check_errdetail("Cannot enable \"log_statement_stats\" when "
                            "\"log_parser_stats\", \"log_planner_stats\", "
                            "or \"log_executor_stats\" is true.");
        return false;
    }

    return true;
}

/*
 * Only a warning is printed to log.
 * Returning false will cause FATAL error and it will not be good.
 */
static bool check_pgxc_maintenance_mode(bool* newval, void** extra, GucSource source)
{
    switch (source) {
        case PGC_S_DYNAMIC_DEFAULT:
        case PGC_S_ENV_VAR:
        case PGC_S_ARGV:
            GUC_check_errmsg("pgxc_maintenance_mode is not allowed here.");
            return false;

        case PGC_S_FILE:
            ereport(WARNING,
                (errmsg("pgxc_maintenance_mode is not allowed in  postgresql.conf.  Set to default (false).")));
            *newval = false;
            return true;

        case PGC_S_DATABASE:
        case PGC_S_USER:
        case PGC_S_DATABASE_USER:
        case PGC_S_INTERACTIVE:
        case PGC_S_TEST:
            ereport(WARNING, (errmsg("pgxc_maintenance_mode is not allowed here.  Set to default (false).")));
            *newval = false;
            return true;

        case PGC_S_DEFAULT:
        case PGC_S_CLIENT:
        case PGC_S_SESSION:
            return true;

        default:
            GUC_check_errmsg("Unknown source");
            return false;
    }
}

bool check_canonical_path(char** newval, void** extra, GucSource source)
{
    /*
     * Since canonicalize_path never enlarges the string, we can just modify
     * newval in-place.  But watch out for NULL, which is the default value
     * for external_pid_file.
     */
    if (*newval)
        canonicalize_path(*newval);

    return true;
}

bool check_directory(char** newval, void** extra, GucSource source)
{
    if (*newval != NULL) {
        if (strlen(*newval) == 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid value for GUC parameter of directory type.")));
        } else {
            canonicalize_path(*newval);
        }
    }
    return true;
}

static bool check_log_filename(char** newval, void** extra, GucSource source)
{
    if (*newval == NULL || strlen(*newval) == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid parameter value for 'log_filename'.")));
    } else {
        check_canonical_path(newval, extra, source);
    }

    return true;
}

static bool check_perf_filename(char** newval)
{
    char* tmp_string = NULL;
    int len = 0;
    tmp_string = pstrdup(*newval);
    bool result = false;

    len = strlen(tmp_string);

    if (len > 4 && strcmp(tmp_string + len - 4, ".csv") == 0)
        result = true;
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("explain_dna_file should be .csv file.")));

    pfree(tmp_string);
    u_sess->attr.attr_sql.guc_explain_perf_mode = EXPLAIN_RUN;
    return result;
}

static bool check_perf_log(char** newval, void** extra, GucSource source)
{
    bool result = false;
    if (*newval == NULL)
        return true;
    if (is_absolute_path(*newval) == false)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("explain_dna_file should be absolute path + .csv file.")));

    result = check_canonical_path(newval, extra, source);

    if (result)
        return check_perf_filename(newval);
    else
        return false;
}
static bool check_timezone_abbreviations(char** newval, void** extra, GucSource source)
{
    /*
     * The boot_val given above for timezone_abbreviations is NULL. When we
     * see this we just do nothing.  If this value isn't overridden from the
     * config file then pg_timezone_abbrev_initialize() will eventually
     * replace it with "Default".  This hack has two purposes: to avoid
     * wasting cycles loading values that might soon be overridden from the
     * config file, and to avoid trying to read the timezone abbrev files
     * during InitializeGUCOptions().  The latter doesn't work in an
     * EXEC_BACKEND subprocess because my_exec_path hasn't been set yet and so
     * we can't locate PGSHAREDIR.
     */
    if (*newval == NULL) {
        Assert(source == PGC_S_DEFAULT);
        return true;
    }

    /* OK, load the file and produce a malloc'd TimeZoneAbbrevTable */
    *extra = load_tzoffsets(*newval);

    /* tzparser.c returns NULL on failure, reporting via GUC_check_errmsg */
    if (!*extra)
        return false;

    return true;
}

static void assign_thread_working_version_num(int newval, void* extra)
{
    if (newval > 0) {
        gs_set_working_version_num(newval);
    }
    return;
}

#ifdef LIBCOMM_SPEED_TEST_ENABLE
static void assign_comm_test_thread_num(int newval, void* extra)
{
    gs_set_test_thread_num(newval);
    return;
}
static void assign_comm_test_msg_len(int newval, void* extra)
{
    gs_set_test_msg_len(newval);
    return;
}
static void assign_comm_test_send_sleep(int newval, void* extra)
{
    gs_set_test_send_sleep(newval);
    return;
}
static void assign_comm_test_send_once(int newval, void* extra)
{
    gs_set_test_send_once(newval);
    return;
}
static void assign_comm_test_recv_sleep(int newval, void* extra)
{
    gs_set_test_recv_sleep(newval);
    return;
}
static void assign_comm_test_recv_once(int newval, void* extra)
{
    gs_set_test_recv_once(newval);
    return;
}
#endif

#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
static void assign_comm_fault_injection(int newval, void* extra)
{
    gs_set_fault_injection(newval);
    return;
}
#endif

static void assign_timezone_abbreviations(const char* newval, void* extra)
{
    /* Do nothing for the boot_val default of NULL */
    if (extra == NULL)
        return;

    InstallTimeZoneAbbrevs((TimeZoneAbbrevTable*)extra);
}

/*
 * pg_timezone_abbrev_initialize --- set default value if not done already
 *
 * This is called after initial loading of postgresql.conf.  If no
 * timezone_abbreviations setting was found therein, select default.
 * If a non-default value is already installed, nothing will happen.
 *
 * This can also be called from ProcessConfigFile to establish the default
 * value after a postgresql.conf entry for it is removed.
 */
static void pg_timezone_abbrev_initialize(void)
{
    SetConfigOption("timezone_abbreviations", "Default", PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT);
}

static void assign_tcp_keepalives_idle(int newval, void *extra)
{
    /*
     * The kernel API provides no way to test a value without setting it; and
     * once we set it we might fail to unset it.  So there seems little point
     * in fully implementing the check-then-assign GUC API for these
     * variables.  Instead we just do the assignment on demand.  pqcomm.c
     * reports any problems via ereport(LOG).
     *
     * This approach means that the GUC value might have little to do with the
     * actual kernel value, so we use a show_hook that retrieves the kernel
     * value rather than trusting GUC's copy.
     */
    (void) pq_setkeepalivesidle(newval, u_sess->proc_cxt.MyProcPort);
}

static const char* show_tcp_keepalives_idle(void)
{
    /* See comments in assign_tcp_keepalives_idle */
    const int maxBufLen = 16;
    static char nbuf[maxBufLen];

    errno_t rc = snprintf_s(nbuf, maxBufLen, maxBufLen - 1, "%d", pq_getkeepalivesidle(u_sess->proc_cxt.MyProcPort));
    securec_check_ss(rc, "\0", "\0");
    return nbuf;
}

static void assign_tcp_keepalives_interval(int newval, void *extra)
{
    /* See comments in assign_tcp_keepalives_idle */
    (void) pq_setkeepalivesinterval(newval, u_sess->proc_cxt.MyProcPort);
}

static const char* show_tcp_keepalives_interval(void)
{
    /* See comments in assign_tcp_keepalives_idle */
    const int maxBufLen = 16;
    static char nbuf[maxBufLen];

    errno_t rc = snprintf_s(nbuf, maxBufLen, maxBufLen - 1, "%d",
                            pq_getkeepalivesinterval(u_sess->proc_cxt.MyProcPort));
    securec_check_ss(rc, "\0", "\0");
    return nbuf;
}

static void assign_tcp_keepalives_count(int newval, void *extra)
{
    /* See comments in assign_tcp_keepalives_idle */
    (void) pq_setkeepalivescount(newval, u_sess->proc_cxt.MyProcPort);
}

static const char* show_tcp_keepalives_count(void)
{
    /* See comments in assign_tcp_keepalives_idle */
    const int maxBufLen = 16;
    static char nbuf[maxBufLen];

    errno_t rc = snprintf_s(nbuf, maxBufLen, maxBufLen - 1, "%d", pq_getkeepalivescount(u_sess->proc_cxt.MyProcPort));
    securec_check_ss(rc, "\0", "\0");
    return nbuf;
}

static bool check_effective_io_concurrency(int* newval, void** extra, GucSource source)
{
#ifdef USE_PREFETCH
    double new_prefetch_pages = 0.0;
    int i;

    /* ----------
     * The user-visible GUC parameter is the number of drives (spindles),
     * which we need to translate to a number-of-pages-to-prefetch target.
     * The target value is stashed in *extra and then assigned to the actual
     * variable by assign_effective_io_concurrency.
     *
     * The expected number of prefetch pages needed to keep N drives busy is:
     *
     * drives |   I/O requests
     * -------+----------------
     *		1 |   1
     *		2 |   2/1 + 2/2 = 3
     *		3 |   3/1 + 3/2 + 3/3 = 5 1/2
     *		4 |   4/1 + 4/2 + 4/3 + 4/4 = 8 1/3
     *		n |   n * H(n)
     *
     * This is called the "coupon collector problem" and H(n) is called the
     * harmonic series.  This could be approximated by n * ln(n), but for
     * reasonable numbers of drives we might as well just compute the series.
     *
     * Alternatively we could set the target to the number of pages necessary
     * so that the expected number of active spindles is some arbitrary
     * percentage of the total.  This sounds the same but is actually slightly
     * different.  The result ends up being ln(1-P)/ln((n-1)/n) where P is
     * that desired fraction.
     *
     * Experimental results show that both of these formulas aren't aggressive
     * enough, but we don't really have any better proposals.
     *
     * Note that if *newval = 0 (disabled), we must set target = 0.
     * ----------
     */

    for (i = 1; i <= *newval; i++)
        new_prefetch_pages += (double)*newval / (double)i;

    /* This range check shouldn't fail, but let's be paranoid */
    if (new_prefetch_pages >= 0.0 && new_prefetch_pages < (double)INT_MAX) {
        int* myextra = (int*)guc_malloc(ERROR, sizeof(int));

        *myextra = (int)rint(new_prefetch_pages);
        *extra = (void*)myextra;

        return true;
    } else
        return false;

#else
    return true;
#endif /* USE_PREFETCH */
}

static void assign_effective_io_concurrency(int newval, void* extra)
{
#ifdef USE_PREFETCH
    u_sess->storage_cxt.target_prefetch_pages = *((int*)extra);
#endif /* USE_PREFETCH */
}

static void assign_pgstat_temp_directory(const char* newval, void* extra)
{
    /* check_canonical_path already canonicalized newval for us */
    char* tname = NULL;
    char* fname = NULL;
    int rcs = 0;

    tname = (char*)guc_malloc(ERROR, strlen(newval) + 12); /* /pgstat.tmp */
    rcs = snprintf_s(tname, strlen(newval) + 12, strlen(newval) + 11, "%s/pgstat.tmp", newval);
    securec_check_ss(rcs, "\0", "\0");
    fname = (char*)guc_malloc(ERROR, strlen(newval) + 13); /* /pgstat.stat */
    rcs = snprintf_s(fname, strlen(newval) + 13, strlen(newval) + 12, "%s/pgstat.stat", newval);
    securec_check_ss(rcs, "\0", "\0");

    if (u_sess->stat_cxt.pgstat_stat_tmpname)
        pfree(u_sess->stat_cxt.pgstat_stat_tmpname);

    u_sess->stat_cxt.pgstat_stat_tmpname = tname;

    if (u_sess->stat_cxt.pgstat_stat_filename)
        pfree(u_sess->stat_cxt.pgstat_stat_filename);

    u_sess->stat_cxt.pgstat_stat_filename = fname;
}

static bool check_application_name(char** newval, void** extra, GucSource source)
{
    /* Only allow clean ASCII chars in the application name */
    char* p = NULL;

    for (p = *newval; *p; p++) {
        if (*p < 32 || *p > 126)
            *p = '?';
    }

    return true;
}

static void assign_application_name(const char* newval, void* extra)
{
    /* Update the pg_stat_activity view */
    pgstat_report_appname(newval);

#ifndef ENABLE_MULTIPLE_NODES
    if (AmPostmasterProcess() && newval[0] != '\0' && g_instance.exec_cxt.global_application_name[0] == '\0') {
        g_instance.exec_cxt.global_application_name = pstrdup(newval);
    }
#endif
}

static const char* show_log_file_mode(void)
{
    char* buf = t_thrd.buf_cxt.show_log_file_mode_buf;
    int size = sizeof(t_thrd.buf_cxt.show_log_file_mode_buf);
    int rcs = 0;

    rcs = snprintf_s(buf, size, size - 1, "%04o", u_sess->attr.attr_common.Log_file_mode);
    securec_check_ss(rcs, "\0", "\0");
    return buf;
}

/*
 * Brief			: Check the value of double-type parameter for security
 * Description		:
 * Notes			:
 */
bool check_double_parameter(double* newval, void** extra, GucSource source)
{
    if (*newval >= 0) {
        return true;
    } else {
        return false;
    }
}

/*
 * Check permission for SET ROLE and SET SESSION AUTHORIZATION
 */
static void check_setrole_permission(const char* rolename, char* passwd, bool IsSetRole) {
    HeapTuple roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(rolename));
    if (!HeapTupleIsValid(roleTup)) {
        str_reset(passwd);
        if (IsSetRole) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Invalid username/password,set role denied.")));
        } else {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Invalid username/password,set session_authorization denied.")));
        }
    }
    Oid roleid = HeapTupleGetOid(roleTup);
    bool is_initialuser = ((Form_pg_authid)GETSTRUCT(roleTup))->rolsuper;
    ReleaseSysCache(roleTup);

    if (is_initialuser && GetUserId() != BOOTSTRAP_SUPERUSERID) {
        str_reset(passwd);
        if (IsSetRole) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to set role \"%s\"", rolename)));
        } else {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to set session authorization")));
        }
    }

    if (IsSetRole) {
        if (!is_member_of_role(GetSessionUserId(), roleid)) {
            str_reset(passwd);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to set role \"%s\"", rolename)));
        }
    } else {
        AssertState(OidIsValid(u_sess->misc_cxt.AuthenticatedUserId));
        if (!t_thrd.xact_cxt.bInAbortTransaction && roleid != u_sess->misc_cxt.AuthenticatedUserId &&
            !u_sess->misc_cxt.AuthenticatedUserIsSuperuser && !superuser()) {
            str_reset(passwd);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to set session authorization")));
        }
    }
}

/*
 * Brief			: Verify the SET Role and Passwd
 * Description		: Both SET ROLE and SET ROLE PASSWORD are handled here
 * Notes			:
 */
static bool verify_setrole_passwd(const char* rolename, char* passwd, bool IsSetRole)
{
    TupleDesc pg_authid_dsc;
    HeapTuple tuple;
    Datum authidPasswdDatum;
    bool authidPasswdIsNull = false;
    Relation pg_authid_rel;
    char* rolepasswd = NULL;
    bool isPwdEqual = true;
    char encrypted_md5_password[MD5_PASSWD_LEN + 1] = {0};
    char encrypted_sha256_password[SHA256_LENGTH + ENCRYPTED_STRING_LENGTH + 1] = {0};
    char encrypted_sm3_password[SM3_LENGTH + ENCRYPTED_STRING_LENGTH + 1] = {0};
    char encrypted_combined_password[MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1] = {0};
    char salt[SALT_LENGTH * 2 + 1] = {0};
    TimestampTz lastTryLoginTime;
    Datum vbegin_datum;
    Datum vuntil_datum;
    TimestampTz vbegin = 0;
    TimestampTz vuntil = 0;
    bool vbegin_null = true;
    bool vuntil_null = true;
    Oid roleid;
    int iteration_count = 0;
    errno_t ss_rc = 0;
    USER_STATUS rolestatus = UNLOCK_STATUS;

    if (passwd == NULL) {
        if (IsSetRole) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("Invalid username/password,set role denied.")));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_NAME),
                    errmsg("Invalid username/password,set session_authorization denied.")));
        }
    }

    /* AccessShareLock is enough for SELECT*/
    pg_authid_rel = heap_open(AuthIdRelationId, AccessShareLock);
    pg_authid_dsc = RelationGetDescr(pg_authid_rel);
    tuple = SearchSysCache1(AUTHNAME, PointerGetDatum(rolename));

    if (!HeapTupleIsValid(tuple)) {
        str_reset(passwd);
        if (IsSetRole) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Invalid username/password,set role denied.")));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("Invalid username/password,set session_authorization denied.")));
        }
    }

    /* get the role password*/
    authidPasswdDatum = heap_getattr(tuple, Anum_pg_authid_rolpassword, pg_authid_dsc, &authidPasswdIsNull);

    /*
     * In default, the role password could not be NULL, except we failed to
     * get the attribute.
     */
    if (authidPasswdIsNull == false &&  (void*)authidPasswdDatum != NULL) {
        rolepasswd = TextDatumGetCString(authidPasswdDatum);
    } else {
        return false;
    }

    /*whether the passwd equal to the rolepasswd*/
    if (!isPWDENCRYPTED(passwd)) {
        if (isMD5(rolepasswd)) {
            if (!pg_md5_encrypt(passwd, rolename, strlen(rolename), encrypted_md5_password)) {
                str_reset(passwd);
                str_reset(rolepasswd);
                ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("md5-password encryption failed")));
            }

            if (strncmp(rolepasswd, encrypted_md5_password, MD5_PASSWD_LEN + 1) != 0) {
                isPwdEqual = false;
            }
        } else if (isSHA256(rolepasswd)) {

            ss_rc = strncpy_s(salt, sizeof(salt), &rolepasswd[SHA256_LENGTH], sizeof(salt) - 1);
            securec_check(ss_rc, "", "");
            salt[sizeof(salt) - 1] = '\0';

            iteration_count = decode_iteration(&rolepasswd[SHA256_PASSWD_LEN]);
            if (!pg_sha256_encrypt(passwd, salt, strlen(salt), encrypted_sha256_password, NULL, iteration_count)) {
                str_reset(passwd);
                str_reset(rolepasswd);
                ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("sha256-password encryption failed")));
            }

            if (strncmp(rolepasswd, encrypted_sha256_password, ENCRYPTED_STRING_LENGTH + 1) != 0) {
                isPwdEqual = false;
            }
        } else if (isSM3(rolepasswd)) {

            ss_rc = strncpy_s(salt, sizeof(salt), &rolepasswd[SM3_LENGTH], sizeof(salt) - 1);
            securec_check(ss_rc, "", "");
            salt[sizeof(salt) - 1] = '\0';

            iteration_count = decode_iteration(&rolepasswd[SM3_PASSWD_LEN]);
            if (!GsSm3Encrypt(passwd, salt, strlen(salt), encrypted_sm3_password, NULL, iteration_count)) {
                str_reset(passwd);
                str_reset(rolepasswd);
                ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("sm3-password encryption failed")));
            }

            if (strncmp(rolepasswd, encrypted_sm3_password, ENCRYPTED_STRING_LENGTH + 1) != 0) {
                isPwdEqual = false;
            }
        } else if (isCOMBINED(rolepasswd)) {
            errno_t rc = EOK;
            rc = strncpy_s(salt, sizeof(salt), &rolepasswd[SHA256_LENGTH], sizeof(salt) - 1);
            securec_check(rc, "\0", "\0");
            salt[sizeof(salt) - 1] = '\0';

            iteration_count = decode_iteration(&rolepasswd[SHA256_PASSWD_LEN + MD5_PASSWD_LEN]);
            if (!pg_sha256_encrypt(passwd, salt, strlen(salt), encrypted_sha256_password, NULL, iteration_count)) {
                str_reset(passwd);
                str_reset(rolepasswd);
                ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("first stage encryption password failed")));
            }

            if (!pg_md5_encrypt(passwd, rolename, strlen(rolename), encrypted_md5_password)) {
                str_reset(passwd);
                str_reset(rolepasswd);
                ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("second stage encryption password failed")));
            }

            rc = snprintf_s(encrypted_combined_password,
                MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1,
                MD5_PASSWD_LEN + SHA256_PASSWD_LEN,
                "%s%s",
                encrypted_sha256_password,
                encrypted_md5_password);
            securec_check_ss(rc, "\0", "\0");

            if (strncmp(rolepasswd, encrypted_sha256_password, ENCRYPTED_STRING_LENGTH + 1) != 0) {
                isPwdEqual = false;
            }
        } else {
            str_reset(passwd);
            str_reset(rolepasswd);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("Invalid password stored")));
        }
    } else {
        if (strcmp(passwd, rolepasswd) != 0) {
            isPwdEqual = false;
        } else {
            ereport(NOTICE,
                (errcode(ERRCODE_INVALID_PASSWORD), errmsg("Using encrypted password directly is not recommended.")));
        }
    }

    lastTryLoginTime = GetCurrentTimestamp();
    if (!TimestampDifferenceExceeds(u_sess->utils_cxt.lastFailedLoginTime, lastTryLoginTime, MS_PER_S * 1)) {
        str_reset(passwd);
        str_reset(rolepasswd);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("you are not allowed to do that operation immediately, please try again later")));
    }

    /* get the roleid for check lock/unlock. */
    roleid = GetRoleOid(rolename);

    /* Check whether the role have been locked. */
    if (!IsConnFromCoord()) {
        if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
            rolestatus = GetAccountLockedStatusFromHashTable(roleid);
        } else {
            rolestatus = GetAccountLockedStatus(roleid);
        }
        if (rolestatus != UNLOCK_STATUS) {
            bool unlocked = false;
            if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
                unlocked = UnlockAccountToHashTable(roleid, false, false);
            } else {
                unlocked = TryUnlockAccount(roleid, false, false);
            }
            if (!unlocked) {
                str_reset(passwd);
                str_reset(rolepasswd);
                ereport(ERROR, (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                    errmsg("The account has been locked.")));
            }
        } else if (isPwdEqual == true) {
                if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
                    (void)UnlockAccountToHashTable(roleid, false, true);
                } else {
                    (void)TryUnlockAccount(roleid, false, true);
                }
        }

        /* Currently inner set statement from pooler don't check pwd. */
        if (isPwdEqual == false) {
            u_sess->utils_cxt.lastFailedLoginTime = GetCurrentTimestamp();
            if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
                UpdateFailCountToHashTable(roleid, 1, false);
            } else {
                TryLockAccount(roleid, 1, false);
            }
            str_reset(passwd);
            str_reset(rolepasswd);
            if (IsSetRole) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("Invalid username/password,set role denied.")));
            } else {
                ereport(ERROR, (errcode(ERRCODE_INVALID_NAME),
                    errmsg("Invalid username/password,set session_authorization denied.")));
            }
        }
    }

    /* whether within the validiity of period */
    vbegin_datum = heap_getattr(tuple, Anum_pg_authid_rolvalidbegin, pg_authid_dsc, &vbegin_null);
    if (!vbegin_null) {
        vbegin = DatumGetTimestampTz(vbegin_datum);
    }

    vuntil_datum = heap_getattr(tuple, Anum_pg_authid_rolvaliduntil, pg_authid_dsc, &vuntil_null);
    if (!vuntil_null) {
        vuntil = DatumGetTimestampTz(vuntil_datum);
    }

    if ((!vbegin_null && vbegin > lastTryLoginTime) || (!vuntil_null && vuntil < lastTryLoginTime)) {
        str_reset(passwd);
        str_reset(rolepasswd);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                errmsg("The account is not within the period of validity.")));
    }

    ReleaseSysCache(tuple);
    heap_close(pg_authid_rel, NoLock);
    str_reset(passwd);
    str_reset(rolepasswd);
    return true;
}

/*
 * @@GaussDB@@
 * Brief			: Check one channel of the replconninfo.
 * Description		: If the format of the channel is right return true, else return false.
 * Notes			:
 */
bool CheckReplChannel(const char* ChannelInfo)
{
    char* iter = NULL;
    char* ReplStr = NULL;

    if (NULL == ChannelInfo) {
        return false;
    } else {
        ReplStr = pstrdup(ChannelInfo);
        if (NULL == ReplStr) {
            return false;
        } else {
            iter = strstr(ReplStr, "localhost");
            if (NULL == iter) {
                pfree(ReplStr);
                return false;
            }
            iter += strlen("localhost");
            while (' ' == *iter || '=' == *iter) {
                iter++;
            }
            if ((!isdigit(*iter) && *iter != ':' && !isalpha(*iter)) ||
                (0 == strncmp(iter, "localport", strlen("localport")))) {
                pfree(ReplStr);
                return false;
            }

            iter = strstr(ReplStr, "localport");
            if (NULL == iter) {
                pfree(ReplStr);
                return false;
            }
            iter += strlen("localport");
            while (' ' == *iter || '=' == *iter) {
                iter++;
            }
            if (!isdigit(*iter)) {
                pfree(ReplStr);
                return false;
            }

            iter = strstr(ReplStr, "remotehost");
            if (NULL == iter) {
                pfree(ReplStr);
                return false;
            }
            iter += strlen("remotehost");
            while (' ' == *iter || '=' == *iter) {
                iter++;
            }
            if ((!isdigit(*iter) && *iter != ':' && !isalpha(*iter)) ||
                (0 == strncmp(iter, "remoteport", strlen("remoteport")))) {
                pfree(ReplStr);
                return false;
            }

            iter = strstr(ReplStr, "remoteport");
            if (NULL == iter) {
                pfree(ReplStr);
                return false;
            }
            iter += strlen("remoteport");
            while (' ' == *iter || '=' == *iter) {
                iter++;
            }
            if (!isdigit(*iter)) {
                pfree(ReplStr);
                return false;
            }
        }
    }

    pfree(ReplStr);
    return true;
}

/*
 * @@GaussDB@@
 * Brief		    : void * pg_malloc(size_t size)
 * Description	: malloc a space of size
 * Notes		    :
 */
void* pg_malloc(size_t size)
{
    void* result = NULL;

    /* Avoid unportable behavior of malloc(0) */
    if (size == 0) {
        write_stderr(_("walreceiver malloc 0\n"));
        gs_thread_exit(1);
    }
#ifdef FRONTEND
    result = malloc(size);
#else
    result = MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), size);
#endif

    if (result == NULL) {
        write_stderr(_("walreceiver out of memory\n"));
        gs_thread_exit(1);
    }
    return result;
}
/*
 * @@GaussDB@@
 * Brief		    : char *xstrdup(const char *s)
 * Description	: copy string
 * Notes		    :
 */
char* xstrdup(const char* s)
{
    char* result = NULL;

#ifndef EXEC_BACKEND
    result = strdup(s);
#else
    result = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), s);
#endif

    if (result == NULL) {
        write_stderr(_("walreceiver out of memory\n"));
        gs_thread_exit(1);
    }
    return result;
}

void release_opt_lines(char** opt_lines)
{
    char** opt_line = NULL;

    if (opt_lines == NULL)
        return;

    for (opt_line = opt_lines; *opt_line != NULL; opt_line++) {
        selfpfree(*opt_line);
        *opt_line = NULL;
    }
    selfpfree(opt_lines);
    opt_lines = NULL;
    return;
}

char** alloc_opt_lines(int opt_lines_num)
{
    char** opt_lines = NULL;
    int i = 0;
    opt_lines = ((char**)pg_malloc(opt_lines_num * sizeof(char*)));
    for (i = 0; i < opt_lines_num; i++) {
        opt_lines[i] = NULL;
    }

    return opt_lines;
}

/*
 * @@GaussDB@@
 * Brief		    : static char ** read_guc_file(const char* path)
 * Description	: get the lines from a text file - return NULL if file can't be opened
 * Notes		    :
 */
char** read_guc_file(const char* path)
{
    FILE* infile = NULL;
    int maxlength = 1, linelen = 0;
    int nlines = 0;
    char** result;
    char* buffer = NULL;
    int c;
    const int limit_of_length = 100000;  // limit of maxlength and nlines
    if ((infile = fopen(path, "r+")) == NULL) {
        (void)fprintf(stderr, _("could not open file for reading: %s\n"), gs_strerror(errno));
        return NULL;
    }
    while ((c = fgetc(infile)) != EOF) { /* pass over the file twice - the first time to size the result */
        linelen++;
        if (c == '\n') {
            nlines++;
            if (linelen > maxlength)
                maxlength = linelen;
            linelen = 0;
        }
    }
    /* handle last line without a terminating newline (yuck) */
    if (linelen)
        nlines++;
    if (linelen > maxlength)
        maxlength = linelen;

    if (maxlength <= 1) {
        fclose(infile);
        infile = NULL;
        return NULL;
    }
    if (maxlength > limit_of_length || nlines > limit_of_length) {
        fclose(infile);
        infile = NULL;
        ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR), errmsg("Length of file or line is too long.")));
    }

    /* set up the result and the line buffer */
    result = (char**)pg_malloc((nlines + 2) * sizeof(char*));  /* Reserve one extra for alter system set. */
    buffer = (char*)pg_malloc(maxlength + 1);

    /* now reprocess the file and store the lines */
    rewind(infile);
    nlines = 0;

    while (fgets(buffer, maxlength + 1, infile) != NULL)
        result[nlines++] = xstrdup(buffer);
    (void)fclose(infile);
    pfree(buffer);
    result[nlines] = result[nlines + 1] = NULL;

    return result;
}
/*
 * @@GaussDB@@
 * Brief		    : write_guc_file
 * Description	: write the config file
 * Notes		     1. free the lines outside. 2. truncate file before open:
 */
ErrCode write_guc_file(const char* path, char** lines)
{
    FILE* out_file = NULL;
    char** line = NULL;
    ErrCode retcode = CODE_OK;
    if ((out_file = fopen(path, "w")) == NULL) {
        (void)fprintf(stderr, _("walreceiver could not open file \"%s\" for writing: %s\n"), path, gs_strerror(errno));
        return CODE_OPEN_CONFFILE_FAILED;
    }
    for (line = lines; *line != NULL; line++) {
        if (fputs(*line, out_file) < 0) {
            retcode = CODE_WRITE_CONFFILE_ERROR;
            break;
        }
        pfree(*line);
        *line = NULL;
    }
    if (retcode == CODE_OK) {
        if (fflush(out_file)) {
            (void)fprintf(stderr, _("walreceiver flush file \"%s\" failed: %s\n"), path, gs_strerror(errno));
            retcode = CODE_WRITE_CONFFILE_ERROR;
        }
    }
    if (fclose(out_file)) {
        (void)fprintf(stderr, _("walreceiver could not write file \"%s\": %s\n"), path, gs_strerror(errno));
        retcode = CODE_CLOSE_CONFFILE_FAILED;
    }
    out_file = NULL;
    return retcode;
}

/*
 * @@GaussDB@@
 * Brief		    : copy_guc_lines
 * Description	: copy and store the specified lines
 * Notes		    :
 */
ErrCode copy_guc_lines(char** copy_to_line, char** optlines, const char** opt_name)
{
    int optvalue_off = 0;
    int optvalue_len = 0;

    int opt_name_index = 0;
    int i = 0;
    int opt_name_len = 0;
    int opt_num = 0;

    if (optlines != NULL) {
        for (i = 0; i < RESERVE_SIZE; i++) {
            opt_name_index = find_guc_option(optlines, opt_name[i], NULL, NULL, &optvalue_off, &optvalue_len, false);
            if (INVALID_LINES_IDX != opt_name_index) {
                errno_t errorno = EOK;
                opt_name_len = strlen(optlines[opt_name_index]) + 1;
                if (opt_name_len > MAX_PARAM_LEN) {
                    ereport(LOG, (errmsg("guc '%s' is out of 1024", optlines[opt_name_index])));
                    return CODE_INTERNAL_ERROR;
                }

                copy_to_line[i] = (char*)pg_malloc(opt_name_len);
                errorno = memset_s(copy_to_line[i], opt_name_len, 0, opt_name_len);
                securec_check(errorno, "\0", "\0");
                errorno = strncpy_s(copy_to_line[i], opt_name_len, optlines[opt_name_index], opt_name_len - 1);
                securec_check(errorno, "\0", "\0");

                opt_num++;
            }
        }

        if (opt_num <= 0) {
            ereport(LOG, (errmsg("the config file has no reserved parameters.")));
            return CODE_INTERNAL_ERROR;
        }
    } else {
        ereport(LOG, (errmsg("the config file is null")));
        return CODE_INTERNAL_ERROR;
    }
    return CODE_OK;
}

/*
 * @@GaussDB@@
 * Brief		    : void modify_guc_one_line
 * Description	: modify one guc config
 * Notes		    :
 */
void modify_guc_one_line(char*** guc_optlines, const char* opt_name, const char* copy_from_line)
{
    int opt_name_index = 0;
    int optvalue_off = 0;
    int optvalue_len = 0;
    char **optlines = *guc_optlines;

    opt_name_index = find_guc_option(optlines, opt_name, NULL, NULL, &optvalue_off, &optvalue_len, false);
    if (INVALID_LINES_IDX != opt_name_index) {
        pfree(optlines[opt_name_index]);
    } else {
        int lines = 0;
        /* get optlines row number */
        for (lines = 0; optlines[lines] != NULL; ++lines) {}
        /* add one line guc item, and set a end flag NULL. */
        char **optlines_copy = (char**)pg_malloc((lines + 2) * sizeof(char*));
        errno_t rc = memset_s(optlines_copy, (lines + 2) * sizeof(char*), 0, (lines + 2) * sizeof(char*));
        securec_check(rc, "\0", "\0");
        for (int cnt = 0; cnt < lines; cnt++) {
            optlines_copy[cnt] = optlines[cnt];
        }
        pfree(optlines);
        *guc_optlines = optlines_copy;
        optlines = *guc_optlines;
        optlines_copy = NULL;

        Assert(optlines[lines] == NULL);
        optlines[lines + 1] = NULL;
        opt_name_index = lines;
    }
    int newsize = strlen(copy_from_line) + 1;

    optlines[opt_name_index] = (char*)pg_malloc(newsize);

    errno_t rc = strncpy_s(optlines[opt_name_index], newsize, copy_from_line, newsize - 1);
    securec_check(rc, "\0", "\0");

    if (newsize > MAX_PARAM_LEN) {
        ereport(WARNING, (errmsg("modify_guc_one_line:opt len '%s' is out of 1024", optlines[opt_name_index])));
    }

}


/*
 * @@GaussDB@@
 * Brief		    : static void modify_config_file
 * Description	: get the lines from a text file - return NULL if file can't be opened
 * Notes		    :
 */
void modify_guc_lines(char*** guc_optlines, const char** opt_name, char** copy_from_line)
{
    int opt_name_index = 0;
    int optvalue_off = 0;
    int optvalue_len = 0;
    char **optlines = *guc_optlines;
    if (optlines == NULL) {
        ereport(LOG, (errmsg("configuration file has not data")));
    } else {
        for (int i = 0; i < RESERVE_SIZE; i++) {
            opt_name_index = find_guc_option(optlines, opt_name[i], NULL, NULL, &optvalue_off, &optvalue_len, false);
            if (NULL != copy_from_line[i]) {
                if (INVALID_LINES_IDX != opt_name_index) {
                    pfree(optlines[opt_name_index]);
                } else {
                    int lines = 0;
                    /* get optlines row number */
                    for (lines = 0; optlines[lines] != NULL; lines++) {}
                    /* add one line guc item, and set a end flag NULL. */
                    char **optlines_copy = (char**)pg_malloc((lines + 2) * sizeof(char*));
                    errno_t rc = memset_s(optlines_copy, (lines + 2) * sizeof(char*), 0, (lines + 2) * sizeof(char*));
                    securec_check(rc, "\0", "\0");
                    for (int cnt = 0; cnt < lines; cnt++) {
                        optlines_copy[cnt] = optlines[cnt];
                    }
                    pfree(optlines);
                    *guc_optlines = optlines_copy;
                    optlines = *guc_optlines;
                    optlines_copy = nullptr;
                    /* read_guc_file function set last tuple optlines[nlines] is NULL, NULL is the end flag of array. */
                    Assert(optlines[lines] == NULL);
                    optlines[lines + 1] = NULL;
                    opt_name_index = lines;
                }
                int newsize = strlen(copy_from_line[i]) + 1;
                int real_newsize = Min(newsize, MAX_PARAM_LEN);
                optlines[opt_name_index] = (char*)pg_malloc(newsize);

                errno_t rc = strncpy_s(optlines[opt_name_index], real_newsize, copy_from_line[i], real_newsize - 1);
                securec_check(rc, "\0", "\0");

                if (newsize > MAX_PARAM_LEN) {
                    ereport(WARNING, (errmsg("guc '%s' is out of 1024", optlines[opt_name_index])));
                }
            }
        }
    }
}

/*
 * @@GaussDB@@
 * Brief            : void delete_guc_lines
 * Description      : get the lines from a text file and comment lines which is chosen
 * Notes            :
 */
void comment_guc_lines(char** optlines, const char** opt_name)
{
    int opt_name_index = 0;
    int optvalue_off = 0;
    int optvalue_len = 0;
    if (optlines == NULL) {
        ereport(LOG, (errmsg("configuration file has not data")));
    } else {
        for (int i = 0; i < RESERVE_SIZE; i++) {
            opt_name_index = find_guc_option(optlines, opt_name[i], NULL, NULL, &optvalue_off, &optvalue_len, false);
            if (opt_name_index != INVALID_LINES_IDX) {
                /* Skip all the blanks at the begin of the optLine */
                char *p = optlines[opt_name_index];
                while (isspace((unsigned char)*p)) {
                    p++;
                }
                if (*p == '#') {
                    continue;
                } else {
                    /* add '#' in the head of optlines[opt_name_index], newsize = copysize + 1 */
                    int copysize = strlen(optlines[opt_name_index]) + 1;
                    int newsize = copysize + 1;
                    int real_newsize = Min(newsize, MAX_PARAM_LEN);
                    errno_t rc = EOK;

                    char *optline_copy = (char *)pg_malloc(copysize);
                    rc = memset_s(optline_copy, copysize, 0, copysize);
                    securec_check(rc, "\0", "\0");
                    rc = strcpy_s(optline_copy, copysize, optlines[opt_name_index]);
                    securec_check(rc, "\0", "\0");
                    pfree(optlines[opt_name_index]);
                    optlines[opt_name_index] = (char*)pg_malloc(newsize);
                    rc = memset_s(optlines[opt_name_index], newsize, 0, newsize);
                    securec_check(rc, "\0", "\0");
                    optlines[opt_name_index][0] = '#';
                    rc = strncpy_s(optlines[opt_name_index] + 1, real_newsize -1, optline_copy, real_newsize - 2);
                    securec_check(rc, "\0", "\0");
                    pfree_ext(optline_copy);

                    if (newsize > MAX_PARAM_LEN) {
                        ereport(WARNING, (errmsg("guc '%s' is out of 1024", optlines[opt_name_index])));
                    }
                }
            }
        }
    }
}

/*
 * @@GaussDB@@
 * Brief            : void add_guc_optlines_to_buffer
 * Description      : add guc file context optlines to buffer. return values is the length of buffer, including '\0'.
 * Notes            : WARNING: After using this function, remember to free the buffer.
 */
int add_guc_optlines_to_buffer(char** optlines, char** buffer)
{
    int length = 0;
    int i = 0;
    char *buf = nullptr;
    errno_t rc = EOK;
    if (optlines == NULL) {
        ereport(LOG, (errmsg("configuration file has not data")));
    } else {
        for (i = 0; optlines[i] != NULL; i++) {
            length += strlen(optlines[i]);
        }
        buf = (char*) pg_malloc(length + 1);
        rc = memset_s(buf, length + 1, 0, length + 1);
        securec_check(rc, "\0", "\0");
        for (i = 0; optlines[i] != NULL; i++) {
            rc = strcat_s(buf, length + 1, optlines[i]);
            securec_check(rc, "\0", "\0");
        }
        *buffer = buf;
    }
    return length + 1;
}


/*
 * @@GaussDB@@
 * Brief		    : find_guc_option
 * Description	: find the line info of the specified parameter in file
 * Notes		    :
 */
int find_guc_option(char** optlines, const char* opt_name,
    int* name_offset, int* name_len, int* value_offset, int* value_len, bool ignore_case)
{
    bool isMatched = false;
    int i = 0;
    size_t paramlen = 0;
    int targetline = 0;
    int matchtimes = 0;

    if (NULL == optlines || NULL == opt_name) {
        return INVALID_LINES_IDX;
    }
    paramlen = (size_t)strnlen(opt_name, MAX_PARAM_LEN);
    if (name_len != NULL) {
        *name_len = (int)paramlen;
    }
    /* The first loop is to deal with the lines not commented by '#' */
    for (i = 0; optlines[i] != NULL; i++) {
        if (!isOptLineCommented(optlines[i])) {
            isMatched = isMatchOptionName(optlines[i], opt_name, paramlen, name_offset,
                                          value_len, value_offset, ignore_case);
            if (isMatched) {
                matchtimes++;
                targetline = i;
            }
        }
    }

    /* The line of last one will be recorded when there are parameters with the same name in postgresql.conf */
    if (matchtimes > 1) {
        ereport(NOTICE, (errmsg("There are %d \"%s\" not commented in \"postgresql.conf\", and only the "
            "last one in %dth line will be set and used.",
            matchtimes, opt_name, (targetline + 1))));
    }
    if (matchtimes > 0) {
        return targetline;
    }

    /* The second loop is to deal with the lines commented by '#' */
    matchtimes = 0;
    for (i = 0; optlines[i] != NULL; i++) {
        if (isOptLineCommented(optlines[i])) {
            isMatched = isMatchOptionName(optlines[i], opt_name, paramlen, name_offset,
                                          value_len, value_offset, ignore_case);
            if (isMatched) {
                matchtimes++;
                targetline = i;
            }
        }
    }

    /* The line of last one will be returned, otherwise it return invaild line */
    return (matchtimes > 0) ? targetline : INVALID_LINES_IDX;
}
/*
 * @@GaussDB@@
 * Brief		    : genarate_temp_file
 * Description	: put the received data into temp file
 * Notes		    :
 */
ErrCode generate_temp_file(char* buf, char* temppath, size_t size)
{
    FILE* fp = NULL;

    if ((fp = fopen(temppath, "w")) == NULL) {
        ereport(LOG, (errmsg("could not open file \"%s\": %m", temppath)));
        return CODE_OPEN_CONFFILE_FAILED;
    }
    if (fwrite((void*)buf, 1, size, fp) != size) {
        ereport(LOG, (errmsg("could not write file \"%s\": %m", temppath)));
        fclose(fp);
        return CODE_WRITE_CONFFILE_ERROR;
    }
    if (fclose(fp)) {
        ereport(LOG, (errmsg("could not close config file \"%s\": %m", temppath)));
        return CODE_CLOSE_CONFFILE_FAILED;
    }

    return CODE_OK;
}
/*
 * @@GaussDB@@
 * Brief		    : copy_asyn_lines
 * Description	: copy and store the asynchronous lines
 * Notes		    :
 */
ErrCode copy_asyn_lines(char* path, char** copy_to_line, const char** opt_name)
{
    char** opt_lines = NULL;
    ErrCode retcode = CODE_OK;
    opt_lines = read_guc_file(path);
    if (opt_lines == NULL) {
        ereport(LOG, (errmsg("the config file has no data,please check it.")));
        retcode = CODE_INTERNAL_ERROR;
    } else {
        retcode = copy_guc_lines(copy_to_line, opt_lines, opt_name);
        if (retcode != CODE_OK) {
            ereport(LOG, (errmsg("copy_guc_lines failed,code error:%d", retcode)));
        }
        release_opt_lines(opt_lines);
        opt_lines = NULL;
    }
    return retcode;
}

/*
 * @@GaussDB@@
 * Brief		    : copy_asyn_lines
 * Description	: update the specified lines of the file
 * Notes		    :
 */
ErrCode update_temp_file(char* tempfilepath, char** copy_to_line, const char** opt_name)
{
    char** opt_lines = NULL;
    ErrCode ret;
    opt_lines = read_guc_file(tempfilepath);
    if (opt_lines == NULL) {
        ereport(LOG, (errmsg("the config file has no data,please check it.")));
        return CODE_INTERNAL_ERROR;
    }
    modify_guc_lines(&opt_lines, opt_name, copy_to_line);
    ret = write_guc_file(tempfilepath, opt_lines);
    release_opt_lines(opt_lines);
    opt_lines = NULL;
    if (ret == CODE_OK) {
        ereport(LOG, (errmsg("the config file %s updates success.", tempfilepath)));
    }
    return ret;
}

/*
 * @@GaussDB@@
 * Brief		    : get the file lock
 * Description	    :
 * Notes		    :
 */
ErrCode get_file_lock(const char* path, ConfFileLock* filelock)
{
    FILE* fp = NULL;
    struct stat statbuf;
    char content[PG_LOCKFILE_SIZE] = {0};
    if (lstat(path, &statbuf) != 0) {
        fp = fopen(path, PG_BINARY_W);
        if (NULL == fp) {
            ereport(LOG, (errmsg("Can't open lock file for write\"%s\",errormsg: %s", path, gs_strerror(errno))));
            return CODE_OPEN_CONFFILE_FAILED;
        } else {
            if (fwrite(content, PG_LOCKFILE_SIZE, 1, fp) != 1) {
                fclose(fp);
                ereport(LOG, (errmsg("Can't write lock file \"%s\",errormsg: %s", path, gs_strerror(errno))));
                return CODE_WRITE_CONFFILE_ERROR;
            }
            fclose(fp);
        }
    }
    if ((fp = fopen(path, PG_BINARY_RW)) == NULL) {
        ereport(LOG, (errmsg("could not open file or directory \"%s\",errormsg: %s", path, gs_strerror(errno))));
        return CODE_OPEN_CONFFILE_FAILED;
    }
    if (flock(fileno(fp), LOCK_EX | LOCK_NB, 0, START_LOCATION, statbuf.st_size) == -1) {
        ereport(LOG, (errmsg("could not lock file or directory \"%s\",errormsg: %s", path, gs_strerror(errno))));
        fclose(fp);
        return CODE_LOCK_CONFFILE_FAILED;
    }
    filelock->fp = fp;
    filelock->size = statbuf.st_size;
    return CODE_OK;
}
/*
 * @@GaussDB@@
 * Brief		    : release the file lock
 * Description	:
 * Notes		    :
 */
void release_file_lock(ConfFileLock* filelock)
{
    int ret;
    ret = flock(fileno(filelock->fp), LOCK_UN, 0, START_LOCATION, filelock->size);
    if (ret != 0)
        ereport(WARNING, (errmsg("unlock file failed")));
    fclose(filelock->fp);
    filelock->fp = NULL;
    filelock->size = 0;
}

/*append set string to input_set_message*/
void append_set_message(const char* str)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));

    if ((*u_sess->utils_cxt.input_set_message).maxlen == 0) {
        initStringInfo(&(*u_sess->utils_cxt.input_set_message));
    }
    appendStringInfoString(&(*u_sess->utils_cxt.input_set_message), str);
    appendStringInfoString(&(*u_sess->utils_cxt.input_set_message), ";");
    MemoryContextSwitchTo(oldcontext);
}

/*
 * @Description: make set message with hash table
 * @IN void
 * @Return: void
 * @See also:
 */
void make_set_message(void)
{
    if (u_sess->utils_cxt.set_params_htab == NULL)
        return;

    MemoryContext oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));

    /*
     * If input_set_message exists, we will destroy it.
     * The reason for this is that the original string
     * contains the parameters that need to be reset. In
     * this case, we need to rebuild the message string
     * based on the hash table
     */
    if ((*u_sess->utils_cxt.input_set_message).maxlen == 0) {
        initStringInfo(&(*u_sess->utils_cxt.input_set_message));
    } else {
        resetStringInfo(&(*u_sess->utils_cxt.input_set_message));
    }

    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, u_sess->utils_cxt.set_params_htab);

    GucParamsEntry* entry = NULL;

    /*Get all session params from the hash table to rebuild message string */
    while ((entry = (GucParamsEntry*)hash_seq_search(&hash_seq)) != NULL) {
        PG_TRY();
        {
            appendStringInfoString(&(*u_sess->utils_cxt.input_set_message), entry->query);
            appendStringInfoString(&(*u_sess->utils_cxt.input_set_message), ";");
        }
        PG_CATCH();
        {
            hash_seq_term(&hash_seq);

            MemoryContextSwitchTo(oldcontext);

            PG_RE_THROW();
        }
        PG_END_TRY();
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * @Description: init params hash table
 * @IN void
 * @Return: void
 * @See also:
 */
void init_set_params_htab(void)
{
    HASHCTL hash_ctl;

    errno_t errval = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check_errval(errval, , LOG);

    hash_ctl.keysize = NAMEDATALEN;
    hash_ctl.hcxt = SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB);
    hash_ctl.entrysize = sizeof(GucParamsEntry);
    hash_ctl.hash = string_hash;

    u_sess->utils_cxt.set_params_htab = hash_create(
        "set params hash table", WORKLOAD_STAT_HASH_SIZE, &hash_ctl, HASH_CONTEXT | HASH_ELEM | HASH_FUNCTION);
}

/*
 * @Description: check message for sending
 * @IN stmt: variable set statement
 * @IN queryString: query string
 * @Return: 0: append
 *          1: rebuild
 *         -1: none
 * @See also:
 */
int check_set_message_to_send(const VariableSetStmt* stmt, const char* queryString)
{
    /* no hash table, we can only choose appending mode */
    if (u_sess->utils_cxt.set_params_htab == NULL)
        return 0;

    /* variable name is invalid, ignore this */
    if (!StringIsValid(stmt->name))
        return -1;

    bool found = false;

    GucParamsEntry* entry =
        (GucParamsEntry*)hash_search(u_sess->utils_cxt.set_params_htab, stmt->name, HASH_ENTER, &found);

    USE_MEMORY_CONTEXT(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));

    /* parameter is set, we will update the entry for making a new message */
    if (found) {
        if (entry->query)
            pfree_ext(entry->query);

        entry->query = pstrdup(queryString);

        return 1;
    }

    errno_t errval = strncpy_s(entry->name, sizeof(entry->name), stmt->name, sizeof(entry->name) - 1);
    securec_check_errval(errval, , LOG);

    entry->query = pstrdup(queryString);

    return 0;
}

/*get set commant in input_set_message*/
char* get_set_string()
{
    if ((*u_sess->utils_cxt.input_set_message).len > 0) {
        return (*u_sess->utils_cxt.input_set_message).data;
    } else {
        return NULL;
    }
}

/*
 * @Description: reset params hash table
 * @IN htab: hash table to be reset
 * @Return: void
 * @See also:
 */
void reset_params_htab(HTAB* htab, bool isCommit)
{
    /* need not process hash table */
    if (htab == NULL || hash_get_num_entries(htab) <= 0)
        return;

    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, htab);

    GucParamsEntry* entry = NULL;

    /* remove all session params from the hash table.*/
    while ((entry = (GucParamsEntry*)hash_seq_search(&hash_seq)) != NULL) {
        if (isCommit)
            (void)register_pooler_session_param(entry->name, entry->query, POOL_CMD_GLOBAL_SET);

        if (entry->query)
            pfree_ext(entry->query);

        hash_search(htab, entry->name, HASH_REMOVE, NULL);
    }
}

void reset_set_message(bool isCommit)
{
    if ((*u_sess->utils_cxt.input_set_message).maxlen != 0) {
        resetStringInfo(&(*u_sess->utils_cxt.input_set_message));
    }

    reset_params_htab(u_sess->utils_cxt.set_params_htab, isCommit);
}

#define ERRMSG_EMPTY_MODULE_NAME "empty module name"
#define ERRMSG_MODNAME_NOT_FOUND "module name \"%s\" not found"

#define ERRMSG_EMPTY_MODULE_LIST "empty module list"
#define ERRMSG_MODLIST_NOT_FOUND "module list not found"

#define HINT_MODULE_LIST_FMT "Module list begins with '(', ends with ')', and is delimited with '%c'."

#define HINT_QUERY_ALL_MODULES "Query all the existing modules by SHOW logging_module."

#define SKIP_SPACE(_p)                  \
    do {                                \
        while (*(_p) && ' ' == *(_p)) { \
            ++(_p);                     \
        }                               \
    } while (0)

/*
 * @Description: check whether keyword "on"/"off" is matched.
 * @IN/OUT start: input text which will be updated if true returned.
 * @OUT turn_on: "on" --> true;  "off" --> false;
 * @Return: return false if any keyword is not found; or return true.
 * @See also:
 */
static inline bool string_guc_keyword_matched(char** start, bool* turn_on)
{
    char* p = *start;

    SKIP_SPACE(p);

    if (0 == pg_strncasecmp("on", p, 2)) {
        p += 2;
        *turn_on = true;
    } else if (0 == pg_strncasecmp("off", p, 3)) {
        p += 3;
        *turn_on = false;
    } else {
        /* keyword "on"/"off" not found */
        goto missing_kw;
    }

    if (*p && !(' ' == *p || '(' == *p)) {
        /* "on"/"off" is the prefix of this word, error */
        goto missing_kw;
    }

    /* update string position */
    *start = p;
    return true;

missing_kw:
    /* bad format: not found key words "on" or "off" */
    GUC_check_errmsg("missing keywords \"on\" or \"off\"");
    GUC_check_errdetail("Must begin with keyword \"on\" or \"off\".");
    return false;
}

/*
 * @Description: check whether list starts with '(' and is not empty.
 * @IN/OUT start: input text which will be updated if true returned.
 * @Return: return true if list starts with '(' and this is not empty;
 *          otherwise return false.
 * @See also:
 */
static inline bool string_guc_list_start(char** start)
{
    char* p = *start;

    SKIP_SPACE(p);

    if ('\0' == *p || '(' != *p) {
        /* bad format: not found ( */
        GUC_check_errmsg(ERRMSG_MODLIST_NOT_FOUND);
        GUC_check_errdetail("'(' is not found.");
        GUC_check_errhint(HINT_MODULE_LIST_FMT, MOD_DELIMITER);
        return false;
    }

    ++p; /* skip '(' */
    SKIP_SPACE(p);

    if ('\0' == *p) {
        /* bad format: '(' not match anything */
        GUC_check_errmsg(ERRMSG_MODLIST_NOT_FOUND);
        GUC_check_errhint(HINT_MODULE_LIST_FMT, MOD_DELIMITER);
        return false;
    } else if (')' == *p) {
        /* bad format: empty list given */
        GUC_check_errmsg(ERRMSG_EMPTY_MODULE_LIST);
        GUC_check_errdetail("Must one or more modules are given, and delimited with '%c'.", MOD_DELIMITER);
        return false;
    }

    *start = p; /* update string position */
    return true;
}

/*
 * @Description: check whether list ends with ')' and well formatted.
 * @IN delim_found: "true" value means the last module name is empty.
 * @IN/OUT start: input text which will be updated if true returned.
 * @Return: true if it's well formatted; otherwise return false.
 * @See also:
 */
static inline bool string_guc_list_end(char** start, bool delim_found)
{
    char* p = *start;

    if ('\0' == *p) {
        /* bad format: ')' not found */
        GUC_check_errmsg(ERRMSG_MODLIST_NOT_FOUND);
        GUC_check_errdetail("Module list does not end with ')'.");
        GUC_check_errhint(HINT_MODULE_LIST_FMT, MOD_DELIMITER);
        return false;
    }
    if (delim_found) {
        /* bad format: the last module name is empty */
        GUC_check_errmsg(ERRMSG_EMPTY_MODULE_NAME);
        GUC_check_errdetail("The last module name is empty.");
        GUC_check_errhint("Module name must be given between '%c' and ')'.", MOD_DELIMITER);
        return false;
    }

    ++p; /* until here *p is ')', skip it */
    SKIP_SPACE(p);

    if ('\0' != *p) {
        /* bad format: non-space found after ')' */
        GUC_check_errmsg("extra text found after the first list of module names");
        GUC_check_errhint("Remove the extra text after the first list of module names.");
        return false;
    }

    *start = p; /* update string position */
    return true;
}

/*
 * @Description: find the next module name and start will point to it.
 * @OUT delim_found: mainly for the last module name which cannot be empty.
 * @IN/OUT start: input text which will be updated if true returned.
 * @Return: true if next name found; otherwise false.
 * @See also:
 */
static inline bool string_guc_list_find_next_name(char** start, bool* delim_found)
{
    char* p = *start;

    /* reset to be false */
    *delim_found = false;
    while (*p && (' ' == *p || MOD_DELIMITER == *p)) {
        if (MOD_DELIMITER == *p) {
            if (!(*delim_found)) {
                *delim_found = true;
            } else {
                /* bad format: empty module name */
                GUC_check_errmsg(ERRMSG_EMPTY_MODULE_NAME);
                GUC_check_errdetail("Module name cannot be empty.");
                GUC_check_errhint("Module name must be given between '%c'.", MOD_DELIMITER);
                return false;
            }
        }
        ++p; /* skip spaces and delimiter */
    }

    *start = p; /* update string position */
    return true;
}

/*
 * @Description: parse module name and check its validation.
 * @OUT all_found: "ALL" modules name is found
 * @IN/OUT mods: to remember all the found module id
 * @IN/OUT mods_num: size of modes[]
 * @IN mod_name: this module name
 * @IN mod_name_len: size of this module name
 * @Return:
 * @See also:
 */
static inline bool logging_module_parse_name(
    char* mod_name, int mod_name_len, ModuleId* mods, int* mods_num, bool* all_found)
{
    if (mod_name_len > 0 && mod_name_len < MODULE_NAME_MAXLEN) {
        ModuleId modid = MOD_MAX;
        const char ch = mod_name[mod_name_len];

        /* judge whether it's a valid module name */
        mod_name[mod_name_len] = '\0';
        modid = get_module_id(mod_name);
        if (MOD_MAX == modid) {
            /* bad format: not existing module name */
            GUC_check_errmsg(ERRMSG_MODNAME_NOT_FOUND, mod_name);
            GUC_check_errhint(HINT_QUERY_ALL_MODULES);
            /* needn't to resotre the replaced char because it's a copy of new value. */
            return false;
        }
        mod_name[mod_name_len] = ch; /* restore the replaced char */
        if (MOD_ALL == modid) {
            *all_found = true;
        }
        mods[*mods_num] = modid;
        (*mods_num)++;
        return true;
    } else {
        /* bad format: empty string or not valid module name */
        if (0 == mod_name_len) {
            GUC_check_errmsg(ERRMSG_EMPTY_MODULE_NAME);
            GUC_check_errdetail("Module name cannot be empty.");
        } else {
            mod_name[mod_name_len] = '\0';
            GUC_check_errmsg(ERRMSG_MODNAME_NOT_FOUND, mod_name);
            /* needn't to resotre the replaced char because it's a copy of new value. */
        }
        GUC_check_errhint(HINT_QUERY_ALL_MODULES);
        return false;
    }
}

/*
 * @Description: check whether the new GUC value is valid.
 *    1. format is a) on(m1,m2,...) or b) off(m3,m4,..)
 *    2. module name is valid
 *    if all the conditions hold, apply these changes to this
 *    session and return true; otherwise return false.
 * @IN newval: logging_module's new value
 * @IN assign: only check the new value if assign is false;
 *     otherwise check and assign.
 * @Return: if newval is well-formatted, return true; otherwise return false.
 * @See also:
 */
template <bool assign>
static bool logging_module_guc_check(char* newval)
{
    char* p = newval;
    bool all_found = false;
    bool turn_on = false;

    /* forbit that no module name appear between two delimiters.
     * forbit that no module name appear between delimiter and ')'
     */
    bool delim_found = false;

    /* treat all the error type as syntax error. */
    GUC_check_errcode(ERRCODE_SYNTAX_ERROR);

    if (!string_guc_keyword_matched(&p, &turn_on)) {
        return false;
    }

    if (!string_guc_list_start(&p)) {
        return false;
    }

    ModuleId* mods = (ModuleId*)palloc(sizeof(ModuleId) * MOD_MAX);
    int mods_num = 0;

    while (*p && ')' != *p) {
        /* remember the start of module name */
        char* mod_name = p;

        /* fine the end char of module name */
        while (*p && (MOD_DELIMITER != *p && ' ' != *p && ')' != *p)) {
            ++p;
        }

        if (!logging_module_parse_name(mod_name, p - mod_name, mods, &mods_num, &all_found) ||
            !string_guc_list_find_next_name(&p, &delim_found)) {
            pfree_ext(mods);
            return false;
        }
    }

    if (!string_guc_list_end(&p, delim_found)) {
        pfree_ext(mods);
        return false;
    }

    if (assign) {
        module_logging_batch_set(mods, mods_num, turn_on, all_found);
    }
    pfree_ext(mods);

    return true;
}

/*
 * @Description: check whether new value is well-formatted for GUC logging_module.
 * @IN extra: unused
 * @IN newval: new value
 * @IN source: unused
 * @Return: if newval is well-formatted, return true; otherwise return false.
 * @See also:
 */
bool logging_module_check(char** newval, void** extra, GucSource source)
{
    ((void)extra);  /* not used argument */
    ((void)source); /* not used argument */

    char* tmpNewVal = pstrdup(*newval);
    bool ret = logging_module_guc_check<false>(tmpNewVal);
    pfree(tmpNewVal);
    return ret;
}

/*
 * @Description: update logging_module according new value.
 * @IN extra: unused
 * @IN newval: new value
 * @See also:
 */
void logging_module_guc_assign(const char* newval, void* extra)
{
    ((void)extra); /* not used argument */

    char* tmpNewVal = pstrdup(newval);
    /* logging_module_check() will be called first, so here don't care returned value */
    (void)logging_module_guc_check<true>(tmpNewVal);
    pfree(tmpNewVal);
}

/* ------------------------------------------------------------ */
/* GUC hooks for inplace/grey upgrade                                                         */
/* ------------------------------------------------------------ */
static bool check_is_upgrade(bool* newval, void** extra, GucSource source)
{
    switch (source) {
        case PGC_S_DYNAMIC_DEFAULT:
        case PGC_S_ENV_VAR:
        case PGC_S_ARGV:
            GUC_check_errmsg("IsInplaceUpgrade GUC can only be set with SET command.");
            return false;

        case PGC_S_FILE:
            switch (currentGucContext) {
                case PGC_SIGHUP:
                case PGC_POSTMASTER:
                    ereport(WARNING,
                        (errmsg("IsInplaceUpgrade GUC can not be set in postgresql.conf. Set to default (false).")));
                    *newval = false;
                    return true;

                /* Should not come here */
                default:
                    GUC_check_errmsg("IsInplaceUpgrade GUC can not be set in postgresql.conf.");
                    return false;
            }
            return false; /* Should not come here */

        case PGC_S_DATABASE:
        case PGC_S_USER:
        case PGC_S_DATABASE_USER:
        case PGC_S_CLIENT:
        case PGC_S_OVERRIDE:
        case PGC_S_INTERACTIVE:
        case PGC_S_TEST:
            ereport(WARNING,
                (errmsg("IsInplaceUpgrade GUC can only be set with SET command.  Set to default (false)"
                        ".")));
            *newval = false;
            return true;

        case PGC_S_DEFAULT:
        case PGC_S_SESSION:
            if (*newval && u_sess->attr.attr_common.upgrade_mode == 0) {
                GUC_check_errmsg("Can not set IsInPlaceUpgrade to true while not performing upgrade.");
                return false;
            }
            return true;

        default:
            GUC_check_errmsg("Unknown GUC source for setting IsInplaceUpgrade.");
            return false;
    }
}

GucContext get_guc_context()
{
    return currentGucContext;
}

static void assign_is_inplace_upgrade(const bool newval, void* extra)
{
    if (newval && u_sess->attr.attr_common.XactReadOnly)
        u_sess->attr.attr_common.XactReadOnly = false;
}

/*
 * @Description	: parse analysis options name and check its validation.
 * @in/out options	: to remember all the found analysis option id
 * @in/out opt_num	: size of AnalysisOpt[]
 * @in anls_opt_name	: this analysis option name
 * @in anls_opt_name_len	: size of this analysis option name
 * @out all_found	: "ALL" analysis option name is found
 * @return		: true if parse options successfully
 */
static inline bool analysis_options_parse_name(
    char* anls_opt_name, int anls_opt_name_len, AnalysisOpt* options, int* opt_num, bool* all_found)
{
    if (anls_opt_name_len > 0 && anls_opt_name_len < ANLS_OPT_NAME_MAXLEN) {
        AnalysisOpt anls_option = ANLS_MAX;
        const char ch = anls_opt_name[anls_opt_name_len];

        /* judge whether it's a valid analysis option name */
        anls_opt_name[anls_opt_name_len] = '\0';
        anls_option = get_anls_opt_id(anls_opt_name);
        if (ANLS_MAX == anls_option) {
            /* bad format: not existing analysis option name */
            GUC_check_errmsg(ERRMSG_MODNAME_NOT_FOUND, anls_opt_name);
            GUC_check_errhint(HINT_QUERY_ALL_MODULES);
            /* needn't to resotre the replaced char because it's a copy of new value. */
            return false;
        }

        /* repeat set is not allow */
        for (int i = 0; i < *opt_num; i++) {
            if (options[i] == anls_option) {
                GUC_check_errmsg("repeat option: %s.", anls_opt_name);
                GUC_check_errdetail("Please don't set analysis option name repeatedly.");
                return false;
            }
        }

        anls_opt_name[anls_opt_name_len] = ch; /* restore the replaced char */
        if (ANLS_ALL == anls_option) {
            *all_found = true;
        }
        options[*opt_num] = anls_option;
        (*opt_num)++;
        return true;
    } else {
        /* bad format: empty string or not valid module name */
        if (0 == anls_opt_name_len) {
            GUC_check_errmsg(ERRMSG_EMPTY_MODULE_NAME);
            GUC_check_errdetail("analysis option name cannot be empty.");
        } else {
            /* needn't to resotre the replaced char because it's a copy of new value. */
            anls_opt_name[anls_opt_name_len] = '\0';
            GUC_check_errmsg(ERRMSG_MODNAME_NOT_FOUND, anls_opt_name);
        }
        GUC_check_errhint(HINT_QUERY_ALL_MODULES);
        return false;
    }
}

/*
 * @Description	: Check whether the new GUC value is valid. Only support the following support:
 *				  1. format is a) on(anls_opt1, anls_opt2, ...) or b) off(anls_opt3, anls_opt4, ...)
 *				  2. analysis option name is valid
 *				  If all the conditions hold, apply these changes to this session and return true;
 *				  otherwise return false.
 * @in newval		: analysis-options's new value
 * @return		: if newval is well-formatted, return true; otherwise return false.
 */
template <bool assign>
static bool analysis_options_guc_check(char* newval)
{
    char* p = newval;
    bool all_found = false;
    bool turn_on = false;

    /* forbit that no option name appear between two delimiters, or between delimiter and ')' */
    bool delim_found = false;

    /* treat all the error type as syntax error. */
    GUC_check_errcode(ERRCODE_SYNTAX_ERROR);

    if (!string_guc_keyword_matched(&p, &turn_on)) {
        return false;
    }

    if (!string_guc_list_start(&p)) {
        return false;
    }

    AnalysisOpt* options = (AnalysisOpt*)palloc(sizeof(AnalysisOpt) * ANLS_MAX);
    int opt_num = 0;

    while (*p && ')' != *p) {
        /* remember the start of option name */
        char* anls_opt_name = p;

        /* fine the end char of option name */
        while (*p && (OPTION_DELIMITER != *p && ' ' != *p && ')' != *p)) {
            ++p;
        }

        if (!analysis_options_parse_name(anls_opt_name, p - anls_opt_name, options, &opt_num, &all_found) ||
            !string_guc_list_find_next_name(&p, &delim_found)) {
            pfree_ext(options);
            return false;
        }
    }

    if (!string_guc_list_end(&p, delim_found)) {
        pfree_ext(options);
        return false;
    }

    if (assign) {
        anls_opt_batch_set(options, opt_num, turn_on, all_found);
    }
    pfree_ext(options);

    return true;
}

/*
 * @Description	: show the content of GUC "analysis_options"
 * @return		: analysis_options descripting text
 */
static const char* analysis_options_guc_show(void)
{
    /*
     * showing results is as following:
     * 	ALL, on(anls_opt1, anls_opt2, ...), off(anls_opt3, anls_opt4, ...)
     */
    StringInfoData outbuf;
    initStringInfo(&outbuf);

    const char* anls_opt_name = NULL;
    bool rm_last_delim = false;

    /* copy the special "ALL" */
    anls_opt_name = get_valid_anls_opt_name(ANLS_ALL);
    appendStringInfo(&outbuf, "%s%c", anls_opt_name, OPTION_DELIMITER);

    /* copy the string "on(" */
    appendStringInfo(&outbuf, "on(");

    /* copy each analysis option name whose analysis option is enable */
    rm_last_delim = false;
    for (int i = ANLS_ALL + 1; i < ANLS_MAX; i++) {
        if (anls_opt_is_on((AnalysisOpt)i)) {
            /* For condition and anls_opt_is_on() will assure its requirement.  */
            anls_opt_name = get_valid_anls_opt_name((AnalysisOpt)i);
            appendStringInfo(&outbuf, "%s%c", anls_opt_name, OPTION_DELIMITER);
            rm_last_delim = true;
        }
    }
    if (rm_last_delim) {
        --outbuf.len;
        outbuf.data[outbuf.len] = '\0';
    }

    /* copy the string "), off(" */
    appendStringInfo(&outbuf, "),off(");

    /* copy each option name whose analysis is disable */
    rm_last_delim = false;
    for (int i = ANLS_ALL + 1; i < ANLS_MAX; i++) {
        if (!anls_opt_is_on((AnalysisOpt)i)) {
            /* For condition will assure its requirement.  */
            anls_opt_name = get_valid_anls_opt_name((AnalysisOpt)i);
            appendStringInfo(&outbuf, "%s%c", anls_opt_name, OPTION_DELIMITER);
            rm_last_delim = true;
        }
    }
    if (rm_last_delim) {
        --outbuf.len;
        outbuf.data[outbuf.len] = '\0';
    }

    /* end with string ")" */
    appendStringInfoChar(&outbuf, ')');

    return outbuf.data;
}

bool transparent_encrypt_kms_url_region_check(char** newval, void** extra, GucSource source)
{
    char RFC3986_chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~!*'();:@&=+$,/?#[]";
    char* val = NULL;
    const int MAX_URL_LEN = 2048; /* The max length of URL */

    if (newval == NULL || *newval == NULL)
        return true;

    /* Check if the url contains illegal characters. */
    for (val = *newval; *val; val++) {
        if (strchr(RFC3986_chars, *val) == NULL)
            return false;
    }

    if ((val - *newval) > MAX_URL_LEN)
        return false;

    return true;
}

/*
 * @Description	: check whether new value is well-formatted for GUC analysis options.
 * @in extra		: unused
 * @in source		: unuse
 * @in newval		: new value
 * @return		: if newval is well-formatted, return true; otherwise return false.
 */
static bool analysis_options_check(char** newval, void** extra, GucSource source)
{
    ((void)extra);  /* not used argument */
    ((void)source); /* not used argument */

    char* tmpNewVal = pstrdup(*newval);
    bool ret = analysis_options_guc_check<false>(tmpNewVal);
    pfree(tmpNewVal);
    return ret;
}

/*
 * @Description	: update analysis_options according new value.
 * @in extra		: unused
 * @in newval		: new value
 */
static void analysis_options_guc_assign(const char* newval, void* extra)
{
    ((void)extra); /* not used argument */

    char* tmpNewVal = pstrdup(newval);
    /* analysis_options_check() will be called first,
     * so here don't care returned value */
    (void)analysis_options_guc_check<true>(tmpNewVal);
    pfree(tmpNewVal);
}

#define DEFAULT_PARTIAL_SEQSCAN false
#define DEFAULT_RECOVERY_UNDO_WORKERS 1
#define DEFAULT_CAND_LIST_USAGE_COUNT false
#define DEFAULT_USTATS_TRACKER_NAPTIME 20
#define DEFAULT_UMAX_PRUNE_SEARCH_LEN 10

static void InitUStoreAttr()
{
    u_sess->attr.attr_storage.enable_ustore_partial_seqscan = DEFAULT_PARTIAL_SEQSCAN;
    u_sess->attr.attr_storage.enable_candidate_buf_usage_count = DEFAULT_CAND_LIST_USAGE_COUNT;
    u_sess->attr.attr_storage.ustats_tracker_naptime = DEFAULT_USTATS_TRACKER_NAPTIME;
    u_sess->attr.attr_storage.umax_search_length_for_prune = DEFAULT_UMAX_PRUNE_SEARCH_LEN;
}

bool check_percentile(char** newval, void** extra, GucSource source)
{
    char* rawname = NULL;
    List* namelist = NIL;

    /* Need a modifiable copy of string */
    rawname = pstrdup(*newval);

    /* Parse string into list of identifiers */
    if (!SplitIdentifierInteger(rawname, ',', &namelist)) {
        /* syntax error in name list */
        GUC_check_errdetail("List syntax is invalid.");
        pfree_ext(rawname);
        list_free_ext(namelist);
        return false;
    }

    /*
     * We used to try to check that the named schemas exist, but there are
     * many valid use-cases for having search_path settings that include
     * schemas that don't exist; and often, we are not inside a transaction
     * here and so can't consult the system catalogs anyway.  So now, the only
     * requirement is syntactic validity of the identifier list.
     */

    pfree_ext(rawname);
    list_free_ext(namelist);

    return true;
}

static void assign_instr_unique_sql_count(int newval, void* extra)
{
/* oid of reset_instr_unique_sql */
#define RESET_UNIQUE_SQL_FUNC 5716

    /* only let WLMProcessThread do the cleanup */
    if (AmWLMWorkerProcess() && (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) &&
        u_sess->attr.attr_common.instr_unique_sql_count > newval) {
        bool result = DatumGetBool(OidFunctionCall3(RESET_UNIQUE_SQL_FUNC,
            CStringGetTextDatum("GLOBAL"),
            CStringGetTextDatum("BY_GUC"),
            Int32GetDatum(newval)));

        ereport(LOG,
            (errmodule(MOD_INSTR),
                errmsg("[UniqueSQL] cleanup unique sql due to instr_unique_sql_count shrunk: %s",
                    result ? "success" : "fail")));
    }
}

bool check_asp_flush_mode(char** newval, void** extra, GucSource source)
{
    if (strcmp(*newval, "table") == 0 || strcmp(*newval, "file") == 0 || strcmp(*newval, "all") == 0) {
        return true;
    }
    return false;
}
/*
 * callback function for numa_distribute_mode checking.
 */
bool check_numa_distribute_mode(char** newval, void** extra, GucSource source)
{
#ifdef __USE_NUMA
    if (strcmp(*newval, "all") == 0) {
        return true;
    }
#endif
    if (strcmp(*newval, "none") == 0) {
        return true;
    }
    return false;
}

#include "guc-file.inc"

#ifdef ENABLE_MULTIPLE_NODES
static void assign_gtm_rw_timeout(int newval, void* extra)
{
    SetGTMrwTimeout(newval);
}

static bool check_gtmhostconninfo(char** newval, void** extra, GucSource source)
{
    char *str = NULL;
    char *tmp = NULL;
    bool bRet = true;

    if (NULL == newval) {
        return false;
    }

    str = pstrdup(*newval);
    if (NULL == str) {
        return false;
    }

    tmp = str;

    while (*tmp != '\0') {
        if (isalpha(*tmp) || isdigit(*tmp) || *tmp == ' ' || *tmp == ',' || *tmp == '.' || *tmp == '*') {
            tmp++;
            continue;
        }

        bRet = false;
        break;
    }

    pfree(str);

    return bRet;
}

GTM_HOST_IP* ParseGtmHostConnInfo(const char* ConnInfoList, int* InfoLength)
{
    char* ReplStr = NULL;
    char* token = NULL;
    char* tmp_token = NULL;
    uint32 len = 0;
    uint32 tmp;
    GTM_HOST_IP* pHostIP = NULL;
    int rc;

    if (ConnInfoList == NULL)
        return NULL;

    uint32 infolen = strlen(ConnInfoList);

    ReplStr = pstrdup(ConnInfoList);
    if (ReplStr == NULL)
        return NULL;

    token = strtok_r(ReplStr, ",", &tmp_token);
    while (token != NULL) {
        len++;
        token = strtok_r(NULL, ",", &tmp_token);
    }
    pfree(ReplStr);

    MemoryContext currCxt = SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB);
    pHostIP = (len > 0) ? static_cast<GTM_HOST_IP *>(MemoryContextAllocZero(currCxt, sizeof(GTM_HOST_IP) * len))
                        : static_cast<GTM_HOST_IP *>(MemoryContextAllocZero(currCxt, sizeof(GTM_HOST_IP)));

    if (pHostIP == NULL)
        return NULL;

    ReplStr = static_cast<char*>(MemoryContextAllocZero(currCxt, infolen + 1));
    if (ReplStr == NULL) {
        pfree(pHostIP);
        return NULL;
    }

    for (len = 0, tmp = 0; len < infolen; len++) {
        if (ConnInfoList[len] != ' ')
            ReplStr[tmp++] = ConnInfoList[len];
    }
    ReplStr[tmp] = '\0';

    len = 0;
    token = strtok_r(ReplStr, ",", &tmp_token);
    while (token != NULL) {
        rc = strncpy_s(pHostIP[len].acHostIP, sizeof(pHostIP[len].acHostIP), token, sizeof(pHostIP[len].acHostIP) - 1);
        securec_check(rc, "\0", "\0");
        pHostIP[len].acHostIP[127] = '\0';
        len++;
        token = strtok_r(NULL, ",", &tmp_token);
    }

    *InfoLength = len;
    pfree(ReplStr);
    return pHostIP;
}

static void assign_gtmhostconninfo0(const char* newval, void* extra)
{
    pfree_ext(u_sess->attr.attr_common.GtmHostArray[0]);
    u_sess->attr.attr_common.GtmHostArray[0] =
        ParseGtmHostConnInfo(newval, &u_sess->attr.attr_common.GtmHostIPNumArray[0]);
}

static void assign_gtmhostconninfo1(const char* newval, void* extra)
{
    pfree_ext(u_sess->attr.attr_common.GtmHostArray[1]);
    u_sess->attr.attr_common.GtmHostArray[1] =
        ParseGtmHostConnInfo(newval, &u_sess->attr.attr_common.GtmHostIPNumArray[1]);
}

static void assign_gtmhostconninfo2(const char* newval, void* extra)
{
    pfree_ext(u_sess->attr.attr_common.GtmHostArray[2]);
    u_sess->attr.attr_common.GtmHostArray[2] =
        ParseGtmHostConnInfo(newval, &u_sess->attr.attr_common.GtmHostIPNumArray[2]);
}

static void assign_gtmhostconninfo3(const char* newval, void* extra)
{
    pfree_ext(u_sess->attr.attr_common.GtmHostArray[3]);
    u_sess->attr.attr_common.GtmHostArray[3] =
        ParseGtmHostConnInfo(newval, &u_sess->attr.attr_common.GtmHostIPNumArray[3]);
}

static void assign_gtmhostconninfo4(const char* newval, void* extra)
{
    pfree_ext(u_sess->attr.attr_common.GtmHostArray[4]);
    u_sess->attr.attr_common.GtmHostArray[4] =
        ParseGtmHostConnInfo(newval, &u_sess->attr.attr_common.GtmHostIPNumArray[4]);
}

static void assign_gtmhostconninfo5(const char* newval, void* extra)
{
    pfree_ext(u_sess->attr.attr_common.GtmHostArray[5]);
    u_sess->attr.attr_common.GtmHostArray[5] =
        ParseGtmHostConnInfo(newval, &u_sess->attr.attr_common.GtmHostIPNumArray[5]);
}

static void assign_gtmhostconninfo6(const char* newval, void* extra)
{
    pfree_ext(u_sess->attr.attr_common.GtmHostArray[6]);
    u_sess->attr.attr_common.GtmHostArray[6] =
        ParseGtmHostConnInfo(newval, &u_sess->attr.attr_common.GtmHostIPNumArray[6]);
}

static void assign_gtmhostconninfo7(const char* newval, void* extra)
{
    pfree_ext(u_sess->attr.attr_common.GtmHostArray[7]);
    u_sess->attr.attr_common.GtmHostArray[7] =
        ParseGtmHostConnInfo(newval, &u_sess->attr.attr_common.GtmHostIPNumArray[7]);
}

/**
 * check value of compaciton strategy one by one
 * min_part in range [2,5]
 * max_part in range [4,10]
 * level_middle in range [4,12]
 * level_max in range [7,20]
 * is_fuzzy in range [0,1]
 */
static bool check_compaciton_strategy(char** newval, void** extra, GucSource source)
{
    char* check_char = TrimStr(*newval);
    if (check_char == NULL) {
        return true;
    }
    if (strlen(check_char) > TsProducer::MAX_STRATEGY_LEN) {
        GUC_check_errdetail("input string too long, lenth is %lu, expected less than 15", strlen(*newval));
        return false;
    }
    /* do not accept any thing except number and comma */
    for (uint i = 0; i < strlen(check_char); i++) {
        if ((check_char[i] < '0' || check_char[i] > '9') && check_char[i] != ',') {
            GUC_check_errdetail("unexpacted letter in input val %c.", check_char[i]);
            return false;
        }
    }
    char* ptoken = NULL;
    char* psave = NULL;
    const char* pdelimiter = ",";
    ptoken = TrimStr(strtok_r(check_char, pdelimiter, &psave));
    /* filter min_parts */
    if (!ts_compaction_guc_filter(ptoken, TsProducer::MIN_PART_MIN, TsProducer::MIN_PART_MAX)) {
        GUC_check_errdetail("min part over range.");
        return false;
    }

    /* filter max_parts */
    ptoken = TrimStr(strtok_r(NULL, pdelimiter, &psave));    
    if (!ts_compaction_guc_filter(ptoken, TsProducer::MAX_PART_MIN, TsProducer::MAX_PART_MAX)) {
        GUC_check_errdetail("max part over range.");
        return false;
    }

    /* filter level middle */
    ptoken = TrimStr(strtok_r(NULL, pdelimiter, &psave));    
    if (!ts_compaction_guc_filter(ptoken, TsProducer::LEVEL_MID_MIN, TsProducer::LEVEL_MID_MAX)) {
        GUC_check_errdetail("level middle over range.");
        return false;
    }

    /* filter level max */
    ptoken = TrimStr(strtok_r(NULL, pdelimiter, &psave));    
    if (!ts_compaction_guc_filter(ptoken, TsProducer::LEVEL_MAX_MIN, TsProducer::LEVEL_MAX_MAX)) {
        GUC_check_errdetail("level max over range.");
        return false;
    }
    
    /* we only accept is_fuzzy to be 0 or 1 */
    ptoken = TrimStr(psave);
    Assert(ptoken != NULL);
    if (strlen(ptoken) != 1 || strcmp(ptoken, ",") == 0 || !ts_compaction_guc_filter(ptoken, 0, 1)) {
        GUC_check_errdetail("fuzzy value invalid!");
        return false;
    }
    pfree(ptoken);
    pfree(check_char);
    return true;
}

/**
 * filter guc based on min/max number
 */
static bool ts_compaction_guc_filter(const char* ptoken, const int min_num, const int max_num)
{
    int32 temp_number = 0;
    if ((ptoken) != NULL && (ptoken)[0] != '\0') {
        temp_number = pg_strtoint32(ptoken);
        if (temp_number >= min_num && temp_number <= max_num) {
            return true;
        } else if (temp_number == 0) {
            return true;
        } else {
            return false;
        }
    } else {
        /* ptoken id null return true */
        return true;
    }
    return false;
}
#endif
