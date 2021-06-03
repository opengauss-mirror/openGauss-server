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
#include "gs_bbox.h"
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
#include "commands/user.h"
#include "flock.h"
#include "gaussdb_version.h"
#include "hll.h"
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
#include "storage/fd.h"
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
#include "utils/guc_mn.h"
#include "workload/cpwlm.h"
#include "workload/workload.h"

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
#define CONFIG_BAK_FILENAME "postgresql.conf.bak"
#define NO_LIMIT_SIZE -1

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

/* max reuse time interval and times */
#define MAX_PASSWORD_REUSE_TIME 3650
#define MAX_PASSWORD_REUSE_MAX 1000
#define MAX_ACCOUNT_LOCK_TIME 365
#define MAX_FAILED_LOGIN_ATTAMPTS 1000
#define MAX_PASSWORD_EFFECTIVE_TIME 999
#define MAX_PASSWORD_NOTICE_TIME 999
/* max num of assign character in password */
#define MAX_PASSWORD_ASSIGNED_CHARACTER 999
/* max length of password */
#define MAX_PASSWORD_LENGTH 999

extern volatile int synchronous_commit;
extern volatile bool most_available_sync;
extern void SetThreadLocalGUC(knl_session_context* session);
THR_LOCAL int comm_ackchk_time;

static THR_LOCAL GucContext currentGucContext;

const char* sync_guc_variable_namelist[] = {"work_mem",
    "query_mem",
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
    "enforce_two_phase_commit",
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
    "gds_debug_mod",
    "ngram_punctuation_ignore",
    "ngram_grapsymbol_ignore",
    "ngram_gram_size",
    "enable_parallel_ddl",
    "enable_orc_cache",
#ifdef ENABLE_MULTIPLE_NODES
    "enable_cluster_resize",
#endif
    "enable_compress_spill",
    "resource_track_level",
    "fault_mon_timeout",
    "trace_sort",
    "ignore_checksum_failure",
    "wal_log_hints",
    "max_cn_temp_file_size",
    "cn_send_buffer_size",
    "enable_compress_hll",
    "hll_default_log2m",
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
    "track_stmt_session_slot",
    "track_stmt_stat_level",
    "track_stmt_details_size",
    "basebackup_timeout"};

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

void free_memory_context_list(memory_context_list* head_node);
memory_context_list* split_string_into_list(const char* source);
static bool check_uncontrolled_memory_context(char** newval, void** extra, GucSource source);
static void assign_uncontrolled_memory_context(const char* newval, void* extra);
static const char* show_uncontrolled_memory_context(void);
// end of GUC variables for uncontrolled_memory_context

static void assign_syslog_facility(int newval, void* extra);
static void assign_syslog_ident(const char* newval, void* extra);
static void assign_session_replication_role(int newval, void* extra);
static bool check_client_min_messages(int* newval, void** extra, GucSource source);
static bool check_temp_buffers(int* newval, void** extra, GucSource source);
static bool check_fencedUDFMemoryLimit(int* newval, void** extra, GucSource source);
static bool check_udf_memory_limit(int* newval, void** extra, GucSource source);
static bool check_phony_autocommit(bool* newval, void** extra, GucSource source);
static bool check_debug_assertions(bool* newval, void** extra, GucSource source);
#ifdef USE_BONJOUR
static bool check_bonjour(bool* newval, void** extra, GucSource source);
#endif

static bool check_gpc_syscache_threshold(bool* newval, void** extra, GucSource source);
static bool check_syscache_threshold_gpc(int* newval, void** extra, GucSource source);

static bool check_ssl(bool* newval, void** extra, GucSource source);
static bool check_stage_log_stats(bool* newval, void** extra, GucSource source);
static bool check_log_stats(bool* newval, void** extra, GucSource source);
#ifdef PGXC
static bool check_pgxc_maintenance_mode(bool* newval, void** extra, GucSource source);
static bool check_pooler_maximum_idle_time(int* newval, void** extra, GucSource source);
static bool check_sctp_support(bool* newval, void** extra, GucSource source);
static void assign_comm_debug_mode(bool newval, void* extra);
static void assign_comm_stat_mode(bool newval, void* extra);
static void assign_comm_timer_mode(bool newval, void* extra);
static void assign_comm_no_delay(bool newval, void* extra);
static void assign_comm_ackchk_time(int newval, void* extra);
#endif

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

static bool check_adio_debug_guc(bool* newval, void** extra, GucSource source);
static bool check_adio_function_guc(bool* newval, void** extra, GucSource source);
static const char* show_enable_memory_limit(void);
static bool check_use_workload_manager(bool* newval, void** extra, GucSource source);
static bool check_canonical_path(char** newval, void** extra, GucSource source);
static bool check_directory(char** newval, void** extra, GucSource source);
static bool check_log_filename(char** newval, void** extra, GucSource source);
static bool check_perf_log(char** newval, void** extra, GucSource source);
static bool check_timezone_abbreviations(char** newval, void** extra, GucSource source);
static void assign_timezone_abbreviations(const char* newval, void* extra);
static void pg_timezone_abbrev_initialize(void);
static const char* show_archive_command(void);
static bool check_maxconnections(int* newval, void** extra, GucSource source);
static bool CheckMaxInnerToolConnections(int* newval, void** extra, GucSource source);
static bool parse_query_dop(int* newval, void** extra, GucSource source);
static bool check_statistics_memory_limit(int* newval, void** extra, GucSource source);
static void assign_statistics_memory(int newval, void* extra);
static void assign_history_memory(int newval, void* extra);
static bool check_history_memory_limit(int* newval, void** extra, GucSource source);
static bool check_autovacuum_max_workers(int* newval, void** extra, GucSource source);
static bool check_job_max_workers(int* newval, void** extra, GucSource source);
static bool check_effective_io_concurrency(int* newval, void** extra, GucSource source);
static void assign_effective_io_concurrency(int newval, void* extra);
static void assign_pgstat_temp_directory(const char* newval, void* extra);
static bool check_ssl_ciphers(char** newval, void** extra, GucSource source);
static bool check_application_name(char** newval, void** extra, GucSource source);
static void assign_application_name(const char* newval, void* extra);
static void assign_connection_info(const char* newval, void* extra);
static bool check_application_type(int* newval, void** extra, GucSource source);
static bool check_cgroup_name(char** newval, void** extra, GucSource source);
static void assign_cgroup_name(const char* newval, void* extra);
static const char* show_cgroup_name(void);
static bool check_query_band_name(char** newval, void** extra, GucSource source);
static bool check_session_respool(char** newval, void** extra, GucSource source);
static void assign_session_respool(const char* newval, void* extra);
static const char* show_session_respool(void);
static bool check_memory_detail_tracking(char** newval, void** extra, GucSource source);
static void assign_memory_detail_tracking(const char* newval, void* extra);
static const char* show_memory_detail_tracking(void);
static void assign_collect_timer(int newval, void* extra);
static bool check_statement_mem(int* newval, void** extra, GucSource source);
static bool check_statement_max_mem(int* newval, void** extra, GucSource source);
static const char* show_unix_socket_permissions(void);
static const char* show_log_file_mode(void);
static char* config_enum_get_options(
    struct config_enum* record, const char* prefix, const char* suffix, const char* separator);
static bool validate_conf_option(struct config_generic* record, const char *name, const char *value,
    GucSource source, int elevel, bool freemem, void *newval, void **newextra);

/* Database Security: Support password complexity */
static bool check_int_parameter(int* newval, void** extra, GucSource source);
static bool check_double_parameter(double* newval, void** extra, GucSource source);
static void check_setrole_permission(const char* rolename, char* passwd, bool IsSetRole);
static bool verify_setrole_passwd(const char* rolename, char* passwd, bool IsSetRole);
static bool CheckReplChannel(const char* ChannelInfo);
static int GetLengthAndCheckReplConn(const char* ConnInfoList);
static ReplConnInfo* ParseReplConnInfo(const char* ConnInfoList, int* InfoLength);
static bool check_replconninfo(char** newval, void** extra, GucSource source);
static void assign_replconninfo1(const char* newval, void* extra);
static void assign_replconninfo2(const char* newval, void* extra);
static void assign_replconninfo3(const char* newval, void* extra);
static void assign_replconninfo4(const char* newval, void* extra);
static void assign_replconninfo5(const char* newval, void* extra);
static void assign_replconninfo6(const char* newval, void* extra);
static void assign_replconninfo7(const char* newval, void* extra);
#ifndef ENABLE_MULTIPLE_NODES
static void assign_replconninfo8(const char* newval, void* extra);
#endif
static bool check_inlist2joininfo(char** newval, void** extra, GucSource source);
static void assign_inlist2joininfo(const char* newval, void* extra);
static bool check_replication_type(int* newval, void** extra, GucSource source);
static bool isOptLineCommented(char* optLine);
static bool isMatchOptionName(char* optLine, const char* paraName,
    int paraLength, int* paraOffset, int* valueLength, int* valueOffset, bool ignore_case);

static const char* logging_module_guc_show(void);
static bool logging_module_check(char** newval, void** extra, GucSource source);
static void logging_module_guc_assign(const char* newval, void* extra);
static void plog_merge_age_assign(int newval, void* extra);

/* Inplace Upgrade GUC hooks */
static bool check_is_upgrade(bool* newval, void** extra, GucSource source);
static void assign_is_inplace_upgrade(const bool newval, void* extra);
static bool check_inplace_upgrade_next_oids(char** newval, void** extra, GucSource source);
static bool transparent_encrypt_kms_url_region_check(char** newval, void** extra, GucSource source);
/* SQL DFx Options : Support different sql dfx option */
static const char* analysis_options_guc_show(void);
static bool analysis_options_check(char** newval, void** extra, GucSource source);
static void analysis_options_guc_assign(const char* newval, void* extra);

static bool check_behavior_compat_options(char** newval, void** extra, GucSource source);
static void assign_behavior_compat_options(const char* newval, void* extra);
static void assign_use_workload_manager(const bool newval, void* extra);
static void assign_convert_string_to_digit(bool newval, void* extra);
static bool check_enable_data_replicate(bool* newval, void** extra, GucSource source);
static void AssignQueryDop(int newval, void* extra);
static void assign_instr_unique_sql_count(int newval, void* extra);
static bool check_auto_explain_level(int* newval, void** extra, GucSource source);
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

static const struct config_enum_entry application_type_options[] =
{
    {"not_perfect_sharding_type", NOT_PERFECT_SHARDING_TYPE, false},
    {"perfect_sharding_type", PERFECT_SHARDING_TYPE, false},
    {NULL, 0, false}
};

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

static const struct config_enum_entry io_priority_level_options[] = {{"None", IOPRIORITY_NONE, false},
    {"Low", IOPRIORITY_LOW, false},
    {"Medium", IOPRIORITY_MEDIUM, false},
    {"High", IOPRIORITY_HIGH, false},
    {NULL, 0, false}};

static const struct config_enum_entry track_function_options[] = {
    {"none", TRACK_FUNC_OFF, false}, {"pl", TRACK_FUNC_PL, false}, {"all", TRACK_FUNC_ALL, false}, {NULL, 0, false}};

static const struct config_enum_entry xmlbinary_options[] = {
    {"base64", XMLBINARY_BASE64, false}, {"hex", XMLBINARY_HEX, false}, {NULL, 0, false}};

static const struct config_enum_entry xmloption_options[] = {
    {"content", XMLOPTION_CONTENT, false}, {"document", XMLOPTION_DOCUMENT, false}, {NULL, 0, false}};

/*change the char * sql_compatibility to enum*/
static const struct config_enum_entry adapt_database[] = {{g_dbCompatArray[DB_CMPT_A].name, A_FORMAT, false},
    {g_dbCompatArray[DB_CMPT_C].name, C_FORMAT, false},
    {g_dbCompatArray[DB_CMPT_B].name, B_FORMAT, false},
    {g_dbCompatArray[DB_CMPT_PG].name, PG_FORMAT, false},
    {NULL, 0, false}};

/*change the char * enable_performance_data to enum*/
static const struct config_enum_entry explain_option[] = {{"normal", EXPLAIN_NORMAL, false},
    {"pretty", EXPLAIN_PRETTY, false},
    {"summary", EXPLAIN_SUMMARY, false},
    {"run", EXPLAIN_RUN, false},
    {NULL, 0, false}};

/*change the char * skew options to enum*/
static const struct config_enum_entry skew_strategy_option[] = {
    {"off", SKEW_OPT_OFF, false}, {"normal", SKEW_OPT_NORMAL, false}, {"lazy", SKEW_OPT_LAZY, false}, {NULL, 0, false}};

/*change the char * codegen_strategy to enum */
static const struct config_enum_entry codegen_strategy_option[] = {
    {"partial", CODEGEN_PARTIAL, false}, {"pure", CODEGEN_PURE, false}, {NULL, 0, false}};
/*change the char * memory_tracking_mode to enum*/
static const struct config_enum_entry memory_tracking_option[] = {{"none", MEMORY_TRACKING_NONE, false},
    {"peak", MEMORY_TRACKING_PEAKMEMORY, false},
    {"normal", MEMORY_TRACKING_NORMAL, false},
    {"executor", MEMORY_TRACKING_EXECUTOR, false},
    {"fullexec", MEMORY_TRACKING_FULLEXEC, false},
    {NULL, 0, false}};

static const struct config_enum_entry resource_track_option[] = {{"none", RESOURCE_TRACK_NONE, false},
    {"query", RESOURCE_TRACK_QUERY, false},
    {"operator", RESOURCE_TRACK_OPERATOR, false},
    {NULL, 0, false}};

/*
 * Although only "on", "off", and "safe_encoding" are documented, we
 * accept all the likely variants of "on" and "off".
 */
static const struct config_enum_entry backslash_quote_options[] = {
    {"safe_encoding", BACKSLASH_QUOTE_SAFE_ENCODING, false},
    {"on", BACKSLASH_QUOTE_ON, false},
    {"off", BACKSLASH_QUOTE_OFF, false},
    {"true", BACKSLASH_QUOTE_ON, true},
    {"false", BACKSLASH_QUOTE_OFF, true},
    {"yes", BACKSLASH_QUOTE_ON, true},
    {"no", BACKSLASH_QUOTE_OFF, true},
    {"1", BACKSLASH_QUOTE_ON, true},
    {"0", BACKSLASH_QUOTE_OFF, true},
    {NULL, 0, false}};

/*
 * Although only "on", "off", and "partition" are documented, we
 * accept all the likely variants of "on" and "off".
 */
static const struct config_enum_entry constraint_exclusion_options[] = {
    {"partition", CONSTRAINT_EXCLUSION_PARTITION, false},
    {"on", CONSTRAINT_EXCLUSION_ON, false},
    {"off", CONSTRAINT_EXCLUSION_OFF, false},
    {"true", CONSTRAINT_EXCLUSION_ON, true},
    {"false", CONSTRAINT_EXCLUSION_OFF, true},
    {"yes", CONSTRAINT_EXCLUSION_ON, true},
    {"no", CONSTRAINT_EXCLUSION_OFF, true},
    {"1", CONSTRAINT_EXCLUSION_ON, true},
    {"0", CONSTRAINT_EXCLUSION_OFF, true},
    {NULL, 0, false}};

#ifdef PGXC
/*
 * Define remote connection types for PGXC
 */
static const struct config_enum_entry pgxc_conn_types[] = {{"application", REMOTE_CONN_APP, false},
    {"coordinator", REMOTE_CONN_COORD, false},
    {"datanode", REMOTE_CONN_DATANODE, false},
    {"gtm", REMOTE_CONN_GTM, false},
    {"gtmproxy", REMOTE_CONN_GTM_PROXY, false},
    {"internaltool", REMOTE_CONN_INTERNAL_TOOL, false},
    {"gtmtool", REMOTE_CONN_GTM_TOOL, false},
    {NULL, 0, false}};

/* autovac mode */
static const struct config_enum_entry autovacuum_mode_options[] = {{"analyze", AUTOVACUUM_DO_ANALYZE, false},
    {"vacuum", AUTOVACUUM_DO_VACUUM, false},
    {"mix", AUTOVACUUM_DO_ANALYZE_VACUUM, false},
    {"none", AUTOVACUUM_DO_NONE, false},
    {NULL, 0, false}};

#endif

/*
 * Although only "on", "off", "remote_write", and "local" are documented, we
 * accept all the likely variants of "on" and "off".
 */
static const struct config_enum_entry synchronous_commit_options[] = {{"local", SYNCHRONOUS_COMMIT_LOCAL_FLUSH, false},
    {"remote_receive", SYNCHRONOUS_COMMIT_REMOTE_RECEIVE, false},
    {"remote_write", SYNCHRONOUS_COMMIT_REMOTE_WRITE, false},
    {"remote_apply", SYNCHRONOUS_COMMIT_REMOTE_APPLY, false},
    {"on", SYNCHRONOUS_COMMIT_ON, false},
    {"off", SYNCHRONOUS_COMMIT_OFF, false},
    {"true", SYNCHRONOUS_COMMIT_ON, true},
    {"false", SYNCHRONOUS_COMMIT_OFF, true},
    {"yes", SYNCHRONOUS_COMMIT_ON, true},
    {"no", SYNCHRONOUS_COMMIT_OFF, true},
    {"1", SYNCHRONOUS_COMMIT_ON, true},
    {"0", SYNCHRONOUS_COMMIT_OFF, true},
    {"2", SYNCHRONOUS_COMMIT_REMOTE_APPLY, true},
    {NULL, 0, false}};

static const struct config_enum_entry plan_cache_mode_options[] = {
	{"auto", PLAN_CACHE_MODE_AUTO, false},
	{"force_generic_plan", PLAN_CACHE_MODE_FORCE_GENERIC_PLAN, false},
	{"force_custom_plan", PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN, false},
	{NULL, 0, false}
};

/*
 * define insert mode for dfs_insert
 */
static const struct config_enum_entry cstore_insert_mode_options[] = {
    {"auto", TO_AUTO, true}, {"main", TO_MAIN, true}, {"delta", TO_DELTA, true}, {NULL, 0, false}};

static const struct config_enum_entry rewrite_options[] = {
    {"none", NO_REWRITE, false},
    {"lazyagg", LAZY_AGG, false},
    {"magicset", MAGIC_SET, false},
    {"partialpush", PARTIAL_PUSH, false},
    {"uniquecheck", SUBLINK_PULLUP_WITH_UNIQUE_CHECK, false},
    {"disablerep", SUBLINK_PULLUP_DISABLE_REPLICATED, false},
    {"intargetlist", SUBLINK_PULLUP_IN_TARGETLIST, false},
    {"predpush", PRED_PUSH, false},
    {"predpushnormal", PRED_PUSH_NORMAL, false},
    {"predpushforce", PRED_PUSH_FORCE, false},
    {NULL, 0, false}};

static const struct config_enum_entry remote_read_options[] = {{"off", REMOTE_READ_OFF, false},
    {"non_authentication", REMOTE_READ_NON_AUTH, false},
    {"authentication", REMOTE_READ_AUTH, false},
    {NULL, 0, false}};

static const struct config_enum_entry resource_track_log_options[] = {
    {"summary", SUMMARY, false}, {"detail", DETAIL, false}, {NULL, 0, false}};

static const struct config_enum_entry opfusion_debug_level_options[] = {
    {"off", BYPASS_OFF, false}, {"log", BYPASS_LOG, false}, {NULL, 0, false}};

static const struct config_enum_entry unique_sql_track_option[] = {
    {"top", UNIQUE_SQL_TRACK_TOP, false}, {"all", UNIQUE_SQL_TRACK_ALL, true}, {NULL, 0, false}};

static const struct config_enum_entry auto_explain_level_options[] =
{
    {"log", LOG, false},
    {"notice", NOTICE, false},
    {NULL, 0, false}

};

static const struct config_enum_entry sql_beta_options[] = {
    {"none", NO_BETA_FEATURE, false},
    {"sel_semi_poisson", SEL_SEMI_POISSON, false},
    {"sel_expr_instr", SEL_EXPR_INSTR, false},
    {"param_path_gen", PARAM_PATH_GEN, false},
    {"rand_cost_opt", RAND_COST_OPT, false},
    {"page_est_opt", PAGE_EST_OPT, false},
    {"param_path_opt", PARAM_PATH_OPT, false},
    {NULL, 0, false}
};

/*
 * Options for enum values stored in other modules
 */
extern struct config_enum_entry wal_level_options[];
extern struct config_enum_entry sync_method_options[];

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
    gettext_noop("Version and Platform Compatibility / Previous PostgreSQL Versions"),
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
 * 5. Add it to src/backend/utils/misc/postgresql.conf.sample, if
 *	  appropriate.
 *
 * 6. Don't forget to document the option (at least in config.sgml).
 *
 * 7. If it's a new GUC_LIST option you must edit pg_dumpall.c to ensure
 *	  it is not single quoted at dump time.
 */

/******** option records follow ********/

static void InitConfigureNamesBool()
{
    struct config_bool localConfigureNamesBool[] = {{{"raise_errors_if_no_files",
                                                         PGC_SUSET,
                                                         LOGGING_WHAT,
                                                         gettext_noop("raise errors if no files to be imported."),
                                                         NULL},
                                                        &u_sess->attr.attr_storage.raise_errors_if_no_files,
                                                        false,
                                                        NULL,
                                                        NULL,
                                                        NULL},
        {{"enable_fast_numeric", PGC_SUSET, QUERY_TUNING_METHOD, gettext_noop("Enable numeric optimize."), NULL},
            &u_sess->attr.attr_sql.enable_fast_numeric,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_wdr_snapshot", PGC_SIGHUP, INSTRUMENTS_OPTIONS, gettext_noop("Enable wdr snapshot"), NULL},
            &u_sess->attr.attr_common.enable_wdr_snapshot,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_asp", PGC_SIGHUP, INSTRUMENTS_OPTIONS, gettext_noop("Enable active session profile"), NULL},
            &u_sess->attr.attr_common.enable_asp,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_stmt_track",
            PGC_SIGHUP,
            INSTRUMENTS_OPTIONS,
            gettext_noop("Enable full/slow sql feature"), NULL},
            &u_sess->attr.attr_common.enable_stmt_track,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_global_stats",
             PGC_SUSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enable global stats for analyze."),
             NULL},
            &u_sess->attr.attr_sql.enable_global_stats,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_hypo_index",
             PGC_USERSET,                                                    
             QUERY_TUNING_METHOD,                                            
             gettext_noop("Enable hypothetical index for explain."),         
             NULL},                                                                
            &u_sess->attr.attr_sql.enable_hypo_index,                           
            false,                                                              
            NULL,                                                               
            NULL,                                                               
            NULL},
        {{"enable_hdfs_predicate_pushdown",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enable hdfs predicate pushdown."),
             NULL},
            &u_sess->attr.attr_sql.enable_hdfs_predicate_pushdown,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_absolute_tablespace",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop(" Enable tablespace using absolute location."),
             NULL},
            &u_sess->attr.attr_sql.enable_absolute_tablespace,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_hadoop_env", PGC_USERSET, QUERY_TUNING_METHOD, gettext_noop(" Enable hadoop enviroment."), NULL},
            &u_sess->attr.attr_sql.enable_hadoop_env,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_valuepartition_pruning",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop(
                 "Enable optimization for partitioned DFS table to be staticly/dynamically-pruned when possible."),
             NULL},
            &u_sess->attr.attr_sql.enable_valuepartition_pruning,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_constraint_optimization",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enable optimize query by using informational constraint."),
             NULL},
            &u_sess->attr.attr_sql.enable_constraint_optimization,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_bloom_filter", PGC_USERSET, QUERY_TUNING_METHOD, gettext_noop("Enable bloom filter check"), NULL},
            &u_sess->attr.attr_sql.enable_bloom_filter,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_codegen", PGC_USERSET, QUERY_TUNING_METHOD, gettext_noop("Enable llvm for executor."), NULL},
            &u_sess->attr.attr_sql.enable_codegen,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_delta_store", PGC_POSTMASTER, QUERY_TUNING, gettext_noop("Enable delta for column store."), NULL},
            &g_instance.attr.attr_storage.enable_delta_store,
            false,
            NULL,
            NULL,
            NULL},
        {{
             "enable_incremental_catchup",
             PGC_SIGHUP,
             REPLICATION_STANDBY,
             gettext_noop("Enable incremental searching bcm files when catchup."),
         },
            &u_sess->attr.attr_storage.enable_incremental_catchup,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_codegen_print",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enable dump() for llvm function."),
             NULL},
            &u_sess->attr.attr_sql.enable_codegen_print,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_sonic_optspill",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enable Sonic optimized spill."),
             NULL},
            &u_sess->attr.attr_sql.enable_sonic_optspill,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_sonic_hashjoin", PGC_USERSET, QUERY_TUNING_METHOD, gettext_noop("Enable Sonic hashjoin."), NULL},
            &u_sess->attr.attr_sql.enable_sonic_hashjoin,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_sonic_hashagg", PGC_USERSET, QUERY_TUNING_METHOD, gettext_noop("Enable Sonic hashagg."), NULL},
            &u_sess->attr.attr_sql.enable_sonic_hashagg,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_csqual_pushdown", PGC_SUSET, LOGGING_WHAT, gettext_noop("Enables colstore qual push down."), NULL},
            &u_sess->attr.attr_sql.enable_csqual_pushdown,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_change_hjcost", PGC_SUSET, LOGGING_WHAT, gettext_noop("Enable change hash join cost"), NULL},
            &u_sess->attr.attr_sql.enable_change_hjcost,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_seqscan",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of sequential-scan plans."),
             NULL},
            &u_sess->attr.attr_sql.enable_seqscan,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_indexscan",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of index-scan plans."),
             NULL},
            &u_sess->attr.attr_sql.enable_indexscan,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_indexonlyscan",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of index-only-scan plans."),
             NULL},
            &u_sess->attr.attr_sql.enable_indexonlyscan,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_bitmapscan",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of bitmap-scan plans."),
             NULL},
            &u_sess->attr.attr_sql.enable_bitmapscan,
            true,
            NULL,
            NULL,
            NULL},
        {{"force_bitmapand",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Force the planner's use of bitmap-and plans."),
             NULL},
            &u_sess->attr.attr_sql.force_bitmapand,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_parallel_ddl",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Allow user to implement DDL parallel without dead lock."),
             NULL},
            &u_sess->attr.attr_sql.enable_parallel_ddl,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_tidscan",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of TID-scan plans."),
             NULL},
            &u_sess->attr.attr_sql.enable_tidscan,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_sort",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of explicit sort steps."),
             NULL},
            &u_sess->attr.attr_sql.enable_sort,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_compress_spill", PGC_USERSET, QUERY_TUNING_METHOD, gettext_noop("Enables spilling compress."), NULL},
            &u_sess->attr.attr_sql.enable_compress_spill,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_hashagg",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of hashed aggregation plans."),
             NULL},
            &u_sess->attr.attr_sql.enable_hashagg,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_material",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of materialization."),
             NULL},
            &u_sess->attr.attr_sql.enable_material,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_nestloop",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of nested-loop join plans."),
             NULL},
            &u_sess->attr.attr_sql.enable_nestloop,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_mergejoin",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of merge join plans."),
             NULL},
            &u_sess->attr.attr_sql.enable_mergejoin,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_hashjoin",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of hash join plans."),
             NULL},
            &u_sess->attr.attr_sql.enable_hashjoin,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_index_nestloop",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of index-nested join plans."),
             NULL},
            &u_sess->attr.attr_sql.enable_index_nestloop,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_nodegroup_debug",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's node group debug mode."),
             NULL},
            &u_sess->attr.attr_sql.enable_nodegroup_debug,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_partitionwise",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of partitionwise join plans."),
             NULL},
            &u_sess->attr.attr_sql.enable_partitionwise,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_dngather",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of dngather plans."),
            NULL},
            &u_sess->attr.attr_sql.enable_dngather,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_compress_hll",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables hll use less memory on datanode."),
             NULL},
            &u_sess->attr.attr_sql.enable_compress_hll,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_vector_engine", PGC_USERSET, QUERY_TUNING_METHOD, gettext_noop("Enables the vector engine."), NULL},
            &u_sess->attr.attr_sql.enable_vector_engine,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_force_vector_engine",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Forces to enable the vector engine."),
             NULL},
            &u_sess->attr.attr_sql.enable_force_vector_engine,
            false,
            NULL,
            NULL,
            NULL},

        {{"support_extended_features",
             PGC_POSTMASTER,
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
             DEVELOPER_OPTIONS,
             gettext_noop("Enable features that ever supported in former version ."),
             NULL},
            &u_sess->attr.attr_common.enable_beta_features,
            false,
            NULL,
            NULL,
            NULL},
        {{"string_hash_compatible",
             PGC_POSTMASTER,
             QUERY_TUNING,
             gettext_noop("Enables the hash compatibility of char() and varchar() datatype"),
             NULL},
            &g_instance.attr.attr_sql.string_hash_compatible,
            false,
            NULL,
            NULL,
            NULL},

        {{"geqo",
             PGC_USERSET,
             QUERY_TUNING_GEQO,
             gettext_noop("Enables genetic query optimization."),
             gettext_noop("This algorithm attempts to do planning without "
                          "exhaustive searching.")},
            &u_sess->attr.attr_sql.enable_geqo,
            true,
            NULL,
            NULL,
            NULL},
        {/* Not for general use --- used by SET SESSION AUTHORIZATION */
            {"is_sysadmin",
                PGC_INTERNAL,
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
             CONN_AUTH_SETTINGS,
             gettext_noop("Enables advertising the server via Bonjour."),
             NULL},
            &g_instance.attr.attr_common.enable_bonjour,
            false,
            check_bonjour,
            NULL,
            NULL},
#endif
        {{"ssl", PGC_POSTMASTER, CONN_AUTH_SECURITY, gettext_noop("Enables SSL connections."), NULL},
            &g_instance.attr.attr_security.EnableSSL,
            false,
            check_ssl,
            NULL,
            NULL},
        {{"require_ssl", PGC_SIGHUP, CONN_AUTH_SECURITY, gettext_noop("Requires SSL connections."), NULL},
            &u_sess->attr.attr_security.RequireSSL,
            false,
            check_ssl,
            NULL,
            NULL},
        {{"fsync",
             PGC_SIGHUP,
             WAL_SETTINGS,
             gettext_noop("Forces synchronization of updates to disk."),
             gettext_noop("The server will use the fsync() system call in several places to make "
                          "sure that updates are physically written to disk. This insures "
                          "that a database cluster will recover to a consistent state after "
                          "an operating system or hardware crashes.")},
            &u_sess->attr.attr_storage.enableFsync,
            true,
            NULL,
            NULL,
            NULL},
        {{"ignore_checksum_failure",
             PGC_SUSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Continues processing after a checksum failure."),
             gettext_noop("Detection of a checksum failure normally causes PostgreSQL to "
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
        {{"zero_damaged_pages",
             PGC_SUSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Continues processing past damaged page headers."),
             gettext_noop("Detection of a damaged page header normally causes PostgreSQL to "
                          "report an error, aborting the current transaction. Setting "
                          "zero_damaged_pages true causes the system to instead report a "
                          "warning, zero out the damaged page, and continue processing. This "
                          "behavior will destroy data, namely all the rows on the damaged page."),
             GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_security.zero_damaged_pages,
            false,
            NULL,
            NULL,
            NULL},
        {{"full_page_writes",
             PGC_SIGHUP,
             WAL_SETTINGS,
             gettext_noop("Writes full pages to WAL when first modified after a checkpoint."),
             gettext_noop("A page write in process during an operating system crash might be "
                          "only partially written to disk.  During recovery, the row changes "
                          "stored in WAL are not enough to recover.  This option writes "
                          "pages when first modified after a checkpoint to WAL so full recovery "
                          "is possible.")},
            &u_sess->attr.attr_storage.fullPageWrites,
            true,
            NULL,
            NULL,
            NULL},
        {{"wal_log_hints",
             PGC_POSTMASTER,
             WAL_SETTINGS,
             gettext_noop("Writes full pages to WAL when first modified after a checkpoint, even for a non-critical "
                          "modifications."),
             NULL},
            &g_instance.attr.attr_storage.wal_log_hints,
            true,
            NULL,
            NULL,
            NULL},
        {{"log_checkpoints", PGC_SIGHUP, LOGGING_WHAT, gettext_noop("Logs each checkpoint."), NULL},
            &u_sess->attr.attr_common.log_checkpoints,
            false,
            NULL,
            NULL,
            NULL},
        {{"log_connections", PGC_BACKEND, LOGGING_WHAT, gettext_noop("Logs each successful connection."), NULL},
            &u_sess->attr.attr_storage.Log_connections,
            false,
            NULL,
            NULL,
            NULL},
        {{"log_disconnections",
             PGC_BACKEND,
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

        {/*
          * security requirements: ordinary users can not set this parameter,  promoting the setting level to PGC_SUSET.
          */
            {"exit_on_error",
                PGC_SUSET,
                ERROR_HANDLING_OPTIONS,
                gettext_noop("Terminates session on any error."),
                NULL},
            &u_sess->attr.attr_common.ExitOnAnyError,
            false,
            NULL,
            NULL,
            NULL},
        {{"restart_after_crash",
             PGC_SIGHUP,
             ERROR_HANDLING_OPTIONS,
             gettext_noop("Reinitializes server after backend crashes."),
             NULL},
            &u_sess->attr.attr_sql.restart_after_crash,
            true,
            NULL,
            NULL,
            NULL},

        // variable to enable memory pool
        {{"memorypool_enable", PGC_POSTMASTER, RESOURCES_MEM, gettext_noop("Using memory pool."), NULL},
            &g_instance.attr.attr_memory.memorypool_enable,
            false,
            NULL,
            NULL,
            NULL},

        // variable to enable memory protect
        {{"enable_memory_limit", PGC_POSTMASTER, RESOURCES_MEM, gettext_noop("Using memory protect feature."), NULL},
            &g_instance.attr.attr_memory.enable_memory_limit,
            true,
            NULL,
            NULL,
            show_enable_memory_limit},

        {{"enable_memory_context_control",
             PGC_SIGHUP,
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
             RESOURCES_MEM,
             gettext_noop("disable memory protect for query execution."),
             NULL},
            &u_sess->attr.attr_memory.disable_memory_protect,
            false,
            NULL,
            NULL,
            NULL},

        // variable to enable early free policy
        {{"enable_early_free", PGC_USERSET, RESOURCES_MEM, gettext_noop("Using memory early free policy."), NULL},
            &u_sess->attr.attr_sql.enable_early_free,
            true,
            NULL,
            NULL,
            NULL},

        // support to kill a working query when drop a user
        {/*
          * security requirements: ordinary users can not set this parameter,  promoting the setting level to PGC_SUSET.
          */
            {"enable_kill_query",
                PGC_SUSET,
                QUERY_TUNING_METHOD,
                gettext_noop("Enables cancelling a query that locks some relations owned by a user "
                             "when the user is dropped."),
                NULL},
            &u_sess->attr.attr_sql.enable_kill_query,
            false,
            NULL,
            NULL,
            NULL},
        // omit untranslatable char error
        {{"omit_encoding_error",
             PGC_USERSET,
             CLIENT_CONN_STATEMENT,
             gettext_noop("Omits encoding convert error."),
             NULL},
            &u_sess->attr.attr_common.omit_encoding_error,
            false,
            NULL,
            NULL,
            NULL},
        {{"log_duration",
             PGC_SUSET,
             LOGGING_WHAT,
             gettext_noop("Logs the duration of each completed SQL statement."),
             NULL},
            &u_sess->attr.attr_sql.log_duration,
            true,
            NULL,
            NULL,
            NULL},
        {{"debug_print_parse", PGC_SIGHUP, LOGGING_WHAT, gettext_noop("Logs each query's parse tree."), NULL},
            &u_sess->attr.attr_sql.Debug_print_parse,
            false,
            NULL,
            NULL,
            NULL},
        {{"debug_print_rewritten",
             PGC_SIGHUP,
             LOGGING_WHAT,
             gettext_noop("Logs each query's rewritten parse tree."),
             NULL},
            &u_sess->attr.attr_sql.Debug_print_rewritten,
            false,
            NULL,
            NULL,
            NULL},
        {{"debug_print_plan", PGC_SIGHUP, LOGGING_WHAT, gettext_noop("Logs each query's execution plan."), NULL},
            &u_sess->attr.attr_sql.Debug_print_plan,
            false,
            NULL,
            NULL,
            NULL},
        {{"debug_pretty_print", PGC_USERSET, LOGGING_WHAT, gettext_noop("Indents parse and plan tree displays."), NULL},
            &u_sess->attr.attr_sql.Debug_pretty_print,
            true,
            NULL,
            NULL,
            NULL},
        {{"log_parser_stats",
             PGC_SUSET,
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
             STATS_MONITORING,
             gettext_noop("Writes cumulative performance statistics to the server log."),
             NULL},
            &u_sess->attr.attr_common.log_statement_stats,
            false,
            check_log_stats,
            NULL,
            NULL},
#ifdef BTREE_BUILD_STATS
        {{"log_btree_build_stats",
             PGC_SUSET,
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

        {{"track_activities",
             PGC_SUSET,
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
             INSTRUMENTS_OPTIONS,
             gettext_noop("Enables instruments cpu timer functionality."),
             NULL},
            &u_sess->attr.attr_common.enable_instr_cpu_timer,
            true,
            NULL,
            NULL,
            NULL},
        {{"track_counts", PGC_SUSET, STATS_COLLECTOR, gettext_noop("Collects statistics on database activity."), NULL},
            &u_sess->attr.attr_common.pgstat_track_counts,
            true,
            NULL,
            NULL,
            NULL},
        {{"track_sql_count",
             PGC_SUSET,
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
             STATS_COLLECTOR,
             gettext_noop("Updates the process title to show the active SQL command."),
             gettext_noop(
                 "Enables updating of the process title every time a new SQL command is received by the server.")},
            &u_sess->attr.attr_common.update_process_title,
            false,
            NULL,
            NULL,
            NULL},

        {{"autovacuum", PGC_SIGHUP, AUTOVACUUM, gettext_noop("Starts the autovacuum subprocess."), NULL},
            &u_sess->attr.attr_storage.autovacuum_start_daemon,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_analyze_check",
             PGC_SUSET,
             STATS,
             gettext_noop("Enable check if table is analyzed when querying."),
             NULL},
            &u_sess->attr.attr_sql.enable_analyze_check,
            false,
            NULL,
            NULL,
            NULL},

        {{"autoanalyze",
             PGC_SUSET,
             STATS,
             gettext_noop("Enable auto-analyze when querying tables with no statistic."),
             NULL},
            &u_sess->attr.attr_sql.enable_autoanalyze,
            false,
            NULL,
            NULL,
            NULL},

        {{"cache_connection", PGC_SIGHUP, RESOURCES, gettext_noop("pooler cache connection"), NULL},
            &u_sess->attr.attr_common.pooler_cache_connection,
            true,
            NULL,
            NULL,
            NULL},

        {{"trace_notify",
             PGC_USERSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Generates debugging output for LISTEN and NOTIFY."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_common.Trace_notify,
            false,
            NULL,
            NULL,
            NULL},

#ifdef LOCK_DEBUG
        {{"trace_locks",
             PGC_SUSET,
             DEVELOPER_OPTIONS,
             gettext_noop("No description available."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_storage.Trace_locks,
            false,
            NULL,
            NULL,
            NULL},
        {{"trace_userlocks",
             PGC_SUSET,
             DEVELOPER_OPTIONS,
             gettext_noop("No description available."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_storage.Trace_userlocks,
            false,
            NULL,
            NULL,
            NULL},
        {{"trace_lwlocks",
             PGC_SUSET,
             DEVELOPER_OPTIONS,
             gettext_noop("No description available."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_storage.Trace_lwlocks,
            false,
            NULL,
            NULL,
            NULL},
        {{"debug_deadlocks",
             PGC_SUSET,
             DEVELOPER_OPTIONS,
             gettext_noop("No description available."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_storage.Debug_deadlocks,
            false,
            NULL,
            NULL,
            NULL},
#endif

        {{"log_lock_waits", PGC_SUSET, LOGGING_WHAT, gettext_noop("Logs long lock waits."), NULL},
            &u_sess->attr.attr_storage.log_lock_waits,
            false,
            NULL,
            NULL,
            NULL},

        {{"log_hostname",
             PGC_SIGHUP,
             LOGGING_WHAT,
             gettext_noop("Logs the host name in the connection logs."),
             gettext_noop("By default, connection logs only show the IP address "
                          "of the connecting host. If you want them to show the host name you "
                          "can turn this on, but depending on your host name resolution "
                          "setup it might impose a non-negligible performance penalty.")},
            &u_sess->attr.attr_common.log_hostname,
            true,
            NULL,
            NULL,
            NULL},
        {{"sql_inheritance",
             PGC_USERSET,
             COMPAT_OPTIONS_PREVIOUS,
             gettext_noop("Causes subtables to be included by default in various commands."),
             NULL},
            &u_sess->attr.attr_sql.SQL_inheritance,
            true,
            NULL,
            NULL,
            NULL},
        {{"transform_null_equals",
             PGC_USERSET,
             COMPAT_OPTIONS_CLIENT,
             gettext_noop("Treats \"expr=NULL\" as \"expr IS NULL\"."),
             gettext_noop("When turned on, expressions of the form expr = NULL "
                          "(or NULL = expr) are treated as expr IS NULL, that is, they "
                          "return true if expr evaluates to the null value, and false "
                          "otherwise. The correct behavior of expr = NULL is to always "
                          "return null (unknown).")},
            &u_sess->attr.attr_sql.Transform_null_equals,
            false,
            NULL,
            NULL,
            NULL},
        {/* only here for backwards compatibility */
            {"autocommit",
                PGC_USERSET,
                CLIENT_CONN_STATEMENT,
                gettext_noop("This parameter doesn't do anything."),
                gettext_noop("It's just here so that we won't choke on SET AUTOCOMMIT TO ON from 7.3-vintage clients."),
                GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_storage.phony_autocommit,
            true,
            check_phony_autocommit,
            NULL,
            NULL},
        {/*
          * security requirements: system/ordinary users can not set current transaction to read-only,
          * promoting the setting level to database level.
          */
            {"default_transaction_read_only",
#ifdef ENABLE_MULTIPLE_NODES
                PGC_SIGHUP,
#else
                PGC_USERSET,
#endif
                CLIENT_CONN_STATEMENT,
                gettext_noop("Sets the default read-only status of new transactions."),
                NULL},
            &u_sess->attr.attr_storage.DefaultXactReadOnly,
            false,
            check_default_transaction_read_only,
            NULL,
            NULL},
        {{"transaction_read_only",
             PGC_USERSET,
             CLIENT_CONN_STATEMENT,
             gettext_noop("Sets the current transaction's read-only status."),
             NULL,
             GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_common.XactReadOnly,
            false,
            check_transaction_read_only,
            NULL,
            NULL},
        {{"default_transaction_deferrable",
             PGC_USERSET,
             CLIENT_CONN_STATEMENT,
             gettext_noop("Sets the default deferrable status of new transactions."),
             NULL},
            &u_sess->attr.attr_storage.DefaultXactDeferrable,
            false,
            NULL,
            NULL,
            NULL},
        {{"transaction_deferrable",
             PGC_USERSET,
             CLIENT_CONN_STATEMENT,
             gettext_noop("Whether to defer a read-only serializable transaction until it can be executed with no "
                          "possible serialization failures."),
             NULL,
             GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_storage.XactDeferrable,
            false,
            check_transaction_deferrable,
            NULL,
            NULL},
        {{"check_function_bodies",
             PGC_USERSET,
             CLIENT_CONN_STATEMENT,
             gettext_noop("Checks function bodies during CREATE FUNCTION."),
             NULL},
            &u_sess->attr.attr_sql.check_function_bodies,
            true,
            NULL,
            NULL,
            NULL},
        {{"array_nulls",
             PGC_USERSET,
             COMPAT_OPTIONS_PREVIOUS,
             gettext_noop("Enables input of NULL elements in arrays."),
             gettext_noop("When turned on, unquoted NULL in an array input "
                          "value means a null value; "
                          "otherwise it is taken literally.")},
            &u_sess->attr.attr_sql.Array_nulls,
            true,
            NULL,
            NULL,
            NULL},
        {{"default_with_oids",
             PGC_USERSET,
             COMPAT_OPTIONS_PREVIOUS,
             gettext_noop("Creates new tables with OIDs by default."),
             NULL},
            &u_sess->attr.attr_sql.default_with_oids,
            false,
            NULL,
            NULL,
            NULL},
        {{"logging_collector",
             PGC_POSTMASTER,
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

#ifdef TRACE_SYNCSCAN
        /* this is undocumented because not exposed in a standard build */
        {{"trace_syncscan",
             PGC_USERSET,
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

#ifdef DEBUG_BOUNDED_SORT
        /* this is undocumented because not exposed in a standard build */
        {{"optimize_bounded_sort",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables bounded sorting using heap sort."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_sql.optimize_bounded_sort,
            true,
            NULL,
            NULL,
            NULL},
#endif

#ifdef WAL_DEBUG
        {{"wal_debug",
             PGC_SUSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Emits WAL-related debugging output."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_storage.XLOG_DEBUG,
            false,
            NULL,
            NULL,
            NULL},
#endif

        {{"integer_datetimes",
             PGC_INTERNAL,
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

        {{"krb_caseins_users",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("Sets whether Kerberos and GSSAPI user names should be treated as case-insensitive."),
             NULL},
            &u_sess->attr.attr_security.pg_krb_caseins_users,
            false,
            NULL,
            NULL,
            NULL},

        {{"escape_string_warning",
             PGC_USERSET,
             COMPAT_OPTIONS_PREVIOUS,
             gettext_noop("Warn about backslash escapes in ordinary string literals."),
             NULL},
            &u_sess->attr.attr_sql.escape_string_warning,
            true,
            NULL,
            NULL,
            NULL},

        {{"standard_conforming_strings",
             PGC_USERSET,
             COMPAT_OPTIONS_PREVIOUS,
             gettext_noop("Causes '...' strings to treat backslashes literally."),
             NULL,
             GUC_REPORT},
            &u_sess->attr.attr_sql.standard_conforming_strings,
            true,
            NULL,
            NULL,
            NULL},

        {{"synchronize_seqscans",
             PGC_USERSET,
             COMPAT_OPTIONS_PREVIOUS,
             gettext_noop("Enables synchronized sequential scans."),
             NULL},
            &u_sess->attr.attr_storage.synchronize_seqscans,
            true,
            NULL,
            NULL,
            NULL},

        {{"archive_mode",
             PGC_SIGHUP,
             WAL_ARCHIVING,
             gettext_noop("Allows archiving of WAL files using archive_command."),
             NULL},
            &u_sess->attr.attr_common.XLogArchiveMode,
            false,
            NULL,
            NULL,
            NULL},

        {{"hot_standby",
             PGC_POSTMASTER,
             REPLICATION_STANDBY,
             gettext_noop("Allows connections and queries during recovery."),
             NULL},
            &g_instance.attr.attr_storage.EnableHotStandby,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_data_replicate", PGC_USERSET, REPLICATION_STANDBY, gettext_noop("Allows data replicate."), NULL},
            &u_sess->attr.attr_storage.enable_data_replicate,
            true,
            check_enable_data_replicate,
            NULL,
            NULL},

        {{"enable_mix_replication",
             PGC_POSTMASTER,
             REPLICATION,
             gettext_noop("All the replication log sent by the wal streaming."),
             NULL},
            &g_instance.attr.attr_storage.enable_mix_replication,
            false,
            check_mix_replication_param,
            NULL,
            NULL},

        {{"ha_module_debug", PGC_USERSET, DEVELOPER_OPTIONS, gettext_noop("debug ha module."), NULL},
            &u_sess->attr.attr_storage.HaModuleDebug,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_alarm", PGC_POSTMASTER, UNGROUPED, gettext_noop("Enables alarm or not."), NULL},
            &g_instance.attr.attr_common.enable_alarm,
            false,
            NULL,
            NULL,
            NULL},

        {{"hot_standby_feedback",
             PGC_SIGHUP,
             REPLICATION_STANDBY,
             gettext_noop("Allows feedback from a hot standby to the primary that will avoid query conflicts."),
             NULL},
            &u_sess->attr.attr_storage.hot_standby_feedback,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_stream_replication",
             PGC_SIGHUP,
             REPLICATION_STANDBY,
             gettext_noop("Allows stream replication to standby or secondary."),
             NULL},
            &u_sess->attr.attr_storage.enable_stream_replication,
            true,
            NULL,
            NULL,
            NULL},

        {{"allow_system_table_mods",
             PGC_POSTMASTER,
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
             UPGRADE_OPTIONS,
             gettext_noop("Enable/disable inplace upgrade mode."),
             NULL,
             GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_common.IsInplaceUpgrade,
            false,
            check_is_upgrade,
            assign_is_inplace_upgrade,
            NULL},

        {{"enable_roach_standby_cluster",
             PGC_POSTMASTER,
             REPLICATION,
             gettext_noop("True if current instance is in ROACH standby cluster."),
             NULL,
             GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_storage.IsRoachStandbyCluster,
            false,
            NULL,
            NULL,
            NULL},

        {{"allow_concurrent_tuple_update",
             PGC_USERSET,
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

        {{"enable_stateless_pooler_reuse",
             PGC_POSTMASTER,
             DEVELOPER_OPTIONS,
             gettext_noop("Pooler stateless reuse mode."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_network.PoolerStatelessReuse,
            false,
            NULL,
            NULL,
            NULL},

        {{"enforce_two_phase_commit",
             PGC_SUSET,
             XC_HOUSEKEEPING_OPTIONS,
             gettext_noop("Enforces the use of two-phase commit on transactions that"
                          "made use of temporary objects."),
             NULL},
            &u_sess->attr.attr_storage.EnforceTwoPhaseCommit,
            true,
            NULL,
            NULL,
            NULL},
        {{"xc_maintenance_mode",
             PGC_SUSET,
             XC_HOUSEKEEPING_OPTIONS,
             gettext_noop("Turns on XC maintenance mode."),
             gettext_noop("Can set ON by SET command by system admin.")},
            &u_sess->attr.attr_common.xc_maintenance_mode,
            false,
            check_pgxc_maintenance_mode,
            NULL,
            NULL},

        {{"enable_twophase_commit",
             PGC_USERSET,
             XC_HOUSEKEEPING_OPTIONS,
             gettext_noop("Enable two phase commit when gtm free is on."),
             NULL},
            &u_sess->attr.attr_storage.enable_twophase_commit,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_light_proxy",
             PGC_SUSET,
             XC_HOUSEKEEPING_OPTIONS,
             gettext_noop("Turns on light proxy on coordinator."),
             NULL},
            &u_sess->attr.attr_sql.enable_light_proxy,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_pbe_optimization",
             PGC_SUSET,
             XC_HOUSEKEEPING_OPTIONS,
             gettext_noop("Turns on pbe optimization: force to reuse generic plan."),
             NULL},
            &u_sess->attr.attr_sql.enable_pbe_optimization,
            true,
            NULL,
            NULL,
            NULL},

        {{"lo_compat_privileges",
             PGC_SUSET,
             COMPAT_OPTIONS_PREVIOUS,
             gettext_noop("Enables backward compatibility mode for privilege checks on large objects."),
             gettext_noop("Skips privilege checks when reading or modifying large objects, "
                          "for compatibility with PostgreSQL releases prior to 9.0.")},
            &u_sess->attr.attr_sql.lo_compat_privileges,
            false,
            NULL,
            NULL,
            NULL},

        {{
             "quote_all_identifiers",
             PGC_USERSET,
             COMPAT_OPTIONS_PREVIOUS,
             gettext_noop("When generating SQL fragments, quotes all identifiers."),
             NULL,
         },
            &u_sess->attr.attr_sql.quote_all_identifiers,
            false,
            NULL,
            NULL,
            NULL},
        {{"enforce_a_behavior",
             PGC_USERSET,
             CLIENT_CONN_STATEMENT,
             gettext_noop("GUC parameter of enforcing adapting to A db."),
             NULL},
            &u_sess->attr.attr_sql.enforce_a_behavior,
            true,
            NULL,
            NULL},
        {{"support_batch_bind",
             PGC_SIGHUP,
             CLIENT_CONN_LOCALE,
             gettext_noop("Sets to use batch bind-execute for PBE."),
             NULL},
            &u_sess->attr.attr_common.support_batch_bind,
            true,
            NULL,
            NULL,
            NULL},
        {{"enableSeparationOfDuty",
             PGC_POSTMASTER,
             CONN_AUTH_SECURITY,
             gettext_noop("Enables the user's separation of privileges."),
             NULL},
            &g_instance.attr.attr_security.enablePrivilegesSeparate,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_nonsysadmin_execute_direct",
             PGC_POSTMASTER,
             UNGROUPED,
             gettext_noop("Enables non-sysadmin users execute direct on CN/DN."),
             NULL},
            &g_instance.attr.attr_security.enable_nonsysadmin_execute_direct,
            false,
            NULL,
            NULL,
            NULL},
        {{"operation_mode",
             PGC_SIGHUP,
             UNGROUPED,
             gettext_noop("Sets the operation mode."),
             NULL},
            &u_sess->attr.attr_security.operation_mode,
            false,
            NULL,
            NULL,
            NULL},
        {{"use_workload_manager",
             PGC_SIGHUP,
             RESOURCES_WORKLOAD,
             gettext_noop("Enables workload manager in the system. "),
             NULL},
            &u_sess->attr.attr_resource.use_workload_manager,
            true,
            check_use_workload_manager,
            assign_use_workload_manager,
            NULL},
        {{"use_elastic_search",
            PGC_POSTMASTER,
            AUDIT_OPTIONS,
            gettext_noop("Enables elastic search in the system. "),
            NULL},
            &g_instance.attr.attr_security.use_elastic_search,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_security_policy",
            PGC_SIGHUP,
            QUERY_TUNING_METHOD,
            gettext_noop("enable security policy features."),
            NULL},
            &u_sess->attr.attr_security.Enable_Security_Policy,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_slot_log", PGC_USERSET, QUERY_TUNING_METHOD, gettext_noop("Enables create slot log"), NULL},
            &u_sess->attr.attr_sql.enable_slot_log,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_resource_track",
             PGC_SIGHUP,
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
             RESOURCES_WORKLOAD,
             gettext_noop("enable insert the session info into the user table. "),
             NULL},
            &u_sess->attr.attr_resource.enable_resource_record,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_bbox_dump",
             PGC_SIGHUP,
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
            RESOURCES_WORKLOAD,
            gettext_noop("Enables First Failure Info Capture. "),
            NULL},
            &g_instance.attr.attr_common.enable_ffic_log,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_thread_pool", PGC_POSTMASTER, CLIENT_CONN, gettext_noop("enable to use thread pool. "), NULL},
            &g_instance.attr.attr_common.enable_thread_pool,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_global_plancache", PGC_POSTMASTER, CLIENT_CONN, gettext_noop("enable to use global plan cache. "), NULL},
            &g_instance.attr.attr_common.enable_global_plancache,
            false,
            check_gpc_syscache_threshold,
            NULL,
            NULL},

        {{"enable_router", PGC_SIGHUP, CLIENT_CONN, gettext_noop("enable to use router."),
             NULL},
            &u_sess->attr.attr_common.enable_router,
            false,
            NULL,
            NULL,
            NULL},

        /* Database Security: Support database audit */
        /* add guc option about audit */
        {{"audit_enabled",
             PGC_SIGHUP,
             AUDIT_OPTIONS,
             gettext_noop("Starts a subprocess to capture audit output into audit files."),
             NULL},
            &u_sess->attr.attr_security.Audit_enabled,
            true,
            NULL,
            NULL,
            NULL},

        /* Database Security: Support database audit */
        /* add guc option about audit */
        {{"audit_resource_policy",
             PGC_SIGHUP,
             AUDIT_OPTIONS,
             gettext_noop("the policy is used to determine how to cleanup the audit files;"
                          " True means to cleanup the audit files based on space limitation and"
                          " False means to cleanup the audit files when the remained time is arriving."),
             NULL},
            &u_sess->attr.attr_security.Audit_CleanupPolicy,
            true,
            NULL,
            NULL,
            NULL},

        {{"modify_initial_password",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("modify the initial password of the initial user."),
             NULL},
            &u_sess->attr.attr_security.Modify_initial_password,
            false,
            NULL,
            NULL,
            NULL},

        {{
             "most_available_sync",
             PGC_SIGHUP,
             REPLICATION_MASTER,
             gettext_noop("Enables master to continue when sync standbys failure."),
             NULL,
         },
            &u_sess->attr.attr_storage.guc_most_available_sync,
            false,
            NULL,
            NULL,
            NULL},
        {{
             "convert_string_to_digit",
             PGC_USERSET,
             UNGROUPED,
             gettext_noop("Convert string to digit when comparing string and digit"),
             NULL,
         },
            &u_sess->attr.attr_sql.convert_string_to_digit,
            true,
            NULL,
            assign_convert_string_to_digit,
            NULL},
        // Stream communication
        //
        {{
             "comm_tcp_mode",
             PGC_POSTMASTER,
             CLIENT_CONN,
             gettext_noop("Whether use tcp commucation mode for stream"),
             NULL,
         },
            &g_instance.attr.attr_network.comm_tcp_mode,
            true,
            check_sctp_support,
            NULL,
            NULL},
        {{
             "comm_debug_mode",
             PGC_USERSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Whether use libcomm debug mode for print debug information"),
             NULL,
         },
            &u_sess->attr.attr_network.comm_debug_mode,
#ifdef ENABLE_LLT
            true,
#else
            false,
#endif
            NULL,
            assign_comm_debug_mode,
            NULL},
        {{
             "comm_stat_mode",
             PGC_USERSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Whether use libcomm stat mode for print stat data"),
             NULL,
         },
            &u_sess->attr.attr_network.comm_stat_mode,
#ifdef ENABLE_LLT
            true,
#else
            false,
#endif
            NULL,
            assign_comm_stat_mode,
            NULL},
        {{
             "comm_timer_mode",
             PGC_USERSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Whether use libcomm timer debug mode for print timer data"),
             NULL,
         },
            &u_sess->attr.attr_network.comm_timer_mode,
#ifdef ENABLE_LLT
            true,
#else
            false,
#endif
            NULL,
            assign_comm_timer_mode,
            NULL},
        {{
             "comm_no_delay",
             PGC_USERSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Whether set NO_DELAY option for libcomm socket"),
             NULL,
         },
            &u_sess->attr.attr_network.comm_no_delay,
            false,
            NULL,
            assign_comm_no_delay,
            NULL},

        {{
             "enable_show_any_tuples",
             PGC_USERSET,
             DEVELOPER_OPTIONS,
             gettext_noop("This parameter is just valid when it's a read-only transction, just for analyse."
                          "The default_transaction_read_only and transaction_read_only should be true."
                          "You'd better keep enable_indexscan and enable_bitmapscan  be false to keep seqscan occurs."
                          "When enable_show_any_tuples is true, all versions of the tuples are visible, including "
                          "dirty versions."),
             NULL,
         },
            &u_sess->attr.attr_storage.enable_show_any_tuples,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_broadcast",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables the planner's use of broadcast stream plans."),
             NULL},
            &u_sess->attr.attr_sql.enable_broadcast,
            true,
            NULL,
            NULL,
            NULL},
        {{
             "enable_debug_vacuum",
             PGC_SIGHUP,
             DEVELOPER_OPTIONS,
             gettext_noop("This parameter is just used for logging some vacuum info."),
             NULL,
         },
            &u_sess->attr.attr_storage.enable_debug_vacuum,
            false,
            NULL,
            NULL,
            NULL},
        {{
             "ngram_punctuation_ignore",
             PGC_USERSET,
             TEXT_SEARCH,
             gettext_noop("Enables N-gram ignore punctuation."),
             NULL,
         },
            &u_sess->attr.attr_sql.ngram_punctuation_ignore,
            true,
            NULL,
            NULL,
            NULL},
        {{
             "ngram_grapsymbol_ignore",
             PGC_USERSET,
             TEXT_SEARCH,
             gettext_noop("Enables N-gram ignore grapsymbol."),
             NULL,
         },
            &u_sess->attr.attr_sql.ngram_grapsymbol_ignore,
            false,
            NULL,
            NULL,
            NULL},
        {{
             "enable_fast_allocate",
             PGC_SUSET,
             UNGROUPED,
             gettext_noop(
                 "enable fallocate to improve file extend performance, make sure filesystem support it, ep:XFS"),
             NULL,
         },
            &u_sess->attr.attr_sql.enable_fast_allocate,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_adio_debug", PGC_SUSET, DEVELOPER_OPTIONS, gettext_noop("Enable log debug adio function."), NULL},
            &u_sess->attr.attr_storage.enable_adio_debug,
            false,
            check_adio_debug_guc,
            NULL,
            NULL},
        /* This is a beta feature, set it to PGC_INTERNAL, so user can't configure it */
        {{"enable_adio_function", PGC_INTERNAL, DEVELOPER_OPTIONS, gettext_noop("Enable adio function."), NULL},
            &g_instance.attr.attr_storage.enable_adio_function,
            false,
            check_adio_function_guc,
            NULL,
            NULL},

        {{"td_compatible_truncation",
             PGC_USERSET,
             QUERY_TUNING_OTHER,
             gettext_noop("Enable string automatically truncated during insertion."),
             NULL},
            &u_sess->attr.attr_sql.td_compatible_truncation,
            false,
            NULL,
            NULL,
            NULL},

        {{"gds_debug_mod", PGC_USERSET, LOGGING_WHEN, gettext_noop("Enable GDS-related troubleshoot-logging."), NULL},
            &u_sess->attr.attr_storage.gds_debug_mod,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_upgrade_merge_lock_mode",
             PGC_USERSET,
             QUERY_TUNING_OTHER,
             gettext_noop("If true, use Exclusive Lock mode for deltamerge."),
             NULL},
            &u_sess->attr.attr_sql.enable_upgrade_merge_lock_mode,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_orc_cache", PGC_POSTMASTER, QUERY_TUNING_METHOD, gettext_noop("Enable orc metadata cache."), NULL},
            &g_instance.attr.attr_sql.enable_orc_cache,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_default_cfunc_libpath", PGC_POSTMASTER, FILE_LOCATIONS, gettext_noop("Enable check for c function lib path."), NULL},
            &g_instance.attr.attr_sql.enable_default_cfunc_libpath,
            true,
            NULL,
            NULL,
            NULL},

        {{"acceleration_with_compute_pool",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("If true, agg/scan may run in compute pool."),
             NULL},
            &u_sess->attr.attr_sql.acceleration_with_compute_pool,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_extrapolation_stats",
             PGC_SUSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enable extrapolation stats for date datatype."),
             NULL},
            &u_sess->attr.attr_sql.enable_extrapolation_stats,
            false,
            NULL,
            NULL,
            NULL},

        {{
             "data_sync_retry",
             PGC_POSTMASTER,
             ERROR_HANDLING_OPTIONS,
             gettext_noop("Whether to continue running after a failure to sync data files."),
         },
            &g_instance.attr.attr_common.data_sync_retry,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_trigger_shipping",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Ship a trigger to DN if possible."),
             NULL},
            &u_sess->attr.attr_sql.enable_trigger_shipping,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_save_datachanged_timestamp",
             PGC_USERSET,
             UNGROUPED,
             gettext_noop("If true, save the timestamp when the data of the table changes."),
             NULL},
            &u_sess->attr.attr_sql.enable_save_datachanged_timestamp,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_online_ddl_waitlock",
             PGC_SIGHUP,
             UNGROUPED,
             gettext_noop("Enable ddl wait advisory lock in online expansion."),
             NULL},
            &u_sess->attr.attr_sql.enable_online_ddl_waitlock,
            false,
            NULL,
            NULL,
            NULL},

        {{"show_acce_estimate_detail",
             PGC_USERSET,
             UNGROUPED,
             gettext_noop("If true, show details whether plan is pushed down to the compute pool."),
             NULL},
            &u_sess->attr.attr_sql.show_acce_estimate_detail,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_cbm_tracking", PGC_SIGHUP, WAL, gettext_noop("Turn on cbm tracking function. "), NULL},
            &u_sess->attr.attr_storage.enable_cbm_tracking,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_copy_server_files",
             PGC_SIGHUP,
             UNGROUPED,
             gettext_noop("enable sysadmin to copy from/to file"),
             NULL},
            &u_sess->attr.attr_storage.enable_copy_server_files,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_user_metric_persistent",
             PGC_SIGHUP,
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
             RESOURCES_WORKLOAD,
             gettext_noop("enable logical io statistics function."),
             NULL},
            &u_sess->attr.attr_resource.enable_logical_io_statistics,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_access_server_directory",
             PGC_SIGHUP,
             UNGROUPED,
             gettext_noop("enable sysadmin to create directory"),
             NULL},
            &g_instance.attr.attr_storage.enable_access_server_directory,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_opfusion", PGC_USERSET, QUERY_TUNING_METHOD, gettext_noop("Enables opfusion."), NULL},
            &u_sess->attr.attr_sql.enable_opfusion,
            true,
            NULL,
            NULL,
            NULL},

#ifndef ENABLE_MULTIPLE_NODES
        {{"enable_beta_opfusion",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Enables beta opfusion features."),
             NULL},
            &u_sess->attr.attr_sql.enable_beta_opfusion,
            false,
            NULL,
            NULL,
            NULL},
#endif

        {{"enable_partition_opfusion", PGC_USERSET, QUERY_TUNING_METHOD,
            gettext_noop("Enables partition opfusion features."),
            NULL},
            &u_sess->attr.attr_sql.enable_partition_opfusion,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_prevent_job_task_startup",
             PGC_SIGHUP,
             JOB,
             gettext_noop("enable control whether the job task thread can be started."),
             NULL},
            &u_sess->attr.attr_sql.enable_prevent_job_task_startup,
            false,
            NULL,
            NULL,
            NULL},

        {{"check_implicit_conversions",
             PGC_USERSET,
             UNGROUPED,
             gettext_noop("check whether there is an implicit conversion on index column"),
             NULL},
            &u_sess->attr.attr_common.check_implicit_conversions_for_indexcol,
            false,
            NULL,
            NULL,
            NULL},

        {{
             "enable_incremental_checkpoint",
             PGC_POSTMASTER,
             WAL_CHECKPOINTS,
             gettext_noop("Enable master incremental checkpoint."),
             NULL,
         },
            &g_instance.attr.attr_storage.enableIncrementalCheckpoint,
            true,
            NULL,
            NULL,
            NULL},

        {{
             "enable_double_write",
             PGC_POSTMASTER,
             WAL_CHECKPOINTS,
             gettext_noop("Enable master double write."),
             NULL,
         },
            &g_instance.attr.attr_storage.enable_double_write,
            true,
            NULL,
            NULL,
            NULL},

        {{"log_pagewriter", PGC_SIGHUP, LOGGING_WHAT, gettext_noop("Logs pagewriter thread."), NULL},
            &u_sess->attr.attr_storage.log_pagewriter,
            false,
            NULL,
            NULL,
            NULL},
        {{
             "enable_page_lsn_check",
             PGC_POSTMASTER,
             WAL,
             gettext_noop("Enable check page lsn when redo"),
             NULL,
         },
            &g_instance.attr.attr_storage.enableWalLsnCheck,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_xlog_prune",
             PGC_SIGHUP,
             WAL,
             gettext_noop("Enable xlog prune when not all standys connected and xlog size is largger than max_xlog_size"),
             NULL},
            &u_sess->attr.attr_storage.enable_xlog_prune,
            true,
            NULL,
            NULL,
            NULL},
       {{"enable_auto_explain",
            PGC_USERSET,
            RESOURCES_WORKLOAD,
            gettext_noop("enable auto explain plans. "),
            NULL},
            &u_sess->attr.attr_resource.enable_auto_explain,
            false,
            NULL,
            NULL,
            NULL},
        /* End-of-list marker */
        {{NULL, (GucContext)0, (config_group)0, NULL, NULL}, NULL, false, NULL, NULL, NULL}};

    int num_of_bool_option = sizeof(localConfigureNamesBool) / sizeof(config_bool);
    for (int i = 0; i < num_of_bool_option - 1; i++) {
        if (strcasecmp(localConfigureNamesBool[i].gen.name, "enable_save_datachanged_timestamp") == 0 &&
            get_product_version() == PRODUCT_VERSION_GAUSSDB300) {
            localConfigureNamesBool[i].boot_val = false;
            break;
        }
    }

    Size bytes = sizeof(localConfigureNamesBool);
    u_sess->utils_cxt.ConfigureNamesBool[SINGLE_GUC] =
        (struct config_bool*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesBool[SINGLE_GUC], bytes, localConfigureNamesBool, bytes);
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
        {{"max_active_global_temporary_table",
            PGC_USERSET,
            UNGROUPED,
            gettext_noop("max active global temporary table."),
            NULL},
            &u_sess->attr.attr_storage.max_active_gtt,
            1000,
            0,
            1000000,
            NULL,
            NULL,
            NULL},
        {{"vacuum_gtt_defer_check_age", 
            PGC_USERSET, 
            CLIENT_CONN_STATEMENT,
            gettext_noop("The defer check age of GTT, used to check expired data after vacuum."),
            NULL},
            &u_sess->attr.attr_storage.vacuum_gtt_defer_check_age,
            10000,
            0,
            1000000,
            NULL,
            NULL,
            NULL},
        {{"archive_timeout",
             PGC_SIGHUP,
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
        {{"post_auth_delay",
             PGC_BACKEND,
             DEVELOPER_OPTIONS,
             gettext_noop("Waits N seconds on connection startup after authentication."),
             gettext_noop("This allows attaching a debugger to the process."),
             GUC_NOT_IN_SAMPLE | GUC_UNIT_S},
            &u_sess->attr.attr_security.PostAuthDelay,
            0,
            0,
            INT_MAX / 1000000,
            NULL,
            NULL,
            NULL},
        {{"default_statistics_target",
             PGC_USERSET,
             QUERY_TUNING_OTHER,
             gettext_noop("Sets the default statistics target."),
             gettext_noop("This applies to table columns that have not had a "
                          "column-specific target set via ALTER TABLE SET STATISTICS."
                          "default_statistics_target < 0 means using percent to set statistics.")},
            &u_sess->attr.attr_sql.default_statistics_target,
            100,
            -100,
            10000,
            NULL,
            NULL,
            NULL},
            
        {{"from_collapse_limit",
             PGC_USERSET,
             QUERY_TUNING_OTHER,
             gettext_noop("Sets the FROM-list size beyond which subqueries "
                          "are not collapsed."),
             gettext_noop("The planner will merge subqueries into upper "
                          "queries if the resulting FROM list would have no more than "
                          "this many items.")},
            &u_sess->attr.attr_sql.from_collapse_limit,
            8,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"join_collapse_limit",
             PGC_USERSET,
             QUERY_TUNING_OTHER,
             gettext_noop("Sets the FROM-list size beyond which JOIN "
                          "constructs are not flattened."),
             gettext_noop("The planner will flatten explicit JOIN "
                          "constructs into lists of FROM items whenever a "
                          "list of no more than this many items would result.")},
            &u_sess->attr.attr_sql.join_collapse_limit,
            8,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"geqo_threshold",
             PGC_USERSET,
             QUERY_TUNING_GEQO,
             gettext_noop("Sets the threshold of FROM items beyond which GEQO is used."),
             NULL},
            &u_sess->attr.attr_sql.geqo_threshold,
            12,
            2,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"geqo_effort",
             PGC_USERSET,
             QUERY_TUNING_GEQO,
             gettext_noop("GEQO: effort is used to set the default for other GEQO parameters."),
             NULL},
            &u_sess->attr.attr_sql.Geqo_effort,
            DEFAULT_GEQO_EFFORT,
            MIN_GEQO_EFFORT,
            MAX_GEQO_EFFORT,
            NULL,
            NULL,
            NULL},
        {{"geqo_pool_size",
             PGC_USERSET,
             QUERY_TUNING_GEQO,
             gettext_noop("GEQO: number of individuals in the population."),
             gettext_noop("Zero selects a suitable default value.")},
            &u_sess->attr.attr_sql.Geqo_pool_size,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"geqo_generations",
             PGC_USERSET,
             QUERY_TUNING_GEQO,
             gettext_noop("GEQO: number of iterations of the algorithm."),
             gettext_noop("Zero selects a suitable default value.")},
            &u_sess->attr.attr_sql.Geqo_generations,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"hll_default_log2m",
             PGC_USERSET,
             UNGROUPED,
             gettext_noop("Set parameter log2m in hll."),
             gettext_noop("It's related to numbers of register and relative error.")},
            &u_sess->attr.attr_sql.g_default_log2m,
            DEFAULT_LOG2M,
            HLL_MIN_LOG2M,
            HLL_MAX_LOG2M,
            NULL,
            NULL,
            NULL},
        {{"hll_default_regwidth",
             PGC_USERSET,
             UNGROUPED,
             gettext_noop("Set parameter regwidth in hll."),
             gettext_noop("It's related to number of bits used per register, max cardinality and memory of hll.")},
            &u_sess->attr.attr_sql.g_default_regwidth,
            DEFAULT_REGWIDTH,
            HLL_MIN_REGWIDTH,
            HLL_MAX_REGWIDTH,
            NULL,
            NULL,
            NULL},
        {{"hll_default_sparseon",
             PGC_USERSET,
             UNGROUPED,
             gettext_noop("Set parameter sparseon for hll."),
             gettext_noop("hll use sparseon mode or not.")},
            &u_sess->attr.attr_sql.g_default_sparseon,
            DEFAULT_SPARSEON,
            HLL_MIN_SPARSEON,
            HLL_MAX_SPARSEON,
            NULL,
            NULL,
            NULL},
        {{"hll_max_sparse", PGC_USERSET, UNGROUPED, gettext_noop("Set parameter max_sparse for hll"), NULL},
            &u_sess->attr.attr_sql.g_max_sparse,
            MAX_SPARSE,
            HLL_MIN_MAX_SPARSE,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"cost_param",
             PGC_USERSET,
             QUERY_TUNING_OTHER,
             gettext_noop("Bitmap controls the use of alternative cost model."),
             NULL},
            &u_sess->attr.attr_sql.cost_param,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"wait_dummy_time",
             PGC_SIGHUP,
             CUSTOM_OPTIONS,
             gettext_noop("Wait for dummy starts or bcm file list received when catchup."),
         },
            &u_sess->attr.attr_storage.wait_dummy_time,
            300,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"catchup2normal_wait_time",
             PGC_POSTMASTER,
             REPLICATION_STANDBY,
             gettext_noop("The maximal allowed duration for waiting from catchup to normal state."),
             NULL,
             GUC_UNIT_MS
         },
            &g_instance.attr.attr_storage.catchup2normal_wait_time,
            -1,
            -1,
            10000,
            NULL,
            NULL,
            NULL},
        {{"max_recursive_times",
             PGC_USERSET,
             QUERY_TUNING_OTHER,
             gettext_noop("max recursive times when execute query with recursive-clause."),
             NULL},
            &u_sess->attr.attr_sql.max_recursive_times,
            200,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"schedule_splits_threshold",
             PGC_USERSET,
             QUERY_TUNING_OTHER,
             gettext_noop("The Max count of splits which can be scheduled in memory."),
             NULL},
            &u_sess->attr.attr_sql.schedule_splits_threshold,
            60000,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {/* This is PGC_SUSET to prevent hiding from log_lock_waits. */
            {"deadlock_timeout",
                PGC_SUSET,
                LOCK_MANAGEMENT,
                gettext_noop("Sets the time to wait on a lock before checking for deadlock."),
                NULL,
                GUC_UNIT_MS},
            &u_sess->attr.attr_storage.DeadlockTimeout,
            1000,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {/* This is PGC_SUSET to prevent hiding from log_lock_waits. */
            {"lockwait_timeout",
                PGC_SUSET,
                LOCK_MANAGEMENT,
                gettext_noop("Sets the max time to wait on a lock acquire."),
                NULL,
                GUC_UNIT_MS},
            &u_sess->attr.attr_storage.LockWaitTimeout,
            3000,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {/* This is PGC_SUSET to prevent hiding from log_lock_waits. */
            {"update_lockwait_timeout",
                PGC_SUSET,
                LOCK_MANAGEMENT,
                gettext_noop("Sets the max time to wait on a lock acquire when concurrently update same tuple."),
                NULL,
                GUC_UNIT_MS},
            &u_sess->attr.attr_storage.LockWaitUpdateTimeout,
            120000,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"max_standby_archive_delay",
             PGC_SIGHUP,
             REPLICATION_STANDBY,
             gettext_noop("Sets the maximum delay before canceling queries when a hot standby server is processing "
                          "archived WAL data."),
             NULL,
             GUC_UNIT_MS},
            &u_sess->attr.attr_storage.max_standby_archive_delay,
            3 * 1000,
            -1,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"max_standby_streaming_delay",
             PGC_SIGHUP,
             REPLICATION_STANDBY,
             gettext_noop("Sets the maximum delay before canceling queries when a hot standby server is processing "
                          "streamed WAL data."),
             NULL,
             GUC_UNIT_MS},
            &u_sess->attr.attr_storage.max_standby_streaming_delay,
            3 * 1000,
            -1,
            INT_MAX,
            NULL,
            NULL,
            NULL},

#ifndef ENABLE_MULTIPLE_NODES
        {{"recovery_min_apply_delay",
            PGC_SIGHUP,
            REPLICATION_STANDBY,
        	gettext_noop("Sets the minimum delay for applying changes during recovery."),
        	NULL,
        	GUC_UNIT_MS},
            &u_sess->attr.attr_storage.recovery_min_apply_delay,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
#endif

        {{"wal_receiver_status_interval",
             PGC_SIGHUP,
             REPLICATION_STANDBY,
             gettext_noop("Sets the maximum interval between WAL receiver status reports to the primary."),
             NULL,
             GUC_UNIT_S},
            &u_sess->attr.attr_storage.wal_receiver_status_interval,
            5,
            0,
            INT_MAX / 1000,
            NULL,
            NULL,
            NULL},

        {{"wal_receiver_timeout",
             PGC_SIGHUP,
             REPLICATION_STANDBY,
             gettext_noop("Sets the maximum wait time to receive data from master."),
             NULL,
             GUC_UNIT_MS},
            &u_sess->attr.attr_storage.wal_receiver_timeout,
            6 * 1000,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL
        },
        {
            {
                "basebackup_timeout",
                PGC_USERSET,
                WAL_SETTINGS,
                gettext_noop("Sets the timeout in seconds for a reponse from gs_basebackup."),
                NULL,
                GUC_UNIT_S
            },
            &u_sess->attr.attr_storage.basebackup_timeout,
            60 * 10,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL
        },
        {{"wal_receiver_connect_timeout",
             PGC_SIGHUP,
             REPLICATION_STANDBY,
             gettext_noop("Sets the maximum wait time to connect master."),
             NULL,
             GUC_UNIT_S},
            &u_sess->attr.attr_storage.wal_receiver_connect_timeout,
            2,
            0,
            INT_MAX / 1000,
            NULL,
            NULL,
            NULL},

        {{"wal_receiver_connect_retries",
             PGC_SIGHUP,
             REPLICATION_STANDBY,
             gettext_noop("Sets the maximum retries to connect master."),
             NULL},
            &u_sess->attr.attr_storage.wal_receiver_connect_retries,
            1,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"wal_receiver_buffer_size",
             PGC_POSTMASTER,
             REPLICATION_STANDBY,
             gettext_noop("Sets the buffer size to receive data from master."),
             NULL,
             GUC_UNIT_KB},
            &g_instance.attr.attr_storage.WalReceiverBufSize,
            64 * 1024,
            4 * 1024,
            1023 * 1024,
            NULL,
            NULL,
            NULL},
        {{"data_replicate_buffer_size",
             PGC_POSTMASTER,
             REPLICATION_SENDING,
             gettext_noop("Sets the buffer size of data replication."),
             NULL,
             GUC_UNIT_KB},
            &g_instance.attr.attr_storage.DataQueueBufSize,
            16 * 1024,
            4 * 1024,
            1023 * 1024 * 1024,
            NULL,
            NULL,
            NULL},
        {
            {
                "max_connections",
                PGC_POSTMASTER,
                CONN_AUTH_SETTINGS,
                gettext_noop("Sets the maximum number of concurrent connections for clients."),
                NULL
            },
            &g_instance.attr.attr_network.MaxConnections,
            200,
            10,
            MAX_BACKENDS,
            check_maxconnections,
            NULL,
            NULL
        },
        {
            {
                "max_inner_tool_connections",
                PGC_POSTMASTER,
                CONN_AUTH_SETTINGS,
                gettext_noop("Sets the maximum number of concurrent connections for inner tools."),
                NULL
            },
            &g_instance.attr.attr_network.maxInnerToolConnections,
            50,
            1,
            MAX_BACKENDS,
            CheckMaxInnerToolConnections,
            NULL,
            NULL
        },
        {{"sysadmin_reserved_connections",
             PGC_POSTMASTER,
             CONN_AUTH_SETTINGS,
             gettext_noop("Sets the number of connection slots reserved for system admin."),
             NULL},
            &g_instance.attr.attr_network.ReservedBackends,
            3,
            0,
            MAX_BACKENDS,
            NULL,
            NULL,
            NULL},
        {{"alarm_report_interval",
             PGC_SIGHUP,
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

        /*
         * We sometimes multiply the number of shared buffers by two without
         * checking for overflow, so we mustn't allow more than INT_MAX / 2.
         */
        {{"shared_buffers",
             PGC_POSTMASTER,
             RESOURCES_MEM,
             gettext_noop("Sets the number of shared memory buffers used by the server."),
             NULL,
             GUC_UNIT_BLOCKS},
            &g_instance.attr.attr_storage.NBuffers,
            1024,
            16,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},

        {{"hashagg_table_size",
             PGC_USERSET,
             QUERY_TUNING_OTHER,
             gettext_noop("Sets the number of slot in the hash table."),
             NULL},
            &u_sess->attr.attr_sql.hashagg_table_size,
            0,
            0,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},

        {{"memorypool_size",
             PGC_POSTMASTER,
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
             PGC_POSTMASTER,
             RESOURCES_MEM,
             gettext_noop("Sets the maximum threshold for cleaning cache."),
             NULL,
             GUC_UNIT_KB},
            &g_instance.attr.attr_memory.local_syscache_threshold,
            256 * 1024,
            1 * 1024,
            512 * 1024,
            check_syscache_threshold_gpc,
            NULL,
            NULL},

        {{"session_statistics_memory",
             PGC_SIGHUP,
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

        {{"udf_memory_limit",
             PGC_POSTMASTER,
             RESOURCES_MEM,
             gettext_noop("Sets the maximum number of memory used by UDF Master and UDF Workers."),
             NULL,
             GUC_UNIT_KB},
            &g_instance.attr.attr_sql.udf_memory_limit,
            200 * 1024,
            200 * 1024,
            INT_MAX,
            check_udf_memory_limit,
            NULL,
            NULL},

        {{"cstore_buffers",
             PGC_POSTMASTER,
             RESOURCES_MEM,
             gettext_noop("Sets the number of CStore buffers used by the server."),
             NULL,
             GUC_UNIT_KB},
            &g_instance.attr.attr_storage.cstore_buffers,
            131072,
            16384,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},

        {{"max_loaded_cudesc",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Sets the number of loaded cudesc per column."),
             NULL},
            &u_sess->attr.attr_storage.max_loaded_cudesc,
            1024,
            100,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},

        {{"temp_buffers",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Sets the maximum number of temporary buffers used by each session."),
             NULL,
             GUC_UNIT_BLOCKS},
            &u_sess->attr.attr_storage.num_temp_buffers,
            128,
            100,
            INT_MAX / 2,
            check_temp_buffers,
            NULL,
            NULL},
        {{"port", PGC_POSTMASTER, CONN_AUTH_SETTINGS, gettext_noop("Sets the TCP port the server listens on."), NULL},
            &g_instance.attr.attr_network.PostPortNumber,
            DEF_PGPORT,
            1,
            65535,
            NULL,
            NULL,
            NULL},
        {{"unix_socket_permissions",
             PGC_POSTMASTER,
             CONN_AUTH_SETTINGS,
             gettext_noop("Sets the access permissions of the Unix-domain socket."),
             gettext_noop("Unix-domain sockets use the usual Unix file system "
                          "permission set. The parameter value is expected "
                          "to be a numeric mode specification in the form "
                          "accepted by the chmod and umask system calls. "
                          "(To use the customary octal format the number must "
                          "start with a 0 (zero).)")},
            &g_instance.attr.attr_network.Unix_socket_permissions,
            0777,
            0000,
            0777,
            NULL,
            NULL,
            show_unix_socket_permissions},

        {{"log_file_mode",
             PGC_SIGHUP,
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

        {{
             "resource_track_cost",
             PGC_USERSET,
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
        {{
             "topsql_retention_time",
             PGC_SIGHUP,
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
        {{
             "unique_sql_retention_time",
             PGC_SIGHUP,
             RESOURCES_WORKLOAD,
             gettext_noop("the retention time of unique sql text"),
             gettext_noop("This value is set to 30 min as default. "),
             GUC_UNIT_MIN
         },
            &u_sess->attr.attr_resource.unique_sql_retention_time,
            30,
            1,
            3650,
            NULL,
            NULL,
            NULL},
        {{"resource_track_duration",
             PGC_USERSET,
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

        {{
             "user_metric_retention_time",
             PGC_SIGHUP,
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

        {{
             "instance_metric_retention_time",
             PGC_SIGHUP,
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

        {{
             "cpu_collect_timer",
             PGC_SIGHUP,
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

        {{"work_mem",
             PGC_USERSET,
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
        {{"UDFWorkerMemHardLimit",
             PGC_POSTMASTER,
             RESOURCES_MEM,
             gettext_noop("Sets the hard memory limit to be used for fenced UDF."),
             NULL,
             GUC_UNIT_KB},
            &g_instance.attr.attr_sql.UDFWorkerMemHardLimit,
            1 * 1024 * 1024,
            0,
            MAX_KILOBYTES,
            NULL,
            NULL,
            NULL},

        {{"FencedUDFMemoryLimit",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Sets the maximum memory to be used for fenced UDF by user."),
             NULL,
             GUC_UNIT_KB},
            &u_sess->attr.attr_sql.FencedUDFMemoryLimit,
            0,
            0,
            MAX_KILOBYTES,
            check_fencedUDFMemoryLimit,
            NULL,
            NULL},

        {{"query_mem",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Sets the memory to be reserved for a statement."),
             NULL,
             GUC_UNIT_KB},
            &u_sess->attr.attr_sql.statement_mem,
            0,
            0,
            MAX_KILOBYTES,
            check_statement_mem,
            NULL,
            NULL},

        {{"query_max_mem",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Sets the max memory to be reserved for a statement."),
             NULL,
             GUC_UNIT_KB},
            &u_sess->attr.attr_sql.statement_max_mem,
            0,
            0,
            MAX_KILOBYTES,
            check_statement_max_mem,
            NULL,
            NULL},

        {{
             "bbox_dump_count",
             PGC_USERSET,
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
        {{
             "io_limits",
             PGC_USERSET,
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
        {{
             "io_control_unit",
             PGC_SIGHUP,
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
        {{
             "autovacuum_io_limits",
             PGC_SIGHUP,
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
        {{
             "transaction_pending_time",
             PGC_USERSET,
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
        {{"psort_work_mem",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Sets the maximum memory to be used for partial sort."),
             gettext_noop("This much memory can be used by each internal "
                          "partial sort operation before switching to "
                          "temporary disk files."),
             GUC_UNIT_KB},
            &u_sess->attr.attr_storage.psort_work_mem,
            512 * 1024,
            64,
            MAX_KILOBYTES,
            NULL,
            NULL,
            NULL},

        {{"maintenance_work_mem",
             PGC_USERSET,
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
        {{"bulk_write_ring_size",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Size of bulk write buffer ring."),
             gettext_noop("size of bulk write buffer ring, default values is "
                          "16M."),
             GUC_UNIT_KB},
            &u_sess->attr.attr_storage.bulk_write_ring_size,
            16384,
            16384,
            MAX_KILOBYTES,
            NULL,
            NULL,
            NULL},
        {{"bulk_read_ring_size",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Size of bulk read buffer ring."),
             gettext_noop("size of bulk read buffer ring, default values is "
                          "256K."),
             GUC_UNIT_KB},
            &u_sess->attr.attr_storage.bulk_read_ring_size,
            16384,
            256,
            MAX_KILOBYTES,
            NULL,
            NULL,
            NULL},
        {{"partition_mem_batch",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Number of partition in-memory batch"),
             gettext_noop("number of partition in-memory batch, default values is "
                          "256.")},
            &u_sess->attr.attr_storage.partition_mem_batch,
            256,
            1,
            65535,
            NULL,
            NULL,
            NULL},
        {{"partition_max_cache_size",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("The max partition cache size for cstore when do insert"),
             gettext_noop("the max partition cache size for cstore when do insert, default values is "
                          "2097152KB."),
             GUC_UNIT_KB},
            &u_sess->attr.attr_storage.partition_max_cache_size,
            2097152,
            4096,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},

        {{"walsender_max_send_size",
             PGC_POSTMASTER,
             RESOURCES_MEM,
             gettext_noop("Size of walsender max send size."),
             gettext_noop("size of walsender max send size, default values is "
                          "8M."),
             GUC_UNIT_KB},
            &g_instance.attr.attr_storage.MaxSendSize,
            8192,
            8,
            MAX_KILOBYTES,
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

        {{"temp_file_limit",
             PGC_SUSET,
             RESOURCES_DISK,
             gettext_noop("Limits the total size of all temporary files used by each session."),
             gettext_noop("-1 means no limit."),
             GUC_UNIT_KB},
            &u_sess->attr.attr_sql.temp_file_limit,
            -1,
            -1,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"sql_use_spacelimit",
             PGC_USERSET,
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

        {{"vacuum_cost_page_hit",
             PGC_USERSET,
             RESOURCES_VACUUM_DELAY,
             gettext_noop("Vacuum cost for a page found in the buffer cache."),
             NULL},
            &u_sess->attr.attr_storage.VacuumCostPageHit,
            1,
            0,
            10000,
            NULL,
            NULL,
            NULL},

        {{"vacuum_cost_page_miss",
             PGC_USERSET,
             RESOURCES_VACUUM_DELAY,
             gettext_noop("Vacuum cost for a page not found in the buffer cache."),
             NULL},
            &u_sess->attr.attr_storage.VacuumCostPageMiss,
            10,
            0,
            10000,
            NULL,
            NULL,
            NULL},

        {{"vacuum_cost_page_dirty",
             PGC_USERSET,
             RESOURCES_VACUUM_DELAY,
             gettext_noop("Vacuum cost for a page dirtied by vacuum."),
             NULL},
            &u_sess->attr.attr_storage.VacuumCostPageDirty,
            20,
            0,
            10000,
            NULL,
            NULL,
            NULL},

        {{"vacuum_cost_limit",
             PGC_USERSET,
             RESOURCES_VACUUM_DELAY,
             gettext_noop("Vacuum cost amount available before napping."),
             NULL},
            &u_sess->attr.attr_storage.VacuumCostLimit,
            200,
            1,
            10000,
            NULL,
            NULL,
            NULL},

        {{"vacuum_cost_delay",
             PGC_USERSET,
             RESOURCES_VACUUM_DELAY,
             gettext_noop("Vacuum cost delay in milliseconds."),
             NULL,
             GUC_UNIT_MS},
            &u_sess->attr.attr_storage.VacuumCostDelay,
            0,
            0,
            100,
            NULL,
            NULL,
            NULL},

        {{"autovacuum_vacuum_cost_delay",
             PGC_SIGHUP,
             AUTOVACUUM,
             gettext_noop("Vacuum cost delay in milliseconds, for autovacuum."),
             NULL,
             GUC_UNIT_MS},
            &u_sess->attr.attr_storage.autovacuum_vac_cost_delay,
            20,
            -1,
            100,
            NULL,
            NULL,
            NULL},

        {{"autovacuum_vacuum_cost_limit",
             PGC_SIGHUP,
             AUTOVACUUM,
             gettext_noop("Vacuum cost amount available before napping, for autovacuum."),
             NULL},
            &u_sess->attr.attr_storage.autovacuum_vac_cost_limit,
            -1,
            -1,
            10000,
            NULL,
            NULL,
            NULL},
        {{"max_files_per_process",
             PGC_POSTMASTER,
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

        /*
         * See also CheckRequiredParameterValues() if this parameter changes
         */
        {{"max_prepared_transactions",
             PGC_POSTMASTER,
             RESOURCES_MEM,
             gettext_noop("Sets the maximum number of simultaneously prepared transactions."),
             NULL},
            &g_instance.attr.attr_storage.max_prepared_xacts,
#ifdef PGXC
            10,
            0,
            INT_MAX / 4,
            NULL,
            NULL,
            NULL
#else
            0,
            0,
            MAX_BACKENDS,
            NULL,
            NULL,
            NULL
#endif
        },
#ifdef LOCK_DEBUG
        {{"trace_lock_oidmin",
             PGC_SUSET,
             DEVELOPER_OPTIONS,
             gettext_noop("No description available."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_storage.Trace_lock_oidmin,
            FirstNormalObjectId,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"trace_lock_table",
             PGC_SUSET,
             DEVELOPER_OPTIONS,
             gettext_noop("No description available."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_storage.Trace_lock_table,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
#endif

        {{"max_query_retry_times",
             PGC_USERSET,
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

        {{"RepOriginId", PGC_USERSET, REPLICATION_STANDBY, gettext_noop("RepOriginId."), NULL, GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_storage.replorigin_sesssion_origin,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"statement_timeout",
             PGC_USERSET,
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

        /*
         * See also CheckRequiredParameterValues() if this parameter changes
         */
        {{"max_locks_per_transaction",
             PGC_POSTMASTER,
             LOCK_MANAGEMENT,
             gettext_noop("Sets the maximum number of locks per transaction."),
             gettext_noop("The shared lock table is sized on the assumption that "
                          "at most max_locks_per_transaction * max_connections distinct "
                          "objects will need to be locked at any one time.")},
            &g_instance.attr.attr_storage.max_locks_per_xact,
            256,
            10,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"max_pred_locks_per_transaction",
             PGC_POSTMASTER,
             LOCK_MANAGEMENT,
             gettext_noop("Sets the maximum number of predicate locks per transaction."),
             gettext_noop("The shared predicate lock table is sized on the assumption that "
                          "at most max_pred_locks_per_transaction * max_connections distinct "
                          "objects will need to be locked at any one time.")},
            &g_instance.attr.attr_storage.max_predicate_locks_per_xact,
            64,
            10,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"authentication_timeout",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("Sets the maximum allowed time to complete client authentication."),
             NULL,
             GUC_UNIT_S},
            &u_sess->attr.attr_security.AuthenticationTimeout,
            60,
            1,
            600,
            NULL,
            NULL,
            NULL},

        {/* Not for general use */
            {"pre_auth_delay",
                PGC_SIGHUP,
                DEVELOPER_OPTIONS,
                gettext_noop("Waits N seconds on connection startup before authentication."),
                gettext_noop("This allows attaching a debugger to the process."),
                GUC_NOT_IN_SAMPLE | GUC_UNIT_S},
            &u_sess->attr.attr_security.PreAuthDelay,
            0,
            0,
            60,
            NULL,
            NULL,
            NULL},

        {{"wal_keep_segments",
             PGC_SIGHUP,
             REPLICATION_SENDING,
             gettext_noop("Sets the number of WAL files held for standby servers."),
             NULL},
            &u_sess->attr.attr_storage.wal_keep_segments,
            16,
            2,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"checkpoint_segments",
             PGC_SIGHUP,
             WAL_CHECKPOINTS,
             gettext_noop("Sets the maximum distance in log segments between automatic WAL checkpoints."),
             NULL},
            &u_sess->attr.attr_storage.CheckPointSegments,
            64,
            1,
            INT_MAX - 1,
            NULL,
            NULL,
            NULL},

        {{"checkpoint_timeout",
             PGC_SIGHUP,
             WAL_CHECKPOINTS,
             gettext_noop("Sets the maximum time between automatic WAL checkpoints."),
             NULL,
             GUC_UNIT_S},
            &u_sess->attr.attr_storage.fullCheckPointTimeout,
            900,
            30,
            3600,
            NULL,
            NULL,
            NULL},

        {{"checkpoint_warning",
             PGC_SIGHUP,
             WAL_CHECKPOINTS,
             gettext_noop("Enables warnings if checkpoint segments are filled more "
                          "frequently than this."),
             gettext_noop("Writes a message to the server log if checkpoints "
                          "caused by the filling of checkpoint segment files happens more "
                          "frequently than this number of seconds. Zero turns off the warning."),
             GUC_UNIT_S},
            &u_sess->attr.attr_storage.CheckPointWarning,
            300,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"checkpoint_flush_after",
             PGC_SIGHUP,
             WAL_CHECKPOINTS,
             gettext_noop("Number of pages after which previously performed writes are flushed to disk."),
             NULL,
             GUC_UNIT_BLOCKS},
            &u_sess->attr.attr_storage.checkpoint_flush_after,
            DEFAULT_CHECKPOINT_FLUSH_AFTER,
            0,
            WRITEBACK_MAX_PENDING_FLUSHES,
            NULL,
            NULL,
            NULL},

        {{"xloginsert_locks",
             PGC_POSTMASTER,
             WAL_SETTINGS,
             gettext_noop("Sets the number of locks used for concurrent xlog insertions."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_storage.num_xloginsert_locks,
            8,
            1,
            1000,
            NULL,
            NULL,
            NULL},

        {{"wal_writer_cpu",
             PGC_POSTMASTER,
             WAL_SETTINGS,
             gettext_noop("Sets the binding CPU number for the WAL writer thread."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_storage.wal_writer_cpu,
            -1,
            -1,
            1023,
            NULL,
            NULL,
            NULL
        },

        {{"wal_file_init_num",
             PGC_POSTMASTER,
             WAL_SETTINGS,
             gettext_noop("Sets the number of xlog segment files that WAL writer auxiliary thread "
                          "creates at one time."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_storage.wal_file_init_num,
            10,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL
        },

        {{"advance_xlog_file_num",
             PGC_POSTMASTER,
             WAL_SETTINGS,
             gettext_noop("Sets the number of xlog files to be initialized in advance."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_storage.advance_xlog_file_num,
            0,
            0,
            100,
            NULL,
            NULL,
            NULL},

        {{"checkpoint_wait_timeout",
             PGC_SIGHUP,
             WAL_CHECKPOINTS,
             gettext_noop("Sets the maximum wait timeout for checkpointer to start."),
             NULL,
             GUC_UNIT_S},
            &u_sess->attr.attr_storage.CheckPointWaitTimeOut,
            60,
            2,
            3600,
            NULL,
            NULL,
            NULL},

        {{"wal_buffers",
             PGC_POSTMASTER,
             WAL_SETTINGS,
             gettext_noop("Sets the number of disk-page buffers in shared memory for WAL."),
             NULL,
             GUC_UNIT_XBLOCKS},
            &g_instance.attr.attr_storage.XLOGbuffers,
            2048,
            -1,
            (INT_MAX / XLOG_BLCKSZ)+1,
            check_wal_buffers,
            NULL,
            NULL},

        {{"wal_writer_delay",
             PGC_SIGHUP,
             WAL_SETTINGS,
             gettext_noop("WAL writer sleep time between WAL flushes."),
             NULL,
             GUC_UNIT_MS},
            &u_sess->attr.attr_storage.WalWriterDelay,
            200,
            1,
            10000,
            NULL,
            NULL,
            NULL},

        {/* see max_connections */
            {"max_wal_senders",
                PGC_POSTMASTER,
                REPLICATION_SENDING,
                gettext_noop("Sets the maximum number of simultaneously running WAL sender processes."),
                NULL},
            &g_instance.attr.attr_storage.max_wal_senders,
#ifdef ENABLE_MULTIPLE_NODES
            4,
#else
            16,
#endif
            0,
            MAX_BACKENDS,
            NULL,
            NULL,
            NULL},
        {/* see max_connections */
            {"max_replication_slots",
                PGC_POSTMASTER,
                REPLICATION_SENDING,
                gettext_noop("Sets the maximum number of simultaneously defined replication slots."),
                NULL},
            &g_instance.attr.attr_storage.max_replication_slots,
            8,
            0,
            MAX_BACKENDS /* XXX?*/,
            NULL,
            NULL,
            NULL},

        {{"recovery_time_target",
             PGC_SIGHUP,
             REPLICATION_SENDING,
             gettext_noop("The target redo time in seconds for recovery"),
             NULL},
            &u_sess->attr.attr_storage.target_rto,
            0,
            0,
            3600,
            NULL,
            NULL,
            NULL},
        {{"time_to_target_rpo",
             PGC_SIGHUP,
             REPLICATION_SENDING,
             gettext_noop("The time to the target recovery point in seconds"),
             NULL},
            &u_sess->attr.attr_storage.time_to_target_rpo,
            0,
            0,
            3600,
            NULL,
            NULL,
            NULL},

        {{"wal_sender_timeout",
             PGC_SIGHUP,
             REPLICATION_SENDING,
             gettext_noop("Sets the maximum time to wait for WAL replication."),
             NULL,
             GUC_UNIT_MS},
            &u_sess->attr.attr_storage.wal_sender_timeout,
            6 * 1000,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"replication_type", PGC_POSTMASTER, WAL_SETTINGS, gettext_noop("Sets the dn's HA mode."), NULL},
            &g_instance.attr.attr_storage.replication_type,
#ifdef ENABLE_MULTIPLE_NODES
            RT_WITH_DUMMY_STANDBY,
#else
            RT_WITH_MULTI_STANDBY,
#endif
            RT_WITH_DUMMY_STANDBY,
            RT_NUM,
            check_replication_type,
            NULL,
            NULL},

        {{"commit_delay",
             PGC_USERSET,
             WAL_SETTINGS,
             gettext_noop("Sets the delay in microseconds between transaction commit and "
                          "flushing WAL to disk."),
             NULL},
            &u_sess->attr.attr_storage.CommitDelay,
            0,
            0,
            100000,
            NULL,
            NULL,
            NULL},

        {{"partition_lock_upgrade_timeout",
             PGC_USERSET,
             LOCK_MANAGEMENT,
             gettext_noop("Sets the timeout for partition lock upgrade, in seconds"),
             NULL},
            &u_sess->attr.attr_storage.partition_lock_upgrade_timeout,
            1800,
            -1,
            3000,
            NULL,
            NULL,
            NULL},

        {{"track_thread_wait_status_interval",
             PGC_SUSET,
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
             INSTRUMENTS_OPTIONS,
             gettext_noop("Sets the active session profile max sample nums in buff"),
             NULL,
             GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_common.asp_sample_num,
            100000,
            10000,
            100000,
            NULL,
            NULL,
            NULL},

         {{"asp_sample_interval",
             PGC_SIGHUP,
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

        {{"commit_siblings",
             PGC_USERSET,
             WAL_SETTINGS,
             gettext_noop("Sets the minimum concurrent open transactions before performing "
                          "commit_delay."),
             NULL},
            &u_sess->attr.attr_storage.CommitSiblings,
            5,
            0,
            1000,
            NULL,
            NULL,
            NULL},

        {{"extra_float_digits",
             PGC_USERSET,
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

        {{"log_min_duration_statement",
             PGC_SUSET,
             LOGGING_WHEN,
             gettext_noop("Sets the minimum execution time above which "
                          "statements will be logged."),
             gettext_noop("Zero prints all queries. -1 turns this feature off."),
             GUC_UNIT_MS},
            &u_sess->attr.attr_storage.log_min_duration_statement,
            -1,
            -1,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"log_autovacuum_min_duration",
             PGC_SIGHUP,
             LOGGING_WHAT,
             gettext_noop("Sets the minimum execution time above which "
                          "autovacuum actions will be logged."),
             gettext_noop("Zero prints all actions. -1 turns autovacuum logging off."),
             GUC_UNIT_MS},
            &u_sess->attr.attr_storage.Log_autovacuum_min_duration,
            -1,
            -1,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"bgwriter_delay",
             PGC_SIGHUP,
             RESOURCES_BGWRITER,
             gettext_noop("Background writer sleep time between rounds."),
             NULL,
             GUC_UNIT_MS},
            &u_sess->attr.attr_storage.BgWriterDelay,
            2000,
            10,
            10000,
            NULL,
            NULL,
            NULL},

        {{"bgwriter_lru_maxpages",
             PGC_SIGHUP,
             RESOURCES_BGWRITER,
             gettext_noop("Background writer maximum number of LRU pages to flush per round."),
             NULL},
            &u_sess->attr.attr_storage.bgwriter_lru_maxpages,
            100,
            0,
            1000,
            NULL,
            NULL,
            NULL},

        {{"bgwriter_flush_after",
             PGC_SIGHUP,
             RESOURCES_BGWRITER,
             gettext_noop("Number of pages after which previously performed writes are flushed to disk."),
             NULL,
             GUC_UNIT_BLOCKS},
            &u_sess->attr.attr_storage.bgwriter_flush_after,
            DEFAULT_BGWRITER_FLUSH_AFTER,
            0,
            WRITEBACK_MAX_PENDING_FLUSHES,
            NULL,
            NULL,
            NULL},

        {{"effective_io_concurrency",
#ifdef USE_PREFETCH
             PGC_USERSET,
#else
             PGC_INTERNAL,
#endif
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

        {{"max_index_keys",
             PGC_INTERNAL,
             PRESET_OPTIONS,
             gettext_noop("Shows the maximum number of index keys."),
             NULL,
             GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_storage.max_index_keys,
            INDEX_MAX_KEYS,
            INDEX_MAX_KEYS,
            INDEX_MAX_KEYS,
            NULL,
            NULL,
            NULL},

        {{"max_identifier_length",
             PGC_INTERNAL,
             PRESET_OPTIONS,
             gettext_noop("Shows the maximum identifier length."),
             NULL,
             GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_storage.max_identifier_length,
            NAMEDATALEN - 1,
            NAMEDATALEN - 1,
            NAMEDATALEN - 1,
            NULL,
            NULL,
            NULL},
        {{"max_user_defined_exception",
             PGC_USERSET,
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
        {{"block_size",
             PGC_INTERNAL,
             PRESET_OPTIONS,
             gettext_noop("Shows the size of a disk block."),
             NULL,
             GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_storage.block_size,
            BLCKSZ,
            BLCKSZ,
            BLCKSZ,
            NULL,
            NULL,
            NULL},

        {{"segment_size",
             PGC_INTERNAL,
             PRESET_OPTIONS,
             gettext_noop("Shows the number of pages per disk file."),
             NULL,
             GUC_UNIT_BLOCKS | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_storage.segment_size,
            RELSEG_SIZE,
            RELSEG_SIZE,
            RELSEG_SIZE,
            NULL,
            NULL,
            NULL},

        {{"wal_block_size",
             PGC_INTERNAL,
             PRESET_OPTIONS,
             gettext_noop("Shows the block size in the write ahead log."),
             NULL,
             GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_storage.wal_block_size,
            XLOG_BLCKSZ,
            XLOG_BLCKSZ,
            XLOG_BLCKSZ,
            NULL,
            NULL,
            NULL},

        {{"wal_segment_size",
             PGC_INTERNAL,
             PRESET_OPTIONS,
             gettext_noop("Shows the number of pages per write ahead log segment."),
             NULL,
             GUC_UNIT_XBLOCKS | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_storage.wal_segment_size,
            (XLOG_SEG_SIZE / XLOG_BLCKSZ),
            (XLOG_SEG_SIZE / XLOG_BLCKSZ),
            (XLOG_SEG_SIZE / XLOG_BLCKSZ),
            NULL,
            NULL,
            NULL},

        {{"autovacuum_naptime",
             PGC_SIGHUP,
             AUTOVACUUM,
             gettext_noop("Time to sleep between autovacuum runs."),
             NULL,
             GUC_UNIT_S},
            &u_sess->attr.attr_storage.autovacuum_naptime,
            600,
            1,
            INT_MAX / 1000,
            NULL,
            NULL,
            NULL},
        {{"autoanalyze_timeout",
             PGC_SIGHUP,
             AUTOVACUUM,
             gettext_noop("Sets the timeout for auto-analyze action."),
             NULL,
             GUC_UNIT_S},
            &u_sess->attr.attr_storage.autoanalyze_timeout,
            300,
            0,
            INT_MAX / 1000,
            NULL,
            NULL,
            NULL},
        {{"autovacuum_vacuum_threshold",
             PGC_SIGHUP,
             AUTOVACUUM,
             gettext_noop("Minimum number of tuple updates or deletes prior to vacuum."),
             NULL},
            &u_sess->attr.attr_storage.autovacuum_vac_thresh,
            50,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"autovacuum_analyze_threshold",
             PGC_SIGHUP,
             AUTOVACUUM,
             gettext_noop("Minimum number of tuple inserts, updates, or deletes prior to analyze."),
             NULL},
            &u_sess->attr.attr_storage.autovacuum_anl_thresh,
            50,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {/* see max_connections */
            {"autovacuum_max_workers",
                PGC_POSTMASTER,
                AUTOVACUUM,
                gettext_noop("Sets the maximum number of simultaneously running autovacuum worker processes."),
                NULL},
            &g_instance.attr.attr_storage.autovacuum_max_workers,
            3,
            0,
            MAX_BACKENDS,
            check_autovacuum_max_workers,
            NULL,
            NULL},
        {{"job_queue_processes",
             PGC_POSTMASTER,
             JOB,
             gettext_noop("Number of concurrent jobs, optional: [1...1000], default: 10."),
             NULL},
            &g_instance.attr.attr_sql.job_queue_processes,
            DEFAULT_JOB_QUEUE_PROCESSES,
            MIN_JOB_QUEUE_PROCESSES,
            MAX_JOB_QUEUE_PROCESSES,
            check_job_max_workers,
            NULL,
            NULL},
        {{"tcp_keepalives_idle",
             PGC_USERSET,
             CLIENT_CONN_OTHER,
             gettext_noop("Time between issuing TCP keepalives."),
             gettext_noop("A value of 0 uses the system default."),
             GUC_UNIT_S},
            &u_sess->attr.attr_common.tcp_keepalives_idle,
            60,
            0,
            3600,
            NULL,
            NULL,
            NULL},

        {{"tcp_keepalives_interval",
             PGC_USERSET,
             CLIENT_CONN_OTHER,
             gettext_noop("Time between TCP keepalive retransmits."),
             gettext_noop("A value of 0 uses the system default."),
             GUC_UNIT_S},
            &u_sess->attr.attr_common.tcp_keepalives_interval,
            30,
            0,
            180,
            NULL,
            NULL,
            NULL},

        {{"ssl_renegotiation_limit",
             PGC_USERSET,
             CONN_AUTH_SECURITY,
             gettext_noop("SSL renegotiation is no longer supported, no matter what value is set."),
             NULL,
             GUC_UNIT_KB,
         },
            &u_sess->attr.attr_security.ssl_renegotiation_limit,
            0,
            0,
            MAX_KILOBYTES,
            NULL,
            NULL,
            NULL},
        {{"ssl_cert_notify_time",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("Alarm days before ssl cert expires."),
         },
            &u_sess->attr.attr_security.ssl_cert_notify_time,
            90,
            7,
            180,
            check_int_parameter,
            NULL,
            NULL},

        {{
             "tcp_keepalives_count",
             PGC_USERSET,
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
            NULL,
            NULL},

        {{
             "password_policy",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("The password complexity-policy of the database system."),
             gettext_noop("This controls the complexity-policy of database system. "
                          "A value of 1 uses the system default."),
         },
            &u_sess->attr.attr_security.Password_policy,
            1,
            0,
            1,
            check_int_parameter,
            NULL,
            NULL},

        {{
             "password_encryption_type",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("The encryption method of password."),
             gettext_noop("This controls the encryption type of the password. "
                          "A value of 2 uses the system default."),
         },
            &u_sess->attr.attr_security.Password_encryption_type,
            2,
            0,
            2,
            check_int_parameter,
            NULL,
            NULL},

        {{"gin_fuzzy_search_limit",
             PGC_USERSET,
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

        {{
             "effective_cache_size",
             PGC_USERSET,
             QUERY_TUNING_COST,
             gettext_noop("Sets the planner's assumption about the size of the disk cache."),
             gettext_noop("That is, the portion of the kernel's disk cache that "
                          "will be used for PostgreSQL data files. This is measured in disk "
                          "pages, which are normally 8 kB each."),
             GUC_UNIT_BLOCKS,
         },
            &u_sess->attr.attr_sql.effective_cache_size,
            DEFAULT_EFFECTIVE_CACHE_SIZE,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {/* Can't be set in postgresql.conf */
            {"server_version_num",
                PGC_INTERNAL,
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

        {{"log_temp_files",
             PGC_SUSET,
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

        {{
             "track_activity_query_size",
             PGC_POSTMASTER,
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

        {{"password_reuse_max",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("max times password can reuse."),
             NULL,
             GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_security.Password_reuse_max,
            0,
            0,
            MAX_PASSWORD_REUSE_MAX,
            check_int_parameter,
            NULL,
            NULL},

        {{"failed_login_attempts",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("max number of login attempts."),
             NULL,
             GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_security.Failed_login_attempts,
            10,
            0,
            MAX_FAILED_LOGIN_ATTAMPTS,
            check_int_parameter,
            NULL,
            NULL},

        {{"password_min_uppercase",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("min number of upper character in password."),
             NULL,
             GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_security.Password_min_upper,
            0,
            0,
            MAX_PASSWORD_ASSIGNED_CHARACTER,
            check_int_parameter,
            NULL,
            NULL},

        {{"password_min_lowercase",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("min number of lower character in password."),
             NULL,
             GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_security.Password_min_lower,
            0,
            0,
            MAX_PASSWORD_ASSIGNED_CHARACTER,
            check_int_parameter,
            NULL,
            NULL},

        {{"password_min_digital",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("min number of digital character in password."),
             NULL,
             GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_security.Password_min_digital,
            0,
            0,
            MAX_PASSWORD_ASSIGNED_CHARACTER,
            check_int_parameter,
            NULL,
            NULL},

        {{"password_min_special",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("min number of special character in password."),
             NULL,
             GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_security.Password_min_special,
            0,
            0,
            MAX_PASSWORD_ASSIGNED_CHARACTER,
            check_int_parameter,
            NULL,
            NULL},

        {{"password_min_length",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("min length of password."),
             NULL,
             GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_security.Password_min_length,
            8,
            6,
            MAX_PASSWORD_LENGTH,
            check_int_parameter,
            NULL,
            NULL},

        {{"password_max_length",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("max length of password."),
             NULL,
             GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_security.Password_max_length,
            32,
            6,
            MAX_PASSWORD_LENGTH,
            check_int_parameter,
            NULL,
            NULL},

        {{"password_notify_time", PGC_SIGHUP, CONN_AUTH_SECURITY, gettext_noop("password deadline notice time."), NULL},
            &u_sess->attr.attr_security.Password_notify_time,
            7,
            0,
            MAX_PASSWORD_NOTICE_TIME,
            check_int_parameter,
            NULL,
            NULL},

        /* Database Security: Support database audit */
        /* add guc option about audit */
        {{"audit_rotation_interval",
             PGC_SIGHUP,
             AUDIT_OPTIONS,
             gettext_noop("Automatic audit file rotation will occur after N minutes."),
             NULL,
             GUC_UNIT_MIN},
            &u_sess->attr.attr_security.Audit_RotationAge,
            HOURS_PER_DAY * MINS_PER_HOUR,
            1,
            INT_MAX / MINS_PER_HOUR,
            NULL,
            NULL,
            NULL},

        {{"audit_rotation_size",
             PGC_SIGHUP,
             AUDIT_OPTIONS,
             gettext_noop("Automatic audit file rotation will occur after N kilobytes."),
             NULL,
             GUC_UNIT_KB},
            &u_sess->attr.attr_security.Audit_RotationSize,
            10 * 1024,
            1024,
            1024 * 1024,
            NULL,
            NULL,
            NULL},

        {{"audit_space_limit",
             PGC_SIGHUP,
             AUDIT_OPTIONS,
             gettext_noop("audit data space limit in MB unit"),
             NULL,
             GUC_UNIT_KB},
            &u_sess->attr.attr_security.Audit_SpaceLimit,
            1024 * 1024,
            1024,
            1024 * 1024 * 1024,
            NULL,
            NULL,
            NULL},

        {{"audit_file_remain_time",
             PGC_SIGHUP,
             AUDIT_OPTIONS,
             gettext_noop("the days of the audit files can be remained"),
             NULL,
             0},
            &u_sess->attr.attr_security.Audit_RemainAge,
            90,
            0,
            730,
            NULL,
            NULL,
            NULL},

        /*
         * Audit_file_remain_threshold is a parameter which is no need and shouldn't be changed for these reason:
         * 1. If customer want to control audit log space, use audit_space_limit is enough.
         * 2. Two limit with audit_space_limit and audit_file_remain_time will serious conflict with this parameter
         *    and always set audit_file_remain_threshold will cause these parameters very difficult to use.
         */
        {{"audit_file_remain_threshold",
             PGC_SIGHUP,
             AUDIT_OPTIONS,
             gettext_noop("audit file remain threshold."),
             NULL,
             0},
            &u_sess->attr.attr_security.Audit_RemainThreshold,
            1024 * 1024,
            1,
            1024 * 1024,
            NULL,
            NULL,
            NULL},

        {{"audit_login_logout", PGC_SIGHUP, AUDIT_OPTIONS, gettext_noop("audit user login logout."), NULL, 0},
            &u_sess->attr.attr_security.Audit_Session,
            7,
            0,
            7,
            NULL,
            NULL,
            NULL},

        {{"audit_database_process",
             PGC_SIGHUP,
             AUDIT_OPTIONS,
             gettext_noop("audit database start, stop, recover and switchover."),
             NULL,
             0},
            &u_sess->attr.attr_security.Audit_ServerAction,
            1,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_user_locked", PGC_SIGHUP, AUDIT_OPTIONS, gettext_noop("audit lock and unlock user."), NULL, 0},
            &u_sess->attr.attr_security.Audit_LockUser,
            1,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_user_violation", PGC_SIGHUP, AUDIT_OPTIONS, gettext_noop("audit user violation."), NULL, 0},
            &u_sess->attr.attr_security.Audit_UserViolation,
            0,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_grant_revoke", PGC_SIGHUP, AUDIT_OPTIONS, gettext_noop("audit grant and revoke privilege."), NULL, 0},
            &u_sess->attr.attr_security.Audit_PrivilegeAdmin,
            1,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_system_object",
             PGC_SIGHUP,
             AUDIT_OPTIONS,
             gettext_noop("audit DDL operation on system object."),
             NULL,
             0},
            &u_sess->attr.attr_security.Audit_DDL,
            12295,
            0,
            2097151,
            NULL,
            NULL,
            NULL},

        {{"audit_dml_state", PGC_SIGHUP, AUDIT_OPTIONS, gettext_noop("audit DML operation."), NULL, 0},
            &u_sess->attr.attr_security.Audit_DML,
            0,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_dml_state_select", PGC_SIGHUP, AUDIT_OPTIONS, gettext_noop("audit DML select operation."), NULL, 0},
            &u_sess->attr.attr_security.Audit_DML_SELECT,
            0,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_function_exec", PGC_SIGHUP, AUDIT_OPTIONS, gettext_noop("audit function execution."), NULL, 0},
            &u_sess->attr.attr_security.Audit_Exec,
            0,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_copy_exec", PGC_SIGHUP, AUDIT_OPTIONS, gettext_noop("audit copy execution."), NULL, 0},
            &u_sess->attr.attr_security.Audit_Copy,
            1,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_set_parameter", PGC_SIGHUP, AUDIT_OPTIONS, gettext_noop("audit set operation."), NULL},
            &u_sess->attr.attr_security.Audit_Set,
            1,
            0,
            1,
            NULL,
            NULL,
            NULL},
#ifdef PGXC
        {{"pooler_maximum_idle_time",
            PGC_USERSET,
            DATA_NODES,
            gettext_noop("Maximum idle time of the pooler links."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_network.PoolerMaxIdleTime,
            600,
            0,
            INT_MAX,
            check_pooler_maximum_idle_time,
            NULL,
            NULL},

        {{"minimum_pool_size",
            PGC_USERSET,
            DATA_NODES,
            gettext_noop("Initial pool size."),
            gettext_noop("If number of active connections decreased below this value, "
                "new connections are established")},
            &u_sess->attr.attr_network.MinPoolSize,
            50,
            1,
            65535,
            NULL,
            NULL,
            NULL},

        /* This is PGC_SIGHUP to set the interval time to push cut_off_csn_num. 0 means never do cleanup */
        {{"defer_csn_cleanup_time",
             PGC_SIGHUP, DATA_NODES,
             gettext_noop("Sets the interval time to push cut off csn num."),
             NULL,
             GUC_UNIT_MS},
            &u_sess->attr.attr_storage.defer_csn_cleanup_time,
            5000,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
#endif
        // user defind max_compile_functions
        {{"max_compile_functions",
             PGC_POSTMASTER,
             RESOURCES_MEM,
             gettext_noop("max compile results in postmaster"),
             gettext_noop("max compile results in postmaster, default values is "
                          "1000, min is 1"),
             0},
            &g_instance.attr.attr_sql.max_compile_functions,
            1000,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        // Stream communication
        //
        {{"comm_sctp_port",
             PGC_POSTMASTER,
             CONN_AUTH_SETTINGS,
             gettext_noop("Sets the STCP port the server listens on."),
             NULL},
            &g_instance.attr.attr_network.comm_sctp_port,
            7000,
            0,
            65535,
            NULL,
            NULL,
            NULL},

        {{"comm_control_port",
             PGC_POSTMASTER,
             CONN_AUTH_SETTINGS,
             gettext_noop("Sets the stream control port the server listens on."),
             NULL},
            &g_instance.attr.attr_network.comm_control_port,
            7001,
            0,
            65535,
            NULL,
            NULL,
            NULL},

        {{"comm_quota_size",
             PGC_POSTMASTER,
             CONN_AUTH_SETTINGS,
             gettext_noop("Sets the stream quota size in kB."),
             NULL,
             GUC_UNIT_KB},
            &g_instance.attr.attr_network.comm_quota_size,
            1024,
            0,
            2048000,
            NULL,
            NULL,
            NULL},

        {{"comm_usable_memory",
             PGC_POSTMASTER,
             CONN_AUTH_SETTINGS,
             gettext_noop("Sets the total usable memory for communication(in kB)."),
             NULL,
             GUC_UNIT_KB},
            &g_instance.attr.attr_network.comm_usable_memory,
            4000 * 1024,
            100 * 1024,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},

        {{"comm_memory_pool",
             PGC_POSTMASTER,
             CONN_AUTH_SETTINGS,
             gettext_noop("Sets the memory pool size for communication(in kB)."),
             NULL,
             GUC_UNIT_KB},
            &g_instance.attr.attr_network.comm_memory_pool,
            2000 * 1024,
            100 * 1024,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},

        {{"comm_memory_pool_percent",
             PGC_POSTMASTER,
             CONN_AUTH_SETTINGS,
             gettext_noop("Sets the percent of comm_memory_pool for dynamic workload."),
             NULL},
            &g_instance.attr.attr_network.comm_memory_pool_percent,
            0,
            0,
            100,
            NULL,
            NULL,
            NULL},

        {{"comm_ackchk_time",
             PGC_USERSET,
             QUERY_TUNING,
             gettext_noop("Send ack check package to stream sender periodically."),
             NULL,
             GUC_UNIT_MS},
            &u_sess->attr.attr_network.comm_ackchk_time,
            2000, 
            0,    
            20000,
            NULL, 
            assign_comm_ackchk_time,
            NULL},

#ifdef LIBCOMM_SPEED_TEST_ENABLE
        {{"comm_test_thread_num",
             PGC_USERSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Libcomm performance testing framework thread number of streams."),
             NULL},
            &u_sess->attr.attr_network.comm_test_thread_num,
            0,
            0,
            65535,
            NULL,
            assign_comm_test_thread_num,
            NULL},
        {{"comm_test_msg_len",
             PGC_USERSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Libcomm performance testing framework message len."),
             NULL},
            &u_sess->attr.attr_network.comm_test_msg_len,
            8192,
            1,
            65535,
            NULL,
            assign_comm_test_msg_len,
            NULL},
        {{"comm_test_send_sleep",
             PGC_USERSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Libcomm performance testing framework producer thread sleep time."),
             NULL},
            &u_sess->attr.attr_network.comm_test_send_sleep,
            0,
            0,
            INT_MAX,
            NULL,
            assign_comm_test_send_sleep,
            NULL},
        {{"comm_test_send_once",
             PGC_USERSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Libcomm performance testing framework producer thread send message size once time."),
             NULL},
            &u_sess->attr.attr_network.comm_test_send_once,
            8192,
            1,
            INT_MAX,
            NULL,
            assign_comm_test_send_once,
            NULL},
        {{"comm_test_recv_sleep",
             PGC_USERSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Libcomm performance testing framework consumer thread sleep time."),
             NULL},
            &u_sess->attr.attr_network.comm_test_recv_sleep,
            0,
            0,
            INT_MAX,
            NULL,
            assign_comm_test_recv_sleep,
            NULL},
        {{"comm_test_rcv_once",
             PGC_USERSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Libcomm performance testing framework consumer thread recv message size once time."),
             NULL},
            &u_sess->attr.attr_network.comm_test_recv_once,
            8192,
            1,
            INT_MAX,
            NULL,
            assign_comm_test_recv_once,
            NULL},
#endif

#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
        {{"comm_fault_injection",
             PGC_USERSET,
             DEVELOPER_OPTIONS,
             gettext_noop("Libcomm fault injection framework."),
             NULL},
            &u_sess->attr.attr_network.comm_fault_injection,
            0,
            -10,
            INT_MAX,
            NULL,
            assign_comm_fault_injection,
            NULL},
#endif

        {{"query_dop", PGC_USERSET, QUERY_TUNING, gettext_noop("User-defined degree of parallelism."), NULL},
            &u_sess->attr.attr_sql.query_dop_tmp,
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
            1,
            MIN_QUERY_DOP,
            INT_MAX,
#else
            1,
#ifdef ENABLE_MULTIPLE_NODES
            MIN_QUERY_DOP,
#else
            1,
#endif
            MAX_QUERY_DOP,
#endif
            parse_query_dop,
            AssignQueryDop,
            NULL},
        {{"comm_max_receiver",
             PGC_POSTMASTER,
             CONN_AUTH_SETTINGS,
             gettext_noop("Maximum number of internal receiver threads."),
             NULL},
            &g_instance.attr.attr_network.comm_max_receiver,
            4,
            1,
            50,
            NULL,
            NULL,
            NULL},

        {{"plan_mode_seed",
             PGC_USERSET,
             QUERY_TUNING_METHOD,
             gettext_noop("Specify which plan mode and seed the optimizer generation used."),
             NULL},
            &u_sess->attr.attr_sql.plan_mode_seed,
            0,
            -1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"ngram_gram_size", PGC_USERSET, TEXT_SEARCH, gettext_noop("N-value for N-gram parser"), NULL},
            &u_sess->attr.attr_common.ngram_gram_size,
            2,
            1,
            4,
            NULL,
            NULL,
            NULL},

        {{"prefetch_quantity",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Sets the IO quantity of prefetch buffers used by async dirct IO interface."),
             NULL,
             GUC_UNIT_BLOCKS},
            &u_sess->attr.attr_storage.prefetch_quantity,
            4096,
            128,
            131072,
            NULL,
            NULL,
            NULL},

        {{"backwrite_quantity",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Sets the IO quantity of backwrite buffers used by async dirct IO interface."),
             NULL,
             GUC_UNIT_BLOCKS},
            &u_sess->attr.attr_storage.backwrite_quantity,
            1024,
            128,
            131072,
            NULL,
            NULL,
            NULL},

        {{"cstore_prefetch_quantity",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Sets the IO quantity of prefetch CUs used by async dirct IO interface for column store."),
             NULL,
             GUC_UNIT_KB},
            &u_sess->attr.attr_storage.cstore_prefetch_quantity,
            32768,
            1024,
            1048576,
            NULL,
            NULL,
            NULL},

        {{"cstore_backwrite_max_threshold",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Cu cache threshold for cstore when do insert by async dirct IO"),
             gettext_noop("Cu cache threshold for cstore when do insert by async dirct IO , default values is "
                          "2097152KB."),
             GUC_UNIT_KB},
            &u_sess->attr.attr_storage.cstore_backwrite_max_threshold,
            2097152,
            4096,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},
        {{"cstore_backwrite_quantity",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Each column write threshold for cstore when do insert by async dirct IO"),
             gettext_noop("Each column write threshold for cstore when do insert by async dirct IO , default values is "
                          "8192KB."),
             GUC_UNIT_KB},
            &u_sess->attr.attr_storage.cstore_backwrite_quantity,
            8192,
            1024,
            1048576,
            NULL,
            NULL,
            NULL},

        {{"fast_extend_file_size",
             PGC_SUSET,
             RESOURCES_MEM,
             gettext_noop("Set fast extend file size used by async dirct IO interface for row store."),
             NULL,
             GUC_UNIT_KB},
            &u_sess->attr.attr_storage.fast_extend_file_size,
            8192,
            1024,
            1048576,
            NULL,
            NULL,
            NULL},

        {{"gin_pending_list_limit",
             PGC_USERSET,
             CLIENT_CONN_STATEMENT,
             gettext_noop("Sets the maximum size of the pending list for GIN index."),
             NULL,
             GUC_UNIT_KB},
            &u_sess->attr.attr_storage.gin_pending_list_limit,
            4096,
            64,
            MAX_KILOBYTES,
            NULL,
            NULL,
            NULL},
        /*
         * set threshold to check if codegen is allowed or not, codegen_cost_threshold
         * represents the number of rows. This will be changed in future.
         */
        {{"codegen_cost_threshold",
             PGC_USERSET,
             QUERY_TUNING_COST,
             gettext_noop("Decided to use LLVM optimization or not."),
             NULL},
            &u_sess->attr.attr_sql.codegen_cost_threshold,
            10000,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"dfs_partition_directory_length",
             PGC_USERSET,
             UNGROUPED,
             gettext_noop("The max length of the value partition directory."),
             NULL},
            &u_sess->attr.attr_storage.dfs_max_parsig_length,
            512,
            92,
            7999, /* 7999 is the max value of dfs.namenode.fs-limits.max-component-length in HDFS of FI */
            NULL,
            NULL,
            NULL},

        /* profile log GUC */
        {{"plog_merge_age",
             PGC_USERSET,
             LOGGING_WHEN,
             gettext_noop("how long to aggregate profile logs."),
             gettext_noop("0 disable logging. suggest setting value is 1000 times."),
             GUC_UNIT_MS},
            &u_sess->attr.attr_storage.plog_merge_age,
            0,
            0,
            INT_MAX,
            NULL,
            plog_merge_age_assign,
            NULL},

        {{"auth_iteration_count",
             PGC_SIGHUP,
             CONN_AUTH_SECURITY,
             gettext_noop("The iteration count used in RFC5802 authenication."),
             NULL},
            &u_sess->attr.attr_security.auth_iteration_count,
            10000,
            2048,
            134217728, /* 134217728 is the max iteration count supported. */
            NULL,
            NULL,
            NULL},

        {{
             "fault_mon_timeout",
             PGC_SIGHUP,
             STATS_MONITORING,
             gettext_noop("how many miniutes to monitor lwlock. 0 will disable that"),
             NULL,
             GUC_UNIT_MIN /* minute */
         },
            &u_sess->attr.attr_common.fault_mon_timeout,
            5,
            0,
            (24 * 60), /* 1 min ~ 1 day, 0 will disable this thread starting */
            NULL,
            NULL,
            NULL},

        {{
             "max_resource_package",
             PGC_POSTMASTER,
             UNGROUPED,
             gettext_noop("The maximum number of the resource package(RP) for DN in the compute pool."),
             gettext_noop("0 means that the cluster is NOT as the compute pool."),
         },
            &g_instance.attr.attr_sql.max_resource_package,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"acce_min_datasize_per_thread",
             PGC_USERSET,
             UNGROUPED,
             gettext_noop("Used to estimate whether pushdown the plan to the compute pool."),
             gettext_noop("0 means that plan always runs in the compute pool."),
             GUC_UNIT_KB},
            &u_sess->attr.attr_sql.acce_min_datasize_per_thread,
            500000,
            0,
            MAX_KILOBYTES,
            NULL,
            NULL,
            NULL},
        {{"max_cn_temp_file_size",
             PGC_SIGHUP,
             CLIENT_CONN_STATEMENT,
             gettext_noop("Sets the maximum tempfile size used in CN, unit in MB."),
             NULL,
             GUC_UNIT_KB},
            &u_sess->attr.attr_sql.max_cn_temp_file_size,
            5 * 1024 * 1024,
            0,
            10 * 1024 * 1024,
            NULL,
            NULL,
            NULL},

        {{"cn_send_buffer_size",
             PGC_POSTMASTER,
             CLIENT_CONN_STATEMENT,
             gettext_noop("Sets the send buffer size used in CN, unit in KB."),
             NULL,
             GUC_UNIT_KB},
            &g_instance.attr.attr_network.cn_send_buffer_size,
            8,
            8,
            128,
            NULL,
            NULL,
            NULL},

        {{"max_changes_in_memory",
             PGC_POSTMASTER,
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

        {{"table_skewness_warning_rows",
             PGC_USERSET,
             STATS,
             gettext_noop("Sets the number of rows returned by DN to enable warning of table skewness."),
             NULL},
            &u_sess->attr.attr_sql.table_skewness_warning_rows,
            100000,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"recovery_max_workers",
             PGC_POSTMASTER,
             RESOURCES_RECOVERY,
             gettext_noop("The max number of recovery threads allowed to run in parallel."),
             gettext_noop("Zero lets the system determine this value.")},
            &g_instance.attr.attr_storage.max_recovery_parallelism,
            1,
            0,
            MOST_FAST_RECOVERY_LIMIT,
            NULL,
            NULL,
            NULL},

        {{"recovery_parallelism",
             PGC_POSTMASTER,
             RESOURCES_RECOVERY,
             gettext_noop("The actual number of recovery threads running in parallel."),
             NULL},
            &g_instance.attr.attr_storage.real_recovery_parallelism,
            1,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},

#ifdef ENABLE_QUNIT
        {{"qunit_case_number",
             PGC_USERSET,
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

        {{"incremental_checkpoint_timeout",
             PGC_SIGHUP,
             WAL_CHECKPOINTS,
             gettext_noop("Sets the maximum time between automatic WAL checkpoints."),
             NULL,
             GUC_UNIT_S},
            &u_sess->attr.attr_storage.incrCheckPointTimeout,
            60,
            1,
            3600,
            NULL,
            NULL,
            NULL},

        {{"pagewriter_sleep", PGC_SIGHUP, WAL_CHECKPOINTS, gettext_noop("PageWriter sleep time."), NULL, GUC_UNIT_MS},
            &u_sess->attr.attr_storage.pageWriterSleep,
            2000,
            0,
            3600 * 1000,
            NULL,
            NULL,
            NULL},

        {{"pagewriter_thread_num",
             PGC_POSTMASTER,
             WAL_CHECKPOINTS,
             gettext_noop("Sets the number of page writer threads."),
             NULL,
             0},
            &g_instance.attr.attr_storage.pagewriter_thread_num,
            2,
            1,
            8,
            NULL,
            NULL,
            NULL},
        {{"bgwriter_thread_num",
            PGC_POSTMASTER,
            WAL_CHECKPOINTS,
            gettext_noop("Sets the number of background writer threads with incremental checkpoint on."),
            NULL,
            0},
            &g_instance.attr.attr_storage.bgwriter_thread_num,
            2,
            0,
            8,
            NULL,
            NULL,
            NULL},

        {{"datanode_heartbeat_interval",
             PGC_SIGHUP,
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
        {{"recovery_parse_workers",
             PGC_POSTMASTER,
             RESOURCES_RECOVERY,
             gettext_noop("The number of recovery threads to do xlog parse."),
             gettext_noop("Zero lets the system determine this value.")},
             &g_instance.attr.attr_storage.recovery_parse_workers,
             1,
             1,
             MAX_PARSE_WORKERS,
             NULL,
             NULL,
             NULL},
        {{"recovery_redo_workers",
             PGC_POSTMASTER,
             RESOURCES_RECOVERY,
             gettext_noop("The number belonging to one parse worker to do xlog redo."),
             gettext_noop("Zero lets the system determine this value.")},
             &g_instance.attr.attr_storage.recovery_redo_workers_per_paser_worker,
             1,
             1,
             MAX_REDO_WORKERS_PER_PARSE,
             NULL,
             NULL,
             NULL},

        {{"force_promote",
            PGC_POSTMASTER,
            WAL,
            gettext_noop("Enable master update min recovery point."),
            NULL,
            0},
            &g_instance.attr.attr_storage.enable_update_max_page_flush_lsn,
            0,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"max_keep_log_seg",
             PGC_SUSET,
             WAL,
             gettext_noop("Sets the threshold for implementing logical replication flow control."),
             NULL},
            &g_instance.attr.attr_storage.max_keep_log_seg,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"comm_sender_buffer_size",
             PGC_POSTMASTER,
             DEVELOPER_OPTIONS,
             gettext_noop("The libcomm sender's buffer size in every interaction between DN and CN, or DN and DN, unit(KB)"),
             NULL},
            &g_instance.comm_cxt.commutil_cxt.g_comm_sender_buffer_size,
            8,
            1,
            1024,
            NULL,
            NULL,
            NULL},

        {{"max_size_for_xlog_prune",
             PGC_SIGHUP,
             WAL,
             gettext_noop("This param set by user is used for xlog to be recycled when not all are connected and the param enable_xlog_prune is on."),
             NULL,
             GUC_UNIT_KB},
            &u_sess->attr.attr_storage.max_size_for_xlog_prune,
            INT_MAX,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
#ifdef EXTREME_RTO_DEBUG_AB
        {{"rto_ab_pos",
             PGC_POSTMASTER,
             RESOURCES_RECOVERY,
             gettext_noop("for extreme rto debug."),
             gettext_noop("for extreme rto debug")},
            &g_instance.attr.attr_storage.extreme_rto_ab_pos,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"rto_ab_type",
             PGC_POSTMASTER,
             RESOURCES_RECOVERY,
             gettext_noop("for extreme rto debug."),
             gettext_noop("for extreme rto debug")},
            &g_instance.attr.attr_storage.extreme_rto_ab_type,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"rto_ab_count",
             PGC_POSTMASTER,
             RESOURCES_RECOVERY,
             gettext_noop("for extreme rto debug."),
             gettext_noop("for extreme rto debug")},
            &g_instance.attr.attr_storage.extreme_rto_ab_count,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
#endif
        /* max_size_for_xlog_prune for xlog recycle size limit, max_redo_log_size for redo log limit */
        {{"max_redo_log_size",
            PGC_SIGHUP,
            WAL,
            gettext_noop("max redo log size."),
            NULL,
            GUC_UNIT_KB},
            &u_sess->attr.attr_storage.max_redo_log_size,
            1048576,    /* 1GB */
            163840,    /* 160MB */
            INT_MAX,
            NULL,
            NULL,
            NULL},

        /* The I/O upper limit of batch flush dirty page every second */
        {{"max_io_capacity",
            PGC_SIGHUP,
            WAL,
            gettext_noop("The I/O upper limit of batch flush dirty page every second."),
            NULL,
            GUC_UNIT_KB},
            &u_sess->attr.attr_storage.max_io_capacity,
            512000,    /* 500MB */
            30720,    /* 30MB */
            10485760,  /* 10 GB */
            NULL,
            NULL,
            NULL},
#ifndef ENABLE_MULTIPLE_NODES
        {{"max_concurrent_autonomous_transactions",
            PGC_POSTMASTER,
            RESOURCES_WORKLOAD,
            gettext_noop("Maximum number of concurrent autonomous transactions processes."),
            NULL},
            &g_instance.attr.attr_storage.max_concurrent_autonomous_transactions,
            8,
            0,
            MAX_BACKENDS,
            NULL,
            NULL,
            NULL},
#endif
		/* The I/O upper limit of batch flush dirty page every second */
        {{"gpc_clean_timeout",
            PGC_SIGHUP,
            CLIENT_CONN,
            gettext_noop("Set the maximum allowed duration of any unused global plancache."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_common.gpc_clean_timeout,
            30 * 60,    /* 30min */
            5 * 60,    /* 5min */
            24 * 60 * 60,    /* 24h */
            NULL,
            NULL,
            NULL},

        /* End-of-list marker */
        {{NULL, (GucContext)0, (config_group)0, NULL, NULL}, NULL, 0, 0, 0, NULL, NULL, NULL}};

    Size bytes = sizeof(localConfigureNamesInt);
    u_sess->utils_cxt.ConfigureNamesInt[SINGLE_GUC] =
        (struct config_int*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt[SINGLE_GUC], bytes, localConfigureNamesInt, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitConfigureNamesReal()
{
    struct config_real localConfigureNamesReal[] =

        {{{"seq_page_cost",
              PGC_USERSET,
              QUERY_TUNING_COST,
              gettext_noop("Sets the planner's estimate of the cost of a "
                           "sequentially fetched disk page."),
              NULL},
             &u_sess->attr.attr_sql.seq_page_cost,
             DEFAULT_SEQ_PAGE_COST,
             0,
             DBL_MAX,
             NULL,
             NULL,
             NULL},
            {{"random_page_cost",
                 PGC_USERSET,
                 QUERY_TUNING_COST,
                 gettext_noop("Sets the planner's estimate of the cost of a "
                              "nonsequentially fetched disk page."),
                 NULL},
                &u_sess->attr.attr_sql.random_page_cost,
                DEFAULT_RANDOM_PAGE_COST,
                0,
                DBL_MAX,
                NULL,
                NULL,
                NULL},
            {{"cpu_tuple_cost",
                 PGC_USERSET,
                 QUERY_TUNING_COST,
                 gettext_noop("Sets the planner's estimate of the cost of "
                              "processing each tuple (row)."),
                 NULL},
                &u_sess->attr.attr_sql.cpu_tuple_cost,
                DEFAULT_CPU_TUPLE_COST,
                0,
                DBL_MAX,
                NULL,
                NULL,
                NULL},
            {{"allocate_mem_cost",
                 PGC_USERSET,
                 QUERY_TUNING_COST,
                 gettext_noop("Sets the planner's estimate of the cost of "
                              "allocate memory."),
                 NULL},
                &u_sess->attr.attr_sql.allocate_mem_cost,
                DEFAULT_ALLOCATE_MEM_COST,
                0,
                DBL_MAX,
                NULL,
                NULL,
                NULL},
            {{"cpu_index_tuple_cost",
                 PGC_USERSET,
                 QUERY_TUNING_COST,
                 gettext_noop("Sets the planner's estimate of the cost of "
                              "processing each index entry during an index scan."),
                 NULL},
                &u_sess->attr.attr_sql.cpu_index_tuple_cost,
                DEFAULT_CPU_INDEX_TUPLE_COST,
                0,
                DBL_MAX,
                NULL,
                NULL,
                NULL},
            {{"cpu_operator_cost",
                 PGC_USERSET,
                 QUERY_TUNING_COST,
                 gettext_noop("Sets the planner's estimate of the cost of "
                              "processing each operator or function call."),
                 NULL},
                &u_sess->attr.attr_sql.cpu_operator_cost,
                DEFAULT_CPU_OPERATOR_COST,
                0,
                DBL_MAX,
                NULL,
                NULL,
                NULL},

            {{"dngather_min_rows",
                 PGC_USERSET,
                 QUERY_TUNING_COST,
                 gettext_noop("minimum rows worth do dn gather, 0 meas always, -1 means disable"),
                 NULL},
                &u_sess->attr.attr_sql.dngather_min_rows,
                500.0,
                -1,
                DBL_MAX,
                NULL,
                NULL,
                NULL},

            {{"cursor_tuple_fraction",
                 PGC_USERSET,
                 QUERY_TUNING_OTHER,
                 gettext_noop("Sets the planner's estimate of the fraction of "
                              "a cursor's rows that will be retrieved."),
                 NULL},
                &u_sess->attr.attr_sql.cursor_tuple_fraction,
                DEFAULT_CURSOR_TUPLE_FRACTION,
                0.0,
                1.0,
                NULL,
                NULL,
                NULL},

            {{"connection_alarm_rate",
                 PGC_SIGHUP,
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

            {{"geqo_selection_bias",
                 PGC_USERSET,
                 QUERY_TUNING_GEQO,
                 gettext_noop("GEQO: selective pressure within the population."),
                 NULL},
                &u_sess->attr.attr_sql.Geqo_selection_bias,
                DEFAULT_GEQO_SELECTION_BIAS,
                MIN_GEQO_SELECTION_BIAS,
                MAX_GEQO_SELECTION_BIAS,
                NULL,
                NULL,
                NULL},
            {{"geqo_seed", PGC_USERSET, QUERY_TUNING_GEQO, gettext_noop("GEQO: seed for random path selection."), NULL},
                &u_sess->attr.attr_sql.Geqo_seed,
                0.0,
                0.0,
                1.0,
                NULL,
                NULL,
                NULL},

            {{"bgwriter_lru_multiplier",
                 PGC_SIGHUP,
                 RESOURCES_BGWRITER,
                 gettext_noop("Multiple of the average buffer usage to free per round."),
                 NULL},
                &u_sess->attr.attr_storage.bgwriter_lru_multiplier,
                2.0,
                0.0,
                10.0,
                NULL,
                NULL,
                NULL},

            {{"standby_shared_buffers_fraction",
                 PGC_SIGHUP,
                 RESOURCES_MEM,
                 gettext_noop("The max fraction of shared_buffers usage to standby."),
                 NULL},
                &u_sess->attr.attr_storage.shared_buffers_fraction,
                0.3,
                0.1,
                1.0,
                NULL,
                NULL,
                NULL},

            {{"seed",
                 PGC_USERSET,
                 UNGROUPED,
                 gettext_noop("Sets the seed for random-number generation."),
                 NULL,
                 GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
                &u_sess->attr.attr_sql.phony_random_seed,
                0.0,
                -1.0,
                1.0,
                check_random_seed,
                assign_random_seed,
                show_random_seed},

            {{"autovacuum_vacuum_scale_factor",
                 PGC_SIGHUP,
                 AUTOVACUUM,
                 gettext_noop("Number of tuple updates or deletes prior to vacuum as a fraction of reltuples."),
                 NULL},
                &u_sess->attr.attr_storage.autovacuum_vac_scale,
                0.2,
                0.0,
                100.0,
                NULL,
                NULL,
                NULL},
            {{"autovacuum_analyze_scale_factor",
                 PGC_SIGHUP,
                 AUTOVACUUM,
                 gettext_noop(
                     "Number of tuple inserts, updates, or deletes prior to analyze as a fraction of reltuples."),
                 NULL},
                &u_sess->attr.attr_storage.autovacuum_anl_scale,
                0.1,
                0.0,
                100.0,
                NULL,
                NULL,
                NULL},
            {{"candidate_buf_percent_target",
                 PGC_SIGHUP,
                 RESOURCES_KERNEL,
                 gettext_noop(
                     "Sets the candidate buffers percent."),
                 NULL},
                &u_sess->attr.attr_storage.candidate_buf_percent_target,
                0.3,
                0.01,
                0.9,
                NULL,
                NULL,
                NULL},
            {{"dirty_page_percent_max",
                 PGC_SIGHUP,
                 RESOURCES_KERNEL,
                 gettext_noop(
                     "Sets the dirty buffers percent."),
                 NULL},
                &u_sess->attr.attr_storage.dirty_page_percent_max,
                0.9,
                0.1,
                1,
                NULL,
                NULL,
                NULL},
            {{"checkpoint_completion_target",
                 PGC_SIGHUP,
                 WAL_CHECKPOINTS,
                 gettext_noop(
                     "Time spent flushing dirty buffers during checkpoint, as fraction of checkpoint interval."),
                 NULL},
                &u_sess->attr.attr_storage.CheckPointCompletionTarget,
                0.5,
                0.0,
                1.0,
                NULL,
                NULL,
                NULL},
            /* defalut values is 60 days */
            {{"password_reuse_time",
                 PGC_SIGHUP,
                 CONN_AUTH_SECURITY,
                 gettext_noop("max days password can reuse."),
                 NULL},
                &u_sess->attr.attr_security.Password_reuse_time,
                60.0,
                0.0,
                MAX_PASSWORD_REUSE_TIME,
                check_double_parameter,
                NULL,
                NULL},

            {{"password_effect_time", PGC_SIGHUP, CONN_AUTH_SECURITY, gettext_noop("password effective time."), NULL},
                &u_sess->attr.attr_security.Password_effect_time,
                90.0,
                0.0,
                MAX_PASSWORD_EFFECTIVE_TIME,
                check_double_parameter,
                NULL,
                NULL},

            {{"password_lock_time",
                PGC_SIGHUP,
                CONN_AUTH_SECURITY,
                gettext_noop("password lock time"),
                NULL,
                GUC_UNIT_DAY},
                &u_sess->attr.attr_security.Password_lock_time,
                1.0,
                0.0,
                MAX_ACCOUNT_LOCK_TIME,
                check_double_parameter,
                NULL,
                NULL},

            {{"table_skewness_warning_threshold", PGC_USERSET, STATS, gettext_noop("table skewness threthold"), NULL},
                &u_sess->attr.attr_sql.table_skewness_warning_threshold,
                1.0,
                0.0,
                1.0,
                check_double_parameter,
                NULL,
                NULL},
            {{"cost_weight_index", PGC_USERSET, QUERY_TUNING_COST, gettext_noop(
                "Sets the planner's discount when evaluating index cost."), NULL},
                &u_sess->attr.attr_sql.cost_weight_index,
                1,
                1e-10,
                1e10,
                NULL,
                NULL,
                NULL,
                0,
                NULL},
            {{"default_limit_rows", PGC_USERSET, QUERY_TUNING_COST, gettext_noop(
                    "Sets the planner's default estimation when limit rows is unknown."
                    "Negative value means using percentage of the left tree rows, whereas positive value "
                    "sets the estimation directly."), 
                NULL},
                &u_sess->attr.attr_sql.default_limit_rows,
                -10,
                -100,
                DBL_MAX,
                NULL,
                NULL,
                NULL,
                0,
                NULL},
            /* End-of-list marker */
            {{NULL, (GucContext)0, (config_group)0, NULL, NULL}, NULL, 0.0, 0.0, 0.0, NULL, NULL, NULL}};

    Size bytes = sizeof(localConfigureNamesReal);
    u_sess->utils_cxt.ConfigureNamesReal[SINGLE_GUC] =
        (struct config_real*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesReal[SINGLE_GUC], bytes, localConfigureNamesReal, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitConfigureNamesInt64()
{
    struct config_int64 localConfigureNamesInt64[] = {
        {{"vacuum_freeze_min_age",
             PGC_USERSET,
             CLIENT_CONN_STATEMENT,
             gettext_noop("Minimum age at which VACUUM should freeze a table row."),
             NULL},
            &u_sess->attr.attr_storage.vacuum_freeze_min_age,
            INT64CONST(2000000000),
            INT64CONST(0),
            INT64CONST(0x7FFFFFFFFFFFFFF),
            NULL,
            NULL,
            NULL},

        {{"hll_default_expthresh",
             PGC_USERSET,
             UNGROUPED,
             gettext_noop("Set parameter expthresh in hll."),
             gettext_noop("It could change promotion hierarchy for hll.")},
            &u_sess->attr.attr_sql.g_default_expthresh,
            DEFAULT_EXPTHRESH,
            HLL_MIN_EXPTHRESH,
            HLL_MAX_EXPTHRESH,
            NULL,
            NULL,
            NULL},

        {{"vacuum_freeze_table_age",
             PGC_USERSET,
             CLIENT_CONN_STATEMENT,
             gettext_noop("Age at which VACUUM should scan whole table to freeze tuples."),
             NULL},
            &u_sess->attr.attr_storage.vacuum_freeze_table_age,
            INT64CONST(4000000000),
            INT64CONST(0),
            INT64CONST(0x7FFFFFFFFFFFFFF),
            NULL,
            NULL,
            NULL},

        {{"vacuum_defer_cleanup_age",
             PGC_SIGHUP,
             REPLICATION_MASTER,
             gettext_noop("Number of transactions by which VACUUM and HOT cleanup should be deferred, if any."),
             NULL},
            &u_sess->attr.attr_storage.vacuum_defer_cleanup_age,
            INT64CONST(0),
            INT64CONST(0),
            INT64CONST(1000000),
            NULL,
            NULL,
            NULL},

        {/* see varsup.c for why this is PGC_POSTMASTER not PGC_SIGHUP */
            {"autovacuum_freeze_max_age",
                PGC_POSTMASTER,
                AUTOVACUUM,
                gettext_noop("Age at which to autovacuum a table."),
                NULL},
            &g_instance.attr.attr_storage.autovacuum_freeze_max_age,
            /* see pg_resetxlog if you change the upper-limit value */
            INT64CONST(4000000000),
            INT64CONST(100000),
            INT64CONST(0x7FFFFFFFFFFFFFF),
            NULL,
            NULL,
            NULL},

        {{"xlog_idle_flushes_before_sleep",
             PGC_POSTMASTER,
             WAL_SETTINGS,
             gettext_noop("Number of idle xlog flushes before xlog flusher goes to sleep."),
             NULL,
             GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_storage.xlog_idle_flushes_before_sleep,
            INT64CONST(500),
            INT64CONST(0),
            INT64CONST(0x7FFFFFFFFFFFFFF),
            NULL,
            NULL,
            NULL
        },

        {{"track_stmt_details_size",
             PGC_USERSET,
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
        {{NULL, (GucContext)0, (config_group)0, NULL, NULL}, NULL, 0, 0, 0, NULL, NULL, NULL}};

    Size bytes = sizeof(localConfigureNamesInt64);
    u_sess->utils_cxt.ConfigureNamesInt64 =
        (struct config_int64*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt64, bytes, localConfigureNamesInt64, bytes);
    securec_check_ss(rc, "\0", "\0");
};

static void InitConfigureNamesString()
{
    struct config_string localConfigureNamesString[] =

        {{{"archive_command",
              PGC_SIGHUP,
              WAL_ARCHIVING,
              gettext_noop("Sets the shell command that will be called to archive a WAL file."),
              NULL},
             &u_sess->attr.attr_storage.XLogArchiveCommand,
             "",
             NULL,
             NULL,
             show_archive_command},

            {{"archive_dest",
                PGC_SIGHUP,
                WAL_ARCHIVING,
                gettext_noop("Sets the path that will be used to archive a WAL file."),
                NULL},
                &u_sess->attr.attr_storage.XLogArchiveDest,
                "",
                NULL,
                NULL,
                NULL},

            {{"client_encoding",
                 PGC_USERSET,
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
                 LOGGING_WHAT,
                 gettext_noop("Controls information prefixed to each log line."),
                 gettext_noop("If blank, no prefix is used.")},
                &u_sess->attr.attr_common.Log_line_prefix,
                "",
                NULL,
                NULL,
                NULL},

            {{"elastic_search_ip_addr",
                PGC_POSTMASTER,
                AUDIT_OPTIONS,
                gettext_noop("Controls elastic search IP address in the system. "),
                NULL},
                &g_instance.attr.attr_security.elastic_search_ip_addr,
                "https://127.0.0.1",
                NULL,
                NULL,
                NULL},

            {{"log_timezone",
                 PGC_SIGHUP,
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

            {{"default_tablespace",
                 PGC_USERSET,
                 CLIENT_CONN_STATEMENT,
                 gettext_noop("Sets the default tablespace to create tables and indexes in."),
                 gettext_noop("An empty string selects the database's default tablespace."),
                 GUC_IS_NAME},
                &u_sess->attr.attr_storage.default_tablespace,
                "",
                check_default_tablespace,
                NULL,
                NULL},

            {{"temp_tablespaces",
                 PGC_USERSET,
                 CLIENT_CONN_STATEMENT,
                 gettext_noop("Sets the tablespace(s) to use for temporary tables and sort files."),
                 NULL,
                 GUC_LIST_INPUT | GUC_LIST_QUOTE},
                &u_sess->attr.attr_storage.temp_tablespaces,
                "",
                check_temp_tablespaces,
                assign_temp_tablespaces,
                NULL},

            {{"dynamic_library_path",
                 PGC_SUSET,
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

            {{"krb_server_keyfile",
                 PGC_SIGHUP,
                 CONN_AUTH_SECURITY,
                 gettext_noop("Sets the location of the Kerberos server key file."),
                 NULL,
                 GUC_SUPERUSER_ONLY},
                &u_sess->attr.attr_security.pg_krb_server_keyfile,
                "",
                NULL,
                NULL,
                NULL},

            {{"krb_srvname",
                 PGC_SIGHUP,
                 CONN_AUTH_SECURITY,
                 gettext_noop("Sets the name of the Kerberos service."),
                 NULL},
                &u_sess->attr.attr_security.pg_krb_srvnam,
                PG_KRB_SRVNAM,
                NULL,
                NULL,
                NULL},
#ifdef USE_BONJOUR
            {{"bonjour_name", PGC_POSTMASTER, CONN_AUTH_SETTINGS, gettext_noop("Sets the Bonjour service name."), NULL},
                &g_instance.attr.attr_common.bonjour_name,
                "",
                NULL,
                NULL,
                NULL},
#endif
            /* See main.c about why defaults for LC_foo are not all alike */

            {{"lc_collate",
                 PGC_INTERNAL,
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
                 CLIENT_CONN,
                 gettext_noop("Spare Cpu that can not be used in thread pool."),
                 NULL,
                 GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY},
                &g_instance.attr.attr_common.thread_pool_attr,
                "16, 2, (nobind)",
                NULL,
                NULL,
                NULL},

            {{"track_stmt_retention_time",
                 PGC_SIGHUP,
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
                 CLIENT_CONN_OTHER,
                 gettext_noop("Lists shared libraries to preload into each backend."),
                 NULL,
                 GUC_LIST_INPUT | GUC_LIST_QUOTE},
                &u_sess->attr.attr_common.local_preload_libraries_string,
                "",
                NULL,
                NULL,
                NULL},
            {{
                 "expected_computing_nodegroup",
                 PGC_USERSET,
                 QUERY_TUNING_METHOD,
                 gettext_noop("Computing node group mode or expected node group for query processing."),
                 NULL,
             },
                &u_sess->attr.attr_sql.expected_computing_nodegroup,
                CNG_OPTION_QUERY,
                NULL,
                NULL,
                NULL},
            {{
                 "default_storage_nodegroup",
                 PGC_USERSET,
                 QUERY_TUNING_METHOD,
                 gettext_noop("Default storage group for create table."),
                 NULL,
             },
                &u_sess->attr.attr_sql.default_storage_nodegroup,
                INSTALLATION_MODE,
                NULL,
                NULL,
                NULL},

            {/* Can't be set in postgresql.conf */
                {"current_logic_cluster",
                    PGC_INTERNAL,
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
                 INSTRUMENTS_OPTIONS,
                 gettext_noop("Sets the percentile of sql responstime that DBA want to know."),
                 NULL,
                 GUC_LIST_INPUT | GUC_LIST_QUOTE},
                &u_sess->attr.attr_common.percentile_values,
                "80,95",
                check_percentile,
                NULL,
                NULL},

            /* for pljava */
            {{"pljava_vmoptions",
                 PGC_SUSET,
                 CUSTOM_OPTIONS,
                 gettext_noop("Options sent to the JVM when it is created"),
                 NULL,
                 GUC_SUPERUSER_ONLY},
                &u_sess->attr.attr_sql.pljava_vmoptions,
                "",
                NULL,
                NULL,
                NULL},

            {{
                 "nls_timestamp_format",
                 PGC_USERSET,
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
                 CLIENT_CONN_STATEMENT,
                 gettext_noop("Sets the schema search order for names that are not schema-qualified."),
                 NULL,
                 GUC_LIST_INPUT | GUC_LIST_QUOTE},
                &u_sess->attr.attr_common.namespace_current_schema,
                "\"$user\",public",
                check_search_path,
                assign_search_path,
                NULL},

            {/* Can't be set in postgresql.conf */
                {"server_encoding",
                    PGC_INTERNAL,
                    CLIENT_CONN_LOCALE,
                    gettext_noop("Sets the server (database) character set encoding."),
                    NULL,
                    GUC_IS_NAME | GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
                &u_sess->attr.attr_common.server_encoding_string,
                "SQL_ASCII",
                NULL,
                NULL,
                NULL},

            {/* Can't be set in postgresql.conf */
                {"server_version",
                    PGC_INTERNAL,
                    PRESET_OPTIONS,
                    gettext_noop("Shows the server version."),
                    NULL,
                    GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
                &u_sess->attr.attr_common.server_version_string,
                PG_VERSION,
                NULL,
                NULL,
                NULL},

            {/* Not for general use --- used by SET ROLE */
                {"role",
                    PGC_USERSET,
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

            {/* Not for general use --- used by SET SESSION AUTHORIZATION */
                {"session_authorization",
                    PGC_USERSET,
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
                 LOGGING_WHERE,
                 gettext_noop("Sets the program name used to identify PostgreSQL "
                              "messages in syslog."),
                 NULL},
                &u_sess->attr.attr_common.syslog_ident_str,
                "postgres",
                NULL,
                assign_syslog_ident,
                NULL},

            {{"event_source",
                 PGC_POSTMASTER,
                 LOGGING_WHERE,
                 gettext_noop("Sets the application name used to identify "
                              "PostgreSQL messages in the event log."),
                 NULL},
                &g_instance.attr.attr_common.event_source,
                "PostgreSQL",
                NULL,
                NULL,
                NULL},

            {{"TimeZone",
                 PGC_USERSET,
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
                 CLIENT_CONN_LOCALE,
                 gettext_noop("Selects a file of time zone abbreviations."),
                 NULL},
                &u_sess->attr.attr_common.timezone_abbreviations_string,
                NULL,
                check_timezone_abbreviations,
                assign_timezone_abbreviations,
                NULL},

            {{"transaction_isolation",
                 PGC_USERSET,
                 CLIENT_CONN_STATEMENT,
                 gettext_noop("Sets the current transaction's isolation level."),
                 NULL,
                 GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
                &u_sess->attr.attr_storage.XactIsoLevel_string,
                "default",
                check_XactIsoLevel,
                assign_XactIsoLevel,
                show_XactIsoLevel},

            {{"unix_socket_group",
                 PGC_POSTMASTER,
                 CONN_AUTH_SETTINGS,
                 gettext_noop("Sets the owning group of the Unix-domain socket."),
                 gettext_noop("The owning user of the socket is always the user "
                              "that starts the server.")},
                &g_instance.attr.attr_network.Unix_socket_group,
                "",
                NULL,
                NULL,
                NULL},

            {{"unix_socket_directory",
                 PGC_POSTMASTER,
                 CONN_AUTH_SETTINGS,
                 gettext_noop("Sets the directory where the Unix-domain socket will be created."),
                 NULL,
                 GUC_SUPERUSER_ONLY},
                &g_instance.attr.attr_network.UnixSocketDir,
                "",
                check_canonical_path,
                NULL,
                NULL},

            {{"listen_addresses",
                 PGC_POSTMASTER,
                 CONN_AUTH_SETTINGS,
                 gettext_noop("Sets the host name or IP address(es) to listen to."),
                 NULL,
                 GUC_LIST_INPUT},
                &g_instance.attr.attr_network.ListenAddresses,
                "localhost",
                NULL,
                NULL,
                NULL},

            {{"local_bind_address",
                 PGC_POSTMASTER,
                 CONN_AUTH_SETTINGS,
                 gettext_noop("Sets the host name or IP address(es) to connect to for sctp."),
                 NULL,
                 GUC_LIST_INPUT},
                &g_instance.attr.attr_network.tcp_link_addr,
                "0.0.0.0",
                NULL,
                NULL,
                NULL},

            {{"data_directory",
                 PGC_POSTMASTER,
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
                 FILE_LOCATIONS,
                 gettext_noop("Writes the postmaster PID to the specified file."),
                 NULL,
                 GUC_SUPERUSER_ONLY},
                &g_instance.attr.attr_common.external_pid_file,
                NULL,
                check_canonical_path,
                NULL,
                NULL},

            {{"ssl_cert_file",
                 PGC_POSTMASTER,
                 CONN_AUTH_SECURITY,
                 gettext_noop("Location of the SSL server certificate file."),
                 NULL},
                &g_instance.attr.attr_security.ssl_cert_file,
                "server.crt",
                NULL,
                NULL,
                NULL},

            {{"ssl_key_file",
                 PGC_POSTMASTER,
                 CONN_AUTH_SECURITY,
                 gettext_noop("Location of the SSL server private key file."),
                 NULL},
                &g_instance.attr.attr_security.ssl_key_file,
                "server.key",
                NULL,
                NULL,
                NULL},

            {{"ssl_ca_file",
                 PGC_POSTMASTER,
                 CONN_AUTH_SECURITY,
                 gettext_noop("Location of the SSL certificate authority file."),
                 NULL},
                &g_instance.attr.attr_security.ssl_ca_file,
                "",
                NULL,
                NULL,
                NULL},

            {{"ssl_crl_file",
                 PGC_POSTMASTER,
                 CONN_AUTH_SECURITY,
                 gettext_noop("Location of the SSL certificate revocation list file."),
                 NULL},
                &g_instance.attr.attr_security.ssl_crl_file,
                "",
                NULL,
                NULL,
                NULL},

            {{"stats_temp_directory",
                 PGC_SIGHUP,
                 STATS_COLLECTOR,
                 gettext_noop("Writes temporary statistics files to the specified directory."),
                 NULL,
                 GUC_SUPERUSER_ONLY},
                &u_sess->attr.attr_common.pgstat_temp_directory,
                "pg_stat_tmp",
                check_canonical_path,
                assign_pgstat_temp_directory,
                NULL},

            {{"synchronous_standby_names",
                 PGC_SIGHUP,
                 REPLICATION_MASTER,
                 gettext_noop("List of names of potential synchronous standbys."),
                 NULL,
                 GUC_LIST_INPUT},
                &u_sess->attr.attr_storage.SyncRepStandbyNames,
                "*",
                check_synchronous_standby_names,
                assign_synchronous_standby_names,
                NULL},

            {{"default_text_search_config",
                 PGC_USERSET,
                 CLIENT_CONN_LOCALE,
                 gettext_noop("Sets default text search configuration."),
                 NULL},
                &u_sess->attr.attr_common.TSCurrentConfig,
                "pg_catalog.simple",
                check_TSCurrentConfig,
                assign_TSCurrentConfig,
                NULL},

#ifdef PGXC
            {{"qrw_inlist2join_optmode",
                 PGC_USERSET,
                 QUERY_TUNING_METHOD,
                 gettext_noop("Specify inlist2join opimitzation mode."),
                 NULL},
                &u_sess->attr.attr_sql.inlist2join_optmode,
                "cost_base",
                check_inlist2joininfo,
                assign_inlist2joininfo,
                NULL},


            {{"behavior_compat_options",
                 PGC_USERSET,
                 COMPAT_OPTIONS,
                 gettext_noop("compatibility options"),
                 NULL,
                 GUC_LIST_INPUT | GUC_REPORT},
                &u_sess->attr.attr_sql.behavior_compat_string,
                "",
                check_behavior_compat_options,
                assign_behavior_compat_options,
                NULL},

            {{"pgxc_node_name",
                 PGC_POSTMASTER,
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
#endif
            {{"ssl_ciphers",
                 PGC_POSTMASTER,
                 CONN_AUTH_SECURITY,
                 gettext_noop("Sets the list of allowed SSL ciphers."),
                 NULL,
                 GUC_SUPERUSER_ONLY},
                &g_instance.attr.attr_security.SSLCipherSuites,
#ifdef USE_SSL
                "ALL",
#else
                "none",
#endif
                check_ssl_ciphers,
                NULL,
                NULL},

            {{"application_name",
                 PGC_USERSET,
                 LOGGING_WHAT,
                 gettext_noop("Sets the application name to be reported in statistics and logs."),
                 NULL,
                 GUC_IS_NAME | GUC_REPORT | GUC_NOT_IN_SAMPLE},
                &u_sess->attr.attr_common.application_name,
                "",
                check_application_name,
                assign_application_name,
                NULL},

            {{"connection_info",
                 PGC_USERSET,
                 LOGGING_WHAT,
                 gettext_noop("Sets the connection info to be reported in statistics and logs."),
                 NULL,
                 GUC_REPORT | GUC_NOT_IN_SAMPLE},
                &u_sess->attr.attr_sql.connection_info,
                "",
                NULL,
                assign_connection_info,
                NULL},

            {{"cgroup_name",
                 PGC_USERSET,
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
                 RESOURCES_WORKLOAD,
                 gettext_noop("Sets the session resource pool to control the queries resource."),
                 NULL,
                 GUC_IS_NAME | GUC_REPORT | GUC_NOT_IN_SAMPLE},
                &u_sess->attr.attr_resource.session_resource_pool,
                "invalid_pool",
                check_session_respool,
                assign_session_respool,
                show_session_respool},

            {{"memory_detail_tracking",
                 PGC_USERSET,
                 RESOURCES_MEM,
                 gettext_noop("Sets the operator name and peak size for triggering the memory logging in that time."),
                 NULL,
                 GUC_IS_NAME | GUC_REPORT | GUC_NOT_IN_SAMPLE},
                &u_sess->attr.attr_memory.memory_detail_tracking,
                "",
                check_memory_detail_tracking,
                assign_memory_detail_tracking,
                show_memory_detail_tracking},

            /* Database Security: Support database audit */
            {{"audit_directory",
                 PGC_POSTMASTER,
                 AUDIT_OPTIONS,
                 gettext_noop("Sets the destination directory for audit files."),
                 gettext_noop("Can be specified as relative to the data directory "
                              "or as absolute path."),
                 GUC_SUPERUSER_ONLY},
                &g_instance.attr.attr_security.Audit_directory,
                "pg_audit",
                check_directory,
                NULL,
                NULL},
            {{"audit_data_format",
                 PGC_POSTMASTER,
                 AUDIT_OPTIONS,
                 gettext_noop("Sets the data format for audit files."),
                 gettext_noop("Currently can be specified as binary only."),
                 GUC_SUPERUSER_ONLY},
                &g_instance.attr.attr_security.Audit_data_format,
                "binary",
                NULL,
                NULL,
                NULL},
	
#ifndef ENABLE_MULTIPLE_NODES			
            /* availablezone of current instance, currently, it is only used in cascade standby */	
            {{
                "available_zone",
                PGC_POSTMASTER,
                AUDIT_OPTIONS,
                gettext_noop("Sets the available zone of current instance."),
                gettext_noop("The available zone is only used in cascade standby scenes"),
                GUC_SUPERUSER_ONLY | GUC_IS_NAME},
               &g_instance.attr.attr_storage.available_zone,
               "",
               NULL,
               NULL,
               NULL},
#endif
		
            /* Get the ReplConnInfo1 from postgresql.conf and assign to ReplConnArray1. */
            {{"replconninfo1",
                 PGC_SIGHUP,
                 REPLICATION_SENDING,
                 gettext_noop("Sets the replconninfo1 of the HA to listen and authenticate."),
                 NULL,
                 GUC_LIST_INPUT},
                &u_sess->attr.attr_storage.ReplConnInfoArr[1],
                "",
                check_replconninfo,
                assign_replconninfo1,
                NULL},

            /* Get the ReplConnInfo2 from postgresql.conf and assign to ReplConnArray2. */
            {{"replconninfo2",
                 PGC_SIGHUP,
                 REPLICATION_SENDING,
                 gettext_noop("Sets the replconninfo2 of the HA to listen and authenticate."),
                 NULL,
                 GUC_LIST_INPUT},
                &u_sess->attr.attr_storage.ReplConnInfoArr[2],
                "",
                check_replconninfo,
                assign_replconninfo2,
                NULL},
            /* Get the ReplConnInfo3 from postgresql.conf and assign to ReplConnArray3. */
            {{"replconninfo3",
                 PGC_SIGHUP,
                 REPLICATION_SENDING,
                 gettext_noop("Sets the replconninfo3 of the HA to listen and authenticate."),
                 NULL,
                 GUC_LIST_INPUT},
                &u_sess->attr.attr_storage.ReplConnInfoArr[3],
                "",
                check_replconninfo,
                assign_replconninfo3,
                NULL},
            /* Get the ReplConnInfo4 from postgresql.conf and assign to ReplConnArray4. */
            {{"replconninfo4",
                 PGC_SIGHUP,
                 REPLICATION_SENDING,
                 gettext_noop("Sets the replconninfo4 of the HA to listen and authenticate."),
                 NULL,
                 GUC_LIST_INPUT},
                &u_sess->attr.attr_storage.ReplConnInfoArr[4],
                "",
                check_replconninfo,
                assign_replconninfo4,
                NULL},
            /* Get the ReplConnInfo5 from postgresql.conf and assign to ReplConnArray5. */
            {{"replconninfo5",
                 PGC_SIGHUP,
                 REPLICATION_SENDING,
                 gettext_noop("Sets the replconninfo5 of the HA to listen and authenticate."),
                 NULL,
                 GUC_LIST_INPUT},
                &u_sess->attr.attr_storage.ReplConnInfoArr[5],
                "",
                check_replconninfo,
                assign_replconninfo5,
                NULL},
            /* Get the ReplConnInfo6 from postgresql.conf and assign to ReplConnArray6. */
            {{"replconninfo6",
                 PGC_SIGHUP,
                 REPLICATION_SENDING,
                 gettext_noop("Sets the replconninfo6 of the HA to listen and authenticate."),
                 NULL,
                 GUC_LIST_INPUT},
                &u_sess->attr.attr_storage.ReplConnInfoArr[6],
                "",
                check_replconninfo,
                assign_replconninfo6,
                NULL},
            /* Get the ReplConnInfo7 from postgresql.conf and assign to ReplConnArray7. */
            {{"replconninfo7",
                 PGC_SIGHUP,
                 REPLICATION_SENDING,
                 gettext_noop("Sets the replconninfo7 of the HA to listen and authenticate."),
                 NULL,
                 GUC_LIST_INPUT},
                &u_sess->attr.attr_storage.ReplConnInfoArr[7],
                "",
                check_replconninfo,
                assign_replconninfo7,
                NULL},
#ifndef ENABLE_MULTIPLE_NODES
             /* Get the ReplConnInfo8 from postgresql.conf and assign to ReplConnArray8. */
            {{
                "replconninfo8",
                PGC_SIGHUP,
                REPLICATION_SENDING,
                gettext_noop("Sets the replconninfo8 of the HA to listen and authenticate."),
                NULL,
                GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.ReplConnInfoArr[8],
            "",
            check_replconninfo,
            assign_replconninfo8,
            NULL},
#endif
            {{"primary_slotname",
                 PGC_SIGHUP,
                 REPLICATION_STANDBY,
                 gettext_noop("Set the primary slot name."),
                 NULL,
                 GUC_NO_RESET_ALL | GUC_IS_NAME},
                &u_sess->attr.attr_storage.PrimarySlotName,
                NULL,
                NULL,
                NULL,
                NULL},
            /* control for logging backend modules */
            {{"logging_module",
                 PGC_USERSET,
                 LOGGING_WHAT,
                 gettext_noop("enable/disable module logging."),
                 NULL,
                 GUC_NOT_IN_SAMPLE},
                &u_sess->attr.attr_storage.logging_module,
                "off(ALL)",
                logging_module_check,
                logging_module_guc_assign,
                logging_module_guc_show},

            {{"inplace_upgrade_next_system_object_oids",
                 PGC_SUSET,
                 UPGRADE_OPTIONS,
                 gettext_noop("Sets oids for the system object to be added next during inplace upgrade."),
                 NULL,
                 GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_SUPERUSER_ONLY | GUC_LIST_INPUT},
                &u_sess->attr.attr_storage.Inplace_upgrade_next_system_object_oids,
                NULL,
                check_inplace_upgrade_next_oids,
                NULL,
                NULL},
            /* analysis options for dfx */
            {{"analysis_options",
                 PGC_USERSET,
                 CLIENT_CONN_STATEMENT,
                 gettext_noop("enable/disable sql dfx option."),
                 NULL,
                 GUC_NOT_IN_SAMPLE},
                &u_sess->attr.attr_common.analysis_options,
                "off(ALL)",
                analysis_options_check,
                analysis_options_guc_assign,
                analysis_options_guc_show},
            /* End-of-list marker */
            {{"transparent_encrypted_string",
                 PGC_POSTMASTER,
                 UNGROUPED,
                 gettext_noop("The encrypted string to test the transparent encryption key."),
                 NULL,
                 GUC_NOT_IN_SAMPLE | GUC_NO_RESET_ALL},
                &g_instance.attr.attr_security.transparent_encrypted_string,
                NULL,
                NULL,
                NULL,
                NULL},

            {{"transparent_encrypt_kms_url",
                 PGC_POSTMASTER,
                 UNGROUPED,
                 gettext_noop("The URL to get transparent encryption key."),
                 NULL,
                 GUC_NOT_IN_SAMPLE | GUC_NO_RESET_ALL},
                &g_instance.attr.attr_common.transparent_encrypt_kms_url,
                "",
                transparent_encrypt_kms_url_region_check,
                NULL,
                NULL},

            {{"transparent_encrypt_kms_region",
                 PGC_POSTMASTER,
                 UNGROUPED,
                 gettext_noop("The region to get transparent encryption key."),
                 NULL,
                 GUC_NOT_IN_SAMPLE | GUC_NO_RESET_ALL},
                &g_instance.attr.attr_security.transparent_encrypt_kms_region,
                "",
                transparent_encrypt_kms_url_region_check,
                NULL,
                NULL},

            /* uncontrolled_memory_context is a white list of MemoryContext allocation. */
            {{"uncontrolled_memory_context",
                 PGC_USERSET,
                 RESOURCES_MEM,
                 gettext_noop("Sets the white list of MemoryContext allocation."),
                 NULL},
                &u_sess->attr.attr_memory.uncontrolled_memory_context,
                "",
                check_uncontrolled_memory_context,
                assign_uncontrolled_memory_context,
                show_uncontrolled_memory_context},
            {{"retry_ecode_list",
                 PGC_USERSET,
                 COORDINATORS,
                 gettext_noop("Set error code list for CN Retry."),
                 NULL,
                 GUC_NOT_IN_SAMPLE},
                &u_sess->attr.attr_sql.retry_errcode_list,
                "YY001 YY002 YY003 YY004 YY005 YY006 YY007 YY008 YY009 YY010 YY011 YY012 YY013 YY014 YY015 53200 08006 "
                "08000 57P01 XX003 XX009 YY016",
                check_errcode_list,
                NULL,
                NULL},

            {{"numa_distribute_mode",
                 PGC_POSTMASTER,
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
                 INSTRUMENTS_OPTIONS,
                 gettext_noop("specify which level statement's statistics to be gathered."),
                 NULL,
                 GUC_LIST_INPUT},
                &u_sess->attr.attr_common.track_stmt_stat_level,
                "OFF,L0",
                check_statement_stat_level,
                assign_statement_stat_level,
                NULL},

            {{NULL, (GucContext)0, (config_group)0, NULL, NULL}, NULL, NULL, NULL, NULL, NULL}};

    Size bytes = sizeof(localConfigureNamesString);
    u_sess->utils_cxt.ConfigureNamesString[SINGLE_GUC] =
        (struct config_string*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesString[SINGLE_GUC], bytes, localConfigureNamesString, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitConfigureNamesEnum()
{
    struct config_enum localConfigureNamesEnum[] = {
#ifndef ENABLE_MULTIPLE_NODES
        {{"sync_config_strategy",
            PGC_POSTMASTER,
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
        {{"backslash_quote",
             PGC_USERSET,
             COMPAT_OPTIONS_PREVIOUS,
             gettext_noop("Sets whether \"\\'\" is allowed in string literals."),
             NULL},
            &u_sess->attr.attr_sql.backslash_quote,
            BACKSLASH_QUOTE_SAFE_ENCODING,
            backslash_quote_options,
            NULL,
            NULL,
            NULL},

        {{"bytea_output", PGC_USERSET, CLIENT_CONN_STATEMENT, gettext_noop("Sets the output format for bytea."), NULL},
            &u_sess->attr.attr_common.bytea_output,
            BYTEA_OUTPUT_HEX,
            bytea_output_options,
            NULL,
            NULL,
            NULL},

        {{"client_min_messages",
             PGC_USERSET,
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

        {{"constraint_exclusion",
             PGC_USERSET,
             QUERY_TUNING_OTHER,
             gettext_noop("Enables the planner to use constraints to optimize queries."),
             gettext_noop("Table scans will be skipped if their constraints"
                          " guarantee that no rows match the query.")},
            &u_sess->attr.attr_sql.constraint_exclusion,
            CONSTRAINT_EXCLUSION_PARTITION,
            constraint_exclusion_options,
            NULL,
            NULL,
            NULL},

        {{"default_transaction_isolation",
             PGC_USERSET,
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

        {{"log_error_verbosity", PGC_SUSET, LOGGING_WHAT, gettext_noop("Sets the verbosity of logged messages."), NULL},
            &u_sess->attr.attr_common.Log_error_verbosity,
            PGERROR_DEFAULT,
            log_error_verbosity_options,
            NULL,
            NULL,
            NULL},

        {{"log_min_messages",
             PGC_SUSET,
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
             LOGGING_WHEN,
             gettext_noop("Sets the message levels for print backtrace that are logged."),
             gettext_noop("Each level includes all the levels that follow it. The later"
                          " the level, the fewer messages are sent.")},
            &u_sess->attr.attr_common.backtrace_min_messages,
            PANIC,
            server_message_level_options,
            NULL,
            NULL,
            NULL},

        {{"log_min_error_statement",
             PGC_SUSET,
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

        {{"log_statement", PGC_SUSET, LOGGING_WHAT, gettext_noop("Sets the type of statements logged."), NULL},
            &u_sess->attr.attr_common.log_statement,
            LOGSTMT_NONE,
            log_statement_options,
            NULL,
            NULL,
            NULL},

        {{"rewrite_rule", PGC_USERSET, QUERY_TUNING, gettext_noop("Sets the rewrite rule."), NULL, GUC_LIST_INPUT},
            &u_sess->attr.attr_sql.rewrite_rule,
            MAGIC_SET,
            rewrite_options,
            NULL,
            NULL,
            NULL},

        {{"resource_track_log",
             PGC_USERSET,
             QUERY_TUNING,
             gettext_noop("Sets resource track log level"),
             NULL,
             GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.resource_track_log,
            SUMMARY,
            resource_track_log_options,
            NULL,
            NULL,
            NULL},

        {{"syslog_facility",
             PGC_SIGHUP,
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
             CLIENT_CONN_STATEMENT,
             gettext_noop("Sets the session's behavior for triggers and rewrite rules."),
             NULL},
            &u_sess->attr.attr_common.SessionReplicationRole,
            SESSION_REPLICATION_ROLE_ORIGIN,
            session_replication_role_options,
            NULL,
            assign_session_replication_role,
            NULL},

        {{"synchronous_commit",
             PGC_USERSET,
             WAL_SETTINGS,
             gettext_noop("Sets the current transaction's synchronization level."),
             NULL},
            &u_sess->attr.attr_storage.guc_synchronous_commit,
            SYNCHRONOUS_COMMIT_ON,
            synchronous_commit_options,
            NULL,
            assign_synchronous_commit,
            NULL},

        {{"sql_compatibility",
             PGC_INTERNAL,
             UNGROUPED,
             gettext_noop("Choose which SQL format to adapt."),
             NULL,
             GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_sql.sql_compatibility,
            B_FORMAT,
            adapt_database,
            NULL,
            NULL,
            NULL},

        {{"explain_perf_mode",
             PGC_USERSET,
             UNGROUPED,
             gettext_noop("Choose which style to print the explain info."),
             NULL},
            &u_sess->attr.attr_sql.guc_explain_perf_mode,
            EXPLAIN_NORMAL,
            explain_option,
            NULL,
            NULL,
            NULL},

        {{"skew_option", PGC_USERSET, UNGROUPED, gettext_noop("Choose data skew optimization strategy."), NULL},
            &u_sess->attr.attr_sql.skew_strategy_store,
            SKEW_OPT_NORMAL,
            skew_strategy_option,
            NULL,
            NULL,
            NULL},

        {{"resource_track_level",
             PGC_USERSET,
             RESOURCES_WORKLOAD,
             gettext_noop("Choose which level info to be collected. "),
             NULL},
            &u_sess->attr.attr_resource.resource_track_level,
            RESOURCE_TRACK_QUERY,
            resource_track_option,
            NULL,
            NULL,
            NULL},

        {{"codegen_strategy",
             PGC_USERSET,
             QUERY_TUNING,
             gettext_noop("Choose whether it is allowed to call C-function in codegen."),
             NULL},
            &u_sess->attr.attr_sql.codegen_strategy,
            CODEGEN_PARTIAL,
            codegen_strategy_option,
            NULL,
            NULL,
            NULL},

        {{"memory_tracking_mode",
             PGC_USERSET,
             RESOURCES_MEM,
             gettext_noop("Choose which style to track the memory usage."),
             NULL},
            &u_sess->attr.attr_memory.memory_tracking_mode,
            MEMORY_TRACKING_NONE,
            memory_tracking_option,
            NULL,
            NULL,
            NULL},

        {{"io_priority", PGC_USERSET, RESOURCES_WORKLOAD, gettext_noop("Sets the IO priority for queries."), NULL},
            &u_sess->attr.attr_resource.io_priority,
            IOPRIORITY_NONE,
            io_priority_level_options,
            NULL,
            NULL,
            NULL},

        {{"trace_recovery_messages",
             PGC_SIGHUP,
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
             STATS_COLLECTOR,
             gettext_noop("Collects function-level statistics on database activity."),
             NULL},
            &u_sess->attr.attr_common.pgstat_track_functions,
            TRACK_FUNC_OFF,
            track_function_options,
            NULL,
            NULL,
            NULL},

        {{"wal_level",
             PGC_POSTMASTER,
             WAL_SETTINGS,
             gettext_noop("Sets the level of information written to the WAL."),
             NULL},
            &g_instance.attr.attr_storage.wal_level,
            WAL_LEVEL_MINIMAL,
            wal_level_options,
            NULL,
            NULL,
            NULL},

        {{"wal_sync_method",
             PGC_SIGHUP,
             WAL_SETTINGS,
             gettext_noop("Selects the method used for forcing WAL updates to disk."),
             NULL},
            &u_sess->attr.attr_storage.sync_method,
            DEFAULT_SYNC_METHOD,
            sync_method_options,
            NULL,
            assign_xlog_sync_method,
            NULL},

        {{"xmlbinary",
             PGC_USERSET,
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

#ifdef PGXC
        {{"remotetype", PGC_BACKEND, CONN_AUTH, gettext_noop("Sets the type of Postgres-XC remote connection"), NULL},
            &u_sess->attr.attr_common.remoteConnType,
            REMOTE_CONN_APP,
            pgxc_conn_types,
            NULL,
            NULL,
            NULL},

        {{"autovacuum_mode", PGC_SIGHUP, AUTOVACUUM, gettext_noop("Sets the behavior of autovacuum"), NULL},
            &u_sess->attr.attr_storage.autovacuum_mode,
            AUTOVACUUM_DO_ANALYZE_VACUUM,
            autovacuum_mode_options,
            NULL,
            NULL,
            NULL},
#endif
        {{"cstore_insert_mode", PGC_USERSET, UNGROUPED, gettext_noop("decide destination of data inserted"), NULL},
            &u_sess->attr.attr_storage.cstore_insert_mode,
            TO_AUTO,
            cstore_insert_mode_options,
            NULL,
            NULL,
            NULL},

        {{"plan_cache_mode", PGC_USERSET, QUERY_TUNING_OTHER,
            gettext_noop("Controls the planner's selection of custom or generic plan."),
            gettext_noop("Prepared statements can have custom and generic plans, and the planner "
                         "will attempt to choose which is better.  This can be set to override "
                         "the default behavior.")
            },
            &u_sess->attr.attr_sql.g_planCacheMode,
            PLAN_CACHE_MODE_AUTO, plan_cache_mode_options,
            NULL, NULL, NULL},

        {{"opfusion_debug_mode", PGC_USERSET, QUERY_TUNING_METHOD, gettext_noop("opfusion debug mode."), NULL},
            &u_sess->attr.attr_sql.opfusion_debug_mode,
            BYPASS_OFF,
            opfusion_debug_level_options,
            NULL,
            NULL,
            NULL},

        {{"instr_unique_sql_track_type",
             PGC_INTERNAL,
             INSTRUMENTS_OPTIONS,
             gettext_noop("unique sql track type"),
             NULL},
            &u_sess->attr.attr_common.unique_sql_track_type,
            UNIQUE_SQL_TRACK_TOP,
            unique_sql_track_option,
            NULL,
            NULL,
            NULL},

        {{"remote_read_mode", PGC_POSTMASTER, UNGROUPED, gettext_noop("decide way of remote read"), NULL},
            &g_instance.attr.attr_storage.remote_read_mode,
            REMOTE_READ_AUTH,
            remote_read_options,
            NULL,
            NULL,
            NULL},

        {
            {
                "application_type", PGC_USERSET, GTM,
                gettext_noop("application distribute type(perfect sharding or not) in gtm free mode."),
                NULL,
                GUC_REPORT
            },
            &u_sess->attr.attr_sql.application_type,
            NOT_PERFECT_SHARDING_TYPE, application_type_options,
            check_application_type, NULL, NULL
        },

        {{"auto_explain_level",
            PGC_USERSET,
            RESOURCES_WORKLOAD,
            gettext_noop("auto_explain_level."),
            gettext_noop("auto_explain_level")},
            &u_sess->attr.attr_resource.auto_explain_level,
            LOG,
            auto_explain_level_options,
            check_auto_explain_level,
            NULL,
            NULL},
        {{"sql_beta_feature",
            PGC_USERSET,
            QUERY_TUNING,
            gettext_noop("Sets the beta feature for SQL engine."), NULL, GUC_LIST_INPUT},
            &u_sess->attr.attr_sql.sql_beta_feature,
            NO_BETA_FEATURE,
            sql_beta_options,
            NULL,
            NULL,
            NULL},
        /* End-of-list marker */
        {{NULL, (GucContext)0, (config_group)0, NULL, NULL}, NULL, 0, NULL, NULL, NULL, NULL}};

    Size bytes = sizeof(localConfigureNamesEnum);
    u_sess->utils_cxt.ConfigureNamesEnum =
        (struct config_enum*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesEnum, bytes, localConfigureNamesEnum, bytes);
    securec_check_ss(rc, "\0", "\0");
}

/******** end of options list ********/

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
    char* str = (char*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), string_length);
    rc = strcpy_s(str, string_length, source);
    securec_check_errno(rc, pfree(str), NULL);

    memory_context_list* head_node = (memory_context_list*)MemoryContextAlloc(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), sizeof(memory_context_list));
    memory_context_list* current_node = head_node;
    current_node->next = NULL;
    current_node->value = NULL;
    char* first_ch = str;
    size_t len = 0;

    for (int i = 0; i < string_length; i++, len++) {
        if (str[i] == ',' || str[i] == '\0') {
            if (NULL != current_node->value) {
                /* append a new node to list */
                current_node->next = (memory_context_list*)MemoryContextAlloc(
                    SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), sizeof(memory_context_list));
                current_node = current_node->next;
                current_node->next = NULL;
                current_node->value = NULL;
            }

            first_ch[len] = '\0';  // replace ',' with '\0'
            first_ch_length = strlen(first_ch) + 1;

            // 'strlen(first_ch) + 1' means including '\0'.
            current_node->value = (char*)MemoryContextAlloc(
                SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), first_ch_length);
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
    char* buff = (char*)MemoryContextAllocZero(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), length);
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
static void ShowAllGUCConfig(DestReceiver* dest);
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
    u_sess->attr.attr_storage.gs_clean_timeout = 300;
    u_sess->attr.attr_storage.twophase_clean_workers = 3;
    /* for Double Guc Variables */
    u_sess->attr.attr_sql.stream_multiple = DEFAULT_STREAM_MULTIPLE;


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
void build_guc_get_variables_num(enum guc_choose_strategy stragety, int *num_vars_out)
{
    int num_vars = 0;
    int i;

    /* Bind thread local GUC variables to their names */
    if (stragety == SINGLE_GUC) {
        InitConfigureNamesBool();
        InitConfigureNamesInt();
        InitConfigureNamesInt64();
        InitConfigureNamesReal();
        InitConfigureNamesString();
        InitConfigureNamesEnum();
    } else if (stragety == DISTRIBUTE_GUC) {
#ifdef ENABLE_MULTIPLE_NODES
        InitConfigureNamesBoolMultipleNode();
        InitConfigureNamesIntMultipleNode();
        InitConfigureNamesRealMultipleNode();
        InitConfigureNamesStringMultipleNode();
#else
        return;
#endif
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION), errmsg("unexpected stragety element: %d", stragety)));
    }

    for (i = 0; u_sess->utils_cxt.ConfigureNamesBool[stragety][i].gen.name; i++) {
        struct config_bool* conf = &u_sess->utils_cxt.ConfigureNamesBool[stragety][i];

        /* Rather than requiring vartype to be filled in by hand, do this: */
        conf->gen.vartype = PGC_BOOL;
        num_vars++;
    }

    for (i = 0; u_sess->utils_cxt.ConfigureNamesInt[stragety][i].gen.name; i++) {
        struct config_int* conf = &u_sess->utils_cxt.ConfigureNamesInt[stragety][i];

        conf->gen.vartype = PGC_INT;
        num_vars++;
    }

    for (i = 0; u_sess->utils_cxt.ConfigureNamesReal[stragety][i].gen.name; i++) {
        struct config_real* conf = &u_sess->utils_cxt.ConfigureNamesReal[stragety][i];

        conf->gen.vartype = PGC_REAL;
        num_vars++;
    }

    for (i = 0; u_sess->utils_cxt.ConfigureNamesString[stragety][i].gen.name; i++) {
        struct config_string* conf = &u_sess->utils_cxt.ConfigureNamesString[stragety][i];

        conf->gen.vartype = PGC_STRING;
        num_vars++;
    }
    
    if (stragety == SINGLE_GUC) {
        for (i = 0; u_sess->utils_cxt.ConfigureNamesInt64[i].gen.name; i++) {
            struct config_int64* conf = &u_sess->utils_cxt.ConfigureNamesInt64[i];

            conf->gen.vartype = PGC_INT64;
            num_vars++;
        }

        for (i = 0; u_sess->utils_cxt.ConfigureNamesEnum[i].gen.name; i++) {
            struct config_enum* conf = &u_sess->utils_cxt.ConfigureNamesEnum[i];

            conf->gen.vartype = PGC_ENUM;
            num_vars++;
        }
    }

    *num_vars_out = num_vars;
    return;
}

void build_guc_variables_internal(enum guc_choose_strategy stragety, struct config_generic** guc_vars)
{
    int i;
    int num_vars = 0;

    for (i = 0; u_sess->utils_cxt.ConfigureNamesBool[stragety][i].gen.name; i++)
        guc_vars[num_vars++] = &u_sess->utils_cxt.ConfigureNamesBool[stragety][i].gen;

    for (i = 0; u_sess->utils_cxt.ConfigureNamesInt[stragety][i].gen.name; i++)
        guc_vars[num_vars++] = &u_sess->utils_cxt.ConfigureNamesInt[stragety][i].gen;

    for (i = 0; u_sess->utils_cxt.ConfigureNamesReal[stragety][i].gen.name; i++)
        guc_vars[num_vars++] = &u_sess->utils_cxt.ConfigureNamesReal[stragety][i].gen;

    for (i = 0; u_sess->utils_cxt.ConfigureNamesString[stragety][i].gen.name; i++)
        guc_vars[num_vars++] = &u_sess->utils_cxt.ConfigureNamesString[stragety][i].gen;

    if (stragety == SINGLE_GUC) {
        for (i = 0; u_sess->utils_cxt.ConfigureNamesInt64[i].gen.name; i++)
            guc_vars[num_vars++] = &u_sess->utils_cxt.ConfigureNamesInt64[i].gen;
        
        for (i = 0; u_sess->utils_cxt.ConfigureNamesEnum[i].gen.name; i++)
            guc_vars[num_vars++] = &u_sess->utils_cxt.ConfigureNamesEnum[i].gen;
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

    build_guc_get_variables_num(SINGLE_GUC, &single_vars_num);
    build_guc_get_variables_num(DISTRIBUTE_GUC, &multi_vars_num);

    num_vars = single_vars_num + multi_vars_num;
    /*
     * Create table with 20% slack
     */
    size_vars = num_vars + num_vars / 4;
    guc_vars = (struct config_generic**)guc_malloc(FATAL, size_vars * sizeof(struct config_generic*));
    
    build_guc_variables_internal(SINGLE_GUC, guc_vars);
#ifdef ENABLE_MULTIPLE_NODES
    build_guc_variables_internal(DISTRIBUTE_GUC, (guc_vars + single_vars_num));
#endif

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

void init_bgwriter_thread_num()
{
    if (g_instance.attr.attr_storage.bgwriter_thread_num == 0) {
        g_instance.attr.attr_storage.bgwriter_thread_num = 1;
    }
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
    init_bgwriter_thread_num();
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

                newval = guc_strdup(FATAL, *source->variable);

                if (strlen(*destination->variable) == strlen(*source->variable) &&
                    0 == strncmp(*destination->variable, *source->variable, strlen(*destination->variable)))
                    break;

                if (destination->check_hook) {
                    if (!call_string_check_hook(destination, &newval, &newextra, destination->gen.source, LOG)) {
                        ereport(LOG,
                            (errmsg("Failed to initialize %s to \"%s\" for stream thread",
                                destination->gen.name,
                                newval ? newval : "")));
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
            if (rename(TempConfigFileName, g_instance.attr.attr_common.ConfigFileName) != 0) {
                write_stderr("%s: rename file %s to %s failed, errmsg: %s\n",
                    progname,
                    TempConfigFileName,
                    g_instance.attr.attr_common.ConfigFileName,
                    gs_strerror(errno));
                pfree(configdir);
                return false;
            }
        }
    }

    ProcessConfigFile(PGC_POSTMASTER);
    synchronous_commit = (volatile int)u_sess->attr.attr_storage.guc_synchronous_commit;
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
        int rs;
        char* set_str = get_set_string();

        if (set_str != NULL) {
            char tmpName[MAX_PARAM_LEN + 1] = {0};
            char *gucName = GetGucName(set_str, tmpName);
            rs = PoolManagerSetCommand(POOL_CMD_GLOBAL_SET, set_str, gucName);
            if (rs < 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Communication failure, failed to send set commands to pool.")));
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
                 * when \parallel in gsql is on, we use "set search_path to pg_temp_XXX"
                 * to tell new started parallel postgres which temp namespace should use.
                 * but we should not clean the temp namespace when parallel postgres
                 * quiting, so a flag deleteTempOnQuiting is set to prevent clean
                 * temp namespace when postgres quiting.
                 */
                if ((strcasecmp(name, "search_path") == 0 || strcasecmp(name, "current_schema") == 0) &&
                    (isTempNamespaceName(value) || isTempNamespaceNameWithQuote(value))) {

                    char* schemaNameList = getSchemaNameList(value);
                    if (schemaNameList == NULL) {
                        return 1;
                    } else {
                        newval = guc_strdup(elevel, schemaNameList);
                        pfree(schemaNameList);
                    }
                } else {
                    /* The value passed by the caller could be transient, so we always strdup it. */
                    newval = guc_strdup(elevel, value);
                }

                if (newval == NULL) {
                    return 0;
                }

                if (!validate_conf_option(record, name, value, source, elevel, false, &newval, &newextra)) {
                    pfree_ext(newval);
                    return 0;
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
        "modify_initial_password",
        "enable_access_server_directory",
        "enable_copy_server_files",
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

    if (!validate_conf_option(record, name, value, PGC_S_FILE, ERROR, true, NULL, &newextra))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid value for parameter \"%s\": \"%s\"", name, value)));

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
            ereport(ERROR, (errcode(ERRCODE_SET_QUERY), errmsg("Postgres-XC: ERROR SET query")));
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
void GetPGVariable(const char* name, DestReceiver* dest)
{
    if (guc_name_compare(name, "all") == 0)
        ShowAllGUCConfig(dest);
    else
        ShowGUCConfigOption(name, dest);
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
static void ShowAllGUCConfig(DestReceiver* dest)
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

        if ((conf->flags & GUC_NO_SHOW_ALL) || ((conf->flags & GUC_SUPERUSER_ONLY) && !am_superuser))
            continue;

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
 * Brief    : Check the value of statistics memory can not bigger than 50 percent of max process memory
 * Description:
 * Notes    :
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

/*
 * Brief    : Check the value of history memory can not bigger than 50 percent of max process memory
 * Description:
 * Notes    :
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
 * Brief    : assignthe value of statistics memory to g_statManager.max_collectinfo_num
 * Description:
 * Notes    :
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

/*
 * Brief    : assignthe value of history memory to g_statManager.max_detail_num
 * Description:
 * Notes    :
 */
static void assign_history_memory(int newval, void* extra)
{
    if (currentGucContext == PGC_SIGHUP) {
        u_sess->attr.attr_resource.session_history_memory = newval;
        g_instance.wlm_cxt->stat_manager.max_detail_num = newval * 1024 / sizeof(WLMStmtDetail);
    }
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

static bool check_temp_buffers(int* newval, void** extra, GucSource source)
{
    /*
     * Once local buffers have been initialized, it's too late to change this.
     */
    if (u_sess->storage_cxt.NLocBuffer && u_sess->storage_cxt.NLocBuffer != *newval) {
        GUC_check_errdetail(
            "\"temp_buffers\" cannot be changed after any temporary tables have been accessed in the session.");
        return false;
    }

    return true;
}

static bool check_fencedUDFMemoryLimit(int* newval, void** extra, GucSource source)
{
    if (*newval > g_instance.attr.attr_sql.UDFWorkerMemHardLimit) {
        GUC_check_errdetail("\"FencedUDFMemoryLimit\" cannot be greater than \"UDFWorkerMemHardLimit\".");
        return false;
    }

    return true;
}

static bool check_auto_explain_level(int* newval, void** extra, GucSource source)
{
   if (*newval == LOG || *newval == NOTICE) {
       return true;
   }
   GUC_check_errdetail("auto_explain_level should be either 'LOG' or 'NOTICE'");
   return false;
}

static bool check_udf_memory_limit(int* newval, void** extra, GucSource source)
{
    if (*newval > g_instance.attr.attr_memory.max_process_memory) {
        GUC_check_errdetail(
            "\"udf_memory_limit\" cannot be greater than \"max_process_memory\". max_process_memory is %dkB",
            g_instance.attr.attr_memory.max_process_memory);
        return false;
    }

    return true;
}

static bool check_phony_autocommit(bool* newval, void** extra, GucSource source)
{
    if (!*newval) {
        GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        GUC_check_errmsg("SET AUTOCOMMIT TO OFF is no longer supported");
        return false;
    }

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

static bool check_syscache_threshold_gpc(int* newval, void** extra, GucSource source)
{
    /* in case local_syscache_threshold is too low cause gpc does not take effect, we set local_syscache_threshold
       at least 16MB if gpc is on. */
    if (g_instance.attr.attr_common.enable_global_plancache && *newval < 16 * 1024) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("cannot set local_syscache_threshold to %dMB in case gpc is on but not valid."
                             "Set local_syscache_threshold to 16MB default.", *newval / 1024)));
        g_instance.attr.attr_memory.local_syscache_threshold = 16 * 1024;
    }

    return true;
}

static bool check_ssl(bool* newval, void** extra, GucSource source)
{
#ifndef USE_SSL

    if (*newval) {
        GUC_check_errmsg("SSL is not supported by this build");
        return false;
    }

#endif
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

#ifdef PGXC
static bool check_pooler_maximum_idle_time(int* newval, void** extra, GucSource source)
{
    if (*newval < 0) {
        ereport(ERROR, (errmsg("GaussDB can't support idle time less than 0 or more than %d seconds.", INT_MAX)));
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

static bool check_enable_data_replicate(bool* newval, void** extra, GucSource source)
{
    /* Always disable data replication in multi standbys mode*/
    if (IS_DN_MULTI_STANDYS_MODE()) {
        *newval = false;
    }
    return true;
}

/*
 * Check if the kernel version greater than suse sp2 (3.0.13)
 * Only a warning is printed to log.
 * Returning false will cause FATAL error and it will not be good.
 */
static bool check_sctp_support(bool* newval, void** extra, GucSource source)
{
    if (*newval == false) {
        GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        GUC_check_errmsg("SET COMM_TCP_MODE TO OFF is no longer supported");
        *newval = true;
    }

    return true;
}
#endif

static bool check_canonical_path(char** newval, void** extra, GucSource source)
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

static bool check_directory(char** newval, void** extra, GucSource source)
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
static void assign_comm_debug_mode(bool newval, void* extra)
{
    gs_set_debug_mode(newval);
    return;
}
static void assign_comm_stat_mode(bool newval, void* extra)
{
    gs_set_stat_mode(newval);
    return;
}
static void assign_comm_timer_mode(bool newval, void* extra)
{
    gs_set_timer_mode(newval);
    return;
}
static void assign_comm_no_delay(bool newval, void* extra)
{
    gs_set_no_delay(newval);
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

static void assign_comm_ackchk_time(int newval, void* extra)
{
    gs_set_ackchk_time(newval);
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

static bool check_adio_debug_guc(bool* newval, void** extra, GucSource source)
{
    /* This value is always false no matter how the user sets it.  */
    if (*newval == true) {
        *newval = false;
    }

    return true;
}

static bool check_adio_function_guc(bool* newval, void** extra, GucSource source)
{
    /* This value is always false no matter how the user sets it.  */
    if (*newval == true) {
        *newval = false;
    }

    return true;
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

static const char* show_archive_command(void)
{
    if (XLogArchivingActive())
        return u_sess->attr.attr_storage.XLogArchiveCommand;
    else
        return "(disabled)";
}

static bool check_maxconnections(int* newval, void** extra, GucSource source)
{
#ifdef PGXC
    if (IS_PGXC_COORDINATOR && *newval > MAX_BACKENDS) {
        ereport(LOG, (errmsg("PGXC can't support max_connections more than %d.", MAX_BACKENDS)));
        return false;
    }
#endif
    if (*newval + g_instance.attr.attr_storage.autovacuum_max_workers + g_instance.attr.attr_sql.job_queue_processes +
            AUXILIARY_BACKENDS + AV_LAUNCHER_PROCS + g_instance.attr.attr_network.maxInnerToolConnections >
        MAX_BACKENDS) {
        return false;
    }
    return true;
}

static bool CheckMaxInnerToolConnections(int* newval, void** extra, GucSource source)
{
    if (*newval + g_instance.attr.attr_storage.autovacuum_max_workers + g_instance.attr.attr_sql.job_queue_processes +
            g_instance.attr.attr_network.MaxConnections + AUXILIARY_BACKENDS + AV_LAUNCHER_PROCS > MAX_BACKENDS) {
        return false;
    }
    return true;
}

static bool check_autovacuum_max_workers(int* newval, void** extra, GucSource source)
{
    if (g_instance.attr.attr_network.MaxConnections + *newval + g_instance.attr.attr_sql.job_queue_processes +
            AUXILIARY_BACKENDS + AV_LAUNCHER_PROCS + g_instance.attr.attr_network.maxInnerToolConnections >
        MAX_BACKENDS) {
        return false;
    }
    return true;
}

/*
 * Description: Check wheth out of max backends after max job worker threads.
 *
 * Parameters:
 * @in newval: the param of job_queue_processes.
 * Returns: bool
 */
static bool check_job_max_workers(int* newval, void** extra, GucSource source)
{
    if (g_instance.attr.attr_network.MaxConnections + g_instance.attr.attr_storage.autovacuum_max_workers + *newval +
            AUXILIARY_BACKENDS + AV_LAUNCHER_PROCS + g_instance.attr.attr_network.maxInnerToolConnections >
        MAX_BACKENDS) {
        return false;
    }
    return true;
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

/*
 * @Description: parse the input of u_sess->opt_cxt.query_dop to get the u_sess->opt_cxt.parallel_debug_mode
 *
 * @param[IN] newval: new value
 * @param[IN] extra: N/A
 * @param[IN] source: N/A
 * @return: true if value is valid
 */
static bool parse_query_dop(int* newval, void** extra, GucSource source)
{
    int dop_mark = *newval;

#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
    if ((dop_mark > 2001) && (dop_mark <= (2000 + MAX_QUERY_DOP))) {
        /*
         * This mode is for llt, we reduce the parallel threshold to parallelize
         * as much plan as possible, and do not print parallel plan so there
         * will not be any fail in fastcheck.
         */
        u_sess->opt_cxt.smp_thread_cost = 0;
        u_sess->opt_cxt.parallel_debug_mode = LLT_MODE;
        u_sess->opt_cxt.max_query_dop = -1; /* turn off dynamic smp */
    } else if ((dop_mark > 1001) && (dop_mark <= (1000 + MAX_QUERY_DOP))) {
        /*
         * This mode is for function test, we reduce the parallel threshold
         * to parallelize as much plan as possible, and print parallel plan
         * so we can analyze the parallel plan.
         */
        u_sess->opt_cxt.smp_thread_cost = 0;
        u_sess->opt_cxt.parallel_debug_mode = DEBUG_MODE;
        u_sess->opt_cxt.max_query_dop = -1; /* turn off dynamic smp */
    } else if ((dop_mark > 0) && (dop_mark <= MAX_QUERY_DOP)) {
        /*
         * This mode is for performance test and release, we enhance the threshold
         * to reject low cost parallel groups, so that the performance will not
         * desend because of SMP.
         */
        u_sess->opt_cxt.smp_thread_cost = DEFAULT_SMP_THREAD_COST;
        u_sess->opt_cxt.parallel_debug_mode = DEFAULT_MODE;
        u_sess->opt_cxt.max_query_dop = -1; /* turn off dynamic smp */
    } else if (dop_mark >= MIN_QUERY_DOP && dop_mark <= 0) {
        u_sess->opt_cxt.max_query_dop = abs(dop_mark);
        /* turn on debug mode when dynsmp */
        u_sess->opt_cxt.smp_thread_cost = 0;
        u_sess->opt_cxt.parallel_debug_mode = DEBUG_MODE;
    } else {
        /* should not reach here */
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Current degree of parallelism can only be set within [-64,64]")));
    }

#else
    if (dop_mark >= MIN_QUERY_DOP && dop_mark <= MAX_QUERY_DOP) {
        /*
         * This mode is for performance test and release, we enhance the threshold
         * to reject low cost parallel groups, so that the performance will not
         * desend because of SMP.
         */
        u_sess->opt_cxt.smp_thread_cost = DEFAULT_SMP_THREAD_COST;
        u_sess->opt_cxt.parallel_debug_mode = DEFAULT_MODE;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Current degree of parallelism can only be set within [-64,64]")));
    }
#endif

    return true;
}

/*
 * @Description: assign new value to query_dop.
 *
 * @param[IN] newval: new value
 * @param[IN] extra: N/A
 * @return: void
 */
static void AssignQueryDop(int newval, void* extra)
{
    if (newval > 0) {
        u_sess->opt_cxt.query_dop = newval % 1000;
        u_sess->opt_cxt.query_dop_store = u_sess->opt_cxt.query_dop;
        u_sess->opt_cxt.max_query_dop = -1; /* turn off dynamic smp */
    } else if (newval <= 0) {
        if (newval == -1) {
            /* -1 means not parallel */
            u_sess->opt_cxt.query_dop = 1;
            u_sess->opt_cxt.query_dop_store = 1;
        } else {
            /*
             * only set query_dop here to indicate we have
             * dop turned on. but later we generate optimal dop
             */
            u_sess->opt_cxt.query_dop = 2;
            u_sess->opt_cxt.query_dop_store = 2;
        }

        u_sess->opt_cxt.max_query_dop = abs(newval);
    }
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

static bool check_ssl_ciphers(char** newval, void**extra, GucSource)
{
	char* cipherStr = NULL;
    char* cipherStr_tmp = NULL;
    char* token = NULL;
    int counter = 1;
    char** ciphers_list = NULL;
    bool find_ciphers_in_list = false;
    int i = 0;
    char* ptok = NULL;
    const char* ssl_ciphers_list[] = {"DHE-RSA-AES256-GCM-SHA384",
    "DHE-RSA-AES128-GCM-SHA256",
    "DHE-RSA-AES256-CCM",
    "DHE-RSA-AES128-CCM",
    NULL};
    if (*newval == NULL || **newval == '\0' || **newval == ';') {
        ereport(ERROR, (errmsg("sslciphers can not be null")));
    } else if (strcasecmp(*newval, "ALL") == 0){
        return true;
    } else {
        cipherStr = (char*)strchr(*newval, ';'); /*if the sslciphers does not contain character ';',the count is 1*/
        while (cipherStr != NULL) {
            counter++;
            cipherStr++;
            if (*cipherStr == '\0') {
                break;
            }
            if (cipherStr == strchr(cipherStr, ';')) {
                ereport(ERROR, (errmsg("unrecognized ssl ciphers name: \"%s\"", *newval)));
            }
            cipherStr = strchr(cipherStr, ';');
        }
        ciphers_list = (char**)palloc(counter * sizeof(char*));

        Assert(ciphers_list != NULL);
        if (ciphers_list == NULL) {
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("malloc failed")));
        }

        cipherStr_tmp = pstrdup(*newval);
        if (cipherStr_tmp == NULL) {
            pfree_ext(ciphers_list);
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("malloc failed")));
        }
        token = strtok_r(cipherStr_tmp, ";", &ptok);
        while (token != NULL) {
            for (int j = 0; ssl_ciphers_list[j] != NULL; j++) {
                if (strlen(ssl_ciphers_list[j]) == strlen(token) &&
                    strncmp(ssl_ciphers_list[j], token, strlen(token)) == 0) {
                    ciphers_list[i] = (char*)ssl_ciphers_list[j];
                    find_ciphers_in_list = true;
                    break;
                }
            }
            if (!find_ciphers_in_list) {
                const int maxCipherStrLen = 64;
                char errormessage[maxCipherStrLen] = {0};
                errno_t errorno = EOK;
                errorno = strncpy_s(errormessage, sizeof(errormessage), token, sizeof(errormessage) - 1);
                securec_check(errorno, cipherStr_tmp, ciphers_list, "\0");
                errormessage[maxCipherStrLen-1] = '\0';
                pfree_ext(cipherStr_tmp);
                pfree_ext(ciphers_list);
                ereport(ERROR, (errmsg("unrecognized ssl ciphers name: \"%s\"", errormessage)));
            }
            token = strtok_r(NULL, ";", &ptok);
            i++;
            find_ciphers_in_list = false;
        }
    }
    pfree_ext(cipherStr_tmp);
    pfree_ext(ciphers_list);
    return true;
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
}

static void assign_connection_info(const char* newval, void* extra)
{
    /* Update the pg_stat_activity view */
    pgstat_report_conninfo(newval);
}

static bool check_application_type(int* newval, void** extra, GucSource source)
{
    switch (source) {
        case PGC_S_DYNAMIC_DEFAULT:
        case PGC_S_ENV_VAR:
        case PGC_S_ARGV:
            return false;

        case PGC_S_FILE:
            *newval = NOT_PERFECT_SHARDING_TYPE;
            return true;

        case PGC_S_DATABASE:
        case PGC_S_USER:
        case PGC_S_DATABASE_USER:
        case PGC_S_INTERACTIVE:
        case PGC_S_TEST:
            *newval = NOT_PERFECT_SHARDING_TYPE;
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

static bool check_cgroup_name(char** newval, void** extra, GucSource source)
{
    /* Only allow clean ASCII chars in the control group name */
    char* p = NULL;

    for (p = *newval; *p; p++) {
        if (*p < 32 || *p > 126)
            *p = '?';
    }

    p = *newval;

    if (StringIsValid(p) && IS_PGXC_COORDINATOR && t_thrd.shemem_ptr_cxt.MyBEEntry &&
        (currentGucContext == PGC_SUSET || currentGucContext == PGC_USERSET)) {
        if (0 == g_instance.wlm_cxt->gscgroup_init_done)
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
    if (IS_PGXC_COORDINATOR && t_thrd.shemem_ptr_cxt.MyBEEntry && newval && *newval) {
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

static bool check_query_band_name(char** newval, void** extra, GucSource source)
{
    /* Only allow clean ASCII chars in the application name */
    char* p = NULL;

    for (p = *newval; *p; p++) {
        if (*p < 32 || *p > 126)
            *p = '?';
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
        if (*p < 32 || *p > 126)
            *p = '?';
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
 * arguments:
 */
static const char* show_session_respool(void)
{
    return u_sess->wlm_cxt->session_respool;
}

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
    if (p && *p != '\0' && (NULL == (q = strchr(p, ':')) || 0 == isdigit(*(q + 1))))
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

/* function: assign_collect_timer
 * description: assign collect timer
 * arguments: standard GUC arguments
 */
static void assign_collect_timer(int newval, void* extra)
{
#define WLM_COLLECT_INVALID_TIME 5
#define WLM_COLLECT_MIN_TIME 30

    if (newval < WLM_COLLECT_INVALID_TIME && u_sess->utils_cxt.guc_new_value)
        *u_sess->utils_cxt.guc_new_value = WLM_COLLECT_MIN_TIME;
}

static bool check_statement_mem(int* newval, void** extra, GucSource source)
{
    if ((*newval < 0) || (*newval > 0 && *newval < SIMPLE_THRESHOLD)) {
        if (source != PGC_S_FILE)
            ereport(WARNING,
                (errmsg(
                    "query mem can not be set lower than %dMB, so the guc variable is not avaiable.", MEM_THRESHOLD)));
        *newval = 0;
    }

    return true;
}

static bool check_statement_max_mem(int* newval, void** extra, GucSource source)
{
    if ((*newval < 0) || (*newval > 0 && *newval < SIMPLE_THRESHOLD)) {
        if (source != PGC_S_FILE)
            ereport(WARNING,
                (errmsg("query max mem can not be set lower than %dMB, so the guc variable is not avaiable.",
                    MEM_THRESHOLD)));
        *newval = 0;
    }

    return true;
}

static const char* show_unix_socket_permissions(void)
{
    char* buf = t_thrd.buf_cxt.show_unix_socket_permissions_buf;
    int size = sizeof(t_thrd.buf_cxt.show_unix_socket_permissions_buf);
    int rcs = 0;

    rcs = snprintf_s(buf, size, size - 1, "%04o", g_instance.attr.attr_network.Unix_socket_permissions);
    securec_check_ss(rcs, "\0", "\0");
    return buf;
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
 * Brief			: Check the value of int-type parameter for security
 * Description		:
 * Notes			:
 */
static bool check_int_parameter(int* newval, void** extra, GucSource source)
{
    if (*newval >= 0) {
        return true;
    } else {
        return false;
    }
}

/*
 * Brief			: Check the value of double-type parameter for security
 * Description		:
 * Notes			:
 */
static bool check_double_parameter(double* newval, void** extra, GucSource source)
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

static bool CheckReplChannel(const char* ChannelInfo)
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
 * Brief			: Get the number of right-format-channel in a replconninfo.
 * Description		: Check each channel of a replconninfo, and return the number of the right-format ones.
 * Notes			:
 */
static int GetLengthAndCheckReplConn(const char* ConnInfoList)
{
    int repl_len = 0;
    char* ReplStr = NULL;
    char* token = NULL;
    char* tmp_token = NULL;

    if (NULL == ConnInfoList) {
        return repl_len;
    } else {
        ReplStr = pstrdup(ConnInfoList);
        if (NULL == ReplStr) {
            ereport(LOG, (errmsg("ConnInfoList is null")));
            return repl_len;
        }
        token = strtok_r(ReplStr, ",", &tmp_token);
        while (token != NULL) {
            if (CheckReplChannel(token)) {
                repl_len++;
            }

            token = strtok_r(NULL, ",", &tmp_token);
        }
    }

    pfree(ReplStr);
    return repl_len;
}

/*
 * @@GaussDB@@
 * Brief			: Parse one replconninfo, and get the connect info of each channel.
 * Description		: Parse one replconninfo, get the connect info of each channel.
 *					  Then fill the ReplConnInfo array, and return it.
 * Notes			:
 */
static ReplConnInfo* ParseReplConnInfo(const char* ConnInfoList, int* InfoLength)
{
    ReplConnInfo* repl = NULL;

    int repl_length = 0;
    char* iter = NULL;
    char* pNext = NULL;
    char* ReplStr = NULL;
    char* token = NULL;
    char* tmp_token = NULL;
    char* ptr = NULL;
    int parsed = 0;
    int iplen = 0;
    int portlen = strlen("localport");
    int rportlen = strlen("remoteport");
    int localservice_len = strlen("localservice");
    int remoteservice_len = strlen("remoteservice");
    int local_heartbeat_len = strlen("localheartbeatport");
    int remote_heartbeat_len = strlen("remoteheartbeatport");
    int cascadeLen = strlen("iscascade");

    errno_t errorno = EOK;

    if (InfoLength != NULL) {
        *InfoLength = 0;
    }

    if (NULL == ConnInfoList || NULL == InfoLength) {
        ereport(LOG, (errmsg("parameter error in ParseReplConnInfo()")));
        return NULL;
    } else {
        ReplStr = pstrdup(ConnInfoList);
        if (NULL == ReplStr) {
            return NULL;
        }

        ptr = ReplStr;
        while (*ptr != '\0') {
            if (*ptr != ' ')
                break;
            ptr++;
        }
        if (*ptr == '\0') {
            pfree(ReplStr);
            return NULL;
        }

        repl_length = GetLengthAndCheckReplConn(ReplStr);
        if (0 == repl_length) {
            pfree(ReplStr);
            return NULL;
        }
        repl = (ReplConnInfo*)MemoryContextAlloc(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB),
            sizeof(ReplConnInfo) * repl_length);
        if (NULL == repl) {
            pfree(ReplStr);
            return NULL;
        }
        errorno = memset_s(repl, sizeof(ReplConnInfo) * repl_length, 0, sizeof(ReplConnInfo) * repl_length);
        securec_check(errorno, "\0", "\0");
        token = strtok_r(ReplStr, ",", &tmp_token);
        while (token != NULL) {
            /* localhost */
            iter = strstr(token, "localhost");
            if (NULL == iter) {
                token = strtok_r(NULL, ",", &tmp_token);
                continue;
            }
            iter += strlen("localhost");
            while (' ' == *iter || '=' == *iter) {
                iter++;
            }
            if (!isdigit(*iter) && *iter != ':' && !isalpha(*iter)) {
                token = strtok_r(NULL, ",", &tmp_token);
                continue;
            }
            pNext = iter;
            iplen = 0;
            while (*pNext != ' ' && 0 != strncmp(pNext, "localport", portlen)) {
                iplen++;
                pNext++;
            }
            errorno = strncpy_s(repl[parsed].localhost, IP_LEN - 1, iter, iplen);
            securec_check(errorno, "\0", "\0");
            repl[parsed].localhost[iplen] = '\0';

            /* localport */
            iter = strstr(token, "localport");
            if (NULL == iter) {
                token = strtok_r(NULL, ",", &tmp_token);
                continue;
            }
            iter += portlen;
            while (' ' == *iter || '=' == *iter) {
                iter++;
            }
            if (!isdigit(*iter)) {
                token = strtok_r(NULL, ",", &tmp_token);
                continue;
            }
            repl[parsed].localport = atoi(iter);

            /* localservice */
            iter = strstr(token, "localservice");
            if (NULL != iter) {
                iter += localservice_len;
                while (' ' == *iter || '=' == *iter) {
                    iter++;
                }

                if (isdigit(*iter)) {
                    repl[parsed].localservice = atoi(iter);
                }
            }

            /* localheartbeatport */
            iter = strstr(token, "localheartbeatport");
            if (NULL != iter) {
                iter += local_heartbeat_len;
                while (' ' == *iter || '=' == *iter) {
                    iter++;
                }

                if (isdigit(*iter)) {
                    repl[parsed].localheartbeatport = atoi(iter);
                }
            }

            /* remotehost */
            iter = strstr(token, "remotehost");
            if (NULL == iter) {
                token = strtok_r(NULL, ",", &tmp_token);
                continue;
            }
            iter += strlen("remotehost");
            while (' ' == *iter || '=' == *iter) {
                iter++;
            }
            if (!isdigit(*iter) && *iter != ':' && !isalpha(*iter)) {
                token = strtok_r(NULL, ",", &tmp_token);
                continue;
            }
            pNext = iter;
            iplen = 0;
            while (*pNext != ' ' && 0 != strncmp(pNext, "remoteport", rportlen)) {
                iplen++;
                pNext++;
            }
            iplen = (iplen >= IP_LEN) ? (IP_LEN - 1) : iplen;
            errorno = strncpy_s(repl[parsed].remotehost, IP_LEN, iter, iplen);
            securec_check(errorno, "\0", "\0");
            repl[parsed].remotehost[iplen] = '\0';

            /* remoteport */
            iter = strstr(token, "remoteport");
            if (NULL == iter) {
                token = strtok_r(NULL, ",", &tmp_token);
                continue;
            }
            iter += rportlen;
            while (' ' == *iter || '=' == *iter) {
                iter++;
            }
            if (!isdigit(*iter)) {
                token = strtok_r(NULL, ",", &tmp_token);
                continue;
            }
            repl[parsed].remoteport = atoi(iter);

            /* remoteservice */
            iter = strstr(token, "remoteservice");
            if (NULL != iter) {
                iter += remoteservice_len;
                while (' ' == *iter || '=' == *iter) {
                    iter++;
                }

                if (isdigit(*iter)) {
                    repl[parsed].remoteservice = atoi(iter);
                }
            }

            /* remote heartbeat port */
            iter = strstr(token, "remoteheartbeatport");
            if (NULL != iter) {
                iter += remote_heartbeat_len;
                while (' ' == *iter || '=' == *iter) {
                    iter++;
                }

                if (isdigit(*iter)) {
                    repl[parsed].remoteheartbeatport = atoi(iter);
                }
            }

            /* is a cascade standby */
            iter = strstr(token, "iscascade");
            if (NULL != iter) {
                iter += cascadeLen;

                if (strstr(iter, "true") != NULL) {
                    repl[parsed].isCascade = true;
                }
            }

            token = strtok_r(NULL, ",", &tmp_token);
            parsed++;
        }
    }

    pfree(ReplStr);

    *InfoLength = repl_length;

    return repl;
}

static bool check_replication_type(int* newval, void** extra, GucSource source)
{
    if (*newval == RT_WITH_MULTI_STANDBY) {
        if (!IsInitdb && (is_feature_disabled(DOUBLE_LIVE_DISASTER_RECOVERY_IN_THE_SAME_CITY) ||
                             is_feature_disabled(DISASTER_RECOVERY_IN_TWO_PLACES_AND_THREE_CENTRES) ||
                             is_feature_disabled(HA_SINGLE_PRIMARY_MULTI_STANDBY)))
            ereport(FATAL,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("replication_type is not allowed set 1 "
                           "in Current Version. Set to default (0).")));
#ifndef ENABLE_MULTIPLE_NODES
    } else {
        ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("replication_type is only allowed set to 1, newval=%d.", *newval)));
#endif
    }
    return true;
}

/*
 * @@GaussDB@@
 * Brief			: Check all the channel of one replconninfo.
 * Description		: Check each channel of one replconninfo. If one channel'format is not right, then return false.
 * Notes			:
 */
static bool check_replconninfo(char** newval, void** extra, GucSource source)
{
    char* ReplStr = NULL;
    char* token = NULL;
    char* tmp_token = NULL;
    char* ptr = NULL;
    if (NULL == newval) {
        return false;
    } else {
        ReplStr = pstrdup(*newval);
        if (NULL == ReplStr) {
            return false;
        }
        ptr = ReplStr;
        while (*ptr != '\0') {
            if (*ptr != ' ')
                break;
            ptr++;
        }
        if (*ptr == '\0') {
            pfree(ReplStr);
            return true;
        }
        token = strtok_r(ReplStr, ",", &tmp_token);
        while (token != NULL) {
            if (!CheckReplChannel(token)) {
                pfree(ReplStr);
                return false;
            }

            token = strtok_r(NULL, ",", &tmp_token);
        }
    }

    pfree(ReplStr);
    return true;
}

#ifndef ENABLE_MULTIPLE_NODES
/*
 * @@GaussDB@@
 * Brief			: Determine if all eight replconninfos are empty.
 * Description		:
 * Notes			:
 */
static inline bool GetReplCurArrayIsNull()
{
    for (int i = 1; i < MAX_REPLNODE_NUM; i++) {
        if (t_thrd.postmaster_cxt.ReplConnArray[i] != NULL) {
            return false;
        }
    }
    return true;
}
#endif

/*
 * @@GaussDB@@
 * Brief			: Parse replconninfo1.
 * Description		:
 * Notes			:
 */
static void assign_replconninfo1(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[1] != NULL)
        pfree(t_thrd.postmaster_cxt.ReplConnArray[1]);

    /*
     * We may need to move ReplConnArray to session level.
     * At present, ReplConnArray is only used by PM, so it is safe.
     */
    t_thrd.postmaster_cxt.ReplConnArray[1] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[1] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.ReplConnInfoArr[1], newval) != 0) {
        t_thrd.postmaster_cxt.ReplConnChanged[1] = true;

#ifndef ENABLE_MULTIPLE_NODES
        /* perceive single --> primary_standby */
        if (t_thrd.postmaster_cxt.HaShmData != NULL &&
            t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE &&
            !GetReplCurArrayIsNull()) {
                t_thrd.postmaster_cxt.HaShmData->current_mode = PRIMARY_MODE;
        }
#endif
    }
}

/*
 * @@GaussDB@@
 * Brief			: Parse replconninfo2.
 * Description		:
 * Notes			:
 */
static void assign_replconninfo2(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[2] != NULL)
        pfree(t_thrd.postmaster_cxt.ReplConnArray[2]);

    t_thrd.postmaster_cxt.ReplConnArray[2] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[2] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.ReplConnInfoArr[2], newval) != 0) {
        t_thrd.postmaster_cxt.ReplConnChanged[2] = true;
    }
}

static void assign_replconninfo3(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[3])
        pfree(t_thrd.postmaster_cxt.ReplConnArray[3]);

    t_thrd.postmaster_cxt.ReplConnArray[3] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[3] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.ReplConnInfoArr[3], newval) != 0) {
        t_thrd.postmaster_cxt.ReplConnChanged[3] = true;
    }
}

static void assign_replconninfo4(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[4])
        pfree(t_thrd.postmaster_cxt.ReplConnArray[4]);

    t_thrd.postmaster_cxt.ReplConnArray[4] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[4] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.ReplConnInfoArr[4], newval) != 0) {
        t_thrd.postmaster_cxt.ReplConnChanged[4] = true;
    }
}

static void assign_replconninfo5(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[5])
        pfree(t_thrd.postmaster_cxt.ReplConnArray[5]);

    t_thrd.postmaster_cxt.ReplConnArray[5] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[5] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.ReplConnInfoArr[5], newval) != 0) {
        t_thrd.postmaster_cxt.ReplConnChanged[5] = true;
    }
}

static void assign_replconninfo6(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[6])
        pfree(t_thrd.postmaster_cxt.ReplConnArray[6]);

    t_thrd.postmaster_cxt.ReplConnArray[6] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[6] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.ReplConnInfoArr[6], newval) != 0) {
        t_thrd.postmaster_cxt.ReplConnChanged[6] = true;
    }
}

static void assign_replconninfo7(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[7])
        pfree(t_thrd.postmaster_cxt.ReplConnArray[7]);

    t_thrd.postmaster_cxt.ReplConnArray[7] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[7] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.ReplConnInfoArr[7], newval) != 0) {
        t_thrd.postmaster_cxt.ReplConnChanged[7] = true;
    }
}

#ifndef ENABLE_MULTIPLE_NODES
static void assign_replconninfo8(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[8]) {
        pfree(t_thrd.postmaster_cxt.ReplConnArray[8]);
    }

    t_thrd.postmaster_cxt.ReplConnArray[8] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[8] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.ReplConnInfoArr[8], newval) != 0) {
        t_thrd.postmaster_cxt.ReplConnChanged[8] = true;
    }
}
#endif
/*
 * @@GaussDB@@
 * Brief			: Check inlist2join info.
 * Description		: If inlist2join'format is not right, then return false.
 * Notes			:
 */
static bool check_inlist2joininfo(char** newval, void** extra, GucSource source)
{
    if (NULL == newval) {
        return false;
    }
    if (strcmp((const char*)*newval, (const char*)"disable") == 0 ||
        strcmp((const char*)*newval, (const char*)"cost_base") == 0 ||
        strcmp((const char*)*newval, (const char*)"rule_base") == 0) {
        return true;
    } else if (atoi((const char*)*newval) <= 0) {
        GUC_check_errdetail(
            "Available values: disable, cost_base, rule_base, or any positive integer as a inlist2join threshold");
        return false;
    } else
        return true;
}

/*
 * @@GaussDB@@
 * Brief			:
 * Description		:  parse inlist2join info.
 * Notes			:
 */
static void assign_inlist2joininfo(const char* newval, void* extra)
{
    int threshold = 0;

    if (strcmp((const char*)newval, (const char*)"disable") == 0) {
        u_sess->opt_cxt.qrw_inlist2join_optmode = QRW_INLIST2JOIN_DISABLE;
    } else if (strcmp((const char*)newval, (const char*)"cost_base") == 0) {
        u_sess->opt_cxt.qrw_inlist2join_optmode = QRW_INLIST2JOIN_CBO;
    } else if (strcmp((const char*)newval, (const char*)"rule_base") == 0) {
        u_sess->opt_cxt.qrw_inlist2join_optmode = QRW_INLIST2JOIN_FORCE;
    } else {
        threshold = atoi((const char*)newval);
        if (threshold > 0) {
            u_sess->opt_cxt.qrw_inlist2join_optmode = threshold;
        }
    }
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
 * @Description: show the content of GUC "logging_module"
 * @Return: logging_module descripting text
 * @See also:
 */
static const char* logging_module_guc_show(void)
{
    /*
     * showing results is as following:
     *     ALL,on(mod1,mod2,...),off(mod3,mod4,...)
     */
    StringInfoData outbuf;
    initStringInfo(&outbuf);

    const char* mod_name = NULL;
    bool rm_last_delim = false;

    /* copy the special "ALL" */
    mod_name = get_valid_module_name(MOD_ALL);
    appendStringInfo(&outbuf, "%s%c", mod_name, MOD_DELIMITER);

    /* copy the string "on(" */
    appendStringInfo(&outbuf, "on(");

    /* copy each module name whose logging is enable */
    rm_last_delim = false;
    for (int i = MOD_ALL + 1; i < MOD_MAX; i++) {
        if (module_logging_is_on((ModuleId)i)) {
            /* FOR condition and module_logging_is_on() will assure its requirement.  */
            mod_name = get_valid_module_name((ModuleId)i);
            appendStringInfo(&outbuf, "%s%c", mod_name, MOD_DELIMITER);
            rm_last_delim = true;
        }
    }
    if (rm_last_delim) {
        --outbuf.len;
        outbuf.data[outbuf.len] = '\0';
    }

    /* copy the string "),off(" */
    appendStringInfo(&outbuf, "),off(");

    /* copy each module name whose logging is disable */
    rm_last_delim = false;
    for (int i = MOD_ALL + 1; i < MOD_MAX; i++) {
        if (!module_logging_is_on((ModuleId)i)) {
            /* FOR condition will assure its requirement.  */
            mod_name = get_valid_module_name((ModuleId)i);
            appendStringInfo(&outbuf, "%s%c", mod_name, MOD_DELIMITER);
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

/*
 * @Description: check whether new value is well-formatted for GUC logging_module.
 * @IN extra: unused
 * @IN newval: new value
 * @IN source: unused
 * @Return: if newval is well-formatted, return true; otherwise return false.
 * @See also:
 */
static bool logging_module_check(char** newval, void** extra, GucSource source)
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
static void logging_module_guc_assign(const char* newval, void* extra)
{
    ((void)extra); /* not used argument */

    char* tmpNewVal = pstrdup(newval);
    /* logging_module_check() will be called first, so here don't care returned value */
    (void)logging_module_guc_check<true>(tmpNewVal);
    pfree(tmpNewVal);
}

/*
 * @Description: update plog_merge_age GUC argument according new value.
 * @IN extra: unused
 * @IN newval: new value about merging plog age, whose unit is MS.
 * @See also:
 */
static void plog_merge_age_assign(int newval, void* extra)
{
    ((void)extra); /* not used argument */
    t_thrd.log_cxt.plog_msg_switch_tm.tv_sec = newval / MS_PER_S;
    t_thrd.log_cxt.plog_msg_switch_tm.tv_usec = (newval - MS_PER_S * t_thrd.log_cxt.plog_msg_switch_tm.tv_sec) * 1000;
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

#define atooid(x) ((Oid)strtoul((x), NULL, 10))

#define catalog_oid_list_length 7
#define proc_oid_list_length 2
#define type_oid_list_length 4
#define namespace_oid_list_length 2
#define general_oid_list_length 2

static bool check_and_assign_catalog_oids(List* elemlist)
{
    if (list_length(elemlist) != catalog_oid_list_length)
        return false;

    bool isshared = false;
    bool needstorage = false;
    Oid relid = InvalidOid;
    Oid comtypeid = InvalidOid;
    Oid toastrelid = InvalidOid;
    Oid indexrelid = InvalidOid;

    if (!parse_bool((char*)list_nth(elemlist, 1), &isshared) ||
        !parse_bool((char*)list_nth(elemlist, 2), &needstorage) ||
        (relid = atooid((char*)list_nth(elemlist, 3))) >= FirstBootstrapObjectId ||
        (comtypeid = atooid((char*)list_nth(elemlist, 4))) >= FirstBootstrapObjectId ||
        (toastrelid = atooid((char*)list_nth(elemlist, 5))) >= FirstBootstrapObjectId ||
        (indexrelid = atooid((char*)list_nth(elemlist, 6))) >= FirstBootstrapObjectId)
        return false;

    u_sess->upg_cxt.new_catalog_isshared = isshared;
    u_sess->upg_cxt.new_catalog_need_storage = needstorage;
    u_sess->upg_cxt.Inplace_upgrade_next_heap_pg_class_oid = relid;
    u_sess->upg_cxt.Inplace_upgrade_next_pg_type_oid = comtypeid;
    u_sess->upg_cxt.Inplace_upgrade_next_toast_pg_class_oid = toastrelid;
    u_sess->upg_cxt.Inplace_upgrade_next_index_pg_class_oid = indexrelid;
    if (isshared) {
        MemoryContext oldcxt;
        oldcxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));

        if (OidIsValid(relid))
            u_sess->upg_cxt.new_shared_catalog_list = lappend_oid(u_sess->upg_cxt.new_shared_catalog_list, relid);
        if (OidIsValid(toastrelid))
            u_sess->upg_cxt.new_shared_catalog_list = lappend_oid(u_sess->upg_cxt.new_shared_catalog_list, toastrelid);
        if (OidIsValid(indexrelid))
            u_sess->upg_cxt.new_shared_catalog_list = lappend_oid(u_sess->upg_cxt.new_shared_catalog_list, indexrelid);

        MemoryContextSwitchTo(oldcxt);
    }

    return true;
}

static bool check_and_assign_proc_oids(List* elemlist)
{
    if (list_length(elemlist) != proc_oid_list_length)
        return false;

    Oid procid = InvalidOid;

    if ((procid = atooid((char*)list_nth(elemlist, 1))) >= FirstBootstrapObjectId)
        return false;

    u_sess->upg_cxt.Inplace_upgrade_next_pg_proc_oid = procid;

    return true;
}

static bool check_and_assign_type_oids(List* elemlist)
{
    if (list_length(elemlist) != type_oid_list_length)
        return false;

    Oid typeoid = InvalidOid;
    Oid arraytypeid = InvalidOid;
    char typtype = TYPTYPE_BASE;

    if ((typeoid = atooid((char*)list_nth(elemlist, 1))) >= FirstBootstrapObjectId ||
        (arraytypeid = atooid((char*)list_nth(elemlist, 2))) >= FirstBootstrapObjectId ||
        ((typtype = *(char*)list_nth(elemlist, 3)) != TYPTYPE_BASE && typtype != TYPTYPE_PSEUDO))
        return false;

    u_sess->upg_cxt.Inplace_upgrade_next_pg_type_oid = typeoid;
    u_sess->upg_cxt.Inplace_upgrade_next_array_pg_type_oid = arraytypeid;
    u_sess->cmd_cxt.TypeCreateType = typtype;

    return true;
}

static bool check_and_assign_namespace_oids(List* elemlist)
{
    if (list_length(elemlist) != namespace_oid_list_length)
        return false;

    Oid namespaceid = InvalidOid;

    if ((namespaceid = atooid((char*)list_nth(elemlist, 1))) >= FirstBootstrapObjectId)
        return false;

    u_sess->upg_cxt.Inplace_upgrade_next_pg_namespace_oid = namespaceid;

    return true;
}

static bool check_and_assign_general_oids(List* elemlist)
{
    if (list_length(elemlist) != general_oid_list_length)
        return false;

    Oid generalid = InvalidOid;

    if ((generalid = atooid((char*)list_nth(elemlist, 1))) >= FirstBootstrapObjectId)
        return false;

    u_sess->upg_cxt.Inplace_upgrade_next_general_oid = generalid;

    return true;
}

static bool check_inplace_upgrade_next_oids(char** newval, void** extra, GucSource source)
{
    if (!u_sess->attr.attr_common.IsInplaceUpgrade && source != PGC_S_DEFAULT) {
        ereport(WARNING, (errmsg("Can't set next object oids while not performing upgrade.")));
        *newval = NULL;
    }

    if (*newval == NULL)
        return true;

    char* rawstring = NULL;
    List* elemlist = NIL;

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

    char* objectkind = (char*)list_nth(elemlist, 0);
    bool checkpass = false;

    if (pg_strcasecmp(objectkind, "IUO_CATALOG") == 0)
        checkpass = check_and_assign_catalog_oids(elemlist);
    else if (pg_strcasecmp(objectkind, "IUO_PROC") == 0)
        checkpass = check_and_assign_proc_oids(elemlist);
    else if (pg_strcasecmp(objectkind, "IUO_TYPE") == 0)
        checkpass = check_and_assign_type_oids(elemlist);
    else if (pg_strcasecmp(objectkind, "IUO_NAMESPACE") == 0)
        checkpass = check_and_assign_namespace_oids(elemlist);
    else if (pg_strcasecmp(objectkind, "IUO_GENERAL") == 0)
        checkpass = check_and_assign_general_oids(elemlist);
    else {
        GUC_check_errdetail("String of object kind is invalid.");
        pfree(rawstring);
        list_free(elemlist);
        return false;
    }

    pfree(rawstring);
    list_free(elemlist);

    if (!checkpass) {
        GUC_check_errdetail("Format of object oid assignment is invalid.");
        return false;
    } else
        return true;
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

static bool transparent_encrypt_kms_url_region_check(char** newval, void** extra, GucSource source)
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

typedef struct behavior_compat_entry {
    const char* name; /* name of behavior compat entry */
    int flag;         /* bit flag position */
} behavior_compat_entry;

static struct behavior_compat_entry behavior_compat_options[OPT_MAX] = {
    {"display_leading_zero", OPT_DISPLAY_LEADING_ZERO},
    {"end_month_calculate", OPT_END_MONTH_CALCULATE},
    {"compat_analyze_sample", OPT_COMPAT_ANALYZE_SAMPLE},
    {"bind_schema_tablespace", OPT_BIND_SCHEMA_TABLESPACE},
    {"return_null_string", OPT_RETURN_NS_OR_NULL},
    {"bind_procedure_searchpath", OPT_BIND_SEARCHPATH},
    {"unbind_divide_bound", OPT_UNBIND_DIVIDE_BOUND},
    {"correct_to_number", OPT_CORRECT_TO_NUMBER},
    {"compat_concat_variadic", OPT_CONCAT_VARIADIC},
    {"merge_update_multi", OPT_MEGRE_UPDATE_MULTI},
    {"convert_string_digit_to_numeric", OPT_CONVERT_TO_NUMERIC}};

/*
 * check_behavior_compat_options: GUC check_hook for behavior compat options
 */
static bool check_behavior_compat_options(char** newval, void** extra, GucSource source)
{
    char* rawstring = NULL;
    List* elemlist = NULL;
    ListCell* cell = NULL;
    int start = 0;

    /* Need a modifiable copy of string */
    rawstring = pstrdup(*newval);

    /* Parse string into list of identifiers */
    if (!SplitIdentifierString(rawstring, ',', &elemlist)) {
        /* syntax error in list */
        GUC_check_errdetail("invalid paramater for behavior compat information.");
        pfree(rawstring);
        list_free(elemlist);

        return false;
    }

    foreach (cell, elemlist) {
        const char* item = (const char*)lfirst(cell);
        bool nfound = true;

        for (start = 0; start < OPT_MAX; start++) {
            if (0 == strcmp(item, behavior_compat_options[start].name)) {
                nfound = false;
                break;
            }
        }

        if (nfound) {
            GUC_check_errdetail("invalid behavior compat option \"%s\"", item);
            pfree(rawstring);
            list_free(elemlist);
            return false;
        }
    }

    pfree(rawstring);
    list_free(elemlist);

    return true;
}

/*
 * assign_distribute_test_param: GUC assign_hook for distribute_test_param
 */
static void assign_behavior_compat_options(const char* newval, void* extra)
{
    char* rawstring = NULL;
    List* elemlist = NULL;
    ListCell* cell = NULL;
    int start = 0;
    int result = 0;

    rawstring = pstrdup(newval);
    (void)SplitIdentifierString(rawstring, ',', &elemlist);

    u_sess->utils_cxt.behavior_compat_flags = 0;
    foreach (cell, elemlist) {
        for (start = 0; start < OPT_MAX; start++) {
            const char* item = (const char*)lfirst(cell);

            if (0 == strcmp(item, behavior_compat_options[start].name))
                result += behavior_compat_options[start].flag;
        }
    }

    pfree(rawstring);
    list_free(elemlist);

    u_sess->utils_cxt.behavior_compat_flags = result;
}

static void assign_convert_string_to_digit(bool newval, void* extra)
{
    if (u_sess->attr.attr_sql.convert_string_to_digit != newval) {
        /*
         * Invalidate the old operator cache whenever the convert_string_to_digit
         * changed, as its change will modify type conversion priority which need
         * refresh operator cache.
         */
        InvalidateOprCacheCallBack(0, 0, 0);
    }
    return;
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
