/* --------------------------------------------------------------------
 * guc_sql.cpp
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
 * src/backend/utils/misc/guc/guc_sql.cpp
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
#include "access/dfs/dfs_insert.h"
#include "access/ustore/knl_whitebox_test.h"
#include "gs_bbox.h"
#include "catalog/namespace.h"
#include "catalog/pgxc_group.h"
#include "catalog/storage_gtt.h"
#include "commands/async.h"
#include "commands/copy.h"
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
#include "utils/guc_sql.h"

static bool parse_query_dop(int* newval, void** extra, GucSource source);
static void AssignQueryDop(int newval, void* extra);
static bool check_job_max_workers(int* newval, void** extra, GucSource source);
static bool check_statement_max_mem(int* newval, void** extra, GucSource source);
static bool check_statement_mem(int* newval, void** extra, GucSource source);
static bool check_fencedUDFMemoryLimit(int* newval, void** extra, GucSource source);
static bool check_udf_memory_limit(int* newval, void** extra, GucSource source);
static bool check_inlist2joininfo(char** newval, void** extra, GucSource source);
static void assign_inlist2joininfo(const char* newval, void* extra);
static bool check_behavior_compat_options(char** newval, void** extra, GucSource source);
static void assign_behavior_compat_options(const char* newval, void* extra);
static void assign_connection_info(const char* newval, void* extra);
static bool check_application_type(int* newval, void** extra, GucSource source);
static void assign_convert_string_to_digit(bool newval, void* extra);
static void AssignUStoreAttr(const char* newval, void* extra);
static bool check_snapshot_delimiter(char** newval, void** extra, GucSource source);
static bool check_snapshot_separator(char** newval, void** extra, GucSource source);


static void InitSqlConfigureNamesBool();
static void InitSqlConfigureNamesInt();
static void InitSqlConfigureNamesInt64();
static void InitSqlConfigureNamesReal();
static void InitSqlConfigureNamesString();
static void InitSqlConfigureNamesEnum();
#define FORBID_GUC_NUM 3
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
    {NULL, 0, false}
};

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
    {NULL, 0, false}
};

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
    {NULL, 0, false}
};

/* change the char * sql_compatibility to enum */
static const struct config_enum_entry adapt_database[] = {
    {g_dbCompatArray[DB_CMPT_A].name, A_FORMAT, false},
    {g_dbCompatArray[DB_CMPT_C].name, C_FORMAT, false},
    {g_dbCompatArray[DB_CMPT_B].name, B_FORMAT, false},
    {g_dbCompatArray[DB_CMPT_PG].name, PG_FORMAT, false},
    {NULL, 0, false}
};

/* change the char * enable_performance_data to enum */
static const struct config_enum_entry explain_option[] = {
    {"normal", EXPLAIN_NORMAL, false},
    {"pretty", EXPLAIN_PRETTY, false},
    {"summary", EXPLAIN_SUMMARY, false},
    {"run", EXPLAIN_RUN, false},
    {NULL, 0, false}
};

/* change the char * skew options to enum */
static const struct config_enum_entry skew_strategy_option[] = {
    {"off", SKEW_OPT_OFF, false},
    {"normal", SKEW_OPT_NORMAL, false},
    {"lazy", SKEW_OPT_LAZY, false},
    {NULL, 0, false}
};

/* change the char * codegen_strategy to enum */
static const struct config_enum_entry codegen_strategy_option[] = {
    {"partial", CODEGEN_PARTIAL, false},
    {"pure", CODEGEN_PURE, false},
    {NULL, 0, false}
};

static const struct config_enum_entry plan_cache_mode_options[] = {
    {"auto", PLAN_CACHE_MODE_AUTO, false},
    {"force_generic_plan", PLAN_CACHE_MODE_FORCE_GENERIC_PLAN, false},
    {"force_custom_plan", PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN, false},
    {NULL, 0, false}
};

static const struct config_enum_entry opfusion_debug_level_options[] = {
    {"off", BYPASS_OFF, false},
    {"log", BYPASS_LOG, false},
    {NULL, 0, false}
};

static const struct config_enum_entry application_type_options[] = {
    {"not_perfect_sharding_type", NOT_PERFECT_SHARDING_TYPE, false},
    {"perfect_sharding_type", PERFECT_SHARDING_TYPE, false},
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
    {"no_unique_index_first", NO_UNIQUE_INDEX_FIRST, false},
    {"join_sel_with_cast_func", JOIN_SEL_WITH_CAST_FUNC, false},
    {"canonical_pathkey", CANONICAL_PATHKEY, false},
    {"index_cost_with_leaf_pages_only", INDEX_COST_WITH_LEAF_PAGES_ONLY, false},
    {"partition_opfusion", PARTITION_OPFUSION , false},
    {"a_style_coerce", A_STYLE_COERCE, false},
    {"plpgsql_stream_fetchall", PLPGSQL_STREAM_FETCHALL, false},
    {"predpush_same_level", PREDPUSH_SAME_LEVEL, false},
    {"partition_fdw_on", PARTITION_FDW_ON, false},
    {NULL, 0, false}
};

static const struct config_enum_entry vector_engine_strategy[] = {
    {"off", OFF_VECTOR_ENGINE, false},
    {"force", FORCE_VECTOR_ENGINE, false},
    {"optimal", OPT_VECTOR_ENGINE, false},
    {NULL, 0, false}
};

typedef struct behavior_compat_entry {
    const char* name; /* name of behavior compat entry */
    int flag;         /* bit flag position */
} behavior_compat_entry;

static const struct behavior_compat_entry behavior_compat_options[OPT_MAX] = {
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
    {"convert_string_digit_to_numeric", OPT_CONVERT_TO_NUMERIC},
    {"plstmt_implicit_savepoint", OPT_PLSTMT_IMPLICIT_SAVEPOINT},
    {"hide_tailing_zero", OPT_HIDE_TAILING_ZERO},
    {"plsql_security_definer", OPT_SECURITY_DEFINER},
    {"skip_insert_gs_source", OPT_SKIP_GS_SOURCE},
    {"proc_outparam_override", OPT_PROC_OUTPARAM_OVERRIDE},
    {"allow_procedure_compile_check", OPT_ALLOW_PROCEDURE_COMPILE_CHECK},
    {"proc_implicit_for_loop_variable", OPT_IMPLICIT_FOR_LOOP_VARIABLE},
    {"aformat_null_test", OPT_AFORMAT_NULL_TEST},
    {"aformat_regexp_match", OPT_AFORMAT_REGEX_MATCH},
    {"rownum_type_compat", OPT_ROWNUM_TYPE_COMPAT},
    {"compat_cursor", OPT_COMPAT_CURSOR},
    {"char_coerce_compat", OPT_CHAR_COERCE_COMPAT}
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
void InitSqlConfigureNames()
{
    InitSqlConfigureNamesBool();
    InitSqlConfigureNamesInt();
    InitSqlConfigureNamesInt64();
    InitSqlConfigureNamesReal();
    InitSqlConfigureNamesString();
    InitSqlConfigureNamesEnum();

    return;
}

static void InitSqlConfigureNamesBool()
{
    struct config_bool localConfigureNamesBool[] = {
        {{"enable_fast_numeric",
            PGC_SUSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enable numeric optimize."),
            NULL},
            &u_sess->attr.attr_sql.enable_fast_numeric,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_global_stats",
            PGC_SUSET,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop(" Enable tablespace using absolute location."),
            NULL},
            &u_sess->attr.attr_sql.enable_absolute_tablespace,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_hadoop_env",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop(" Enable hadoop enviroment."),
            NULL},
            &u_sess->attr.attr_sql.enable_hadoop_env,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_valuepartition_pruning",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enable optimize query by using informational constraint."),
            NULL},
            &u_sess->attr.attr_sql.enable_constraint_optimization,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_bloom_filter",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enable bloom filter check"),
            NULL},
            &u_sess->attr.attr_sql.enable_bloom_filter,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_codegen",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enable llvm for executor."),
            NULL},
            &u_sess->attr.attr_sql.enable_codegen,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_codegen_print",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enable Sonic optimized spill."),
            NULL},
            &u_sess->attr.attr_sql.enable_sonic_optspill,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_sonic_hashjoin",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enable Sonic hashjoin."),
            NULL},
            &u_sess->attr.attr_sql.enable_sonic_hashjoin,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_sonic_hashagg",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enable Sonic hashagg."),
            NULL},
            &u_sess->attr.attr_sql.enable_sonic_hashagg,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_csqual_pushdown",
            PGC_SUSET,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Enables colstore qual push down."),
            NULL},
            &u_sess->attr.attr_sql.enable_csqual_pushdown,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_change_hjcost",
            PGC_SUSET,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Enable change hash join cost"),
            NULL},
            &u_sess->attr.attr_sql.enable_change_hjcost,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_seqscan",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_DISTRIBUTE,
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
            NODE_ALL,
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
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables the planner's use of explicit sort steps."),
            NULL},
            &u_sess->attr.attr_sql.enable_sort,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_compress_spill",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables spilling compress."),
            NULL},
            &u_sess->attr.attr_sql.enable_compress_spill,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_hashagg",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables the planner's use of materialization."),
            NULL},
            &u_sess->attr.attr_sql.enable_material,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_startwith_debug",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables debug infomation for start with."),
            NULL},
            &u_sess->attr.attr_sql.enable_startwith_debug,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_nestloop",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_DISTRIBUTE,
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
            NODE_ALL,
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
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables the planner's use of dngather plans."),
            NULL},
            &u_sess->attr.attr_sql.enable_dngather,
            false,
            NULL,
            NULL,
            NULL},

        /* this hll guc is no longer used, just keep it for forward compatibility */
        {{"enable_compress_hll",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables hll use less memory on datanode."),
            NULL},
            &u_sess->attr.attr_sql.enable_compress_hll,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_vector_engine",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables the vector engine."),
            NULL},
            &u_sess->attr.attr_sql.enable_vector_engine,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_force_vector_engine",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Forces to enable the vector engine."),
            NULL},
            &u_sess->attr.attr_sql.enable_force_vector_engine,
            false,
            NULL,
            NULL,
            NULL},
        {{"string_hash_compatible",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
            QUERY_TUNING_GEQO,
            gettext_noop("Enables genetic query optimization."),
            gettext_noop("This algorithm attempts to do planning without "
                        "exhaustive searching.")},
            &u_sess->attr.attr_sql.enable_geqo,
            true,
            NULL,
            NULL,
            NULL},
        {{"restart_after_crash",
            PGC_SIGHUP,
            NODE_ALL,
            ERROR_HANDLING_OPTIONS,
            gettext_noop("Reinitializes server after backend crashes."),
            NULL},
            &u_sess->attr.attr_sql.restart_after_crash,
            true,
            NULL,
            NULL,
            NULL},
        // variable to enable early free policy
        {{"enable_early_free",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Using memory early free policy."),
            NULL},
            &u_sess->attr.attr_sql.enable_early_free,
            true,
            NULL,
            NULL,
            NULL},

        // support to kill a working query when drop a user
        /*
         * security requirements: ordinary users can not set this parameter,  promoting the setting level to PGC_SUSET.
         */
        {{"enable_kill_query",
            PGC_SUSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables cancelling a query that locks some relations owned by a user "
                            "when the user is dropped."),
            NULL},
            &u_sess->attr.attr_sql.enable_kill_query,
            false,
            NULL,
            NULL,
            NULL},
        {{"log_duration",
            PGC_SUSET,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Logs the duration of each completed SQL statement."),
            NULL},
            &u_sess->attr.attr_sql.log_duration,
            false,
            NULL,
            NULL,
            NULL},
        {{"debug_print_parse",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Logs each query's parse tree."),
            NULL},
            &u_sess->attr.attr_sql.Debug_print_parse,
            false,
            NULL,
            NULL,
            NULL},
        {{"debug_print_rewritten",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Logs each query's rewritten parse tree."),
            NULL},
            &u_sess->attr.attr_sql.Debug_print_rewritten,
            false,
            NULL,
            NULL,
            NULL},
        {{"debug_print_plan",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Logs each query's execution plan."),
            NULL},
            &u_sess->attr.attr_sql.Debug_print_plan,
            false,
            NULL,
            NULL,
            NULL},
        {{"debug_pretty_print",
            PGC_USERSET,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Indents parse and plan tree displays."),
            NULL},
            &u_sess->attr.attr_sql.Debug_pretty_print,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_analyze_check",
            PGC_SUSET,
            NODE_ALL,
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
            NODE_ALL,
            STATS,
            gettext_noop("Enable auto-analyze when querying tables with no statistic."),
            NULL},
            &u_sess->attr.attr_sql.enable_autoanalyze,
            false,
            NULL,
            NULL,
            NULL},
        {{"sql_inheritance",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
        {{"check_function_bodies",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
            COMPAT_OPTIONS_PREVIOUS,
            gettext_noop("Creates new tables with OIDs by default."),
            NULL},
            &u_sess->attr.attr_sql.default_with_oids,
            false,
            NULL,
            NULL,
            NULL},
#ifdef DEBUG_BOUNDED_SORT
        /* this is undocumented because not exposed in a standard build */
        {{"optimize_bounded_sort",
            PGC_USERSET,
            NODE_ALL,
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
        {{"escape_string_warning",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
            COMPAT_OPTIONS_PREVIOUS,
            gettext_noop("Causes '...' strings to treat backslashes literally."),
            NULL,
            GUC_REPORT},
            &u_sess->attr.attr_sql.standard_conforming_strings,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_light_proxy",
            PGC_SUSET,
            NODE_DISTRIBUTE,
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
            NODE_ALL,
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
            NODE_ALL,
            COMPAT_OPTIONS_PREVIOUS,
            gettext_noop("Enables backward compatibility mode for privilege checks on large objects."),
            gettext_noop("Skips privilege checks when reading or modifying large objects, "
                        "for compatibility with PostgreSQL releases prior to 9.0.")},
            &u_sess->attr.attr_sql.lo_compat_privileges,
            false,
            NULL,
            NULL,
            NULL},

        {{"quote_all_identifiers",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("GUC parameter of enforcing adapting to A db."),
            NULL},
            &u_sess->attr.attr_sql.enforce_a_behavior,
            true,
            NULL,
            NULL},
        {{"enable_slot_log",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables create slot log"),
            NULL},
            &u_sess->attr.attr_sql.enable_slot_log,
            false,
            NULL,
            NULL,
            NULL},
        {{"convert_string_to_digit",
            PGC_USERSET,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Convert string to digit when comparing string and digit"),
            NULL,
            },
            &u_sess->attr.attr_sql.convert_string_to_digit,
            true,
            NULL,
            assign_convert_string_to_digit,
            NULL},
        {{"enable_broadcast",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables the planner's use of broadcast stream plans."),
            NULL},
            &u_sess->attr.attr_sql.enable_broadcast,
            true,
            NULL,
            NULL,
            NULL},
        {{"ngram_punctuation_ignore",
            PGC_USERSET,
            NODE_ALL,
            TEXT_SEARCH,
            gettext_noop("Enables N-gram ignore punctuation."),
            NULL,
            },
            &u_sess->attr.attr_sql.ngram_punctuation_ignore,
            true,
            NULL,
            NULL,
            NULL},
        {{"ngram_grapsymbol_ignore",
            PGC_USERSET,
            NODE_ALL,
            TEXT_SEARCH,
            gettext_noop("Enables N-gram ignore grapsymbol."),
            NULL,
            },
            &u_sess->attr.attr_sql.ngram_grapsymbol_ignore,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_fast_allocate",
            PGC_SUSET,
            NODE_ALL,
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
        {{"td_compatible_truncation",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_OTHER,
            gettext_noop("Enable string automatically truncated during insertion."),
            NULL},
            &u_sess->attr.attr_sql.td_compatible_truncation,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_upgrade_merge_lock_mode",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_OTHER,
            gettext_noop("If true, use Exclusive Lock mode for deltamerge."),
            NULL},
            &u_sess->attr.attr_sql.enable_upgrade_merge_lock_mode,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_orc_cache",
            PGC_POSTMASTER,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enable orc metadata cache."),
            NULL},
            &g_instance.attr.attr_sql.enable_orc_cache,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_default_cfunc_libpath",
            PGC_POSTMASTER,
            NODE_ALL,
            FILE_LOCATIONS,
            gettext_noop("Enable check for c function lib path."),
            NULL},
            &g_instance.attr.attr_sql.enable_default_cfunc_libpath,
            true,
            NULL,
            NULL,
            NULL},

        {{"acceleration_with_compute_pool",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enable extrapolation stats for date datatype."),
            NULL},
            &u_sess->attr.attr_sql.enable_extrapolation_stats,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_trigger_shipping",
            PGC_USERSET,
            NODE_DISTRIBUTE,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
            UNGROUPED,
            gettext_noop("If true, show details whether plan is pushed down to the compute pool."),
            NULL},
            &u_sess->attr.attr_sql.show_acce_estimate_detail,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_opfusion",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables opfusion."),
            NULL},
            &u_sess->attr.attr_sql.enable_opfusion,
            true,
            NULL,
            NULL,
            NULL},

#ifndef ENABLE_MULTIPLE_NODES
        {{"enable_beta_opfusion",
            PGC_USERSET,
            NODE_SINGLENODE,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables beta opfusion features."),
            NULL},
            &u_sess->attr.attr_sql.enable_beta_opfusion,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_custom_parser",
             PGC_USERSET,
             NODE_SINGLENODE,
             UNGROUPED,
             gettext_noop("Enables custom parser"),
             NULL},
            &u_sess->attr.attr_sql.enable_custom_parser,
            false,
            NULL,
            NULL,
            NULL},
#endif

        {{"enable_partition_opfusion",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables partition opfusion features."),
            NULL},
            &u_sess->attr.attr_sql.enable_partition_opfusion,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_prevent_job_task_startup",
            PGC_SIGHUP,
            NODE_ALL,
            JOB,
            gettext_noop("enable control whether the job task thread can be started."),
            NULL},
            &u_sess->attr.attr_sql.enable_prevent_job_task_startup,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_remotejoin",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables the planner's use of remote join plans."),
            NULL},
            &u_sess->attr.attr_sql.enable_remotejoin,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_fast_query_shipping",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables the planner's use of fast query shipping to ship query directly to datanode."),
            NULL},
            &u_sess->attr.attr_sql.enable_fast_query_shipping,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_remotegroup",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables the planner's use of remote group plans."),
            NULL},
            &u_sess->attr.attr_sql.enable_remotegroup,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_remotesort",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables the planner's use of remote sort plans."),
            NULL},
            &u_sess->attr.attr_sql.enable_remotesort,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_remotelimit",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables the planner's use of remote limit plans."),
            NULL},
            &u_sess->attr.attr_sql.enable_remotelimit,
            true,
            NULL,
            NULL,
            NULL},
        {{"gtm_backup_barrier",
            PGC_SUSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables coordinator to report barrier id to GTM for backup."),
            NULL},
            &u_sess->attr.attr_sql.gtm_backup_barrier,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_stream_operator",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables the planner's use of stream operator."),
            NULL},
            &u_sess->attr.attr_sql.enable_stream_operator,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_unshipping_log",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            LOGGING_WHEN,
            gettext_noop("Enables output the unshipping log."),
            NULL},
            &u_sess->attr.attr_sql.enable_unshipping_log,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_stream_concurrent_update",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables concurrent update under stream mode."),
            NULL},
            &u_sess->attr.attr_sql.enable_stream_concurrent_update,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_stream_recursive",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables the planner's use of streaming plan for with-recursive"),
            NULL},
            &u_sess->attr.attr_sql.enable_stream_recursive,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_random_datanode",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables the planner's use of random datanode."),
            NULL},
            &u_sess->attr.attr_sql.enable_random_datanode,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_fstream",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("use stream operator to replace remotequery "),
            NULL},
            &u_sess->attr.attr_sql.enable_fstream,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_cluster_resize",
            PGC_SUSET,
            NODE_DISTRIBUTE,
            XC_HOUSEKEEPING_OPTIONS,
            gettext_noop("Enables redistribute at nodes mis-match."),
            NULL},
            &u_sess->attr.attr_sql.enable_cluster_resize,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_acceleration_cluster_wlm",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("Enables dynamic workload management in acceleration cluster. "
                        "Just for the compute pool, it doesn't work to enable the option for the DWS cluster."),
            NULL},
            &g_instance.attr.attr_sql.enable_acceleration_cluster_wlm,
            false,
            NULL,
            NULL,
            NULL},
        {{"agg_redistribute_enhancement",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Whether choosing of agg redistribute key is enhanced"),
            NULL,
            },
            &u_sess->attr.attr_sql.agg_redistribute_enhancement,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_agg_pushdown_for_ca",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            DEVELOPER_OPTIONS,
            gettext_noop("Enable agg node pushdown for cooperation analysis."),
            NULL},
            &u_sess->attr.attr_sql.enable_agg_pushdown_for_cooperation_analysis,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_streaming",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            STREAMING,
            gettext_noop("Sets the streaming engine enable flag."),
            NULL},
            &g_instance.streaming_cxt.enable,
            false,
            NULL,
            NULL,
            NULL},
#ifndef ENABLE_MULTIPLE_NODES
        {{"plsql_show_all_error",
            PGC_USERSET,
            NODE_SINGLENODE,
            QUERY_TUNING,
            gettext_noop("Enable plsql debug mode"),
            NULL},
            &u_sess->attr.attr_common.plsql_show_all_error,
            false,
            NULL,
            NULL,
            NULL
        },
        {{"uppercase_attribute_name",
            PGC_USERSET,
            NODE_SINGLENODE,
            COMPAT_OPTIONS,
            gettext_noop("Set to ON will force attname displayed in upper case when all characters in lower case "
                        "(comapatible with ORA), otherwise do nothing."),
            NULL,
            GUC_REPORT},
            &u_sess->attr.attr_sql.uppercase_attribute_name,
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
        
    int num_of_bool_option = sizeof(localConfigureNamesBool) / sizeof(config_bool);
    for (int i = 0; i < num_of_bool_option - 1; i++) {
        if (strcasecmp(localConfigureNamesBool[i].gen.name, "enable_save_datachanged_timestamp") == 0 &&
            get_product_version() == PRODUCT_VERSION_GAUSSDB300) {
            localConfigureNamesBool[i].boot_val = false;
            break;
        }
    }

    Size bytes = sizeof(localConfigureNamesBool);
    u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_SQL] =
        (struct config_bool*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_SQL], bytes, localConfigureNamesBool, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitSqlConfigureNamesInt()
{
    struct config_int localConfigureNamesInt[] = {
        {{"default_statistics_target",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Set parameter log2Registers in hll."),
            gettext_noop("It's related to numbers of registers and relative error.")},
            &u_sess->attr.attr_sql.hll_default_log2m,
            HLL_LOG2_REGISTER,
            HLL_LOG2_REGISTER_MIN,
            HLL_LOG2_REGISTER_MAX,
            NULL,
            NULL,
            NULL},
        {{"hll_default_log2explicit",
            PGC_USERSET,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Set parameter log2Explicitsize for hll."),
            gettext_noop("hll explicit mode max size.")},
            &u_sess->attr.attr_sql.hll_default_log2explicit,
            HLL_LOG2_EXPLICIT_SIZE,
            HLL_LOG2_EXPLICIT_SIZE_MIN,
            HLL_LOG2_EXPLICIT_SIZE_MAX,
            NULL,
            NULL,
            NULL},
        {{"hll_default_log2sparse",
            PGC_USERSET,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Set parameter log2Sparsesize for hll."),
            gettext_noop("hll sparse mode max size.")},
            &u_sess->attr.attr_sql.hll_default_log2sparse,
            HLL_LOG2_SPARSE_SIZE,
            HLL_LOG2_SPARSE_SIZE_MIN,
            HLL_LOG2_SPARSE_SIZE_MAX,
            NULL,
            NULL,
            NULL},
        {{"hll_duplicate_check",
            PGC_USERSET,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Set parameter duplicateCheck for hll."),
            gettext_noop("Enables hll use duplicateCheck.")},
            &u_sess->attr.attr_sql.hll_duplicate_check,
            HLL_DUPLICATE_CHECK,
            HLL_DUPLICATE_CHECK_MIN,
            HLL_DUPLICATE_CHECK_MAX,
            NULL,
            NULL,
            NULL},
        /* this hll guc is no longer used, just keep it for forward compatibility */
        {{"hll_default_regwidth",
            PGC_USERSET,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Set parameter regwidth in hll."),
            gettext_noop("It's related to number of bits used per register, max cardinality and memory of hll.")},
            &u_sess->attr.attr_sql.g_default_regwidth,
            5,
            1,
            5,
            NULL,
            NULL,
            NULL},
        /* this hll guc is no longer used, just keep it for forward compatibility */
        {{"hll_default_sparseon",
            PGC_USERSET,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Set parameter sparseon for hll."),
            gettext_noop("hll use sparseon mode or not.")},
            &u_sess->attr.attr_sql.g_default_sparseon,
            1,
            0,
            1,
            NULL,
            NULL,
            NULL},
        /* this hll guc is no longer used, just keep it for forward compatibility */
        {{"hll_max_sparse",
            PGC_USERSET,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Set parameter max_sparse for hll"),
            NULL},
            &u_sess->attr.attr_sql.g_max_sparse,
            -1,
            -1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"cost_param",
            PGC_USERSET,
            NODE_ALL,
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
        {{"max_recursive_times",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
        {{"hashagg_table_size",
            PGC_USERSET,
            NODE_ALL,
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
        {{"udf_memory_limit",
            PGC_POSTMASTER,
            NODE_ALL,
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
        {{"UDFWorkerMemHardLimit",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
        {{"temp_file_limit",
            PGC_SUSET,
            NODE_ALL,
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
        {{"job_queue_processes",
            PGC_POSTMASTER,
            NODE_ALL,
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
        {{"effective_cache_size",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_COST,
            gettext_noop("Sets the planner's assumption about the size of the disk cache."),
            gettext_noop("That is, the portion of the kernel's disk cache that "
                        "will be used for openGauss data files. This is measured in disk "
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
        // user defind max_compile_functions
        {{"max_compile_functions",
            PGC_POSTMASTER,
            NODE_ALL,
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
        {{"query_dop",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING,
            gettext_noop("User-defined degree of parallelism."),
            NULL},
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
        {{"plan_mode_seed",
            PGC_USERSET,
            NODE_ALL,
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
        /*
         * set threshold to check if codegen is allowed or not, codegen_cost_threshold
         * represents the number of rows. This will be changed in future.
         */
        {{"codegen_cost_threshold",
            PGC_USERSET,
            NODE_ALL,
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
        {{"max_resource_package",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_DISTRIBUTE,
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
        {{"table_skewness_warning_rows",
            PGC_USERSET,
            NODE_ALL,
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
        {{"best_agg_plan",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Specify which path to do aggregation in stream environment."),
            NULL},
            &u_sess->attr.attr_sql.best_agg_plan,
            0,
            0,
            3,
            NULL,
            NULL,
            NULL},
        {{"streaming_router_port",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            STREAMING,
            gettext_noop("Sets the port the streaming router listens on."),
            NULL},
            &g_instance.streaming_cxt.router_port,
            0,
            0,
            65535,
            NULL,
            NULL,
            NULL},
        {{"streaming_num_workers",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            STREAMING,
            gettext_noop("Sets the number of streaming engine worker threads."),
            NULL},
            &g_instance.streaming_cxt.workers,
            1,
            1,
            64,
            NULL,
            NULL,
            NULL},
        {{"streaming_num_collectors",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            STREAMING,
            gettext_noop("Sets the number of streaming engine collector threads."),
            NULL},
            &g_instance.streaming_cxt.combiners,
            1,
            1,
            64,
            NULL,
            NULL,
            NULL},
        {{"streaming_num_queues",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            STREAMING,
            gettext_noop("Sets the number of streaming engine queue threads."),
            NULL},
            &g_instance.streaming_cxt.queues,
            1,
            1,
            64,
            NULL,
            NULL,
            NULL},
        {{"streaming_batch_size",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            STREAMING,
            gettext_noop("Sets the max packed tuples of streaming microbatch."),
            NULL},
            &g_instance.streaming_cxt.batch_size,
            10000,
            1,
            100000000,
            NULL,
            NULL,
            NULL},
        {{"streaming_batch_memory",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            STREAMING,
            gettext_noop("Sets the max process memory (KB) of streaming microbatch."),
            NULL,
            GUC_UNIT_KB},
            &g_instance.streaming_cxt.batch_mem,
            65536,
            4096,
            1048576,
            NULL,
            NULL,
            NULL},
        {{"streaming_batch_timeout",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            STREAMING,
            gettext_noop("Sets the receive timeout (ms) of streaming microbatch."),
            NULL,
            GUC_UNIT_MS},
            &g_instance.streaming_cxt.batch_wait,
            500,
            1,
            60000,
            NULL,
            NULL,
            NULL},
        {{"streaming_collect_memory",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            STREAMING,
            gettext_noop("Sets the max collect memory (KB) of streaming collector thread."),
            NULL,
            GUC_UNIT_KB},
            &g_instance.streaming_cxt.flush_mem,
            65536,
            4096,
            33554432,
            NULL,
            NULL,
            NULL},
        {{"streaming_flush_interval",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            STREAMING,
            gettext_noop("Sets the flush interval (ms) of streaming collector thread."),
            NULL,
            GUC_UNIT_MS},
            &g_instance.streaming_cxt.flush_wait,
            500,
            1,
            1200000,
            NULL,
            NULL,
            NULL},
        {{"streaming_gather_window_interval",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            STREAMING,
            gettext_noop("Sets the gather window interval (min) of streaming gather task."),
            NULL},
            &g_instance.streaming_cxt.gather_window_interval,
            5,
            5,
            1440,
            NULL,
            NULL,
            NULL},
#ifndef ENABLE_MULTIPLE_NODES
        {{"pldebugger_timeout",
            PGC_USERSET,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Sets the receive timeout (s) of pldebugger."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_sql.pldebugger_timeout,
            15 * 60,
            1,
            24 * 60 * 60,
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
            0,
            0,
            0,
            NULL,
            NULL,
            NULL}
    };

    Size bytes = sizeof(localConfigureNamesInt);
    u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_SQL] =
        (struct config_int*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_SQL], bytes, localConfigureNamesInt, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitSqlConfigureNamesReal()
{
    struct config_real localConfigureNamesReal[] = {
        {{"seq_page_cost",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_DISTRIBUTE,
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
            NODE_ALL,
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
        {{"geqo_selection_bias",
            PGC_USERSET,
            NODE_ALL,
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
        {{"geqo_seed",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_GEQO,
            gettext_noop("GEQO: seed for random path selection."),
            NULL},
            &u_sess->attr.attr_sql.Geqo_seed,
            0.0,
            0.0,
            1.0,
            NULL,
            NULL,
            NULL},
        {{"seed",
            PGC_USERSET,
            NODE_ALL,
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
        {{"table_skewness_warning_threshold",
            PGC_USERSET,
            NODE_ALL,
            STATS,
            gettext_noop("table skewness threthold"),
            NULL},
            &u_sess->attr.attr_sql.table_skewness_warning_threshold,
            1.0,
            0.0,
            1.0,
            check_double_parameter,
            NULL,
            NULL},
        {{"cost_weight_index",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_COST,
            gettext_noop("Sets the planner's discount when evaluating index cost."),
            NULL},
            &u_sess->attr.attr_sql.cost_weight_index,
            1,
            1e-10,
            1e10,
            NULL,
            NULL,
            NULL,
            0,
            NULL},
        {{"default_limit_rows",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_COST,
            gettext_noop(
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
        {{"stream_multiple",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_COST,
            gettext_noop("Sets the planner's estimate of the cost of "
                        "stream multiple."),
            NULL},
            &u_sess->attr.attr_sql.stream_multiple,
            DEFAULT_STREAM_MULTIPLE,
            0,
            DBL_MAX,
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
    u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_SQL] =
        (struct config_real*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_SQL], bytes, localConfigureNamesReal, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitSqlConfigureNamesInt64()
{
    struct config_int64 localConfigureNamesInt64[] = {
        /* this hll guc is no longer used, just keep it for forward compatibility */
        {{"hll_default_expthresh",
            PGC_USERSET,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Set parameter expthresh in hll."),
            gettext_noop("It could change promotion hierarchy for hll.")},
            &u_sess->attr.attr_sql.g_default_expthresh,
            -1,
            -1,
            7,
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
    u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_SQL] =
        (struct config_int64*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_SQL], bytes, localConfigureNamesInt64, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitSqlConfigureNamesString()
{
    struct config_string localConfigureNamesString[] = {
        {{"expected_computing_nodegroup",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Computing node group mode or expected node group for query processing."),
            NULL,
            },
            &u_sess->attr.attr_sql.expected_computing_nodegroup,
            CNG_OPTION_QUERY,
            NULL,
            NULL,
            NULL},
        {{"default_storage_nodegroup",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            QUERY_TUNING_METHOD,
            gettext_noop("Default storage group for create table."),
            NULL,
            },
            &u_sess->attr.attr_sql.default_storage_nodegroup,
            INSTALLATION_MODE,
            NULL,
            NULL,
            NULL},
        /* for pljava */
        {{"pljava_vmoptions",
            PGC_SUSET,
            NODE_ALL,
            CUSTOM_OPTIONS,
            gettext_noop("Options sent to the JVM when it is created"),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_sql.pljava_vmoptions,
            "",
            NULL,
            NULL,
            NULL},
        {{"qrw_inlist2join_optmode",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
            COMPAT_OPTIONS,
            gettext_noop("compatibility options"),
            NULL,
            GUC_LIST_INPUT | GUC_REPORT},
            &u_sess->attr.attr_sql.behavior_compat_string,
            "",
            check_behavior_compat_options,
            assign_behavior_compat_options,
            NULL},
        {{"connection_info",
            PGC_USERSET,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Sets the connection info to be reported in statistics and logs."),
            NULL,
            GUC_REPORT | GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_sql.connection_info,
            "",
            NULL,
            assign_connection_info,
            NULL},
        {{"retry_ecode_list",
            PGC_USERSET,
            NODE_ALL,
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
        {{"ustore_attr",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN,
            gettext_noop("Configure UStore optimizations."),
            NULL,
            GUC_LIST_INPUT | GUC_LIST_QUOTE},
            &u_sess->attr.attr_sql.ustore_attr,
            "",
            NULL,
            AssignUStoreAttr,
            NULL},
        {{"db4ai_snapshot_mode",
             PGC_USERSET,
             NODE_SINGLENODE,
             RESOURCES,
             gettext_noop("Sets the snapshot storage model."),
             NULL},
            &u_sess->attr.attr_sql.db4ai_snapshot_mode,
            "MSS",
            NULL,
            NULL,
            NULL},
        {{"db4ai_snapshot_version_separator",
             PGC_USERSET,
             NODE_SINGLENODE,
             RESOURCES,
             gettext_noop("Sets the separator for internal snapshot names."),
             NULL},
            &u_sess->attr.attr_sql.db4ai_snapshot_version_separator,
            ".",
            check_snapshot_separator,
            NULL,
            NULL},
        {{"db4ai_snapshot_version_delimiter",
             PGC_USERSET,
             NODE_SINGLENODE,
             RESOURCES,
             gettext_noop("Sets the delimiter for internal snapshot names."),
             NULL},
            &u_sess->attr.attr_sql.db4ai_snapshot_version_delimiter,
            "@",
            check_snapshot_delimiter,
            NULL,
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
    u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_SQL] =
        (struct config_string*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_SQL], bytes,
        localConfigureNamesString, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitSqlConfigureNamesEnum()
{
    struct config_enum localConfigureNamesEnum[] = {
        {{"backslash_quote",
            PGC_USERSET,
            NODE_ALL,
            COMPAT_OPTIONS_PREVIOUS,
            gettext_noop("Sets whether \"\\'\" is allowed in string literals."),
            NULL},
            &u_sess->attr.attr_sql.backslash_quote,
            BACKSLASH_QUOTE_SAFE_ENCODING,
            backslash_quote_options,
            NULL,
            NULL,
            NULL},
        {{"constraint_exclusion",
            PGC_USERSET,
            NODE_ALL,
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
        {{"rewrite_rule",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING,
            gettext_noop("Sets the rewrite rule."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_sql.rewrite_rule,
            MAGIC_SET,
            rewrite_options,
            NULL,
            NULL,
            NULL},
        {{"sql_compatibility",
            PGC_INTERNAL,
            NODE_ALL,
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
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Choose which style to print the explain info."),
            NULL},
            &u_sess->attr.attr_sql.guc_explain_perf_mode,
            EXPLAIN_NORMAL,
            explain_option,
            NULL,
            NULL,
            NULL},
        {{"skew_option",
            PGC_USERSET,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Choose data skew optimization strategy."),
            NULL},
            &u_sess->attr.attr_sql.skew_strategy_store,
            SKEW_OPT_NORMAL,
            skew_strategy_option,
            NULL,
            NULL,
            NULL},
        {{"codegen_strategy",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING,
            gettext_noop("Choose whether it is allowed to call C-function in codegen."),
            NULL},
            &u_sess->attr.attr_sql.codegen_strategy,
            CODEGEN_PARTIAL,
            codegen_strategy_option,
            NULL,
            NULL,
            NULL},
        {{"plan_cache_mode",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_OTHER,
            gettext_noop("Controls the planner's selection of custom or generic plan."),
            gettext_noop("Prepared statements can have custom and generic plans, and the planner "
                         "will attempt to choose which is better.  This can be set to override "
                         "the default behavior.")},
            &u_sess->attr.attr_sql.g_planCacheMode,
            PLAN_CACHE_MODE_AUTO,
            plan_cache_mode_options,
            NULL,
            NULL,
            NULL},
        {{"opfusion_debug_mode",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("opfusion debug mode."),
            NULL},
            &u_sess->attr.attr_sql.opfusion_debug_mode,
            BYPASS_OFF,
            opfusion_debug_level_options,
            NULL,
            NULL,
            NULL},
        {{"application_type",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("application distribute type(perfect sharding or not) in gtm free mode."),
            NULL,
            GUC_REPORT},
            &u_sess->attr.attr_sql.application_type,
            NOT_PERFECT_SHARDING_TYPE,
            application_type_options,
            check_application_type,
            NULL,
            NULL},
        {{"sql_beta_feature",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING,
            gettext_noop("Sets the beta feature for SQL engine."), NULL, GUC_LIST_INPUT},
            &u_sess->attr.attr_sql.sql_beta_feature,
            NO_BETA_FEATURE,
            sql_beta_options,
            NULL,
            NULL,
            NULL},
        {{"try_vector_engine_strategy",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING,
            gettext_noop("Sets the strategy of using vector engine for row table."),
            NULL},
            &u_sess->attr.attr_sql.vectorEngineStrategy,
            OFF_VECTOR_ENGINE,
            vector_engine_strategy,
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
    u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_SQL] =
        (struct config_enum*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_SQL], bytes, localConfigureNamesEnum, bytes);
    securec_check_ss(rc, "\0", "\0");
}

/* ******* end of options list ******* */

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

/*
 * Description: Check wheth out of max backends after max job worker threads.
 *
 * @in newval: the param of job_queue_processes.
 * Returns: bool
 */
static bool check_job_max_workers(int* newval, void** extra, GucSource source)
{
    if (g_instance.attr.attr_network.MaxConnections + g_instance.attr.attr_storage.autovacuum_max_workers + *newval +
        AUXILIARY_BACKENDS + AV_LAUNCHER_PROCS +
        g_instance.attr.attr_network.maxInnerToolConnections > MAX_BACKENDS) {
        return false;
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

static bool check_fencedUDFMemoryLimit(int* newval, void** extra, GucSource source)
{
    if (*newval > g_instance.attr.attr_sql.UDFWorkerMemHardLimit) {
        GUC_check_errdetail("\"FencedUDFMemoryLimit\" cannot be greater than \"UDFWorkerMemHardLimit\".");
        return false;
    }

    return true;
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

static bool is_inlist2join_parameter_positive_integer(const char* newval)
{
    int64 number = 0;
    for (const char* p = newval; *p; p++) {
        int tmp = *p - '0';
        if (tmp < 0 || tmp > 9) {
            return false;
        }
        number = number * 10 + tmp;
        if (number > INT_MAX) {
            return false;
        }
    }
    if (number == 0) {
        return false;
    }
    return true;
}

/*
 * @@GaussDB@@
 * Brief			: Check inlist2join info.
 * Description		: If inlist2join'format is not right, then return false.
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
    } else if (!is_inlist2join_parameter_positive_integer((const char*)*newval)) {
        GUC_check_errdetail(
            "Available values: disable, cost_base, rule_base, or any positive integer as a inlist2join threshold");
        return false;
    } else {
        return true;
    }
}

/*
 * @@GaussDB@@
 * Description		:  parse inlist2join info.
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
#ifdef ENABLE_MULTIPLE_NODES
static bool ForbidDistributeParameter(const char* elem)
{
    const char *forbidList[] = {
        "proc_outparam_override",
        "skip_insert_gs_source",
        "rownum_type_compat"
    };
    for (int i = 0; i < FORBID_GUC_NUM; i++) {
        if (strcmp(forbidList[i], elem) == 0) {
            return true;
        }
    }
    return false;
}
#endif
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
#ifdef ENABLE_MULTIPLE_NODES
            if (ForbidDistributeParameter(item)) {
                GUC_check_errdetail("behavior compat option %s can not use"
                                    " in distributed database system", item);
                pfree(rawstring);
                list_free(elemlist);
                return false;
            }
#endif
            if (strcmp(item, behavior_compat_options[start].name) == 0) {
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

            if (strcmp(item, behavior_compat_options[start].name) == 0)
                result += behavior_compat_options[start].flag;
        }
    }

    pfree(rawstring);
    list_free(elemlist);

    u_sess->utils_cxt.behavior_compat_flags = result;
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

#define IS_NULL_STR(str) ((str) == NULL || (str)[0] == '\0')

const int MIN_USTATS_TRACKER_NAPTIME = 1;
const int MAX_USTATS_TRACKER_NAPTIME = INT_MAX / 1000;
const int MIN_UMAX_PRUNE_SEARCH_LEN = 1;
const int MAX_UMAX_PRUNE_SEARCH_LEN = INT_MAX / 1000;

static void ParseUStoreBool(bool* pBool, const char* ptoken, const char* pdelimiter, char* psave)
{
    ptoken = TrimStr(strtok_r(NULL, pdelimiter, &psave));
    if (!parse_bool(ptoken, pBool)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("a parameter inside ustore_attr contains an invalid Boolean value")));
    }
}

static void ParseUStoreInt(int* pInt, const char* ptoken, const char* pdelimiter, char* psave,
                           int gucMin, int gucMax)
{
    ptoken = TrimStr(strtok_r(NULL, pdelimiter, &psave));
    if (!IS_NULL_STR(ptoken)) {
        int newValue = pg_strtoint32(ptoken);
        if (newValue >= gucMin && newValue <= gucMax) {
            *pInt = newValue;
        }
    }
}

static void AssignUStoreAttr(const char* newval, void* extra)
{
    char* rawstring = NULL;
    char* rawstringNoparent  = NULL; // rawstring without parenthese
    List* elemlist = NULL;
    ListCell* cell = NULL;

    rawstring = pstrdup(newval);
    rawstringNoparent  = rawstring;

    // if rawstring is surrounded by quotes, remove the quotes
    if (rawstringNoparent  && rawstringNoparent[0] == '\"') {
        rawstringNoparent++;
        if (rawstringNoparent[strlen(rawstringNoparent) - 1] == '\"') {
            rawstringNoparent[strlen(rawstringNoparent) - 1] = '\0';
        }
    }

    if (!SplitIdentifierString(rawstringNoparent, ';', &elemlist, false, false)) {
        pfree(rawstring);
        list_free(elemlist);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("an error occured while parsing ustore_attr")));
        return;
    }

    foreach (cell, elemlist) {
        char* item = static_cast<char*>(lfirst(cell));

        char* ptoken = NULL;
        char* psave = NULL;
        const char* pdelimiter = "=";

        ptoken = TrimStr(strtok_r(item, pdelimiter, &psave));
        if (!IS_NULL_STR(ptoken)) {
            if (strcasecmp(ptoken, "enable_ustore_partial_seqscan") == 0) {
                ParseUStoreBool(&u_sess->attr.attr_storage.enable_ustore_partial_seqscan, ptoken, pdelimiter, psave);
            } else if (strcasecmp(ptoken, "enable_candidate_buf_usage_count") == 0) {
                ParseUStoreBool(&u_sess->attr.attr_storage.enable_candidate_buf_usage_count, ptoken, pdelimiter, psave);
            } else if (strcasecmp(ptoken, "ustats_tracker_naptime") == 0) {
                ParseUStoreInt(&u_sess->attr.attr_storage.ustats_tracker_naptime, ptoken, pdelimiter, psave,
                    MIN_USTATS_TRACKER_NAPTIME, MAX_USTATS_TRACKER_NAPTIME);
            } else if (strcasecmp(ptoken, "umax_search_length_for_prune") == 0) {
                ParseUStoreInt(&u_sess->attr.attr_storage.ustats_tracker_naptime, ptoken, pdelimiter, psave,
                    MIN_UMAX_PRUNE_SEARCH_LEN, MAX_UMAX_PRUNE_SEARCH_LEN);
#ifdef ENABLE_WHITEBOX
            } else if (strcasecmp(ptoken, "ustore_unit_test") == 0) {
                AssignUStoreUnitTest(psave, extra);
#endif
            }
        }
    }

    pfree(rawstring);
    list_free(elemlist);
}

static bool check_snapshot_delimiter(char** newval, void** extra, GucSource source)
{
    return (strlen(*newval) == 1 && (!u_sess->attr.attr_sql.db4ai_snapshot_version_separator
                                     || **newval != *u_sess->attr.attr_sql.db4ai_snapshot_version_separator));
}
static bool check_snapshot_separator(char** newval, void** extra, GucSource source)
{
    return (strlen(*newval) == 1 && (!u_sess->attr.attr_sql.db4ai_snapshot_version_delimiter
                                     || **newval != *u_sess->attr.attr_sql.db4ai_snapshot_version_delimiter));
}
