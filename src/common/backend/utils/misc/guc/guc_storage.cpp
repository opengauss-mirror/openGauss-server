/* --------------------------------------------------------------------
 * guc_storage.cpp
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
 * src/backend/utils/misc/guc/guc_storage.cpp
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
#include "replication/dcf_replication.h"
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
#include "utils/guc_storage.h"

#define atooid(x) ((Oid)strtoul((x), NULL, 10))

const int PROC_OID_LIST_LENGTH = 2;
const int TYPE_OID_LIST_LENGTH = 4;
const int CATALOG_OID_LIST_LENGTH = 7;
const int NAMESPACE_OID_LIST_LENGTH = 2;
const int GENERAL_OID_LIST_LENGTH = 2;
const int MS_PER_S = 1000;
const int BUFSIZE = 1024;
const int MAX_CPU_NUMS = 104;
const int MAXLENS = 5;

static bool check_and_assign_catalog_oids(List* elemlist);
static const char* show_archive_command(void);
bool check_enable_gtm_free(bool* newval, void** extra, GucSource source);
static bool check_phony_autocommit(bool* newval, void** extra, GucSource source);
static bool check_enable_data_replicate(bool* newval, void** extra, GucSource source);
static bool check_adio_debug_guc(bool* newval, void** extra, GucSource source);
static bool check_adio_function_guc(bool* newval, void** extra, GucSource source);
static bool check_temp_buffers(int* newval, void** extra, GucSource source);
static bool check_replication_type(int* newval, void** extra, GucSource source);
static void plog_merge_age_assign(int newval, void* extra);
static bool check_replconninfo(char** newval, void** extra, GucSource source);
static void assign_replconninfo1(const char* newval, void* extra);
static void assign_replconninfo2(const char* newval, void* extra);
static void assign_replconninfo3(const char* newval, void* extra);
static void assign_replconninfo4(const char* newval, void* extra);
static void assign_replconninfo5(const char* newval, void* extra);
static void assign_replconninfo6(const char* newval, void* extra);
static void assign_replconninfo7(const char* newval, void* extra);
static void assign_replconninfo8(const char* newval, void* extra);
static void assign_replconninfo9(const char* newval, void* extra);
static void assign_replconninfo10(const char* newval, void* extra);
static void assign_replconninfo11(const char* newval, void* extra);
static void assign_replconninfo12(const char* newval, void* extra);
static void assign_replconninfo13(const char* newval, void* extra);
static void assign_replconninfo14(const char* newval, void* extra);
static void assign_replconninfo15(const char* newval, void* extra);
static void assign_replconninfo16(const char* newval, void* extra);
static void assign_replconninfo17(const char* newval, void* extra);
static void assign_replconninfo18(const char* newval, void* extra);
static void assign_cross_cluster_replconninfo1(const char* newval, void* extra);
static void assign_cross_cluster_replconninfo2(const char* newval, void* extra);
static void assign_cross_cluster_replconninfo3(const char* newval, void* extra);
static void assign_cross_cluster_replconninfo4(const char* newval, void* extra);
static void assign_cross_cluster_replconninfo5(const char* newval, void* extra);
static void assign_cross_cluster_replconninfo6(const char* newval, void* extra);
static void assign_cross_cluster_replconninfo7(const char* newval, void* extra);
static void assign_cross_cluster_replconninfo8(const char* newval, void* extra);
static const char* logging_module_guc_show(void);
static bool check_inplace_upgrade_next_oids(char** newval, void** extra, GucSource source);
static bool check_autovacuum_max_workers(int* newval, void** extra, GucSource source);
static ReplConnInfo* ParseReplConnInfo(const char* ConnInfoList, int* InfoLength);
static bool check_and_assign_proc_oids(List* elemlist);
static bool check_and_assign_type_oids(List* elemlist);
static bool check_and_assign_namespace_oids(List* elemlist);
static bool check_and_assign_general_oids(List* elemlist);
static int GetLengthAndCheckReplConn(const char* ConnInfoList);

#ifndef ENABLE_MULTIPLE_NODES
static void assign_dcf_election_timeout(int newval, void* extra);
static void assign_dcf_auto_elc_priority_en(int newval, void* extra);
static void assign_dcf_election_switch_threshold(int newval, void* extra);
static void assign_dcf_run_mode(int newval, void* extra);
static void assign_dcf_log_level(const char* newval, void* extra);
static void assign_dcf_max_log_file_size(int newval, void* extra);
static void assign_dcf_flow_control_cpu_threshold(int newval, void* extra);
static void assign_dcf_flow_control_net_queue_message_num_threshold(int newval, void* extra);
static void assign_dcf_flow_control_disk_rawait_threshold(int newval, void* extra);
static void assign_dcf_log_backup_file_count(int newval, void* extra);
static void assign_dcf_flow_control_rto(int newval, void *extra);
static void assign_dcf_flow_control_rpo(int newval, void *extra);
#endif

static void InitStorageConfigureNamesBool();
static void InitStorageConfigureNamesInt();
static void InitStorageConfigureNamesInt64();
static void InitStorageConfigureNamesReal();
static void InitStorageConfigureNamesString();
static void InitStorageConfigureNamesEnum();

static const struct config_enum_entry resource_track_log_options[] = {
    {"summary", SUMMARY, false},
    {"detail", DETAIL, false},
    {NULL, 0, false}
};

/* autovac mode */
static const struct config_enum_entry autovacuum_mode_options[] = {
    {"analyze", AUTOVACUUM_DO_ANALYZE, false},
    {"vacuum", AUTOVACUUM_DO_VACUUM, false},
    {"mix", AUTOVACUUM_DO_ANALYZE_VACUUM, false},
    {"none", AUTOVACUUM_DO_NONE, false},
    {NULL, 0, false}
};

/*
 * define insert mode for dfs_insert
 */
static const struct config_enum_entry cstore_insert_mode_options[] = {
    {"auto", TO_AUTO, true},
    {"main", TO_MAIN, true},
    {"delta", TO_DELTA, true},
    {NULL, 0, false}
};

static const struct config_enum_entry remote_read_options[] = {
    {"off", REMOTE_READ_OFF, false},
    {"non_authentication", REMOTE_READ_NON_AUTH, false},
    {"authentication", REMOTE_READ_AUTH, false},
    {NULL, 0, false}
};

/*
 * Although only "on", "off", "remote_write", and "local" are documented, we
 * accept all the likely variants of "on" and "off".
 */
static const struct config_enum_entry synchronous_commit_options[] = {
    {"local", SYNCHRONOUS_COMMIT_LOCAL_FLUSH, false},
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
    {NULL, 0, false}
};

#ifndef ENABLE_MULTIPLE_NODES
static const struct config_enum_entry dcf_log_file_permission_options[] = {
        {"600", DCF_LOG_FILE_PERMISSION_600, false},
        {"640", DCF_LOG_FILE_PERMISSION_640, false},
        {NULL, 0, false}
};

static const struct config_enum_entry dcf_log_path_permission_options[] = {
        {"700", DCF_LOG_PATH_PERMISSION_700, false},
        {"750", DCF_LOG_PATH_PERMISSION_750, false},
        {NULL, 0, false}
};
static const struct config_enum_entry dcf_run_mode_options[] = {
        {"0", DCF_RUN_MODE_AUTO, false},
        {"2", DCF_RUN_MODE_DISABLE, false},
        {NULL, 0, false}
};
#endif

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
void InitStorageConfigureNames()
{
    InitStorageConfigureNamesBool();
    InitStorageConfigureNamesInt();
    InitStorageConfigureNamesInt64();
    InitStorageConfigureNamesReal();
    InitStorageConfigureNamesString();
    InitStorageConfigureNamesEnum();

    return;
}

static void InitStorageConfigureNamesBool()
{
    struct config_bool localConfigureNamesBool[] = {
        {{"raise_errors_if_no_files",
            PGC_SUSET,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("raise errors if no files to be imported."),
            NULL},
            &u_sess->attr.attr_storage.raise_errors_if_no_files,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_delta_store",
            PGC_POSTMASTER,
            NODE_ALL,
            QUERY_TUNING,
            gettext_noop("Enable delta for column store."),
            NULL},
            &g_instance.attr.attr_storage.enable_delta_store,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_incremental_catchup",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_STANDBY,
            gettext_noop("Enable incremental searching bcm files when catchup."),
            },
            &u_sess->attr.attr_storage.enable_incremental_catchup,
            true,
            NULL,
            NULL,
            NULL},
        {{"fsync",
            PGC_SIGHUP,
            NODE_ALL,
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
        {{"full_page_writes",
            PGC_SIGHUP,
            NODE_ALL,
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
            NODE_ALL,
            WAL_SETTINGS,
            gettext_noop("Writes full pages to WAL when first modified after a checkpoint, even for a non-critical "
                        "modifications."),
            NULL},
            &g_instance.attr.attr_storage.wal_log_hints,
            true,
            NULL,
            NULL,
            NULL},
        {{"log_connections",
            PGC_BACKEND,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Logs each successful connection."),
            NULL},
            &u_sess->attr.attr_storage.Log_connections,
            false,
            NULL,
            NULL,
            NULL},
        {{"autovacuum",
            PGC_SIGHUP,
            NODE_ALL,
            AUTOVACUUM,
            gettext_noop("Starts the autovacuum subprocess."),
            NULL},
            &u_sess->attr.attr_storage.autovacuum_start_daemon,
            true,
            NULL,
            NULL,
            NULL},
#ifdef LOCK_DEBUG
        {{"trace_locks",
            PGC_SUSET,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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

        {{"log_lock_waits",
            PGC_SUSET,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Logs long lock waits."),
            NULL},
            &u_sess->attr.attr_storage.log_lock_waits,
            false,
            NULL,
            NULL,
            NULL},
        /* only here for backwards compatibility */
        {{"autocommit",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("This parameter doesn't do anything."),
            gettext_noop("It's just here so that we won't choke on SET AUTOCOMMIT TO ON from 7.3-vintage clients."),
            GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_storage.phony_autocommit,
            true,
            check_phony_autocommit,
            NULL,
            NULL},
        /*
         * security requirements: system/ordinary users can not set current transaction to read-only,
         * promoting the setting level to database level.
         */
        {{"default_transaction_read_only",
#ifdef ENABLE_MULTIPLE_NODES
            PGC_SIGHUP,
#else
            PGC_USERSET,
#endif
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets the default read-only status of new transactions."),
            NULL},
            &u_sess->attr.attr_storage.DefaultXactReadOnly,
            false,
            check_default_transaction_read_only,
            NULL,
            NULL},
        {{"default_transaction_deferrable",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
#ifdef WAL_DEBUG
        {{"wal_debug",
            PGC_SUSET,
            NODE_ALL,
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
        {{"synchronize_seqscans",
            PGC_USERSET,
            NODE_ALL,
            COMPAT_OPTIONS_PREVIOUS,
            gettext_noop("Enables synchronized sequential scans."),
            NULL},
            &u_sess->attr.attr_storage.synchronize_seqscans,
            true,
            NULL,
            NULL,
            NULL},
        {{"hot_standby",
            PGC_POSTMASTER,
            NODE_ALL,
            REPLICATION_STANDBY,
            gettext_noop("Allows connections and queries during recovery."),
            NULL},
            &g_instance.attr.attr_storage.EnableHotStandby,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_data_replicate",
            PGC_USERSET,
            NODE_ALL,
            REPLICATION_STANDBY,
            gettext_noop("Allows data replicate."),
            NULL},
            &u_sess->attr.attr_storage.enable_data_replicate,
            true,
            check_enable_data_replicate,
            NULL,
            NULL},

        {{"enable_wal_shipping_compression",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("enable compress xlog during xlog shipping."),
            NULL},
            &g_instance.attr.attr_storage.enable_wal_shipping_compression,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_mix_replication",
            PGC_POSTMASTER,
            NODE_ALL,
            REPLICATION,
            gettext_noop("All the replication log sent by the wal streaming."),
            NULL},
            &g_instance.attr.attr_storage.enable_mix_replication,
            false,
            check_mix_replication_param,
            NULL,
            NULL},

#ifndef ENABLE_MULTIPLE_NODES
        {{"enable_dcf",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Enable DCF to replicate redo Log."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.enable_dcf,
            false,
            NULL,
            NULL,
            NULL},
        {{"dcf_ssl",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("DCF enable ssl on."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_ssl,
            true,
            NULL,
            NULL,
            NULL},
#endif
        {{"ha_module_debug",
            PGC_USERSET,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("debug ha module."),
            NULL},
            &u_sess->attr.attr_storage.HaModuleDebug,
            false,
            NULL,
            NULL,
            NULL},
        {{"hot_standby_feedback",
            PGC_SIGHUP,
            NODE_ALL,
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
            NODE_ALL,
            REPLICATION_STANDBY,
            gettext_noop("Allows stream replication to standby or secondary."),
            NULL},
            &u_sess->attr.attr_storage.enable_stream_replication,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_roach_standby_cluster",
            PGC_POSTMASTER,
            NODE_ALL,
            REPLICATION,
            gettext_noop("True if current instance is in ROACH standby cluster."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_storage.IsRoachStandbyCluster,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_twophase_commit",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            XC_HOUSEKEEPING_OPTIONS,
            gettext_noop("Enable two phase commit when gtm free is on."),
            NULL},
            &u_sess->attr.attr_storage.enable_twophase_commit,
            true,
            NULL,
            NULL,
            NULL},
        {{"most_available_sync",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_MASTER,
            gettext_noop("Enables master to continue when sync standbys failure."),
            NULL,
            },
            &u_sess->attr.attr_storage.guc_most_available_sync,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_show_any_tuples",
            PGC_USERSET,
            NODE_ALL,
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
        {{"enable_debug_vacuum",
            PGC_SIGHUP,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("This parameter is just used for logging some vacuum info."),
            NULL,
            },
            &u_sess->attr.attr_storage.enable_debug_vacuum,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_adio_debug",
            PGC_SUSET,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Enable log debug adio function."),
            NULL},
            &u_sess->attr.attr_storage.enable_adio_debug,
            false,
            check_adio_debug_guc,
            NULL,
            NULL},

        {{"enable_adio_function",
            PGC_INTERNAL,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Enable adio function."),
            NULL},
            &g_instance.attr.attr_storage.enable_adio_function,
            false,
            check_adio_function_guc,
            NULL,
            NULL},
        {{"gds_debug_mod",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            LOGGING_WHEN,
            gettext_noop("Enable GDS-related troubleshoot-logging."),
            NULL},
            &u_sess->attr.attr_storage.gds_debug_mod,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_cbm_tracking",
            PGC_SIGHUP,
            NODE_ALL,
            WAL,
            gettext_noop("Turn on cbm tracking function. "),
            NULL},
            &u_sess->attr.attr_storage.enable_cbm_tracking,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_copy_server_files",
            PGC_SIGHUP,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("enable sysadmin to copy from/to file"),
            NULL},
            &u_sess->attr.attr_storage.enable_copy_server_files,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_access_server_directory",
            PGC_SIGHUP,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("enable sysadmin to create directory"),
            NULL},
            &g_instance.attr.attr_storage.enable_access_server_directory,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_incremental_checkpoint",
            PGC_POSTMASTER,
            NODE_ALL,
            WAL_CHECKPOINTS,
            gettext_noop("Enable master incremental checkpoint."),
            NULL,
            },
            &g_instance.attr.attr_storage.enableIncrementalCheckpoint,
            true,
            NULL,
            NULL,
            NULL},

        {{"enable_double_write",
            PGC_POSTMASTER,
            NODE_ALL,
            WAL_CHECKPOINTS,
            gettext_noop("Enable master double write."),
            NULL,
            },
            &g_instance.attr.attr_storage.enable_double_write,
            true,
            NULL,
            NULL,
            NULL},

        {{"log_pagewriter",
            PGC_SIGHUP,
            NODE_ALL,
            LOGGING_WHAT,
            gettext_noop("Logs pagewriter thread."),
            NULL},
            &u_sess->attr.attr_storage.log_pagewriter,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_page_lsn_check",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
            WAL,
            gettext_noop("Enable xlog prune when not all standys connected "
                         "and xlog size is largger than max_xlog_size"),
            NULL},
            &u_sess->attr.attr_storage.enable_xlog_prune,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_recyclebin",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            UNGROUPED,
            gettext_noop("Enable recyclebin for user-defined objects restore."),
            NULL},
            &u_sess->attr.attr_storage.enable_recyclebin,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_defer_calculate_snapshot",
            PGC_SIGHUP,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Enable defer to calculate mvcc snapshot when commit"),
            NULL},
            &u_sess->attr.attr_storage.enable_defer_calculate_snapshot,
            true,
            NULL,
            NULL,
            NULL},
#ifdef USE_ASSERT_CHECKING
        {{"enable_segment",
            PGC_SIGHUP,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("create segment table, only used in debug mode"),
            NULL},
            &u_sess->attr.attr_storage.enable_segment,
            false,
            NULL,
            NULL,
            NULL},
#endif
        {{"enable_gtm_free",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            XC_HOUSEKEEPING_OPTIONS,
            gettext_noop("Turns on gtm free mode."),
            NULL},
            &g_instance.attr.attr_storage.enable_gtm_free,
            false,
            check_enable_gtm_free,
            NULL,
            NULL},
        /* logic connection between cn & dn */
        {{"comm_cn_dn_logic_conn",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            CLIENT_CONN,
            gettext_noop("Whether use logic connection between cn and dn"),
            NULL,
            },
            &g_instance.attr.attr_storage.comm_cn_dn_logic_conn,
            false,
            NULL,
            NULL,
            NULL},

        {{"enable_consider_usecount",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_KERNEL,
            gettext_noop("Whether consider buffer usecount when get buffer slot"),
            NULL,
            },
            &u_sess->attr.attr_storage.enable_candidate_buf_usage_count,
            false,
            NULL,
            NULL,
            NULL},

#ifdef USE_ASSERT_CHECKING
        {{"enable_hashbucket",
            PGC_SUSET,
            NODE_DISTRIBUTE,
            UNGROUPED,
            gettext_noop("create hash bucket table, only used in debug mode"),
            NULL},
            &u_sess->attr.attr_storage.enable_hashbucket,
            false,
            NULL,
            NULL,
            NULL},
#endif

#ifdef ENABLE_MULTIPLE_NODES
        {{"auto_csn_barrier",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            WAL,
            gettext_noop("Enable auto csn barrier creation."),
            NULL},
            &g_instance.attr.attr_storage.auto_csn_barrier,
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
    u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_STORAGE] =
        (struct config_bool*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_STORAGE], bytes,
        localConfigureNamesBool, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitStorageConfigureNamesInt()
{
    struct config_int localConfigureNamesInt[] = {
        {{"max_active_global_temporary_table",
            PGC_USERSET,
            NODE_SINGLENODE,
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
        /*
         * We sometimes multiply the number of shared buffers by two without
         * checking for overflow, so we mustn't allow more than INT_MAX / 2.
         */
        {{"shared_buffers",
            PGC_POSTMASTER,
            NODE_ALL,
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
        {{"segment_buffers",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the number of shared memory buffers used by the server."),
            NULL,
            GUC_UNIT_BLOCKS},
            &g_instance.attr.attr_storage.NSegBuffers,
            1024,
            16,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},
        {{"vacuum_gtt_defer_check_age",
            PGC_USERSET,
            NODE_SINGLENODE,
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
        {{"wait_dummy_time",
            PGC_SIGHUP,
            NODE_ALL,
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
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_STANDBY,
            gettext_noop("The maximal allowed duration for waiting from catchup to normal state."),
            NULL,
            GUC_UNIT_MS},
            &u_sess->attr.attr_storage.catchup2normal_wait_time,
            -1,
            -1,
            10000,
            NULL,
            NULL,
            NULL},
        /* This is PGC_SUSET to prevent hiding from log_lock_waits. */
        {{"deadlock_timeout",
            PGC_SUSET,
            NODE_ALL,
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
        /* This is PGC_SUSET to prevent hiding from log_lock_waits. */
        {{"lockwait_timeout",
            PGC_SUSET,
            NODE_ALL,
            LOCK_MANAGEMENT,
            gettext_noop("Sets the max time to wait on a lock acquire."),
            NULL,
            GUC_UNIT_MS},
            &u_sess->attr.attr_storage.LockWaitTimeout,
            1200000,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        /* This is PGC_SUSET to prevent hiding from log_lock_waits. */
        {{"update_lockwait_timeout",
            PGC_SUSET,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_SINGLENODE,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NULL},
        {{"basebackup_timeout",
            PGC_USERSET,
            NODE_SINGLENODE,
            WAL_SETTINGS,
            gettext_noop("Sets the timeout in seconds for a reponse from gs_basebackup."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_storage.basebackup_timeout,
            60 * 10,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"wal_receiver_connect_timeout",
            PGC_SIGHUP,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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

#ifndef ENABLE_MULTIPLE_NODES
        {{"dcf_node_id",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Local node id in DCF replication group."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_node_id,
            1,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"dcf_max_workers",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the maximum number of DCF worker threads."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_max_workers,
            10,
            10,
            MAX_BACKENDS,
            NULL,
            NULL,
            NULL},
        {{"dcf_election_timeout",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the election timeout of local DCF node."),
            NULL},
            &u_sess->attr.attr_storage.dcf_attr.dcf_election_timeout,
            3,
            1,
            600,
            NULL,
            assign_dcf_election_timeout,
            NULL},
        {{"dcf_enable_auto_election_priority",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the election priority of local DCF node."),
            NULL},
            &u_sess->attr.attr_storage.dcf_attr.dcf_auto_elc_priority_en,
            1,
            0,
            1,
            NULL,
            assign_dcf_auto_elc_priority_en,
            NULL},
        {{"dcf_election_switch_threshold",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the election switch threshold of local DCF node."),
            NULL},
            &u_sess->attr.attr_storage.dcf_attr.dcf_election_switch_threshold,
            0,
            0,
            INT_MAX,
            NULL,
            assign_dcf_election_switch_threshold,
            NULL},
        {{"dcf_max_log_file_size",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the max log file size of local DCF node."),
            NULL},
            &u_sess->attr.attr_storage.dcf_attr.dcf_max_log_file_size,
            10,
            1,
            1000,
            NULL,
            assign_dcf_max_log_file_size,
            NULL},
        {{"dcf_flow_control_cpu_threshold",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the dcf flow control cpu threshold of local DCF node."),
            NULL},
            &u_sess->attr.attr_storage.dcf_attr.dcf_flow_control_cpu_threshold,
            100,
            0,
            INT_MAX,
            NULL,
            assign_dcf_flow_control_cpu_threshold,
            NULL},
        {{"dcf_flow_control_net_queue_message_num_threshold",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the dcf cm default net queue mess num of local DCF node."),
            NULL},
            &u_sess->attr.attr_storage.dcf_attr.dcf_flow_control_net_queue_message_num_threshold,
            1024,
            0,
            INT_MAX,
            NULL,
            assign_dcf_flow_control_net_queue_message_num_threshold,
            NULL},
        {{"dcf_flow_control_disk_rawait_threshold",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the dcf flow control disk rawait threshold of local DCF node."),
            NULL},
            &u_sess->attr.attr_storage.dcf_attr.dcf_flow_control_disk_rawait_threshold,
            100000,
            0,
            INT_MAX,
            NULL,
            assign_dcf_flow_control_disk_rawait_threshold,
            NULL},
        {{"dcf_truncate_threshold",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the dcf truncate threshold of local DCF node."),
            NULL},
            &u_sess->attr.attr_storage.dcf_attr.dcf_truncate_threshold,
            100000,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"dcf_log_backup_file_count",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the log backup file count of local DCF node."),
            NULL},
            &u_sess->attr.attr_storage.dcf_attr.dcf_log_backup_file_count,
            10,
            1,
            100,
            NULL,
            assign_dcf_log_backup_file_count,
            NULL},
        {{"dcf_mec_agent_thread_num",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the mec agent thread num of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_mec_agent_thread_num,
            10,
            1,
            1000,
            NULL,
            NULL,
            NULL},
        {{"dcf_mec_reactor_thread_num",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the mec reactor thread num of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_mec_reactor_thread_num,
            1,
            1,
            100,
            NULL,
            NULL,
            NULL},
        {{"dcf_mec_channel_num",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the mec channel num of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_mec_channel_num,
            1,
            1,
            64,
            NULL,
            NULL,
            NULL},
        {{"dcf_mem_pool_init_size",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the mem pool init size of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_mem_pool_init_size,
            32,
            32,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"dcf_mem_pool_max_size",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the mem pool max size of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_mem_pool_max_size,
            2048,
            32,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"dcf_compress_algorithm",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the compress algorithm of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_compress_algorithm,
            0,
            0,
            2,
            NULL,
            NULL,
            NULL},
        {{"dcf_compress_level",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the compress level of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_compress_level,
            1,
            1,
            22,
            NULL,
            NULL,
            NULL},
        {{"dcf_socket_timeout",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the socket timeout of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_socket_timeout,
            5000,
            10,
            600000,
            NULL,
            NULL,
            NULL},
        {{"dcf_connect_timeout",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the connect timeout of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_connect_timeout,
            60000,
            10,
            600000,
            NULL,
            NULL,
            NULL},
        {{"dcf_rep_append_thread_num",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the rep append thread num of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_rep_append_thread_num,
            2,
            1,
            1000,
            NULL,
            NULL,
            NULL},
        {{"dcf_mec_fragment_size",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the mec fragment size of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_mec_fragment_size,
            64,
            32,
            10 * 1024,
            NULL,
            NULL,
            NULL},
        {{"dcf_stg_pool_init_size",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the stg pool init size of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_stg_pool_init_size,
            32,
            32,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"dcf_stg_pool_max_size",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the stg pool max size of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_stg_pool_max_size,
            2048,
            32,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"dcf_mec_pool_max_size",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the mec pool max size of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_mec_pool_max_size,
            200,
            32,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"dcf_mec_batch_size",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the mec batch size of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_mec_batch_size,
            0,
            0,
            1024,
            NULL,
            NULL,
            NULL},
#endif

        {{"cstore_buffers",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
        {{"psort_work_mem",
            PGC_USERSET,
            NODE_ALL,
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
        {{"bulk_write_ring_size",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
        {{"vacuum_cost_page_hit",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
        /*
         * See also CheckRequiredParameterValues() if this parameter changes
         */
        {{"max_prepared_transactions",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the maximum number of simultaneously prepared transactions."),
            NULL},
            &g_instance.attr.attr_storage.max_prepared_xacts,
#ifdef PGXC
            10,
            0,
            MAX_BACKENDS,
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
            NODE_ALL,
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
            NODE_ALL,
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
        {{"RepOriginId",
            PGC_USERSET,
            NODE_ALL,
            REPLICATION_STANDBY,
            gettext_noop("RepOriginId."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_storage.replorigin_sesssion_origin,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        /*
         * See also CheckRequiredParameterValues() if this parameter changes
         */
        {{"max_locks_per_transaction",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
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
        {{"wal_keep_segments",
            PGC_SIGHUP,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
        {{"walwriter_cpu_bind",
            PGC_POSTMASTER,
            NODE_ALL,
            WAL_SETTINGS,
            gettext_noop("Sets the binding CPU number for the WAL writer thread."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_storage.walwriter_cpu_bind,
            -1,
            -1,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"wal_file_init_num",
            PGC_POSTMASTER,
            NODE_ALL,
            WAL_SETTINGS,
            gettext_noop("Sets the number of xlog segment files that WAL writer auxiliary thread "
                        "creates at one time."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_storage.wal_file_init_num,
            10,
            0,
            1000000,
            NULL,
            NULL,
            NULL},

        {{"advance_xlog_file_num",
            PGC_POSTMASTER,
            NODE_ALL,
            WAL_SETTINGS,
            gettext_noop("Sets the number of xlog files to be initialized in advance."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_storage.advance_xlog_file_num,
            0,
            0,
            1000000,
            NULL,
            NULL,
            NULL},
        {{"wal_flush_timeout",
            PGC_SIGHUP,
            NODE_ALL,
            WAL_SETTINGS,
            gettext_noop("set timeout when iterator table entry."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_storage.wal_flush_timeout,
            2,
            0,
            90000000,
            NULL,
            NULL,
            NULL},
        {{"wal_flush_delay",
            PGC_SIGHUP,
            NODE_ALL,
            WAL_SETTINGS,
            gettext_noop("set delay time when iterator table entry."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_storage.wal_flush_delay,
            1,
            0,
            90000000,
            NULL,
            NULL,
            NULL},
        {{"checkpoint_wait_timeout",
            PGC_SIGHUP,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
        {{"archive_interval",
            PGC_SIGHUP,
            NODE_ALL,
            WAL_ARCHIVING,
            gettext_noop("OBS archive time interval."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_storage.archive_interval,
            1,
            1,
            1000,
            NULL,
            NULL,
            NULL},
        /* see max_connections */
        {{"max_wal_senders",
            PGC_POSTMASTER,
            NODE_ALL,
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
            1024,
            NULL,
            NULL,
            NULL},
        /* see max_connections */
        {{"max_replication_slots",
            PGC_POSTMASTER,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("Sets the maximum number of simultaneously defined replication slots."),
            NULL},
            &g_instance.attr.attr_storage.max_replication_slots,
            8,
            0,
            1024, /* XXX? */
            NULL,
            NULL,
            NULL},

        {{"max_logical_replication_workers",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION,
            gettext_noop("Maximum number of logical replication worker processes."),
            NULL},
            &g_instance.attr.attr_storage.max_logical_replication_workers,
            4,
            0,
            MAX_BACKENDS,
            NULL,
            NULL,
            NULL},

        {{"recovery_time_target",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("The target redo time in seconds for recovery"),
            NULL},
            &u_sess->attr.attr_storage.target_rto,
            0,
            0,
            3600,
            NULL,
#ifndef ENABLE_MULTIPLE_NODES
            assign_dcf_flow_control_rto,
#else
            NULL,
#endif
            NULL},
        {{"time_to_target_rpo",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("The time to the target recovery point in seconds"),
            NULL},
            &u_sess->attr.attr_storage.time_to_target_rpo,
            0,
            0,
            3600,
            NULL,
#ifndef ENABLE_MULTIPLE_NODES
            assign_dcf_flow_control_rpo,
#else
            NULL,
#endif
            NULL},
        {{"hadr_recovery_time_target",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("The target redo time in seconds for recovery in disaster mode"),
            NULL},
            &u_sess->attr.attr_storage.hadr_recovery_time_target,
            0,
            0,
            3600,
            NULL,
            NULL,
            NULL},
        {{"hadr_recovery_point_target",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("The time to the target recovery point in seconds in disaster mode"),
            NULL},
            &u_sess->attr.attr_storage.hadr_recovery_point_target,
            0,
            0,
            3600,
            NULL,
            NULL,
            NULL},

        {{"wal_sender_timeout",
            PGC_SIGHUP,
            NODE_ALL,
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

        {{"replication_type",
            PGC_POSTMASTER,
            NODE_ALL,
            WAL_SETTINGS,
            gettext_noop("Sets the dn's HA mode."),
            NULL},
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
            NODE_ALL,
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
            NODE_ALL,
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
        {{"commit_siblings",
            PGC_USERSET,
            NODE_ALL,
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
        {{"log_min_duration_statement",
            PGC_SUSET,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
        {{"max_index_keys",
            PGC_INTERNAL,
            NODE_ALL,
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
            NODE_ALL,
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
        {{"block_size",
            PGC_INTERNAL,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
        /* see max_connections */
        {{"autovacuum_max_workers",
            PGC_POSTMASTER,
            NODE_ALL,
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
        /* see max_connections */
        {{"max_undo_workers",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            CLIENT_CONN_OTHER,
            gettext_noop("Sets the maximum number of simultaneously running undo worker processes."),
            NULL},
            &g_instance.attr.attr_storage.max_undo_workers,
            5,
            1,
            100,
            NULL,
            NULL,
            NULL},
        /* This is PGC_SIGHUP to set the interval time to push cut_off_csn_num. 0 means never do cleanup */
        {{"defer_csn_cleanup_time",
            PGC_SIGHUP,
            NODE_ALL,
            DATA_NODES,
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
        {{"prefetch_quantity",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
        {{"dfs_partition_directory_length",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
        {{"recovery_max_workers",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
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
        {{"incremental_checkpoint_timeout",
            PGC_SIGHUP,
            NODE_ALL,
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

        {{"pagewriter_sleep",
            PGC_SIGHUP,
            NODE_ALL,
            WAL_CHECKPOINTS,
            gettext_noop("PageWriter sleep time."),
            NULL,
            GUC_UNIT_MS},
            &u_sess->attr.attr_storage.pageWriterSleep,
            2000,
            0,
            3600 * 1000,
            NULL,
            NULL,
            NULL},

        {{"pagewriter_thread_num",
            PGC_POSTMASTER,
            NODE_ALL,
            WAL_CHECKPOINTS,
            gettext_noop("Sets the number of page writer threads."),
            NULL,
            0},
            &g_instance.attr.attr_storage.pagewriter_thread_num,
            4,
            1,
            16,
            NULL,
            NULL,
            NULL},
         {{"dw_file_num",
            PGC_POSTMASTER,
            NODE_ALL,
            WAL_CHECKPOINTS,
            gettext_noop("Sets the number of dw batch files."),
            NULL,
            0},
            &g_instance.attr.attr_storage.dw_file_num,
            1,
            1,
            16,
            NULL,
            NULL,
            NULL},
         {{"dw_file_size",
            PGC_POSTMASTER,
            NODE_ALL,
            WAL_CHECKPOINTS,
            gettext_noop("Sets the size of each dw batch file."),
            NULL,
            0},
            &g_instance.attr.attr_storage.dw_file_size,
            256,
            32,
            256,
            NULL,
            NULL,
            NULL},
        {{"recovery_parse_workers",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
        {{"max_size_for_xlog_prune",
            PGC_SIGHUP,
            NODE_ALL,
            WAL,
            gettext_noop("This param set by user is used for xlog to be recycled when not all are connected "
                         "and the ""param enable_xlog_prune is on."),
            NULL,
            GUC_UNIT_KB},
            &u_sess->attr.attr_storage.max_size_for_xlog_prune,
            INT_MAX,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"hadr_max_size_for_xlog_receiver",
             PGC_SIGHUP,
             NODE_ALL,
             WAL,
             gettext_noop(
                 "This param set by user is used for xlog to stop receiving when "
                 "the gap is larger than this param between replay xlog location and walreceiver received location."),
             NULL,
             GUC_UNIT_KB},
            &g_instance.attr.attr_storage.max_size_for_xlog_receiver,
            268435456,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},

#ifdef EXTREME_RTO_DEBUG_AB
        {{"rto_ab_pos",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
        {{"undo_space_limit_size",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            RESOURCES_DISK,
            gettext_noop("Undo space limit size for force recycle."),
            NULL,
            GUC_UNIT_BLOCKS},
            &u_sess->attr.attr_storage.undo_space_limit_size,
            33554432,  /* 256 GB */
            102400,   /* 800 MB */
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"undo_limit_size_per_transaction",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            RESOURCES_DISK,
            gettext_noop("Undo limit size per transaction."),
            NULL,
            GUC_UNIT_BLOCKS},
            &u_sess->attr.attr_storage.undo_limit_size_transaction,
            4194304,  /* 32 GB */
            256,      /* 2 MB */
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"undo_zone_count",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Undo zone total count."),
            NULL},
            &g_instance.attr.attr_storage.undo_zone_count,
            0,  /* default undo zone count */
            0,      /* disable undo */
            1048576,
            NULL,
            NULL,
            NULL},
        {{"keep_sync_window",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_MASTER,
            gettext_noop("The length of keep sync window."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_storage.keep_sync_window,
            0,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"max_concurrent_autonomous_transactions",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Maximum number of concurrent autonomous transactions processes."),
            NULL},
            &g_instance.attr.attr_storage.max_concurrent_autonomous_transactions,
            10,
            0,
            1024,
            NULL,
            NULL,
            NULL},
        {{"gs_clean_timeout",
            PGC_SIGHUP,
            NODE_ALL,
            STATS_MONITORING,
            gettext_noop("Sets the timeout to call gs_clean."),
            gettext_noop("A value of 0 turns off the timeout."),
            GUC_UNIT_S},
            &u_sess->attr.attr_storage.gs_clean_timeout,
            60,
            0,
            INT_MAX / 1000,
            NULL,
            NULL,
            NULL},
        {{"twophase_clean_workers",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            RESOURCES_KERNEL,
            gettext_noop("Sets the parallel job number of gs_clean to clean prepared transactions."),
            NULL},
            &u_sess->attr.attr_storage.twophase_clean_workers,
            3,
            1,
            10,
            NULL,
            NULL,
            NULL},
        {{"gtm_option",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Set gtm option, 0 for GTM ,1 GTMLite and 2 GTMFree"),
            NULL},
            &g_instance.attr.attr_storage.gtm_option,
            GTMOPTION_GTMLITE,
            GTMOPTION_GTM,
            GTMOPTION_GTMFREE,
            NULL,
            NULL,
            NULL},
        {{"gtm_connect_retries",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            GTM,
            gettext_noop("Sets the max retry times to connect GTM."),
            NULL},
            &u_sess->attr.attr_storage.gtm_connect_retries,
            30,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"gtm_conn_check_interval",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            STATS_MONITORING,
            gettext_noop("Sets the interval in seconds to cancel those backends "
                        "connected to a demoted GTM."),
            gettext_noop("A value of 0 turns off the function."),
            GUC_UNIT_S},
            &u_sess->attr.attr_storage.gtm_conn_check_interval,
            10,
            0,
            INT_MAX / 1000,
            NULL,
            NULL,
            NULL},
        {{"default_index_kind",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            DEVELOPER_OPTIONS,
            gettext_noop("Sets default index kind (global or local) when creating index."),
            NULL},
            &u_sess->attr.attr_storage.default_index_kind,
            DEFAULT_INDEX_KIND_GLOBAL,
            DEFAULT_INDEX_KIND_NONE,
            DEFAULT_INDEX_KIND_GLOBAL,
            NULL,
            NULL,
            NULL},
        {{"recyclebin_retention_time",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            UNGROUPED,
            gettext_noop("Sets the maximum retention time of objects in recyclebin."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_storage.recyclebin_retention_time,
            900,
            1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"undo_retention_time",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            UNGROUPED,
            gettext_noop("Sets the maximum retention time of undo record."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_storage.undo_retention_time,
            0,
            0,
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
    u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_STORAGE] =
        (struct config_int*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_STORAGE], bytes, localConfigureNamesInt, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitStorageConfigureNamesReal()
{
    struct config_real localConfigureNamesReal[] = {
        {{"bgwriter_lru_multiplier",
            PGC_SIGHUP,
            NODE_ALL,
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
            NODE_ALL,
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
        {{"autovacuum_vacuum_scale_factor",
            PGC_SIGHUP,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
            RESOURCES_KERNEL,
            gettext_noop(
                "Sets the candidate buffers percent."),
            NULL},
            &u_sess->attr.attr_storage.candidate_buf_percent_target,
            0.3,
            0.1,
            0.85,
            NULL,
            NULL,
            NULL},
        {{"dirty_page_percent_max",
            PGC_SIGHUP,
            NODE_ALL,
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
            NODE_ALL,
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
    u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_STORAGE] =
        (struct config_real*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_STORAGE], bytes,
        localConfigureNamesReal, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitStorageConfigureNamesInt64()
{
    struct config_int64 localConfigureNamesInt64[] = {
        {{"version_retention_age",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_MASTER,
            gettext_noop("Retention age of tuple versions for MVCC-based timecapsule."),
            NULL},
            &u_sess->attr.attr_storage.version_retention_age,
            INT64CONST(0),
            INT64CONST(0),
            INT64CONST(0x7FFFFFFFFFFFFFF),
            NULL,
            NULL,
            NULL},
        {{"vacuum_freeze_min_age",
            PGC_USERSET,
            NODE_ALL,
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
        {{"vacuum_freeze_table_age",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
        /* see varsup.c for why this is PGC_POSTMASTER not PGC_SIGHUP */
        {{"autovacuum_freeze_max_age",
            PGC_POSTMASTER,
            NODE_ALL,
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
        {{"walwriter_sleep_threshold",
            PGC_SIGHUP,
            NODE_ALL,
            WAL_SETTINGS,
            gettext_noop("Number of idle xlog flushes before xlog flusher goes to sleep."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_storage.walwriter_sleep_threshold,
            INT64CONST(500),
            INT64CONST(1),
            INT64CONST(50000),
            NULL,
            NULL,
            NULL},
        {{"xlog_file_size",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            WAL,
            gettext_noop("share storage xlog file size"),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_storage.xlog_file_size,
            INT64CONST(0x8000000000),  // 512*1024*1024*1024
            INT64CONST(128 * 1024 * 1024),
            INT64CONST(0x7FFFFFFFFFFFFFF),
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
    u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_STORAGE] =
        (struct config_int64*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_STORAGE], bytes,
        localConfigureNamesInt64, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitStorageConfigureNamesString()
{
    struct config_string localConfigureNamesString[] = {
        {{"archive_command",
            PGC_SIGHUP,
            NODE_ALL,
            WAL_ARCHIVING,
            gettext_noop("Sets the shell command that will be called to archive a WAL file."),
            NULL},
            &u_sess->attr.attr_storage.XLogArchiveCommand,
            "",
            NULL,
            NULL,
            show_archive_command},
        {{"transaction_isolation",
            PGC_USERSET,
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets the current transaction's isolation level."),
            NULL,
            GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE},
            &u_sess->attr.attr_storage.XactIsoLevel_string,
            "default",
            check_XactIsoLevel,
            assign_XactIsoLevel,
            show_XactIsoLevel},

        {{"archive_dest",
            PGC_SIGHUP,
            NODE_ALL,
            WAL_ARCHIVING,
            gettext_noop("Sets the path that will be used to archive a WAL file."),
            NULL},
            &u_sess->attr.attr_storage.XLogArchiveDest,
            "",
            NULL,
            NULL,
            NULL},

#ifndef ENABLE_MULTIPLE_NODES
        {{"dcf_config",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets config string of DCF group."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_config,
            "",
            NULL,
            NULL,
            NULL},

        {{"dcf_data_path",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets data path of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_data_path,
            "",
            NULL,
            NULL,
            NULL},

        {{"dcf_log_path",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets log path of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_log_path,
            "",
            NULL,
            NULL,
            NULL},

        {{"dcf_log_level",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the log level of local DCF node."),
            NULL},
            &u_sess->attr.attr_storage.dcf_attr.dcf_log_level,
            "RUN_ERR|RUN_WAR|DEBUG_ERR|OPER|RUN_INF|PROFILE",
            NULL,
            assign_dcf_log_level,
            NULL},
#endif

        {{"default_tablespace",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
            CLIENT_CONN_STATEMENT,
            gettext_noop("Sets the tablespace(s) to use for temporary tables and sort files."),
            NULL,
            GUC_LIST_INPUT | GUC_LIST_QUOTE},
            &u_sess->attr.attr_storage.temp_tablespaces,
            "",
            check_temp_tablespaces,
            assign_temp_tablespaces,
            NULL},
        {{"synchronous_standby_names",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_MASTER,
            gettext_noop("List of names of potential synchronous standbys."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.SyncRepStandbyNames,
            "*",
            check_synchronous_standby_names,
            assign_synchronous_standby_names,
            NULL},
#ifndef ENABLE_MULTIPLE_NODES
        /* availablezone of current instance, currently, it is only used in cascade standby */
        {{"available_zone",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("Sets the replconninfo7 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.ReplConnInfoArr[7],
            "",
            check_replconninfo,
            assign_replconninfo7,
            NULL},
        /* Get the ReplConnInfo8 from postgresql.conf and assign to ReplConnArray8. */
        {{"replconninfo8",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_SENDING,
            gettext_noop("Sets the replconninfo8 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.ReplConnInfoArr[8],
            "",
            check_replconninfo,
            assign_replconninfo8,
            NULL},
        /* Get the ReplConnInfo9 from postgresql.conf and assign to ReplConnArray9. */
        {{"replconninfo9",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("Sets the replconninfo9 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.ReplConnInfoArr[9],
            "",
            check_replconninfo,
            assign_replconninfo9,
            NULL},
        /* Get the ReplConnInfo10 from postgresql.conf and assign to ReplConnArray10. */
        {{"replconninfo10",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("Sets the replconninfo10 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.ReplConnInfoArr[10],
            "",
            check_replconninfo,
            assign_replconninfo10,
            NULL},
        /* Get the ReplConnInfo11 from postgresql.conf and assign to ReplConnArray11. */
        {{"replconninfo11",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("Sets the replconninfo11 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.ReplConnInfoArr[11],
            "",
            check_replconninfo,
            assign_replconninfo11,
            NULL},
        /* Get the ReplConnInfo12 from postgresql.conf and assign to ReplConnArray12. */
        {{"replconninfo12",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("Sets the replconninfo12 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.ReplConnInfoArr[12],
            "",
            check_replconninfo,
            assign_replconninfo12,
            NULL},
        /* Get the ReplConnInfo13 from postgresql.conf and assign to ReplConnArray13. */
        {{"replconninfo13",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("Sets the replconninfo13 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.ReplConnInfoArr[13],
            "",
            check_replconninfo,
            assign_replconninfo13,
            NULL},
        /* Get the ReplConnInfo14 from postgresql.conf and assign to ReplConnArray14. */
        {{"replconninfo14",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("Sets the replconninfo14 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.ReplConnInfoArr[14],
            "",
            check_replconninfo,
            assign_replconninfo14,
            NULL},
        /* Get the ReplConnInfo15 from postgresql.conf and assign to ReplConnArray15. */
        {{"replconninfo15",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("Sets the replconninfo15 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.ReplConnInfoArr[15],
            "",
            check_replconninfo,
            assign_replconninfo15,
            NULL},
        /* Get the ReplConnInfo16 from postgresql.conf and assign to ReplConnArray16. */
        {{"replconninfo16",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("Sets the replconninfo16 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.ReplConnInfoArr[16],
            "",
            check_replconninfo,
            assign_replconninfo16,
            NULL},
        /* Get the ReplConnInfo17 from postgresql.conf and assign to ReplConnArray17. */
        {{"replconninfo17",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("Sets the replconninfo17 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.ReplConnInfoArr[17],
            "",
            check_replconninfo,
            assign_replconninfo17,
            NULL},
        /* Get the ReplConnInfo18 from postgresql.conf and assign to ReplConnArray18. */
        {{"replconninfo18",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("Sets the replconninfo18 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.ReplConnInfoArr[18],
            "",
            check_replconninfo,
            assign_replconninfo18,
            NULL},

        {{"primary_slotname",
            PGC_SIGHUP,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
            UPGRADE_OPTIONS,
            gettext_noop("Sets oids for the system object to be added next during inplace upgrade."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_SUPERUSER_ONLY | GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.Inplace_upgrade_next_system_object_oids,
            NULL,
            check_inplace_upgrade_next_oids,
            NULL,
            NULL},
        {{"num_internal_lock_partitions",
            PGC_POSTMASTER,
            NODE_ALL,
            LOCK_MANAGEMENT,
            gettext_noop("num of internal lwlock partitions."),
            NULL,
            GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_storage.num_internal_lock_partitions_str,
            "CLOG_PART=256,CSNLOG_PART=512,LOG2_LOCKTABLE_PART=4,TWOPHASE_PART=1,FASTPATH_PART=20",
            NULL,
            NULL,
            NULL},
        /* Get the cross_cluster_ReplConnInfo1 from postgresql.conf and assign to cross_cluster_ReplConnArray1. */
        {{"cross_cluster_replconninfo1",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_SENDING,
            gettext_noop("Sets the cross_cluster_replconninfo1 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[1],
            "",
            check_replconninfo,
            assign_cross_cluster_replconninfo1,
            NULL},
        /* Get the cross_cluster_ReplConnInfo2 from postgresql.conf and assign to cross_cluster_ReplConnArray2. */
        {{"cross_cluster_replconninfo2",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_SENDING,
            gettext_noop("Sets the cross_cluster_replconninfo2 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[2],
            "",
            check_replconninfo,
            assign_cross_cluster_replconninfo2,
            NULL},
        /* Get the cross_cluster_ReplConnInfo3 from postgresql.conf and assign to cross_cluster_ReplConnArray3. */
        {{"cross_cluster_replconninfo3",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_SENDING,
            gettext_noop("Sets the cross_cluster_replconninfo3 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[3],
            "",
            check_replconninfo,
            assign_cross_cluster_replconninfo3,
            NULL},
        /* Get the cross_cluster_ReplConnInfo4 from postgresql.conf and assign to cross_cluster_ReplConnArray4. */
        {{"cross_cluster_replconninfo4",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_SENDING,
            gettext_noop("Sets the cross_cluster_replconninfo4 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[4],
            "",
            check_replconninfo,
            assign_cross_cluster_replconninfo4,
            NULL},
        /* Get the cross_cluster_ReplConnInfo5 from postgresql.conf and assign to cross_cluster_ReplConnArray5. */
        {{"cross_cluster_replconninfo5",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_SENDING,
            gettext_noop("Sets the cross_cluster_replconninfo5 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[5],
            "",
            check_replconninfo,
            assign_cross_cluster_replconninfo5,
            NULL},
        /* Get the cross_cluster_ReplConnInfo6 from postgresql.conf and assign to cross_cluster_ReplConnArray6. */
        {{"cross_cluster_replconninfo6",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_SENDING,
            gettext_noop("Sets the cross_cluster_replconninfo6 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[6],
            "",
            check_replconninfo,
            assign_cross_cluster_replconninfo6,
            NULL},
        /* Get the cross_cluster_ReplConnInfo7 from postgresql.conf and assign to cross_cluster_ReplConnArray7. */
        {{"cross_cluster_replconninfo7",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_SENDING,
            gettext_noop("Sets the cross_cluster_replconninfo7 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[7],
            "",
            check_replconninfo,
            assign_cross_cluster_replconninfo7,
            NULL},

        /* Get the cross_cluster_ReplConnInfo8 from postgresql.conf and assign to cross_cluster_ReplConnArray8. */
        {{
            "cross_cluster_replconninfo8",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_SENDING,
            gettext_noop("Sets the cross_cluster_replconninfo8 of the HA to listen and authenticate."),
            NULL,
            GUC_LIST_INPUT},
        &u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[8],
        "",
        check_replconninfo,
        assign_cross_cluster_replconninfo8,
        NULL},

        {{"xlog_file_path",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            WAL,
            gettext_noop("use only one xlog file, the path of it"),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_storage.xlog_file_path,
            NULL,
            NULL,
            NULL,
            NULL},
        {{"hadr_super_user_record_path",
            PGC_SIGHUP,
            NODE_ALL,
            REPLICATION_SENDING,
            gettext_noop("Sets the secure file path for initial user."),
            NULL},
            &u_sess->attr.attr_storage.hadr_super_user_record_path,
            "",
            NULL,
            NULL,
            NULL},
        {{"xlog_lock_file_path",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            WAL,
            gettext_noop("used to control write to xlog_file_path"),
            NULL,
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_storage.xlog_lock_file_path,
            NULL,
            NULL,
            NULL,
            NULL},
        {{"redo_bind_cpu_attr",
            PGC_POSTMASTER,
            NODE_ALL,
            WAL_SETTINGS,
            gettext_noop("bind redo worker threads to specified cpus"),
            NULL,
            GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_storage.redo_bind_cpu_attr,
            "nobind",
            NULL,
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
    u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_STORAGE] =
        (struct config_string*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_STORAGE], bytes,
        localConfigureNamesString, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitStorageConfigureNamesEnum()
{
    struct config_enum localConfigureNamesEnum[] = {
        {{"resource_track_log",
            PGC_USERSET,
            NODE_ALL,
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
        {{"synchronous_commit",
            PGC_USERSET,
            NODE_ALL,
            WAL_SETTINGS,
            gettext_noop("Sets the current transaction's synchronization level."),
            NULL},
            &u_sess->attr.attr_storage.guc_synchronous_commit,
            SYNCHRONOUS_COMMIT_ON,
            synchronous_commit_options,
            NULL,
            assign_synchronous_commit,
            NULL},
        {{"wal_level",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
            WAL_SETTINGS,
            gettext_noop("Selects the method used for forcing WAL updates to disk."),
            NULL},
            &u_sess->attr.attr_storage.sync_method,
            DEFAULT_SYNC_METHOD,
            sync_method_options,
            NULL,
            assign_xlog_sync_method,
            NULL},
        {{"autovacuum_mode",
            PGC_SIGHUP,
            NODE_ALL,
            AUTOVACUUM,
            gettext_noop("Sets the behavior of autovacuum"),
            NULL},
            &u_sess->attr.attr_storage.autovacuum_mode,
            AUTOVACUUM_DO_ANALYZE_VACUUM,
            autovacuum_mode_options,
            NULL,
            NULL,
            NULL},
        {{"cstore_insert_mode",
            PGC_USERSET,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("decide destination of data inserted"),
            NULL},
            &u_sess->attr.attr_storage.cstore_insert_mode,
            TO_AUTO,
            cstore_insert_mode_options,
            NULL,
            NULL,
            NULL},
        {{"remote_read_mode",
            PGC_POSTMASTER,
            NODE_ALL,
            UNGROUPED,
            gettext_noop("decide way of remote read"),
            NULL},
            &g_instance.attr.attr_storage.remote_read_mode,
            REMOTE_READ_AUTH,
            remote_read_options,
            NULL,
            NULL,
            NULL},
#ifndef ENABLE_MULTIPLE_NODES
        {{"dcf_log_file_permission",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the log file permission of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_log_file_permission,
            DCF_LOG_FILE_PERMISSION_600,
            dcf_log_file_permission_options,
            NULL,
            NULL,
            NULL},
        {{"dcf_log_path_permission",
            PGC_POSTMASTER,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the log path permission of local DCF node."),
            NULL},
            &g_instance.attr.attr_storage.dcf_attr.dcf_log_path_permission,
            DCF_LOG_PATH_PERMISSION_700,
            dcf_log_path_permission_options,
            NULL,
            NULL,
            NULL},
        {{"dcf_run_mode",
            PGC_SIGHUP,
            NODE_SINGLENODE,
            REPLICATION_PAXOS,
            gettext_noop("Sets the run mode of local DCF node."),
            NULL},
            &u_sess->attr.attr_storage.dcf_attr.dcf_run_mode,
            DCF_RUN_MODE_AUTO,
            dcf_run_mode_options,
            NULL,
            assign_dcf_run_mode,
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
            NULL,
            NULL,
            NULL,
            NULL}
    };

    Size bytes = sizeof(localConfigureNamesEnum);
    u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_STORAGE] =
        (struct config_enum*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_STORAGE], bytes,
        localConfigureNamesEnum, bytes);
    securec_check_ss(rc, "\0", "\0");
}

/* ******* end of options list ******* */

static bool check_and_assign_catalog_oids(List* elemlist)
{
    if (list_length(elemlist) != CATALOG_OID_LIST_LENGTH)
        return false;

    bool isshared = false;
    bool needstorage = false;
    Oid relid = InvalidOid;
    Oid comtypeid = InvalidOid;
    Oid toastrelid = InvalidOid;
    Oid indexrelid = InvalidOid;

    if (!parse_bool(static_cast<char*>(list_nth(elemlist, 1)), &isshared) ||
        !parse_bool(static_cast<char*>(list_nth(elemlist, 2)), &needstorage) ||
        (relid = atooid(static_cast<char*>(list_nth(elemlist, 3)))) >= FirstBootstrapObjectId ||
        (comtypeid = atooid(static_cast<char*>(list_nth(elemlist, 4)))) >= FirstBootstrapObjectId ||
        (toastrelid = atooid(static_cast<char*>(list_nth(elemlist, 5)))) >= FirstBootstrapObjectId ||
        (indexrelid = atooid(static_cast<char*>(list_nth(elemlist, 6)))) >= FirstBootstrapObjectId)
        return false;

    u_sess->upg_cxt.new_catalog_isshared = isshared;
    u_sess->upg_cxt.new_catalog_need_storage = needstorage;
    u_sess->upg_cxt.Inplace_upgrade_next_heap_pg_class_oid = relid;
    u_sess->upg_cxt.Inplace_upgrade_next_pg_type_oid = comtypeid;
    u_sess->upg_cxt.Inplace_upgrade_next_toast_pg_class_oid = toastrelid;
    u_sess->upg_cxt.Inplace_upgrade_next_index_pg_class_oid = indexrelid;
    if (isshared) {
        MemoryContext oldcxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));

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

static const char* show_archive_command(void)
{
    if (XLogArchivingActive())
        return u_sess->attr.attr_storage.XLogArchiveCommand;
    else
        return "(disabled)";
}

bool check_enable_gtm_free(bool* newval, void** extra, GucSource source)
{
    /* Always gtm free for any version when it is single database */
    if (IS_SINGLE_NODE) {
        *newval = true;
    } else {
        /* check our gaussdb version num */
        if (is_feature_disabled(GTM_FREE)) {
            *newval = false;
        }
    }

    return true;
}

/* Initialize storage critical lwlock partition num */
void InitializeNumLwLockPartitions(void)
{
    Assert(lengthof(LWLockPartInfo) == LWLOCK_PART_KIND);
    /* set default values */
    SetLWLockPartDefaultNum();
    /* Do str copy and remove space. */
    char* attr = TrimStr(g_instance.attr.attr_storage.num_internal_lock_partitions_str);
    if (attr == NULL || attr[0] == '\0') { /* use default values */
        return;
    }
    const char* pdelimiter = ",";
    List *res = NULL;
    char* nextToken = NULL;
    char* token = strtok_s(attr, pdelimiter, &nextToken);
    while (token != NULL) {
        res = lappend(res, TrimStr(token));
        token = strtok_s(NULL, pdelimiter, &nextToken);
    }
    pfree(attr);
    /* check input string and set lwlock num */
    CheckAndSetLWLockPartInfo(res);
    /* check range */
    CheckLWLockPartNumRange();

    list_free_deep(res);
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

static bool check_enable_data_replicate(bool* newval, void** extra, GucSource source)
{
    /* Always disable data replication in multi standbys mode */
    if (IS_DN_MULTI_STANDYS_MODE()) {
        *newval = false;
    }
    return true;
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

static bool check_replication_type(int* newval, void** extra, GucSource source)
{
    if (*newval == RT_WITH_MULTI_STANDBY) {
        if (!IsInitdb && (is_feature_disabled(DOUBLE_LIVE_DISASTER_RECOVERY_IN_THE_SAME_CITY) ||
            is_feature_disabled(DISASTER_RECOVERY_IN_TWO_PLACES_AND_THREE_CENTRES) ||
            is_feature_disabled(HA_SINGLE_PRIMARY_MULTI_STANDBY))) {
            ereport(FATAL,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("replication_type is not allowed set 1 "
                           "in Current Version. Set to default (0).")));
        }
#ifndef ENABLE_MULTIPLE_NODES
    } else {
        ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("replication_type is only allowed set to 1, newval=%d.", *newval)));
#endif
    }
    return true;
}

/*
 * @Description: update plog_merge_age GUC argument according new value.
 * @IN extra: unused
 * @IN newval: new value about merging plog age, whose unit is MS.
 */
static void plog_merge_age_assign(int newval, void* extra)
{
    ((void)extra); /* not used argument */
    t_thrd.log_cxt.plog_msg_switch_tm.tv_sec = newval / MS_PER_S;
    t_thrd.log_cxt.plog_msg_switch_tm.tv_usec = (newval - MS_PER_S * t_thrd.log_cxt.plog_msg_switch_tm.tv_sec) * 1000;
}

/*
 * @@GaussDB@@
 * Brief			: Check all the channel of one replconninfo.
 * Description		: Check each channel of one replconninfo. If one channel'format is not right, then return false.
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
            if (*ptr != ' ') {
                break;
            }
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

static int IsReplConnInfoChanged(const char* replConnInfo, const char* newval)
{
    char* temptok = NULL;
    char* toker = NULL;
    char* temp = NULL;
    char* token = NULL;
    char* tmpToken = NULL;
    char* oldReplStr = NULL;
    char* newReplStr = NULL;
    int repl_length = 0;
    replconninfo* newReplInfo = NULL;
    replconninfo* ReplInfo_1 = t_thrd.postmaster_cxt.ReplConnArray[1];
    if (replConnInfo == NULL || newval == NULL) {
        return NO_CHANGE;
    }

    newReplInfo = ParseReplConnInfo(newval, &repl_length);
    oldReplStr = pstrdup(replConnInfo);
    newReplStr = pstrdup(newval);

    /* Added replication info and enabled new ip or port */
    if (strcmp(oldReplStr, "") == 0) {
        if (strcmp(newReplStr, "") != 0) {
            /* ReplConnInfo_1 is not configured, it is considered to be new repconninfo */
            if (ReplInfo_1 == NULL) {
                pfree_ext(newReplInfo);
                pfree_ext(oldReplStr);
                pfree_ext(newReplStr);
                return ADD_REPL_CONN_INFO_WITH_NEW_LOCAL_IP_PORT;
            }
        
            if (strcmp(ReplInfo_1->localhost, newReplInfo->localhost) != 0 || 
                ReplInfo_1->localport != newReplInfo->localport ||
                ReplInfo_1->localheartbeatport != newReplInfo->localheartbeatport) {
                pfree_ext(newReplInfo);
                pfree_ext(oldReplStr);
                pfree_ext(newReplStr);
                return ADD_REPL_CONN_INFO_WITH_NEW_LOCAL_IP_PORT;
            } else {
                pfree_ext(newReplInfo);
                pfree_ext(oldReplStr);
                pfree_ext(newReplStr);
                return ADD_REPL_CONN_INFO_WITH_OLD_LOCAL_IP_PORT;
            }
        } else {
            pfree_ext(newReplInfo);
            pfree_ext(oldReplStr);
            pfree_ext(newReplStr);
            return NO_CHANGE;
        }
    }

    pfree_ext(newReplInfo);
    temp = strstr(oldReplStr, "iscascade");
    if (temp == NULL) {
        temptok = strstr(newReplStr, "iscascade");
        if (temptok == NULL) {
            /* Modify the old replication info,
            excluding disaster recovery configuration information */
            if (strcmp(oldReplStr, newReplStr) == 0) {
                pfree_ext(oldReplStr);
                pfree_ext(newReplStr);
                return NO_CHANGE;
            } else {
                pfree_ext(oldReplStr);
                pfree_ext(newReplStr);
                return OLD_REPL_CHANGE_IP_OR_PORT;
            }
        } else {
            toker = strstr(newReplStr, "iscrossregion");
            if (toker == NULL) {
                pfree_ext(oldReplStr);
                pfree_ext(newReplStr);
                return OLD_REPL_CHANGE_IP_OR_PORT;
            } else {
                /* Modify the old replication info and
                add disaster recovery configuration information */
                pfree_ext(oldReplStr);
                pfree_ext(newReplStr);
                return ADD_DISASTER_RECOVERY_INFO;
            }
        }
    } else {
        temptok = strstr(newReplStr, "iscascade");
        if (temptok == NULL) {
            /* Modify the replication info message,
            the new message does not carry disaster recovery information */
            token = strtok_r(oldReplStr, "d", &tmpToken);
            if (strncasecmp(token, newReplStr, strlen(newReplStr)) == 0) {
                pfree_ext(oldReplStr);
                pfree_ext(newReplStr);
                return NO_CHANGE;
            } else {
                pfree_ext(oldReplStr);
                pfree_ext(newReplStr);
                return OLD_REPL_CHANGE_IP_OR_PORT;
            }
        } else {
            /* Modify the replication info carrying disaster recovery information */
            if (strcmp(oldReplStr, newReplStr) == 0) {
                pfree_ext(oldReplStr);
                pfree_ext(newReplStr);
                return NO_CHANGE;
            } else {
                pfree_ext(oldReplStr);
                pfree_ext(newReplStr);
                return OLD_REPL_CHANGE_IP_OR_PORT;
            }
        }
    }
}

/*
 * @@GaussDB@@
 * Brief			: Parse replconninfo1.
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
    if (u_sess->attr.attr_storage.ReplConnInfoArr[1] != NULL && newval != NULL) {
        t_thrd.postmaster_cxt.ReplConnChangeType[1] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[1], newval);

#ifndef ENABLE_MULTIPLE_NODES
        /* perceive single --> primary_standby */
        if (t_thrd.postmaster_cxt.HaShmData != NULL &&
            t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE &&
            !GetReplCurArrayIsNull()) {
                t_thrd.postmaster_cxt.HaShmData->current_mode = PRIMARY_MODE;
                g_instance.global_sysdbcache.RefreshHotStandby();
        }
#endif
    }
}

/*
 * @@GaussDB@@
 * Brief			: Parse replconninfo2.
 */
static void assign_replconninfo2(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[2] != NULL)
        pfree(t_thrd.postmaster_cxt.ReplConnArray[2]);

    t_thrd.postmaster_cxt.ReplConnArray[2] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[2] != NULL && newval != NULL) {
        t_thrd.postmaster_cxt.ReplConnChangeType[2] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[2], newval);
    }
}

static void assign_replconninfo3(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[3])
        pfree(t_thrd.postmaster_cxt.ReplConnArray[3]);

    t_thrd.postmaster_cxt.ReplConnArray[3] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[3] != NULL && newval != NULL) {
        t_thrd.postmaster_cxt.ReplConnChangeType[3] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[3], newval);
    }
}

static void assign_replconninfo4(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[4])
        pfree(t_thrd.postmaster_cxt.ReplConnArray[4]);

    t_thrd.postmaster_cxt.ReplConnArray[4] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[4] != NULL && newval != NULL) {
        t_thrd.postmaster_cxt.ReplConnChangeType[4] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[4], newval);
    }
}

static void assign_replconninfo5(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[5])
        pfree(t_thrd.postmaster_cxt.ReplConnArray[5]);

    t_thrd.postmaster_cxt.ReplConnArray[5] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[5] != NULL && newval != NULL) {
        t_thrd.postmaster_cxt.ReplConnChangeType[5] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[5], newval);
    }
}

static void assign_replconninfo6(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[6])
        pfree(t_thrd.postmaster_cxt.ReplConnArray[6]);

    t_thrd.postmaster_cxt.ReplConnArray[6] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[6] != NULL && newval != NULL) {
        t_thrd.postmaster_cxt.ReplConnChangeType[6] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[6], newval);
    }
}

static void assign_replconninfo7(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[7])
        pfree(t_thrd.postmaster_cxt.ReplConnArray[7]);

    t_thrd.postmaster_cxt.ReplConnArray[7] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[7] != NULL && newval != NULL) {
        t_thrd.postmaster_cxt.ReplConnChangeType[7] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[7], newval);
    }
}

static void assign_replconninfo8(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[8]) {
        pfree(t_thrd.postmaster_cxt.ReplConnArray[8]);
    }

    t_thrd.postmaster_cxt.ReplConnArray[8] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[8] != NULL && newval != NULL) {
        t_thrd.postmaster_cxt.ReplConnChangeType[8] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[8], newval);
    }
}

static void assign_replconninfo9(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[9]) {
        pfree(t_thrd.postmaster_cxt.ReplConnArray[9]);
    }

    t_thrd.postmaster_cxt.ReplConnArray[9] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[9] != NULL && newval != NULL) {
        t_thrd.postmaster_cxt.ReplConnChangeType[9] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[9], newval);
    }
}

static void assign_replconninfo10(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[10]) {
        pfree(t_thrd.postmaster_cxt.ReplConnArray[10]);
    }

    t_thrd.postmaster_cxt.ReplConnArray[10] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[10] != NULL && newval != NULL &&
        IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[10], newval)) {
        t_thrd.postmaster_cxt.ReplConnChangeType[10] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[10], newval);
    }
}

static void assign_replconninfo11(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[11]) {
        pfree(t_thrd.postmaster_cxt.ReplConnArray[11]);
    }

    t_thrd.postmaster_cxt.ReplConnArray[11] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[11] != NULL && newval != NULL &&
        IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[11], newval)) {
        t_thrd.postmaster_cxt.ReplConnChangeType[11] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[11], newval);
    }
}

static void assign_replconninfo12(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[12]) {
        pfree(t_thrd.postmaster_cxt.ReplConnArray[12]);
    }

    t_thrd.postmaster_cxt.ReplConnArray[12] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[12] != NULL && newval != NULL &&
        IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[12], newval)) {
        t_thrd.postmaster_cxt.ReplConnChangeType[12] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[12], newval);
    }
}

static void assign_replconninfo13(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[13]) {
        pfree(t_thrd.postmaster_cxt.ReplConnArray[13]);
    }

    t_thrd.postmaster_cxt.ReplConnArray[13] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[13] != NULL && newval != NULL &&
        IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[13], newval)) {
        t_thrd.postmaster_cxt.ReplConnChangeType[13] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[13], newval);
    }
}

static void assign_replconninfo14(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[14]) {
        pfree(t_thrd.postmaster_cxt.ReplConnArray[14]);
    }

    t_thrd.postmaster_cxt.ReplConnArray[14] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[14] != NULL && newval != NULL &&
        IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[14], newval)) {
        t_thrd.postmaster_cxt.ReplConnChangeType[14] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[14], newval);
    }
}

static void assign_replconninfo15(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[15]) {
        pfree(t_thrd.postmaster_cxt.ReplConnArray[15]);
    }

    t_thrd.postmaster_cxt.ReplConnArray[15] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[15] != NULL && newval != NULL &&
        IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[15], newval)) {
        t_thrd.postmaster_cxt.ReplConnChangeType[15] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[15], newval);
    }
}

static void assign_replconninfo16(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[16]) {
        pfree(t_thrd.postmaster_cxt.ReplConnArray[16]);
    }

    t_thrd.postmaster_cxt.ReplConnArray[16] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[16] != NULL && newval != NULL &&
        IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[16], newval)) {
        t_thrd.postmaster_cxt.ReplConnChangeType[16] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[16], newval);
    }
}

static void assign_replconninfo17(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[17]) {
        pfree(t_thrd.postmaster_cxt.ReplConnArray[17]);
    }

    t_thrd.postmaster_cxt.ReplConnArray[17] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[17] != NULL && newval != NULL &&
        IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[17], newval)) {
        t_thrd.postmaster_cxt.ReplConnChangeType[17] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[17], newval);
    }
}

static void assign_replconninfo18(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[18]) {
        pfree(t_thrd.postmaster_cxt.ReplConnArray[18]);
    }

    t_thrd.postmaster_cxt.ReplConnArray[18] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.ReplConnInfoArr[18] != NULL && newval != NULL &&
        IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[18], newval)) {
        t_thrd.postmaster_cxt.ReplConnChangeType[18] =
            IsReplConnInfoChanged(u_sess->attr.attr_storage.ReplConnInfoArr[18], newval);
    }
}

static void assign_cross_cluster_replconninfo1(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.CrossClusterReplConnArray[1] != NULL)
        pfree(t_thrd.postmaster_cxt.CrossClusterReplConnArray[1]);

    t_thrd.postmaster_cxt.CrossClusterReplConnArray[1] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[1] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[1], newval) != 0) {
        t_thrd.postmaster_cxt.CrossClusterReplConnChanged[1] = true;
    }
}

static void assign_cross_cluster_replconninfo2(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.CrossClusterReplConnArray[2] != NULL)
        pfree(t_thrd.postmaster_cxt.CrossClusterReplConnArray[2]);

    t_thrd.postmaster_cxt.CrossClusterReplConnArray[2] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[2] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[2], newval) != 0) {
        t_thrd.postmaster_cxt.CrossClusterReplConnChanged[2] = true;
    }
}

static void assign_cross_cluster_replconninfo3(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.CrossClusterReplConnArray[3])
        pfree(t_thrd.postmaster_cxt.CrossClusterReplConnArray[3]);

    t_thrd.postmaster_cxt.CrossClusterReplConnArray[3] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[3] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[3], newval) != 0) {
        t_thrd.postmaster_cxt.CrossClusterReplConnChanged[3] = true;
    }
}

static void assign_cross_cluster_replconninfo4(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.CrossClusterReplConnArray[4])
        pfree(t_thrd.postmaster_cxt.CrossClusterReplConnArray[4]);

    t_thrd.postmaster_cxt.CrossClusterReplConnArray[4] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[4] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[4], newval) != 0) {
        t_thrd.postmaster_cxt.CrossClusterReplConnChanged[4] = true;
    }
}

static void assign_cross_cluster_replconninfo5(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.CrossClusterReplConnArray[5])
        pfree(t_thrd.postmaster_cxt.CrossClusterReplConnArray[5]);

    t_thrd.postmaster_cxt.CrossClusterReplConnArray[5] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[5] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[5], newval) != 0) {
        t_thrd.postmaster_cxt.CrossClusterReplConnChanged[5] = true;
    }
}

static void assign_cross_cluster_replconninfo6(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.CrossClusterReplConnArray[6])
        pfree(t_thrd.postmaster_cxt.CrossClusterReplConnArray[6]);

    t_thrd.postmaster_cxt.CrossClusterReplConnArray[6] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[6] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[6], newval) != 0) {
        t_thrd.postmaster_cxt.CrossClusterReplConnChanged[6] = true;
    }
}

static void assign_cross_cluster_replconninfo7(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.CrossClusterReplConnArray[7])
        pfree(t_thrd.postmaster_cxt.CrossClusterReplConnArray[7]);

    t_thrd.postmaster_cxt.CrossClusterReplConnArray[7] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[7] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[7], newval) != 0) {
        t_thrd.postmaster_cxt.CrossClusterReplConnChanged[7] = true;
    }
}

static void assign_cross_cluster_replconninfo8(const char* newval, void* extra)
{
    int repl_length = 0;

    if (t_thrd.postmaster_cxt.CrossClusterReplConnArray[8]) {
        pfree(t_thrd.postmaster_cxt.CrossClusterReplConnArray[8]);
    }

    t_thrd.postmaster_cxt.CrossClusterReplConnArray[8] = ParseReplConnInfo(newval, &repl_length);
    if (u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[8] != NULL && newval != NULL &&
        strcmp(u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[8], newval) != 0) {
        t_thrd.postmaster_cxt.CrossClusterReplConnChanged[8] = true;
    }
}

/*
 * @Description: show the content of GUC "logging_module"
 * @Return: logging_module descripting text
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

    char* objectkind = static_cast<char*>(list_nth(elemlist, 0));
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
    } else {
        return true;
    }
}

static bool check_autovacuum_max_workers(int* newval, void** extra, GucSource source)
{
    if (g_instance.attr.attr_network.MaxConnections + *newval + g_instance.attr.attr_sql.job_queue_processes +
        AUXILIARY_BACKENDS + AV_LAUNCHER_PROCS +
        g_instance.attr.attr_network.maxInnerToolConnections + g_max_worker_processes > MAX_BACKENDS) {
        return false;
    }
    return true;
}

/*
 * @@GaussDB@@
 * Brief			: Parse one replconninfo, and get the connect info of each channel.
 * Description		: Parse one replconninfo, get the connect info of each channel.
 *					  Then fill the ReplConnInfo array, and return it.
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
    char cascadeToken[MAXLENS] = {0};
    char crossRegionToken[MAXLENS] = {0};
    char* ptr = NULL;
    int parsed = 0;
    int iplen = 0;
    int portlen = strlen("localport");
    int rportlen = strlen("remoteport");
    int local_heartbeat_len = strlen("localheartbeatport");
    int remote_heartbeat_len = strlen("remoteheartbeatport");
    int cascadeLen = strlen("iscascade");
    int corssRegionLen = strlen("isCrossRegion");

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
            if (*ptr != ' ') {
                break;
            }
            ptr++;
        }
        if (*ptr == '\0') {
            pfree(ReplStr);
            return NULL;
        }

        repl_length = GetLengthAndCheckReplConn(ReplStr);
        if (repl_length == 0) {
            pfree(ReplStr);
            return NULL;
        }
        repl = static_cast<ReplConnInfo*>(MemoryContextAlloc(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB),
            sizeof(ReplConnInfo) * repl_length));
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
            while (*iter == ' ' || *iter == '=') {
                iter++;
            }
            if (!isdigit(*iter) && *iter != ':' && !isalpha(*iter)) {
                token = strtok_r(NULL, ",", &tmp_token);
                continue;
            }
            pNext = iter;
            iplen = 0;
            while (*pNext != ' ' && strncmp(pNext, "localport", portlen) != 0) {
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
            while (*iter == ' ' || *iter == '=') {
                iter++;
            }
            if (!isdigit(*iter)) {
                token = strtok_r(NULL, ",", &tmp_token);
                continue;
            }
            repl[parsed].localport = atoi(iter);

            /* localheartbeatport */
            iter = strstr(token, "localheartbeatport");
            if (NULL != iter) {
                iter += local_heartbeat_len;
                while (*iter == ' ' || *iter == '=') {
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
            while (*iter == ' ' || *iter == '=') {
                iter++;
            }
            if (!isdigit(*iter) && *iter != ':' && !isalpha(*iter)) {
                token = strtok_r(NULL, ",", &tmp_token);
                continue;
            }
            pNext = iter;
            iplen = 0;
            while (*pNext != ' ' && strncmp(pNext, "remoteport", rportlen) != 0) {
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
            while (*iter == ' ' || *iter == '=') {
                iter++;
            }
            if (!isdigit(*iter)) {
                token = strtok_r(NULL, ",", &tmp_token);
                continue;
            }
            repl[parsed].remoteport = atoi(iter);

            /* remote heartbeat port */
            iter = strstr(token, "remoteheartbeatport");
            if (NULL != iter) {
                iter += remote_heartbeat_len;
                while (*iter == ' ' || *iter == '=') {
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
                while (*iter == ' ' || *iter == '=') {
                    iter++;
                }
                errorno = strncpy_s(cascadeToken, MAXLENS, iter, strlen("true"));
                securec_check(errorno, "\0", "\0");
                if (strcmp(cascadeToken, "true") == 0) {
                    repl[parsed].isCascade = true;
                }
            }

            /* is cross region */
            iter = strstr(token, "iscrossregion");
            if (iter != NULL) {
                iter += corssRegionLen;
                while (*iter == ' ' || *iter == '=') {
                    iter++;
                }
                strncpy_s(crossRegionToken, MAXLENS, iter, strlen("true"));
                securec_check(errorno, "\0", "\0");
                if (strcmp(crossRegionToken, "true") == 0) {
                    repl[parsed].isCrossRegion = true;
                }
            }

#ifdef ENABLE_LITE_MODE
            iter = strstr(token, "sslmode");
            if (iter != NULL) {
                iter += sizeof("sslmode");
                while (*iter == ' ' || *iter == '=') {
                    iter++;
                }

                pNext = iter;
                iplen = 0;
                while (*pNext != ' ' && *pNext != '\0') {
                    iplen++;
                    pNext++;
                }
                errorno = strncpy_s(repl[parsed].sslmode, SSL_MODE_LEN - 1, iter, iplen);
                securec_check(errorno, "\0", "\0");
                repl[parsed].sslmode[iplen] = '\0';

                if (strcmp(repl[parsed].sslmode, "disable") != 0 && strcmp(repl[parsed].sslmode, "allow") != 0 &&
                    strcmp(repl[parsed].sslmode, "prefer") != 0 && strcmp(repl[parsed].sslmode, "require") != 0 &&
                    strcmp(repl[parsed].sslmode, "verify-ca") != 0 && strcmp(repl[parsed].sslmode, "verify-full") != 0) {
                    errorno = strcpy_s(repl[parsed].sslmode, SSL_MODE_LEN, "prefer");
                    securec_check(errorno, "\0", "\0");
                }
            }
#endif

            token = strtok_r(NULL, ",", &tmp_token);
            parsed++;
        }
    }

    pfree(ReplStr);

    *InfoLength = repl_length;

    return repl;
}

static bool check_and_assign_proc_oids(List* elemlist)
{
    if (list_length(elemlist) != PROC_OID_LIST_LENGTH)
        return false;

    Oid procid = InvalidOid;

    if ((procid = atooid(static_cast<char*>(list_nth(elemlist, 1)))) >= FirstBootstrapObjectId)
        return false;

    u_sess->upg_cxt.Inplace_upgrade_next_pg_proc_oid = procid;

    return true;
}

static bool check_and_assign_type_oids(List* elemlist)
{
    if (list_length(elemlist) != TYPE_OID_LIST_LENGTH)
        return false;

    Oid typeoid = InvalidOid;
    Oid arraytypeid = InvalidOid;
    char typtype = TYPTYPE_BASE;

    if ((typeoid = atooid(static_cast<char*>(list_nth(elemlist, 1)))) >= FirstBootstrapObjectId ||
        (arraytypeid = atooid(static_cast<char*>(list_nth(elemlist, 2)))) >= FirstBootstrapObjectId ||
        ((typtype = *static_cast<char*>(list_nth(elemlist, 3))) != TYPTYPE_BASE && typtype != TYPTYPE_PSEUDO))
        return false;

    u_sess->upg_cxt.Inplace_upgrade_next_pg_type_oid = typeoid;
    u_sess->upg_cxt.Inplace_upgrade_next_array_pg_type_oid = arraytypeid;
    u_sess->cmd_cxt.TypeCreateType = typtype;

    return true;
}

static bool check_and_assign_namespace_oids(List* elemlist)
{
    if (list_length(elemlist) != NAMESPACE_OID_LIST_LENGTH)
        return false;

    Oid namespaceid = InvalidOid;

    if ((namespaceid = atooid(static_cast<char*>(list_nth(elemlist, 1)))) >= FirstBootstrapObjectId)
        return false;

    u_sess->upg_cxt.Inplace_upgrade_next_pg_namespace_oid = namespaceid;

    return true;
}

static bool check_and_assign_general_oids(List* elemlist)
{
    if (list_length(elemlist) != GENERAL_OID_LIST_LENGTH)
        return false;

    Oid generalid = InvalidOid;

    if ((generalid = atooid(static_cast<char*>(list_nth(elemlist, 1)))) >= FirstBootstrapObjectId)
        return false;

    u_sess->upg_cxt.Inplace_upgrade_next_general_oid = generalid;

    return true;
}

/*
 * @@GaussDB@@
 * Brief			: Get the number of right-format-channel in a replconninfo.
 * Description		: Check each channel of a replconninfo, and return the number of the right-format ones.
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

#ifndef ENABLE_MULTIPLE_NODES

static void assign_dcf_election_timeout(int newval, void* extra)
{
    u_sess->attr.attr_storage.dcf_attr.dcf_election_timeout = newval;
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid)
        dcf_set_param("ELECTION_TIMEOUT", std::to_string(newval).c_str());
}

static void assign_dcf_auto_elc_priority_en(int newval, void* extra)
{
    u_sess->attr.attr_storage.dcf_attr.dcf_auto_elc_priority_en = newval;
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid)
        dcf_set_param("AUTO_ELC_PRIORITY_EN", std::to_string(newval).c_str());
}

static void assign_dcf_election_switch_threshold(int newval, void* extra)
{
    u_sess->attr.attr_storage.dcf_attr.dcf_election_switch_threshold = newval;
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid)
        dcf_set_param("ELECTION_SWITCH_THRESHOLD", std::to_string(newval).c_str());
}

static void assign_dcf_run_mode(int newval, void* extra)
{
    u_sess->attr.attr_storage.dcf_attr.dcf_run_mode = newval;
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid)
        dcf_set_param("RUN_MODE", std::to_string(newval).c_str());
}

static void assign_dcf_log_level(const char* newval, void* extra)
{
    if (newval == nullptr)
        return;
    u_sess->attr.attr_storage.dcf_attr.dcf_log_level = const_cast<char*>(newval);
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid)
        dcf_set_param("LOG_LEVEL", const_cast<char*>(newval));
}

static void assign_dcf_max_log_file_size(int newval, void* extra)
{
    u_sess->attr.attr_storage.dcf_attr.dcf_max_log_file_size = newval;
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid) {
        uint64_t dcf_guc_param = newval;
        dcf_set_param("MAX_LOG_FILE_SIZE", std::to_string(dcf_guc_param).c_str());
    }
}

static void assign_dcf_flow_control_cpu_threshold(int newval, void* extra)
{
    u_sess->attr.attr_storage.dcf_attr.dcf_flow_control_cpu_threshold = newval;
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid)
        dcf_set_param("FLOW_CONTROL_CPU_THRESHOLD", std::to_string(newval).c_str());
}

static void assign_dcf_flow_control_net_queue_message_num_threshold(int newval, void* extra)
{
    u_sess->attr.attr_storage.dcf_attr.dcf_flow_control_net_queue_message_num_threshold = newval;
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid)
        dcf_set_param("FLOW_CONTROL_NET_QUEUE_MESSAGE_NUM_THRESHOLD", std::to_string(newval).c_str());
}

static void assign_dcf_flow_control_disk_rawait_threshold(int newval, void* extra)
{
    u_sess->attr.attr_storage.dcf_attr.dcf_flow_control_disk_rawait_threshold = newval;
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid)
        dcf_set_param("FLOW_CONTROL_DISK_RAWAIT_THRESHOLD", std::to_string(newval).c_str());
}

static void assign_dcf_log_backup_file_count(int newval, void* extra)
{
    u_sess->attr.attr_storage.dcf_attr.dcf_log_backup_file_count = newval;
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid)
        dcf_set_param("LOG_BACKUP_FILE_COUNT", std::to_string(newval).c_str());
}

static void assign_dcf_flow_control_rto(int newval, void *extra)
{
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid) {
        dcf_set_param("DN_FLOW_CONTROL_RTO", std::to_string(newval).c_str());
    }
}

static void assign_dcf_flow_control_rpo(int newval, void *extra)
{
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid) {
        dcf_set_param("DN_FLOW_CONTROL_RPO", std::to_string(newval).c_str());
    }
}

#endif
