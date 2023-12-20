/* --------------------------------------------------------------------
 * guc_security.cpp
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
 * src/backend/utils/misc/guc/guc_security.cpp
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
#include "utils/guc_security.h"

/* max reuse time interval and times */
const int MAX_PASSWORD_REUSE_TIME = 3650;
const int MAX_PASSWORD_REUSE_MAX = 1000;
const int MAX_ACCOUNT_LOCK_TIME = 365;
const int MAX_FAILED_LOGIN_ATTAMPTS = 1000;
const int MAX_PASSWORD_EFFECTIVE_TIME = 999;
const int MAX_PASSWORD_NOTICE_TIME = 999;
/* max num of assign character in password */
const int MAX_PASSWORD_ASSIGNED_CHARACTER = 999;
/* max length of password */
const int MAX_PASSWORD_LENGTH = 999;

static bool check_ssl(bool* newval, void** extra, GucSource source);
/* Database Security: Support password complexity */
static bool check_int_parameter(int* newval, void** extra, GucSource source);
static bool check_ssl_ciphers(char** newval, void** extra, GucSource source);

static void InitSecurityConfigureNamesBool();
static void InitSecurityConfigureNamesInt();
static void InitSecurityConfigureNamesInt64();
static void InitSecurityConfigureNamesReal();
static void InitSecurityConfigureNamesString();
static void InitSecurityConfigureNamesEnum();

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
void InitSecurityConfigureNames()
{
    InitSecurityConfigureNamesBool();
    InitSecurityConfigureNamesInt();
    InitSecurityConfigureNamesInt64();
    InitSecurityConfigureNamesReal();
    InitSecurityConfigureNamesString();
    InitSecurityConfigureNamesEnum();

    return;
}

static void InitSecurityConfigureNamesBool()
{
    struct config_bool localConfigureNamesBool[] = {
        {{"ssl",
            PGC_POSTMASTER,
            NODE_ALL,
            CONN_AUTH_SECURITY,
            gettext_noop("Enables SSL connections."),
            NULL},
            &g_instance.attr.attr_security.EnableSSL,
            false,
            check_ssl,
            NULL,
            NULL},
#ifdef USE_TASSL
        {{"ssl_use_tlcp",
            PGC_POSTMASTER,
            NODE_ALL,
            CONN_AUTH_SECURITY,
            gettext_noop("Enables tlcp in ssl connection. "),
            NULL},
            &g_instance.attr.attr_security.ssl_use_tlcp,
            false,
            NULL,
            NULL,
            NULL},
#endif
        {{"require_ssl",
            PGC_SIGHUP,
            NODE_ALL,
            CONN_AUTH_SECURITY,
            gettext_noop("Requires SSL connections."),
            NULL},
            &u_sess->attr.attr_security.RequireSSL,
            false,
            check_ssl,
            NULL,
            NULL},
        {{"zero_damaged_pages",
            PGC_SUSET,
            NODE_ALL,
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
        {{"krb_caseins_users",
            PGC_SIGHUP,
            NODE_ALL,
            CONN_AUTH_SECURITY,
            gettext_noop("Sets whether Kerberos and GSSAPI user names should be treated as case-insensitive."),
            NULL},
            &u_sess->attr.attr_security.pg_krb_caseins_users,
            false,
            NULL,
            NULL,
            NULL},
        {{"enableSeparationOfDuty",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
            UNGROUPED,
            gettext_noop("Sets the operation mode."),
            NULL},
            &u_sess->attr.attr_security.operation_mode,
            false,
            NULL,
            NULL,
            NULL},
        {{"use_elastic_search",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("enable security policy features."),
            NULL},
            &u_sess->attr.attr_security.Enable_Security_Policy,
            false,
            NULL,
            NULL,
            NULL},
        /* Database Security: Support database audit */
        /* add guc option about audit */
        {{"audit_enabled",
            PGC_SIGHUP,
            NODE_ALL,
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
            NODE_ALL,
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
        /* Database Security: Support Transparent Data Encryption */
        /* add guc option about TDE */
#ifndef ENABLE_FINANCE_MODE
        {{"enable_tde",
            PGC_POSTMASTER,
            NODE_ALL,
            TRANSPARENT_DATA_ENCRYPTION,
            gettext_noop("Enable Transparent Data Encryption feature."),
            NULL},
            &g_instance.attr.attr_security.enable_tde,
            false,
            NULL,
            NULL,
            NULL},
#else
            {{"enable_tde",
            PGC_INTERNAL,
            NODE_ALL,
            TRANSPARENT_DATA_ENCRYPTION,
            gettext_noop("Enable Transparent Data Encryption feature."),
            NULL},
            &g_instance.attr.attr_security.enable_tde,
            false,
            NULL,
            NULL,
            NULL},
#endif
        {{"modify_initial_password",
            PGC_SIGHUP,
            NODE_ALL,
            CONN_AUTH_SECURITY,
            gettext_noop("modify the initial password of the initial user."),
            NULL},
            &u_sess->attr.attr_security.Modify_initial_password,
            false,
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
    u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_SECURITY] =
        (struct config_bool*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_SECURITY], bytes,
        localConfigureNamesBool, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitSecurityConfigureNamesInt()
{
    struct config_int localConfigureNamesInt[] = {
        {{"post_auth_delay",
            PGC_BACKEND,
            NODE_ALL,
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
        {{"authentication_timeout",
            PGC_SIGHUP,
            NODE_ALL,
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
        /* Not for general use */
        {{"pre_auth_delay",
            PGC_SIGHUP,
            NODE_ALL,
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
        {{"ssl_renegotiation_limit",
            PGC_USERSET,
            NODE_ALL,
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
            NODE_ALL,
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
        {{"password_policy",
            PGC_SIGHUP,
            NODE_ALL,
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

        {{"password_encryption_type",
            PGC_SIGHUP,
            NODE_ALL,
            CONN_AUTH_SECURITY,
            gettext_noop("The encryption method of password."),
            gettext_noop("This controls the encryption type of the password. "
                        "A value of 2 uses the system default."),
            },
            &u_sess->attr.attr_security.Password_encryption_type,
            2,
            0,
            3,
            check_int_parameter,
            NULL,
            NULL},
        {{"password_reuse_max",
            PGC_SIGHUP,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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

        {{"password_notify_time",
            PGC_SIGHUP,
            NODE_ALL,
            CONN_AUTH_SECURITY,
            gettext_noop("password deadline notice time."),
            NULL},
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("audit file remain threshold."),
            NULL,
            0},
            &u_sess->attr.attr_security.Audit_RemainThreshold,
            1024 * 1024,
            100,
            1024 * 1024,
            NULL,
            NULL,
            NULL},

        {{"audit_login_logout",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("audit user login logout."),
            NULL,
            0},
            &u_sess->attr.attr_security.Audit_Session,
            7,
            0,
            7,
            NULL,
            NULL,
            NULL},

        {{"audit_database_process",
            PGC_SIGHUP,
            NODE_ALL,
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

        {{"audit_user_locked",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("audit lock and unlock user."),
            NULL,
            0},
            &u_sess->attr.attr_security.Audit_LockUser,
            1,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_user_violation",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("audit user violation."),
            NULL,
            0},
            &u_sess->attr.attr_security.Audit_UserViolation,
            0,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_grant_revoke",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("audit grant and revoke privilege."),
            NULL,
            0},
            &u_sess->attr.attr_security.Audit_PrivilegeAdmin,
            1,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_system_object",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("audit DDL operation on system object."),
            NULL,
            0},
            &u_sess->attr.attr_security.Audit_DDL,
            67121159,
            0,
            268435455,
            NULL,
            NULL,
            NULL},

        {{"audit_dml_state",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("audit DML operation."),
            NULL,
            0},
            &u_sess->attr.attr_security.Audit_DML,
            0,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_dml_state_select",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("audit DML select operation."),
            NULL,
            0},
            &u_sess->attr.attr_security.Audit_DML_SELECT,
            0,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_function_exec",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("audit function execution."),
            NULL,
            0},
            &u_sess->attr.attr_security.Audit_Exec,
            0,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_copy_exec",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("audit copy execution."),
            NULL,
            0},
            &u_sess->attr.attr_security.Audit_Copy,
            1,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_set_parameter",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("audit set operation."),
            NULL},
            &u_sess->attr.attr_security.Audit_Set,
            0,
            0,
            1,
            NULL,
            NULL,
            NULL},
        {{"auth_iteration_count",
            PGC_SIGHUP,
            NODE_ALL,
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

        {{"audit_xid_info",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("whether record xid info in audit log."),
            NULL,
            0},
            &u_sess->attr.attr_security.audit_xid_info,
            0,
            0,
            1,
            NULL,
            NULL,
            NULL},

        {{"audit_thread_num",
            PGC_POSTMASTER,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("Sets the number of audit threads."),
            NULL,
            0},
            &g_instance.attr.attr_security.audit_thread_num,
            1,
            1,
            48,
            NULL,
            NULL,
            NULL},
        {{"audit_system_function_exec",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("Sets whether audit system function execution or not."),
            NULL,
            0},
            &u_sess->attr.attr_security.audit_system_function_exec,
            0,
            0,
            1,
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
    u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_SECURITY] =
        (struct config_int*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_SECURITY], bytes, localConfigureNamesInt, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitSecurityConfigureNamesReal()
{
    struct config_real localConfigureNamesReal[] = {
        /* defalut values is 60 days */
        {{"password_reuse_time",
            PGC_SIGHUP,
            NODE_ALL,
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
        {{"password_effect_time",
            PGC_SIGHUP,
            NODE_ALL,
            CONN_AUTH_SECURITY,
            gettext_noop("password effective time."),
            NULL},
            &u_sess->attr.attr_security.Password_effect_time,
            90.0,
            0.0,
            MAX_PASSWORD_EFFECTIVE_TIME,
            check_double_parameter,
            NULL,
            NULL},

        {{"password_lock_time",
            PGC_SIGHUP,
            NODE_ALL,
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
    u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_SECURITY] =
        (struct config_real*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_SECURITY], bytes,
        localConfigureNamesReal, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitSecurityConfigureNamesInt64()
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
    u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_SECURITY] =
        (struct config_int64*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_SECURITY], bytes,
        localConfigureNamesInt64, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitSecurityConfigureNamesString()
{
    struct config_string localConfigureNamesString[] = {
        {{"elastic_search_ip_addr",
            PGC_POSTMASTER,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("Controls elastic search IP address in the system. "),
            NULL},
            &g_instance.attr.attr_security.elastic_search_ip_addr,
            "https://127.0.0.1",
            NULL,
            NULL,
            NULL},
        {{"krb_server_keyfile",
            PGC_SIGHUP,
            NODE_ALL,
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
            NODE_ALL,
            CONN_AUTH_SECURITY,
            gettext_noop("Sets the name of the Kerberos service."),
            NULL},
            &u_sess->attr.attr_security.pg_krb_srvnam,
            PG_KRB_SRVNAM,
            NULL,
            NULL,
            NULL},
        {{"ssl_cert_file",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
            CONN_AUTH_SECURITY,
            gettext_noop("Location of the SSL server private key file."),
            NULL},
            &g_instance.attr.attr_security.ssl_key_file,
            "server.key",
            NULL,
            NULL,
            NULL},
#ifdef USE_TASSL
        {{"ssl_enc_cert_file",
            PGC_POSTMASTER,
            NODE_ALL,
            CONN_AUTH_SECURITY,
            gettext_noop("Location of the SSL server encryption certificate file."),
            NULL},
            &g_instance.attr.attr_security.ssl_enc_cert_file,
            "server_enc.crt",
            NULL,
            NULL,
            NULL},

        {{"ssl_enc_key_file",
            PGC_POSTMASTER,
            NODE_ALL,
            CONN_AUTH_SECURITY,
            gettext_noop("Location of the SSL server encryption private key file."),
            NULL},
            &g_instance.attr.attr_security.ssl_enc_key_file,
            "server_enc.key",
            NULL,
            NULL,
            NULL},
#endif
        {{"ssl_ca_file",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
            CONN_AUTH_SECURITY,
            gettext_noop("Location of the SSL certificate revocation list file."),
            NULL},
            &g_instance.attr.attr_security.ssl_crl_file,
            "",
            NULL,
            NULL,
            NULL},
        {{"ssl_ciphers",
            PGC_POSTMASTER,
            NODE_ALL,
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
        /* Database Security: Support database audit */
        {{"audit_directory",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("Sets the data format for audit files."),
            gettext_noop("Currently can be specified as binary only."),
            GUC_SUPERUSER_ONLY},
            &g_instance.attr.attr_security.Audit_data_format,
            "binary",
            NULL,
            NULL,
            NULL},
        /* add guc for TDE cmk id */
        {{"tde_cmk_id",
            PGC_SIGHUP,
            NODE_ALL,
            TRANSPARENT_DATA_ENCRYPTION,
            gettext_noop("The Cloud KMS CMK ID to get DEK."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_NO_RESET_ALL},
            &u_sess->attr.attr_security.tde_cmk_id,
            "",
            NULL,
            NULL,
            NULL},
        /* End-of-list marker */
        {{"transparent_encrypted_string",
            PGC_POSTMASTER,
            NODE_ALL,
            TRANSPARENT_DATA_ENCRYPTION,
            gettext_noop("The encrypted string to test the transparent encryption key."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_NO_RESET_ALL},
            &g_instance.attr.attr_security.transparent_encrypted_string,
            NULL,
            NULL,
            NULL,
            NULL},
        {{"transparent_encrypt_kms_region",
            PGC_POSTMASTER,
            NODE_ALL,
            TRANSPARENT_DATA_ENCRYPTION,
            gettext_noop("The region to get transparent encryption key."),
            NULL,
            GUC_NOT_IN_SAMPLE | GUC_NO_RESET_ALL},
            &g_instance.attr.attr_security.transparent_encrypt_kms_region,
            "",
            transparent_encrypt_kms_url_region_check,
            NULL,
            NULL},
        {{"no_audit_client",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("Sets the appname@ip should not be audited."),
            NULL,
            GUC_SUPERUSER_ONLY},
            &u_sess->attr.attr_security.no_audit_client,
            "",
            NULL,
            NULL,
            NULL},
        {{"full_audit_users",
            PGC_SIGHUP,
            NODE_ALL,
            AUDIT_OPTIONS,
            gettext_noop("Sets the users under comprehensive audit."),
            NULL},
            &u_sess->attr.attr_security.full_audit_users,
            "",
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
    u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_SECURITY] =
        (struct config_string*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_SECURITY], bytes,
        localConfigureNamesString, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitSecurityConfigureNamesEnum()
{
    struct config_enum localConfigureNamesEnum[] = {

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
    u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_SECURITY] =
        (struct config_enum*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_SECURITY], bytes,
        localConfigureNamesEnum, bytes);
    securec_check_ss(rc, "\0", "\0");
}

/* ******* end of options list ******* */

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

/*
 * Brief			: Check the value of int-type parameter for security
 */
static bool check_int_parameter(int* newval, void** extra, GucSource source)
{
    if (*newval >= 0) {
        return true;
    } else {
        return false;
    }
}

static bool check_ssl_ciphers(char** newval, void** extra, GucSource)
{
    char* cipherStr = NULL;
    char* cipherStr_tmp = NULL;
    char* token = NULL;
    int counter = 1;
    char** ciphers_list = NULL;
    bool find_ciphers_in_list = false;
    int i = 0;
    char* ptok = NULL;
    const char* ssl_ciphers_list[] = {
        "ECDHE-RSA-AES128-GCM-SHA256",
        "ECDHE-RSA-AES256-GCM-SHA384",
        "ECDHE-ECDSA-AES128-GCM-SHA256",
        "ECDHE-ECDSA-AES256-GCM-SHA384",
        "DHE-RSA-AES128-GCM-SHA256",
       
#ifndef USE_TASSL
        "DHE-RSA-AES256-GCM-SHA384"
#else
        "DHE-RSA-AES256-GCM-SHA384",
        "ECDHE-SM4-SM3", //6
        "ECDHE-SM4-GCM-SM3", //7
        "ECC-SM4-SM3",//8
        "ECC-SM4-GCM-SM3"//9
#endif
    };
    int maxCnt = lengthof(ssl_ciphers_list);

    if (*newval == NULL || **newval == '\0' || **newval == ';') {
        return false;
    } else if (strcasecmp(*newval, "ALL") == 0) {
        return true;
    } else {
        /* if the sslciphers does not contain character ';',the count is 1 */
        cipherStr = static_cast<char*>(strchr(*newval, ';'));
        while (cipherStr != NULL) {
            counter++;
            cipherStr++;
            if (*cipherStr == '\0') {
                break;
            }
            if (cipherStr == strchr(cipherStr, ';')) {
                return false;
            }
            cipherStr = strchr(cipherStr, ';');
        }

        if (counter > maxCnt) {
            return false;
        }
        ciphers_list = static_cast<char**>(palloc(counter * sizeof(char*)));
        cipherStr_tmp = pstrdup(*newval);
        token = strtok_r(cipherStr_tmp, ";", &ptok);
        while (token != NULL) {
            for (int j = 0; j < maxCnt; j++) {
                if (strlen(ssl_ciphers_list[j]) == strlen(token) &&
                    strncmp(ssl_ciphers_list[j], token, strlen(token)) == 0) {
                    ciphers_list[i] = const_cast<char*>(ssl_ciphers_list[j]);
                    find_ciphers_in_list = true;
                    break;
                }
            }
            if (!find_ciphers_in_list) {
                pfree_ext(cipherStr_tmp);
                pfree_ext(ciphers_list);
                return false;
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

