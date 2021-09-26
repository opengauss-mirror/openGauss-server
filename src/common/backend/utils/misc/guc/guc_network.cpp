/* --------------------------------------------------------------------
 * guc_network.cpp
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
 * src/backend/utils/misc/guc/guc_network.cpp
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
#include "utils/guc_network.h"

static bool check_maxconnections(int* newval, void** extra, GucSource source);
static bool check_pooler_maximum_idle_time(int* newval, void** extra, GucSource source);
static bool check_sctp_support(bool* newval, void** extra, GucSource source);
static void assign_comm_debug_mode(bool newval, void* extra);
static void assign_comm_stat_mode(bool newval, void* extra);
static void assign_comm_timer_mode(bool newval, void* extra);
static void assign_comm_no_delay(bool newval, void* extra);
static void assign_comm_ackchk_time(int newval, void* extra);
static bool CheckMaxInnerToolConnections(int* newval, void** extra, GucSource source);
static bool check_ssl(bool* newval, void** extra, GucSource source);

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

static const char* show_unix_socket_permissions(void);
static bool check_max_datanode(int* newval, void** extra, GucSource source);
static void comm_change_datanode(int newval, void* extra);
static const char* show_max_datanode(void);
static bool check_max_coordnode(int* newval, void** extra, GucSource source);
static void comm_change_coordnode(int newval, void* extra);
static const char* show_max_coordnode(void);

static void InitNetworkConfigureNamesBool();
static void InitNetworkConfigureNamesInt();
static void InitNetworkConfigureNamesInt64();
static void InitNetworkConfigureNamesReal();
static void InitNetworkConfigureNamesString();
static void InitNetworkConfigureNamesEnum();

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
void InitNetworkConfigureNames()
{
    InitNetworkConfigureNamesBool();
    InitNetworkConfigureNamesInt();
    InitNetworkConfigureNamesInt64();
    InitNetworkConfigureNamesReal();
    InitNetworkConfigureNamesString();
    InitNetworkConfigureNamesEnum();

    return;
}

static void InitNetworkConfigureNamesBool()
{
    struct config_bool localConfigureNamesBool[] = {
        {{"enable_stateless_pooler_reuse",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            DEVELOPER_OPTIONS,
            gettext_noop("Pooler stateless reuse mode."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &g_instance.attr.attr_network.PoolerStatelessReuse,
            false,
            NULL,
            NULL,
            NULL},
        // Stream communication
        {{"comm_tcp_mode",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            CLIENT_CONN,
            gettext_noop("Whether use tcp commucation mode for stream"),
            NULL,
            },
            &g_instance.attr.attr_network.comm_tcp_mode,
            true,
            check_sctp_support,
            NULL,
            NULL},
        {{"comm_debug_mode",
            PGC_USERSET,
            NODE_DISTRIBUTE,
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
        {{"comm_stat_mode",
            PGC_USERSET,
            NODE_DISTRIBUTE,
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
        {{"comm_timer_mode",
            PGC_USERSET,
            NODE_DISTRIBUTE,
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
        {{"comm_no_delay",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            DEVELOPER_OPTIONS,
            gettext_noop("Whether set NO_DELAY option for libcomm socket"),
            NULL,
            },
            &u_sess->attr.attr_network.comm_no_delay,
            false,
            NULL,
            assign_comm_no_delay,
            NULL},
        {{"enable_force_reuse_connections",
            PGC_BACKEND,
            NODE_DISTRIBUTE,
            DEVELOPER_OPTIONS,
            gettext_noop("Session force reuse pooler connections."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_network.PoolerForceReuse,
            false,
            NULL,
            NULL,
            NULL},
        {{"comm_client_bind",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            DEVELOPER_OPTIONS,
            gettext_noop("Whether client use bind function"),
            NULL,
            },
            &u_sess->attr.attr_network.comm_client_bind,
            false,
            NULL,
            NULL,
            NULL},
        {{"comm_ssl",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            CONN_AUTH_SECURITY,
            gettext_noop("Enables libcomm SSL connections."),
            NULL},
            &g_instance.attr.attr_network.comm_enable_SSL,
            false,
            check_ssl,
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
    u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_NETWORK] =
        (struct config_bool*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_NETWORK], bytes,
        localConfigureNamesBool, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitNetworkConfigureNamesInt()
{
    struct config_int localConfigureNamesInt[] = {
        {{"max_connections",
            PGC_POSTMASTER,
            NODE_ALL,
            CONN_AUTH_SETTINGS,
            gettext_noop("Sets the maximum number of concurrent connections for clients."),
            NULL},
            &g_instance.attr.attr_network.MaxConnections,
            200,
            10,
            MAX_BACKENDS,
            check_maxconnections,
            NULL,
            NULL},
        {{"max_inner_tool_connections",
            PGC_POSTMASTER,
            NODE_ALL,
            CONN_AUTH_SETTINGS,
            gettext_noop("Sets the maximum number of concurrent connections for inner tools."),
            NULL},
            &g_instance.attr.attr_network.maxInnerToolConnections,
            50,
            1,
            MAX_BACKENDS,
            CheckMaxInnerToolConnections,
            NULL,
            NULL},
        {{"sysadmin_reserved_connections",
            PGC_POSTMASTER,
            NODE_ALL,
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
        {{"port",
            PGC_POSTMASTER,
            NODE_ALL,
            CONN_AUTH_SETTINGS,
            gettext_noop("Sets the TCP port the server listens on."),
            NULL},
            &g_instance.attr.attr_network.PostPortNumber,
            DEF_PGPORT,
            1,
            65535,
            NULL,
            NULL,
            NULL},
        {{"unix_socket_permissions",
            PGC_POSTMASTER,
            NODE_ALL,
            CONN_AUTH_SETTINGS,
            gettext_noop("Sets the access permissions of the Unix-domain socket."),
            gettext_noop("Unix-domain sockets use the usual Unix file system "
                         "permission set. The parameter value is expected "
                         "to be a numeric mode specification in the form "
                         "accepted by the chmod and umask system calls. "
                         "(To use the customary octal format the number must "
                         "start with a 0 (zero).)")},
            &g_instance.attr.attr_network.Unix_socket_permissions,
            0700,
            0000,
            0777,
            NULL,
            NULL,
            show_unix_socket_permissions},
        {{"pooler_maximum_idle_time",
            PGC_USERSET,
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
        // Stream communication
        {{"comm_sctp_port",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
            NODE_DISTRIBUTE,
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
        {{"comm_max_receiver",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
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
        {{"cn_send_buffer_size",
            PGC_POSTMASTER,
            NODE_ALL,
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
        {{"comm_sender_buffer_size",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            DEVELOPER_OPTIONS,
            gettext_noop("The libcomm sender's buffer size in every interaction between DN and CN, "
                         "or DN and DN, unit(KB)"),
            NULL},
            &g_instance.comm_cxt.commutil_cxt.g_comm_sender_buffer_size,
            8,
            1,
            1024,
            NULL,
            NULL,
            NULL},
        {{"max_pool_size",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            DATA_NODES,
            gettext_noop("Max pool size."),
            gettext_noop("If number of active connections reaches this value, "
                         "other connection requests will be refused")},
            &g_instance.attr.attr_network.MaxPoolSize,
            400,
            1,
            65535,
            NULL,
            NULL,
            NULL},
        {{"pooler_port",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            COORDINATORS,
            gettext_noop("Legacy port of the Pool Manager. Now it is used for cn HA port for build and replication "
                         "under thread pool mode."),
            NULL},
            &g_instance.attr.attr_network.PoolerPort,
            6667,
            1,
            65535,
            NULL,
            NULL,
            NULL},

        {{"pooler_timeout",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            DATA_NODES,
            gettext_noop("Timeout of the Pool Communication with Other Nodes."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_network.PoolerTimeout,
            600,
            0,
            7200,
            NULL,
            NULL,
            NULL},

        {{"pooler_connect_max_loops",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            DATA_NODES,
            gettext_noop("Max retries of the Pooler Connecting to Other Nodes."),
            NULL},
            &u_sess->attr.attr_network.PoolerConnectMaxLoops,
            1,
            0,
            20,
            NULL,
            NULL,
            NULL},

        {{"pooler_connect_interval_time",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            DATA_NODES,
            gettext_noop("Indicates the interval for each retry."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_network.PoolerConnectIntervalTime,
            15,
            0,
            7200,
            NULL,
            NULL,
            NULL},

        {{"pooler_connect_timeout",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            DATA_NODES,
            gettext_noop("Timeout of the Pooler Connecting to Other Nodes."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_network.PoolerConnectTimeout,
            60,
            0,
            7200,
            NULL,
            NULL,
            NULL},

        {{"pooler_cancel_timeout",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            DATA_NODES,
            gettext_noop("Timeout of the Pooler Cancel Connections to Other Nodes."),
            NULL,
            GUC_UNIT_S},
            &u_sess->attr.attr_network.PoolerCancelTimeout,
            15,
            0,
            7200,
            NULL,
            NULL,
            NULL},
        {{"max_coordinators",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            DATA_NODES,
            gettext_noop("Maximum number of Coordinators in the cluster."),
            gettext_noop("It is not possible to create more Coordinators in the cluster than "
                         "this maximum number.")},
            &g_instance.attr.attr_network.MaxCoords,
            1024,
            2,
            MAX_CN_NODE_NUM,
            check_max_coordnode,
            comm_change_coordnode,
            show_max_coordnode},
        {{"comm_max_datanode",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            DEVELOPER_OPTIONS,
            gettext_noop("Currently number of Datanodes."),
            NULL},
            &u_sess->attr.attr_network.comm_max_datanode,
            256,
            1,
            MAX_DN_NODE_NUM,
            check_max_datanode,
            comm_change_datanode,
            show_max_datanode},

        {{"comm_max_stream",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            CONN_AUTH_SETTINGS,
            gettext_noop("Maximum number of streams."),
            NULL},
            &g_instance.attr.attr_network.comm_max_stream,
            1024,
            1,
            60000,
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
    u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_NETWORK] =
        (struct config_int*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_NETWORK], bytes,
        localConfigureNamesInt, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitNetworkConfigureNamesReal()
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
    u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_NETWORK] =
        (struct config_real*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_NETWORK], bytes,
        localConfigureNamesReal, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitNetworkConfigureNamesInt64()
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
    u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_NETWORK] =
        (struct config_int64*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_NETWORK], bytes,
        localConfigureNamesInt64, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitNetworkConfigureNamesString()
{
    struct config_string localConfigureNamesString[] = {
        {{"unix_socket_group",
            PGC_POSTMASTER,
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
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
            NODE_ALL,
            CONN_AUTH_SETTINGS,
            gettext_noop("Sets the host name or IP address(es) to connect to for sctp."),
            NULL,
            GUC_LIST_INPUT},
            &g_instance.attr.attr_network.tcp_link_addr,
            "0.0.0.0",
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
    u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_NETWORK] =
        (struct config_string*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_NETWORK], bytes,
        localConfigureNamesString, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitNetworkConfigureNamesEnum()
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
    u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_NETWORK] =
        (struct config_enum*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_NETWORK], bytes,
        localConfigureNamesEnum, bytes);
    securec_check_ss(rc, "\0", "\0");
}

/* ******* end of options list ******* */


static bool check_maxconnections(int* newval, void** extra, GucSource source)
{
    const int factor = 4;
    const int min = 64;
    const int max = 1024;
    int bgworkers = *newval / factor;

    /* g_max_worker_processes should be a quarter of max_connections, and between 64 and 1024 */
    g_max_worker_processes = Max(bgworkers, min);
    g_max_worker_processes = Min(g_max_worker_processes, max);

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && *newval > MAX_BACKENDS) {
        ereport(LOG, (errmsg("PGXC can't support max_connections more than %d.", MAX_BACKENDS)));
        return false;
    }
#endif
    if (*newval + g_instance.attr.attr_storage.autovacuum_max_workers + g_instance.attr.attr_sql.job_queue_processes +
        AUXILIARY_BACKENDS + AV_LAUNCHER_PROCS +
        g_instance.attr.attr_network.maxInnerToolConnections + g_max_worker_processes > MAX_BACKENDS) {
        return false;
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

static bool CheckMaxInnerToolConnections(int* newval, void** extra, GucSource source)
{
    if (*newval + g_instance.attr.attr_storage.autovacuum_max_workers + g_instance.attr.attr_sql.job_queue_processes +
        g_instance.attr.attr_network.MaxConnections +
        AUXILIARY_BACKENDS + AV_LAUNCHER_PROCS + g_max_worker_processes > MAX_BACKENDS) {
        return false;
    }
    return true;
}

static bool check_ssl(bool* newval, void** extra, GucSource source)
{
#ifndef USE_SSL

    if (*newval) {
        GUC_check_errmsg("COMM SSL is not supported by this build");
        return false;
    }

#endif
    return true;
}

static bool check_pooler_maximum_idle_time(int* newval, void** extra, GucSource source)
{
    if (*newval < 0) {
        ereport(ERROR, (errmsg("GaussDB can't support idle time less than 0 or more than %d seconds.", INT_MAX)));
        return false;
    }

    return true;
}

static void assign_comm_ackchk_time(int newval, void* extra)
{
    gs_set_ackchk_time(newval);
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

static const char* show_unix_socket_permissions(void)
{
    char* buf = t_thrd.buf_cxt.show_unix_socket_permissions_buf;
    int size = sizeof(t_thrd.buf_cxt.show_unix_socket_permissions_buf);
    int rcs = 0;

    rcs = snprintf_s(buf, size, size - 1, "%04o", g_instance.attr.attr_network.Unix_socket_permissions);
    securec_check_ss(rcs, "\0", "\0");
    return buf;
}

static bool check_max_datanode(int* newval, void** extra, GucSource source)
{
    if (*newval + g_instance.attr.attr_network.MaxCoords > MAX_CN_DN_NODE_NUM) {
        ereport(LOG,
            (errmsg("PGXC can't support comm_max_datanode more than %d.",
                MAX_CN_DN_NODE_NUM - g_instance.attr.attr_network.MaxCoords)));
        return false;
    }

    return true;
}

static void comm_change_datanode(int newval, void* extra)
{
    // newval is the number of DN.
    // MaxCoords is CN=16.
    gs_change_capacity(newval + g_instance.attr.attr_network.MaxCoords);
    return;
}

static const char* show_max_datanode(void)
{
    char nbuf[16];
    int rcs = 0;

    rcs = snprintf_s(
        nbuf, sizeof(nbuf), sizeof(nbuf) - 1, "%d", gs_get_cur_node() - g_instance.attr.attr_network.MaxCoords);
    securec_check_ss(rcs, "\0", "\0");
    return pstrdup(nbuf);
}

static bool check_max_coordnode(int* newval, void** extra, GucSource source)
{
    if (*newval + u_sess->attr.attr_network.comm_max_datanode > MAX_CN_DN_NODE_NUM) {
        ereport(LOG,
            (errmsg("PGXC can't support max_coordinators more than %d.",
                MAX_CN_DN_NODE_NUM - u_sess->attr.attr_network.comm_max_datanode)));
        return false;
    }

    return true;
}

static void comm_change_coordnode(int newval, void* extra)
{
    // newval is the number of DN.
    // MaxCoords is CN=16.
    gs_change_capacity(newval + u_sess->attr.attr_network.comm_max_datanode);
    return;
}

static const char* show_max_coordnode(void)
{
    char nbuf[16];
    int rcs = 0;

    rcs = snprintf_s(nbuf, sizeof(nbuf), sizeof(nbuf) - 1, "%d", g_instance.attr.attr_network.MaxCoords);
    securec_check_ss(rcs, "\0", "\0");
    return pstrdup(nbuf);
}
