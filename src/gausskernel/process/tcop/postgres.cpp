/* -------------------------------------------------------------------------
 *
 * postgres.cpp
 *	  POSTGRES C Backend Interface
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/tcop/postgres.cpp
 *
 * NOTES
 *	  this is the "main" module of the postgres backend and
 *	  hence the main module of the "traffic cop".
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/pg_user_status.h"

#include <fcntl.h>
#include <limits.h>

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#ifndef HAVE_GETRUSAGE
#include "rusagestub.h"
#endif

#ifdef HAVE_POLL_H
#include <poll.h>
#endif

#include "access/printtup.h"
#include "access/xact.h"
#include "access/xlogdefs.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/double_write.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_extension.h"
#include "commands/async.h"
#include "commands/extension.h"
#include "commands/matview.h"
#include "commands/prepare.h"
#include "commands/user.h"
#include "commands/vacuum.h"
#ifdef PGXC
#include "commands/trigger.h"
#endif
#include "executor/spi.h"
#include "executor/node/nodeRecursiveunion.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "libpq/crypt.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "optimizer/planner.h"
#include "optimizer/bucketpruning.h"
#include "pgstat.h"
#include "pg_trace.h"
#include "parser/analyze.h"
#include "parser/parse_hint.h"
#include "parser/parser.h"
#include "parser/parse_coerce.h"
#ifdef PGXC
#include "parser/parse_type.h"
#endif /* PGXC */
#include "postmaster/autovacuum.h"
#include "postmaster/postmaster.h"
#include "postmaster/snapcapturer.h"
#include "postmaster/cfs_shrinker.h"
#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "replication/dataqueue.h"
#include "replication/datasender.h"
#include "replication/walsender.h"
#include "replication/slot.h"
#include "replication/syncrep.h"
#include "rewrite/rewriteHandler.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/xlog_share_storage/xlog_share_storage.h"
#include "storage/sinval.h"
#include "tcop/fastpath.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/be_module.h"
#include "utils/hotkey.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/plog.h"
#include "utils/selfuncs.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/fmgroids.h"
#include "mb/pg_wchar.h"
#include "pgaudit.h"
#include "auditfuncs.h"
#include "funcapi.h"
#ifdef PGXC
#include "distributelayer/streamMain.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxc.h"
#include "access/gtm.h"
/* PGXC_COORD */
#include "pgxc/execRemote.h"
#include "pgxc/barrier.h"
#include "optimizer/pgxcplan.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "pgxc/poolutils.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/locator.h"
#include "commands/copy.h"
#include "commands/matview.h"
#include "workload/cpwlm.h"
#include "workload/workload.h"
/* PGXC_DATANODE */
#include "access/transam.h"
#include "catalog/namespace.h"
#endif
#include "gssignal/gs_signal.h"
#include "optimizer/streamplan.h"
#include "optimizer/randomplan.h"
#ifdef HAVE_INT_OPTRESET
extern int optreset; /* might not be declared by system headers */
#endif
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "executor/exec/execStream.h"
#include "executor/lightProxy.h"
#include "executor/node/nodeIndexscan.h"
#include "executor/node/nodeModifyTable.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/tcop_gstrace.h"
#include "gs_policy/policy_common.h"
#include "instruments/instr_unique_sql.h"
#include "instruments/snapshot.h"
#include "instruments/instr_slow_query.h"
#include "instruments/unique_query.h"
#include "instruments/instr_handle_mgr.h"
#include "instruments/instr_statement.h"
#include "nodes/parsenodes.h"
#include "opfusion/opfusion.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "pgxc/route.h"
#include "tcop/stmt_retry.h"
#include "threadpool/threadpool.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "pgxc/pgFdwRemote.h"
#include "tsdb/utils/ts_redis.h"
#endif
#include "utils/acl.h"
#include "utils/distribute_test.h"
#include "utils/elog.h"
#include "utils/guc_storage.h"
#include "utils/guc_tables.h"
#include "utils/memprot.h"
#include "utils/memtrack.h"
#include "utils/plpgsql.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#include "utils/plancache.h"
#include "commands/tablecmds.h"
#include "catalog/gs_matview.h"
#include "streaming/dictcache.h"
#include "parser/parse_target.h"
#include "streaming/streaming_catalog.h"
#include "postmaster/bgworker.h"
#ifdef ENABLE_MOT
#include "storage/mot/jit_exec.h"
#endif
#include "commands/sqladvisor.h"
#include "storage/file/fio_device.h"
#include "storage/dss/dss_adaptor.h"
#include "storage/dss/dss_log.h"
#include "replication/libpqsw.h"
#include "replication/walreceiver.h"
#include "libpq/libpq-int.h"
#include "tcop/autonomoustransaction.h"


THR_LOCAL VerifyCopyCommandIsReparsed copy_need_to_be_reparse = NULL;

#define GSCGROUP_ATTACH_TASK()                                                                                   \
    {                                                                                                            \
        if (u_sess->attr.attr_resource.use_workload_manager && g_instance.wlm_cxt->gscgroup_init_done &&         \
            !IsAbortedTransactionBlockState() && u_sess->wlm_cxt->cgroup_state == CG_USERSET &&                  \
            CGroupIsValid(u_sess->wlm_cxt->control_group) && !CGroupIsDefault(u_sess->wlm_cxt->control_group)) { \
            gscgroup_attach_task(t_thrd.wlm_cxt.thread_node_group, u_sess->wlm_cxt->control_group);              \
        }                                                                                                        \
    }

#define IS_CLIENT_CONN_VALID(port) \
    (((port) == NULL)              \
            ? false                \
            : (((port)->is_logic_conn) ? ((port)->gs_sock.type != GSOCK_INVALID) : ((port)->sock != NO_SOCKET)))

typedef struct AttachInfoContext {
    char* info_query_string;
    Node* info_node;
    int info_index;
} AttachInfoContext;

#define PARAMS_LEN 4096
#define PRINFT_DST_MAX_DOUBLE 64
#define MEMCPY_DST_NUM 4

#define MAXSTRLEN ((1 << 11) - 1)

extern PgBackendStatus* GetMyBEEntry(void);
extern THR_LOCAL bool g_pq_interrupt_happened;

/*
 * global node definition, process-wise global variable, mainly for debug purpose
 * when using view pgxc_thread_wait_status. It will be updated only if cluster size
 * changes.
 */
GlobalNodeDefinition* global_node_definition = NULL;
pthread_mutex_t nodeDefCopyLock;

Id64Gen gt_queryId = {0, 0, false};
IdGen gt_tempId = {0, 0, false};

/*
 * On IA64 we also have to remember the register stack base.
 */
#if defined(__ia64__) || defined(__ia64)
char* register_stack_base_ptr = NULL;
#endif

extern THR_LOCAL DistInsertSelectState* distInsertSelectState;

extern void InitQueryHashTable(void);
/*
 * @hdfs
 * Define different mesage type used for exec_simple_query
 */
typedef enum { QUERY_MESSAGE = 0, HYBRID_MESSAGE } MessageType;

/* For shared storage mode */
typedef enum {
    XLOG_COPY_NOT = 0,
    XLOG_COPY_FROM_LOCAL,
    XLOG_FORCE_COPY_FROM_LOCAL,
    XLOG_COPY_FROM_SHARE
} XLogCopyMode;
static XLogCopyMode xlogCopyMode = XLOG_COPY_NOT;
static XLogRecPtr xlogCopyStart = InvalidXLogRecPtr;

/* ----------------------------------------------------------------
 *		decls for routines only used in this file
 * ----------------------------------------------------------------
 */
static int InteractiveBackend(StringInfo inBuf);
static int interactive_getc(void);
static int ReadCommand(StringInfo inBuf);
bool check_log_statement(List* stmt_list);
static int errdetail_execute(List* raw_parsetree_list);
int errdetail_params(ParamListInfo params);
static List* pg_rewrite_query(Query* query);
static int errdetail_recovery_conflict(void);
bool IsTransactionExitStmt(Node* parsetree);
static bool IsTransactionExitStmtList(List* parseTrees);
static bool IsTransactionStmtList(List* parseTrees);
#ifdef ENABLE_MOT
static bool IsTransactionPrepareStmt(const Node* parsetree);
#endif
static void drop_unnamed_stmt(void);
static void SigHupHandler(SIGNAL_ARGS);
static void ForceModifyInitialPwd(const char* query_string, List* parsetree_list);
static void ForceModifyExpiredPwd(const char* queryString, const List* parsetreeList);
#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)
static void InitGlobalNodeDefinition(PlannedStmt* planstmt);
extern void SetRemoteDestTupleReceiverParams(DestReceiver *self);
#endif
static int getSingleNodeIdx_internal(ExecNodes* exec_nodes, ParamListInfo params);
extern void CancelAutoAnalyze();

void EnableDoingCommandRead()
{
    t_thrd.postgres_cxt.DoingCommandRead = true;
}
void DisableDoingCommandRead()
{
    t_thrd.postgres_cxt.DoingCommandRead = false;
}

extern void CodeGenThreadInitialize();

extern CmdType set_cmd_type(const char* commandTag);
static void exec_batch_bind_execute(StringInfo input_message);

/*
 * MPP with recursive support
 */
void InitRecursiveCTEGlobalVariables(const PlannedStmt* planstmt)
{
    producer_top_plannode_str = NULL;
    is_syncup_producer = false;

    if (!IS_PGXC_DATANODE) {
        return;
    }

    if (StreamTopConsumerAmI() && !u_sess->attr.attr_sql.enable_stream_recursive) {
        return;
    }

    Plan* top_plan = planstmt->planTree;

    if (!EXEC_IN_RECURSIVE_MODE(top_plan)) {
        return;
    }

    /* build the top plan node string */
    /* set is_sync_producer flag */
    is_syncup_producer = IsSyncUpProducerThread();

    /* set top producer plannode string */
    StringInfoData si;
    initStringInfo(&si);

    appendStringInfo(&si, "TopPlanNode:%s[%d]", nodeTagToString(nodeTag(top_plan)), top_plan->plan_node_id);

    if (is_syncup_producer) {
        appendStringInfo(&si, " <<sync_producer>>");
    }

    producer_top_plannode_str = (char*)pstrdup(si.data);

    pfree_ext(si.data);
}

#ifdef ENABLE_MULTIPLE_NODES
/* ----------------------------------------------------------------
 *		PG-XC routines
 * ----------------------------------------------------------------
 */

/*
 * Called when the backend is ending.
 */
static void DataNodeShutdown(int code, Datum arg)
{
    /* Close connection with GTM, if active */
    CloseGTM();

    /* Free remote xact state */
    free_RemoteXactState();
    /* Free gxip */
    UnsetGlobalSnapshotData();
}

/*
 * Check if the SIGUSR2 is trying to cancel GTM connection.
 */
static bool SignalCancelGTMConnection()
{
    if (t_thrd.proc != NULL) {
        uint32 gtmhost_flag = pg_atomic_fetch_add_u32(&t_thrd.proc->signal_cancel_gtm_conn_flag, 0);
        (void)pg_atomic_exchange_u32(&t_thrd.proc->signal_cancel_gtm_conn_flag, 0);
        if (gtmhost_flag > 0) {
            SetGTMInterruptFlag();
            t_thrd.proc->suggested_gtmhost = FLAG2HOST(gtmhost_flag);
            return true;
        }
    }

    return false;
}

#endif

/* ----------------------------------------------------------------
 *		routines to obtain user input
 * ----------------------------------------------------------------
 */
/* ----------------
 *	InteractiveBackend() is called for user interactive connections
 *
 *	the string entered by the user is placed in its parameter inBuf,
 *	and we act like a Q message was received.
 *
 *	EOF is returned if end-of-file input is seen; time to shut down.
 * ----------------
 */

static int InteractiveBackend(StringInfo inBuf)
{
    int c;                      /* character read from getc() */
    bool end = false;           /* end-of-input flag */
    bool backslashSeen = false; /* have we seen a \ ? */

    /*
     * display a prompt and obtain input from the user
     */
    printf("backend> ");
    fflush(stdout);

    resetStringInfo(inBuf);

    if (t_thrd.postgres_cxt.UseNewLine) {
        /*
         * if we are using \n as a delimiter, then read characters until the
         * \n.
         */
        while ((c = interactive_getc()) != EOF) {
            if (c == '\n') {
                if (backslashSeen) {
                    /* discard backslash from inBuf */
                    if (inBuf->len > 0) {
                        inBuf->data[--inBuf->len] = '\0';
                    }
                    backslashSeen = false;
                    continue;
                } else {
                    /* keep the newline character */
                    appendStringInfoChar(inBuf, '\n');
                    break;
                }
            } else if (c == '\\')
                backslashSeen = true;
            else
                backslashSeen = false;

            appendStringInfoChar(inBuf, (char)c);
        }

        if (c == EOF)
            end = true;
    } else {
        /*
         * otherwise read characters until EOF.
         */
        while ((c = interactive_getc()) != EOF)
            appendStringInfoChar(inBuf, (char)c);

        /* No input before EOF signal means time to quit. */
        if (inBuf->len == 0)
            end = true;
    }

    if (end)
        return EOF;

    /*
     * otherwise we have a user query so process it.
     */
    /* Add '\0' to make it look the same as message case. */
    appendStringInfoChar(inBuf, (char)'\0');

    /*
     * if the query echo flag was given, print the query..
     */
    if (t_thrd.postgres_cxt.EchoQuery)
        printf("statement: %s\n", inBuf->data);
    fflush(stdout);

    return 'Q';
}

/*
 * interactive_getc -- collect one character from stdin
 *
 * Even though we are not reading from a "client" process, we still want to
 * respond to signals, particularly SIGTERM/SIGQUIT.
 */
static int interactive_getc(void)
{
    int c;
    if (g_instance.attr.attr_common.light_comm == FALSE) {
        prepare_for_client_read();
        c = getc(stdin);
        client_read_ended();
        return c;
    } else {
        /*
         * With light_comm, do not process catchup interrupts or notifications
         * while reading.
         */
        CHECK_FOR_INTERRUPTS();
        c = getc(stdin);
        process_client_read_interrupt(true);
	return c;
    }
}

/* ----------------
 *	SocketBackend()		Is called for frontend-backend connections
 *
 *	Returns the message type code, and loads message body data into inBuf.
 *
 *	EOF is returned if the connection is lost.
 * ----------------
 */
int SocketBackend(StringInfo inBuf)
{
    int qtype;
#ifdef ENABLE_MULTIPLE_NODES
    int aCount = 0;
#endif
    /*
     * Get message type code from the frontend.
     */
    qtype = pq_getbyte();
    while (1) {
        if (qtype == EOF) {
            /* frontend disconnected */
            if (IsTransactionState()) {
                ereport(COMMERROR,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                        errmsg("unexpected EOF on client connection with an open transaction")));
            } else {
                /*
                 * Can't send DEBUG log messages to client at this point. Since
                 * we're disconnecting right away, we don't need to restore
                 * whereToSendOutput.
                 */
                t_thrd.postgres_cxt.whereToSendOutput = DestNone;
                ereport(DEBUG1,
                    (errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST), errmsg("unexpected EOF on client connection")));
            }
            u_sess->tri_cxt.exec_row_trigger_on_datanode = false;
            return qtype;
        } else if (qtype == 'a') { /* on DN only */
#ifdef ENABLE_MULTIPLE_NODES      
            if (aCount > MSG_A_REPEAT_NUM_MAX) {
                ereport(DEBUG1, (errmsg("the character is repeat : %c", qtype)));
                break;
            }
            aCount++;
            u_sess->tri_cxt.exec_row_trigger_on_datanode = true;
            ereport(DEBUG1, (errmsg("received trigger shipping message : %c", qtype)));
            qtype = pq_getbyte();
#else
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("a_type message is invalid")));
#endif
        } else {
            break;
        }
    }

    /* reset doing_extended_query_message */
    u_sess->postgres_cxt.doing_extended_query_message = false;

    /*
     * Validate message type code before trying to read body; if we have lost
     * sync, better to say "command unknown" than to run out of memory because
     * we used garbage as a length word.
     *
     * This also gives us a place to set the doing_extended_query_message flag
     * as soon as possible.
     */
    switch (qtype) {
        case 'u': /* AUTONOMOUS_TRANSACTION simple query */
        case 'Q': /* simple query */
        case 'O': /* to reset openGauss thread in pooler stateless reuse mode */
        case 'Z': /* simple plan  */
        case 'h': /* hybrid message query */
        case 'Y': /* plan with params */
            if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3) {
                /* old style without length word; convert */
                if (pq_getstring(inBuf)) {
                    if (IsTransactionState())
                        ereport(COMMERROR,
                            (errcode(ERRCODE_CONNECTION_FAILURE),
                                errmsg("unexpected EOF on client connection with an open transaction")));
                    else {
                        /*
                         * Can't send DEBUG log messages to client at this
                         * point.Since we're disconnecting right away, we
                         * don't need to restore whereToSendOutput.
                         */
                        t_thrd.postgres_cxt.whereToSendOutput = DestNone;
                        ereport(DEBUG1,
                            (errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
                                errmsg("unexpected EOF on client connection")));
                    }
                    return EOF;
                }
            }
            break;

        case 'F': /* fastpath function call */
        case 'I': /* Push, Pop schema name */
        case 'L': /* Link gc_fdw */
        case 'J': /* Trace ID */
            break;

        case 'X': /* terminate */
            u_sess->postgres_cxt.doing_extended_query_message = false;
            u_sess->postgres_cxt.ignore_till_sync = false;
            break;

        case 'U': /* batch bind-execute */
        case 'B': /* bind */
        case 'C': /* close */
        case 'D': /* describe */
        case 'E': /* execute */
        case 'H': /* flush */
        case 'P': /* parse */
            u_sess->postgres_cxt.doing_extended_query_message = true;
            /* these are only legal in protocol 3 */
            if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
                ereport(
                    FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid frontend message type %d", qtype)));
            break;

        case 'S': /* sync */
            /* stop any active skip-till-Sync */
            u_sess->postgres_cxt.ignore_till_sync = false;
            /* mark not-extended, so that a new error doesn't begin skip */
            u_sess->postgres_cxt.doing_extended_query_message = false;
            /* only legal in protocol 3 */
            if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
                ereport(
                    FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid frontend message type %d", qtype)));
            break;

        case 'd': /* copy data */
        case 'c': /* copy done */
        case 'f': /* copy fail */
            u_sess->postgres_cxt.doing_extended_query_message = false;
            /* these are only legal in protocol 3 */
            if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
                ereport(
                    FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid frontend message type %d", qtype)));
            break;
#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)         /* PGXC_DATANODE */
        case 'q': /* Query ID */
        case 'r': /* Plan ID with sync */
        case 'M': /* Command ID */
        case 'g': /* GXID */
        case 's': /* Snapshot */
        case 't': /* Timestamp */
        case 'p': /* Process Pid */
        case 'b': /* Barrier */
        case 'W': /* WLM Control Group */
        case 'i': /* Instrumentation */
        case 'A': /* AC WLM */
        case 'e': /* Thread ID */
        case 'w': /* dynamic WLM */
        case 'R': /* Reply collect info */
        case 'n': /* Committing */
        case 'N': /* Commit csn */
        case 'l': /* get and handle csn for csnminsync */
        case 'G': /* PGXCBucketMap and PGXCNodeId */
        case 'j': /* Check gtm mode */
        case 'a': /* Not reach : Trigger shipped to DN */
        case 'k': /* Global session ID */
        case 'z': /* PBE for DDL */
        case 'y': /* sequence from cn 2 dn */
        case 'T': /* consistency point */
        case 'o': /* role name */
            break;
#endif

        default:

            /*
             * Otherwise we got garbage from the frontend.	We treat this as
             * fatal because we have probably lost message boundary sync, and
             * there's no good way to recover.
             */
            ereport(FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid frontend message type %c", qtype)));
            break;
    }

    /*
     * In protocol version 3, all frontend messages have a length word next
     * after the type code; we can read the message contents independently of
     * the type.
     */
    if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3) {
        if (pq_getmessage(inBuf, 0))
            return EOF; /* suitable message already logged */
    }

    return qtype;
}

/* if online sqladvsior collect workload is running, check query->rtable */
static bool checkCollectSimpleQuery(bool isCollect, List* querytreeList)
{
    if (isCollect && querytreeList != NULL) {
        TableConstraint tableConstraint;
        initTableConstraint(&tableConstraint);
        checkQuery(querytreeList, &tableConstraint);
        if (tableConstraint.isHasTable && !tableConstraint.isHasTempTable && !tableConstraint.isHasFunction) {
            return true;
        }
    }
    return false;
}

/*
 * Check replication uuid options for wal senders.
 * Return false if gucName is not repl_uuid.
 */
static bool CheckReplUuid(char *gucName, char *gucValue, bool doCheck)
{
    if (gucName == NULL || strcmp(gucName, "repl_uuid") != 0) {
        return false;
    }

    if (doCheck && (gucValue == NULL || strcmp(u_sess->attr.attr_storage.repl_uuid, gucValue) != 0)) {
        /* If UUID auth is required, check standby's UUID passed with startup packet -c option. */
        ereport(FATAL,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Standby UUID \"%s\" does not match primary UUID.", gucValue ? gucValue : "NULL")));
    }

    return true;
}

static void collectSimpleQuery(const char* queryString, bool isCollect)
{
    if (isCollect && checkAdivsorState()) {
        collectDynWithArgs(queryString, NULL, 0);
    }
}

/* ----------------
 *		ReadCommand reads a command from either the frontend or
 *		standard input, places it in inBuf, and returns the
 *		message type code (first byte of the message).
 *		EOF is returned if end of file.
 * ----------------
 */
static int ReadCommand(StringInfo inBuf)
{
    int result;

    /*
     * At begin of ReadCommand, reset extended-query-message flag, so that any
     * errors encountered in "idle" state don't provoke skip.
     */
    u_sess->postgres_cxt.doing_extended_query_message = false;

    /* Start a timer for session timeout. */
    if (!enable_session_sig_alarm(u_sess->attr.attr_common.SessionTimeout * 1000)) {
        ereport(FATAL, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("could not set timer for session timeout")));
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* if idle_in_transaction_session_timeout > 0 and it is in a transaction in idle,
     * then start a timer for idle_in_transaction_session.
     * In addition, if u_sess->attr.attr_common.SessionTimeout > 0 and
     * u_sess->attr.attr_common.SessionTimeout > u_sess->attr.attr_common.IdleInTransactionSessionTimeout,
     * we will select the smaller one as timeout, that is to say, only start the timer for session timeout.
     */
    if (u_sess->attr.attr_common.IdleInTransactionSessionTimeout > 0 &&
        (u_sess->attr.attr_common.SessionTimeout == 0 ||
        u_sess->attr.attr_common.SessionTimeout > u_sess->attr.attr_common.IdleInTransactionSessionTimeout) &&
        (IsAbortedTransactionBlockState() || IsTransactionOrTransactionBlock())) {
        if (!enable_idle_in_transaction_session_sig_alarm(
            u_sess->attr.attr_common.IdleInTransactionSessionTimeout * 1000)) {
            ereport(FATAL, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("could not set timer for idle-in-transaction timeout")));
        }
    }
#endif

    if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote)
        result = u_sess->proc_cxt.MyProcPort->protocol_config->fn_read_command(inBuf);
    else if (t_thrd.postgres_cxt.whereToSendOutput == DestDebug)
        result = InteractiveBackend(inBuf);
    else
        result = EOF;

    /* Disable a timer for session timeout. */
    if(!disable_session_sig_alarm()) {
        ereport(FATAL, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("could not disable timer for session timeout")));
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* Disable a timer for idle_in_transaction_session. */
    if (u_sess->attr.attr_common.IdleInTransactionSessionTimeout > 0 &&
        (u_sess->attr.attr_common.SessionTimeout == 0 ||
        u_sess->attr.attr_common.SessionTimeout > u_sess->attr.attr_common.IdleInTransactionSessionTimeout) &&
        (IsAbortedTransactionBlockState() || IsTransactionOrTransactionBlock())) {
        if (!disable_idle_in_transaction_session_sig_alarm()) {
            ereport(FATAL, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("could not disable timer for idle-in-transaction timeout")));
        }
    }
#endif

    u_sess->proc_cxt.firstChar = (char)result;
    return result;
}

/*
 * process_client_read_interrupt- Process interrupts specific to client reads
 *
 * interrupted by 2 ways:
 * 1. read finished
 * 2. interrupt by user
 *
 * Must preserve errno!
 */
void process_client_read_interrupt(bool blocked)
{
    int save_errno = errno;

    if (t_thrd.postgres_cxt.DoingCommandRead) {
        /* Check for general interrupts that arrived while reading */
        CHECK_FOR_INTERRUPTS();

        /* Process sinval catchup interrupts that happened while reading */
        if(catchupInterruptPending) {
            ProcessCatchupInterrupt();
        }

        /* Process sinval catchup interrupts that happened while reading */
        if(notifyInterruptPending) {
            ProcessNotifyInterrupt();
        }
    } else if (t_thrd.int_cxt.ProcDiePending && blocked) {
        /*
         * We're dying It's safe (and sane) to handle that now.
         */
        CHECK_FOR_INTERRUPTS();
    }
    errno = save_errno;
}

/*
 * process_client_write_interrupt - Process interrupts specific to client writes
 *
 * interrupted by 2 ways:
 * 1. write finished
 * 2. interrupt by user
 * 'blocked' tells whether the socket is in blocked mode
 *
 * Must preserve errno!
 */
void process_client_write_interrupt (bool blocked)
{
    int save_errno = errno;

    if (t_thrd.int_cxt.ProcDiePending && blocked) {
        /*
         * No error message here, since:
         * a) would possibly block again
         * b) would possibly lead to sending an error message to the client,
         *    while we already started to send something else.
         */
         if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote) {
             t_thrd.postgres_cxt.whereToSendOutput = DestNone;
         }
         CHECK_FOR_INTERRUPTS();
    }

    errno = save_errno;
}


/*
 * prepare_for_client_read -- set up to possibly block on client input
 *
 * This must be called immediately before any low-level read from the
 * client connection.  It is necessary to do it at a sufficiently low level
 * that there won't be any other operations except the read kernel call
 * itself between this call and the subsequent client_read_ended() call.
 * In particular there mustn't be use of malloc() or other potentially
 * non-reentrant libc functions.  This restriction makes it safe for us
 * to allow interrupt service routines to execute nontrivial code while
 * we are waiting for input.
 */
void prepare_for_client_read(void)
{
    if (t_thrd.postgres_cxt.DoingCommandRead) {
        /* Allow cancel/die interrupts to be processed while waiting */
        t_thrd.int_cxt.ImmediateInterruptOK = true;

        /* And don't forget to detect one that already arrived */
        CHECK_FOR_INTERRUPTS();
    }
}

/*
 * prepare_for_logic_conn_read
 */
void prepare_for_logic_conn_read(void)
{
    if (t_thrd.postgres_cxt.DoingCommandRead) {
        /* Allow cancel/die interrupts to be processed while waiting */
        t_thrd.int_cxt.ImmediateInterruptOK = true;

        /* And don't forget to detect one that already arrived */
        CHECK_FOR_INTERRUPTS();
    }
}

/*
 * logic_conn_read_ended
 */
void logic_conn_read_check_ended(void)
{
    if (t_thrd.postgres_cxt.DoingCommandRead) {
        int save_errno = errno;

        /* Process sinval catchup interrupts that happened while reading */
        if (catchupInterruptPending)
            ProcessCatchupInterrupt();

        /* Process sinval catchup interrupts that happened while reading */
        if (notifyInterruptPending)
            ProcessNotifyInterrupt();

        t_thrd.int_cxt.ImmediateInterruptOK = false;
        errno = save_errno;
    }
}

/*
 * client_read_ended -- get out of the client-input state
 */
void client_read_ended(void)
{
    if (t_thrd.postgres_cxt.DoingCommandRead) {
        int save_errno = errno;
        /* Should reset ImmediateInterruptOK before proccessing catchup interrupts */
        t_thrd.int_cxt.ImmediateInterruptOK = false;

        /* Process sinval catchup interrupts that happened while reading */
        if (catchupInterruptPending)
            ProcessCatchupInterrupt();

        /* Process sinval catchup interrupts that happened while reading */
        if (notifyInterruptPending)
            ProcessNotifyInterrupt();

        errno = save_errno;
    }
}

#if (!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS))

void ExecuteFunctionIfExisted(const char *filename, char *funcname)
{
    CFunInfo tmpCF;
    tmpCF = load_external_function(filename, funcname, false, false);
    if (tmpCF.user_fn != NULL) {
        ((void* (*)(void))(tmpCF.user_fn))();
    }
}

bool IsFileExisted(const char *filename)
{
    char* fullname = expand_dynamic_library_name(filename);
    return file_exists(fullname);
}

#define APACEH_AGE "age"
void InitAGESqlPluginHookIfNeeded()
{
    if (get_extension_oid(APACEH_AGE, true) != InvalidOid && IsFileExisted(APACEH_AGE)) {
        load_file(APACEH_AGE, !superuser());
    }
}

#define INIT_PLUGIN_OBJECT "init_plugin_object"
#define WHALE "whale"
#define DOLPHIN "dolphin"
void InitASqlPluginHookIfNeeded()
{
    ExecuteFunctionIfExisted(WHALE, INIT_PLUGIN_OBJECT);
}

void InitBSqlPluginHookIfNeeded()
{
    ExecuteFunctionIfExisted(DOLPHIN, INIT_PLUGIN_OBJECT);
}

#define LOAD_DOLPHIN "create_dolphin_extension"
void LoadDolphinIfNeeded()
{
    if (IsFileExisted(DOLPHIN)) {
        ExecuteFunctionIfExisted(DOLPHIN, LOAD_DOLPHIN);
    }
}

#define INIT_DOLPHIN_PROTO "init_dolphin_proto"
typedef void (*init_dolphin_proto_fn)(char *database_name);

void InitDolpinProtoIfNeeded()
{
    if (IsFileExisted(DOLPHIN) && g_instance.attr.attr_network.enable_dolphin_proto) {
        CFunInfo func = load_external_function(DOLPHIN, INIT_DOLPHIN_PROTO, false, false);
        if (func.user_fn != NULL) {
            ((init_dolphin_proto_fn)func.user_fn)("");
        }
    }
}
#endif

/*
 * Do raw parsing (only).
 *
 * A list of parsetrees is returned, since there might be multiple
 * commands in the given string.
 *
 * NOTE: for interactive queries, it is important to keep this routine
 * separate from the analysis & rewrite stages.  Analysis and rewriting
 * cannot be done in an aborted transaction, since they require access to
 * database tables.  So, we rely on the raw parser to determine whether
 * we've seen a COMMIT or ABORT command; when we are in abort state, other
 * commands are not processed any further than the raw parse stage.
 */
List* pg_parse_query(const char* query_string, List** query_string_locationlist,
                     List* (*parser_hook)(const char*, List**))
{
    List* raw_parsetree_list = NULL;
    OgRecordAutoController _local_opt(PARSE_TIME);

    TRACE_POSTGRESQL_QUERY_PARSE_START(query_string);

    if (u_sess->attr.attr_common.log_parser_stats)
        ResetUsage();

    if (parser_hook == NULL) {
        parser_hook = raw_parser;
#if (!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS))
        if (u_sess->attr.attr_sql.whale || u_sess->attr.attr_sql.dolphin) {
            int id = GetCustomParserId();
            if (id >= 0 && g_instance.raw_parser_hook[id] != NULL) {
                parser_hook = (List* (*)(const char*, List**))g_instance.raw_parser_hook[id];
            }
        }
#endif
    }
    raw_parsetree_list = parser_hook(query_string, query_string_locationlist);
    if (u_sess->parser_cxt.hasPartitionComment) {
        ereport(WARNING, (errmsg("comment is not allowed in partition/subpartition.")));
    }

    if (u_sess->attr.attr_common.log_parser_stats)
        ShowUsage("PARSER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
    /* Optional debugging check: pass raw parsetrees through copyObject() */
    {
        List* new_list = (List*)copyObject(raw_parsetree_list);

        /* This checks both copyObject() and the equal() routines... */
        if (!equal(new_list, raw_parsetree_list)) {
            ereport(WARNING, (errmsg("copyObject() failed to produce an equal raw parse tree")));
        } else {
            raw_parsetree_list = new_list;
        }
    }
#endif

    TRACE_POSTGRESQL_QUERY_PARSE_DONE(query_string);

    return raw_parsetree_list;
}

/*
 * Given a raw parsetree (gram.y output), and optionally information about
 * types of parameter symbols ($n), perform parse analysis and rule rewriting.
 *
 * A list of Query nodes is returned, since either the analyzer or the
 * rewriter might expand one query to several.
 *
 * NOTE: for reasons mentioned above, this must be separate from raw parsing.
 */
List* pg_analyze_and_rewrite(Node* parsetree, const char* query_string, Oid* paramTypes, int numParams)
{
    OgRecordAutoController _local_opt(SRT3_ANALYZE_REWRITE);
    Query* query = NULL;
    List* querytree_list = NULL;

    TRACE_POSTGRESQL_QUERY_REWRITE_START(query_string);

    /*
     * (1) Perform parse analysis.
     */
    if (u_sess->attr.attr_common.log_parser_stats)
        ResetUsage();

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                IsA(parsetree, SelectStmt) && 
                !is_streaming_thread() &&
                !streaming_context_is_ddl() &&
                is_contquery_with_dict((Node *)((SelectStmt *) parsetree)->fromClause)) {
        transform_contquery_selectstmt_with_dict((SelectStmt *) parsetree);
    }
#endif

    query = parse_analyze(parsetree, query_string, paramTypes, numParams);

    if (u_sess->attr.attr_common.log_parser_stats)
        ShowUsage("PARSE ANALYSIS STATISTICS");

    /*
     * (2) Rewrite the queries, as necessary
     */
    querytree_list = pg_rewrite_query(query);

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        ListCell* lc = NULL;

        foreach (lc, querytree_list) {
            Query* query_tmp = (Query*)lfirst(lc);

            if (query_tmp->sql_statement == NULL)
                query_tmp->sql_statement = (char*)query_string;
        }
    }
#endif

    TRACE_POSTGRESQL_QUERY_REWRITE_DONE(query_string);

    return querytree_list;
}

/*
 * Do parse analysis and rewriting.  This is the same as pg_analyze_and_rewrite
 * except that external-parameter resolution is determined by parser callback
 * hooks instead of a fixed list of parameter datatypes.
 */
List* pg_analyze_and_rewrite_params(
    Node* parsetree, const char* query_string, ParserSetupHook parserSetup, void* parserSetupArg)
{
    ParseState* pstate = NULL;
    Query* query = NULL;
    List* querytree_list = NULL;

    Assert(query_string != NULL); /* required as of 8.4 */

    TRACE_POSTGRESQL_QUERY_REWRITE_START(query_string);

    /*
     * (1) Perform parse analysis.
     */
    if (u_sess->attr.attr_common.log_parser_stats)
        ResetUsage();

    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = query_string;
    (*parserSetup)(pstate, parserSetupArg);

    query = transformTopLevelStmt(pstate, parsetree);

    /* it's unsafe to deal with plugins hooks as dynamic lib may be released */
    if (post_parse_analyze_hook && !(g_instance.status > NoShutdown)) {
        (*post_parse_analyze_hook)(pstate, query);
    }

    free_parsestate(pstate);

    if (u_sess->attr.attr_common.log_parser_stats)
        ShowUsage("PARSE ANALYSIS STATISTICS");

#ifdef PGXC
    if (query->commandType == CMD_UTILITY && IsA(query->utilityStmt, CreateTableAsStmt)) {
        CreateTableAsStmt* cts = (CreateTableAsStmt*)query->utilityStmt;

        cts->parserSetup = (void*)parserSetup;
        cts->parserSetupArg = parserSetupArg;
    }
#endif

    /*
     * (2) Rewrite the queries, as necessary
     */
    querytree_list = pg_rewrite_query(query);

    TRACE_POSTGRESQL_QUERY_REWRITE_DONE(query_string);

    return querytree_list;
}

/*
 * Perform rewriting of a query produced by parse analysis.
 *
 * Note: query must just have come from the parser, because we do not do
 * AcquireRewriteLocks() on it.
 */
static List* pg_rewrite_query(Query* query)
{
    List* querytree_list = NIL;
    PGSTAT_INIT_TIME_RECORD();

    if (u_sess->attr.attr_sql.Debug_print_parse && u_sess->attr.attr_common.log_min_messages <= LOG)
        elog_node_display(LOG, "parse tree", query, u_sess->attr.attr_sql.Debug_pretty_print);

    if (u_sess->attr.attr_common.log_parser_stats)
        ResetUsage();

    PGSTAT_START_TIME_RECORD();

    if (query->commandType == CMD_UTILITY) {
#ifdef ENABLE_MULTIPLE_NODES
        if (IsA(query->utilityStmt, CreateTableAsStmt)) {
            /*
             * CREATE TABLE AS SELECT and SELECT INTO are rewritten so that the
             * target table is created first. The SELECT query is then transformed
             * into an INSERT INTO statement
             */
             /* handle materilized view. */
            CreateTableAsStmt *stmt = (CreateTableAsStmt *)query->utilityStmt;
            if (IS_PGXC_COORDINATOR && stmt->relkind == OBJECT_MATVIEW) {
                /* Check if OK to create matview */
                check_basetable((Query *)stmt->query, true, stmt->into->ivm);
                /* Check for select privilege */
                (void)ExecCheckRTPerms(query->rtable, true);

                if (stmt->into) {
                    if (stmt->into->ivm) {
                        check_matview_op_supported(stmt);
                        stmt->into->distributeby = infer_incmatview_distkey(stmt);
                    }
                }
            }
             
            if (stmt->relkind == OBJECT_MATVIEW && IS_PGXC_DATANODE) {
                querytree_list = list_make1(query);
            } else {
                /*
                 * CREATE TABLE AS SELECT and SELECT INTO are rewritten so that the
                 * target table is created first. The SELECT query is then transformed
                 * into an INSERT INTO statement
                 */
                querytree_list = QueryRewriteCTAS(query);
            }
        } else if (IsA(query->utilityStmt, CopyStmt)) {
            querytree_list = query_rewrite_copy(query);
        } else if(IsA(query->utilityStmt, RefreshMatViewStmt)) {
            RefreshMatViewStmt *stmt = (RefreshMatViewStmt *)query->utilityStmt;
            if (!stmt->incremental && !isIncMatView(stmt->relation)) {
                querytree_list = QueryRewriteRefresh(query);
            } else {
                querytree_list = list_make1(query);
            }
        } else if (IsA(query->utilityStmt, AlterTableStmt)) {
            querytree_list = query_rewrite_alter_table(query);
        } else {
            /* don't rewrite utilities, just dump them into result list */
            querytree_list = list_make1(query);
        }
#else
        if (IsA(query->utilityStmt, CreateTableAsStmt)) {
            CreateTableAsStmt *stmt = (CreateTableAsStmt *)query->utilityStmt;
            if (stmt->relkind == OBJECT_MATVIEW) {

                /* Check if OK to create matview */
                check_basetable((Query *)stmt->query, true, stmt->into->ivm);
                /* Check for select privilege */
                (void)ExecCheckRTPerms(query->rtable, true);

                if (stmt->into && stmt->into->ivm) {
                    check_matview_op_supported(stmt);
                }

                querytree_list = list_make1(query);
            } else {
                querytree_list = QueryRewriteCTAS(query);
            }
        } else if (IsA(query->utilityStmt, PrepareStmt)) {
            PrepareStmt *stmt = (PrepareStmt *)query->utilityStmt;
            if (IsA(stmt->query, Const)) {
                if (((Const *)stmt->query)->constisnull) {
                    ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("userdefined variable in prepare statement must be text type.")));
                }
            } else {
                querytree_list = list_make1(query);
            }
        } else if (IsA(query->utilityStmt, VariableMultiSetStmt)) {
            querytree_list = query_rewrite_multiset_stmt(query);
        } else if (IsA(query->utilityStmt, VariableSetStmt)) {
            querytree_list = query_rewrite_set_stmt(query);
        } else {
            querytree_list = list_make1(query);
        }
#endif
    } else {
        /* rewrite regular queries */
        querytree_list = QueryRewrite(query);
    }

    /*
     * After rewriting the querytree_list, we need to check every query to ensure they can execute
     * in new expression framework.  If we find a query's is_flt_frame is false, which indicates the query
     * not support the new expression framework, the top level query need be reverted to old.
     * One more things, CMD_UTILITY statements cannot be reverted here.  We solve it at explain_proc_query()
     */
    if (u_sess->attr.attr_common.enable_expr_fusion && u_sess->attr.attr_sql.query_dop_tmp == 1) {
        ListCell* lc = NULL;
        foreach(lc, querytree_list) {
            Query* query_tmp = (Query*)lfirst(lc);
            query_tmp->is_flt_frame = !query_check_no_flt(query_tmp);
        }
    }

    PGSTAT_END_TIME_RECORD(REWRITE_TIME);

    if (u_sess->attr.attr_common.log_parser_stats)
        ShowUsage("REWRITER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
    /* Optional debugging check: pass querytree output through copyObject() */
    {
        List* new_list = NIL;

        new_list = (List*)copyObject(querytree_list);
        /* This checks both copyObject() and the equal() routines... */
        if (!equal(new_list, querytree_list))
            ereport(WARNING, (errmsg("copyObject() failed to produce equal parse tree")));
        else
            querytree_list = new_list;
    }
#endif

    if (u_sess->attr.attr_sql.Debug_print_rewritten && u_sess->attr.attr_common.log_min_messages <= LOG)
        elog_node_display(LOG, "rewritten parse tree", querytree_list, u_sess->attr.attr_sql.Debug_pretty_print);

    return querytree_list;
}

/*
 * check compute privilieges for expected_computing_modegroup .
 */
static void check_query_acl(Query* query)
{
    if (containing_ordinary_table((Node*)query)) {
        ComputingNodeGroupMode cng_mode = ng_get_computing_nodegroup_mode();
        if (cng_mode == CNG_MODE_COSTBASED_EXPECT || cng_mode == CNG_MODE_FORCE) {
            Distribution* distribution = ng_get_group_distribution(u_sess->attr.attr_sql.expected_computing_nodegroup);

            if (InvalidOid != distribution->group_oid) {
                /* Check current user has privilige to this group */
                AclResult aclresult =
                    pg_nodegroup_aclcheck(distribution->group_oid, GetUserId(), ACL_COMPUTE | ACL_USAGE);
                if (aclresult != ACLCHECK_OK) {
                    aclcheck_error(aclresult, ACL_KIND_NODEGROUP, u_sess->attr.attr_sql.expected_computing_nodegroup);
                }
            }
        }
    }
}

/*
 * Generate a plan for a single already-rewritten query.
 * This is a thin wrapper around planner() and takes the same parameters.
 */
PlannedStmt* pg_plan_query(Query* querytree, int cursorOptions, ParamListInfo boundParams, bool underExplain)
{
    PlannedStmt* plan = NULL;
    OgRecordAutoController _local_opt(PLAN_TIME);
    bool multi_node_hint = false;

    /* Utility commands have no plans. */
    if (querytree->commandType == CMD_UTILITY)
        return NULL;

    if (querytree->hintState != NULL) {
        multi_node_hint = querytree->hintState->multi_node_hint;
    }

    /* Planner must have a snapshot in case it calls user-defined functions. */
    if (!GTM_LITE_MODE) {  /* Skip if GTMLite */
       Assert(ActiveSnapshotSet());
    }
    TRACE_POSTGRESQL_QUERY_PLAN_START();

    if (u_sess->attr.attr_common.log_planner_stats)
        ResetUsage();

    /* Update hard parse counter for Unique SQL */
    UniqueSQLStatCountHardParse(1);

    /* check perssion for expect_computing_nodegroup */
    if (!OidIsValid(lc_replan_nodegroup))
        check_query_acl(querytree);

    if (u_sess->hook_cxt.plannerHook != NULL) {
        plan = ((plannerFunc)(u_sess->hook_cxt.plannerHook))(querytree, cursorOptions, boundParams);
    } else {
        /* call the optimizer */
        plan = planner(querytree, cursorOptions, boundParams);
    }

    if (u_sess->attr.attr_common.log_planner_stats)
        ShowUsage("PLANNER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
    /* Optional debugging check: pass plan output through copyObject() */
    {
        PlannedStmt* new_plan = (PlannedStmt*)copyObject(plan);

        /*
         * equal() currently does not have routines to compare Plan nodes, so
         * don't try to test equality here.  Perhaps fix someday?
         */
#ifdef NOT_USED
        /* This checks both copyObject() and the equal() routines... */
        if (!equal(new_plan, plan))
            ereport(WARNING, (errmsg("copyObject() failed to produce an equal plan tree")));
        else
#endif
            plan = new_plan;
    }
#endif

    /*
     * Print plan if debugging.
     */
    if (u_sess->attr.attr_sql.Debug_print_plan && u_sess->attr.attr_common.log_min_messages <= LOG)
        elog_node_display(LOG, "plan", plan, u_sess->attr.attr_sql.Debug_pretty_print);

    if (!underExplain)
        output_hint_warning(plan->plan_hint_warning, DEBUG1);

    TRACE_POSTGRESQL_QUERY_PLAN_DONE();

    plan->is_stream_plan = u_sess->opt_cxt.is_stream;

    plan->multi_node_hint = multi_node_hint;

    plan->is_flt_frame = false;
    if (querytree->is_flt_frame) {
        plan->is_flt_frame = true;
    }

    if (plan->planTree) {
        if (IS_ENABLE_RIGHT_REF(querytree->rightRefState)) {
            plan->is_flt_frame = false;
            plan->planTree->rightRefState = querytree->rightRefState;
        } else {
            plan->planTree->rightRefState = nullptr;
        }
    }

    return plan;
}

/*
 * Check whether the query is insert multiple values query.
 * In the insert query, only one RTE_VALUES in the ratble is a multi-values query..
 */
__attribute__((unused)) static bool is_insert_multiple_values_query_in_gtmfree(Query* query)
{
    if (query->commandType != CMD_INSERT || !g_instance.attr.attr_storage.enable_gtm_free) {
        return false;
    }
    List* rtables = query->rtable;
    ListCell* cell = NULL;
    RangeTblEntry* rte = NULL;
    RangeTblEntry* values_rte = NULL;
    foreach(cell, rtables) {
        rte = (RangeTblEntry*)lfirst(cell);
        if (rte->rtekind == RTE_VALUES) {
            if (values_rte != NULL) {
                return false;
            }
            values_rte = rte;
        }
    }
    if (values_rte != NULL) {
        return true;
    } else {
        return false;
    }
}


/*
 * Generate plans for a list of already-rewritten queries.
 *
 * Normal optimizable statements generate PlannedStmt entries in the result
 * list.  Utility statements are simply represented by their statement nodes.
 */
List* pg_plan_queries(List* querytrees, int cursorOptions, ParamListInfo boundParams)
{
    OgRecordAutoController _local_opt(SRT4_PLAN_QUERY);
    List* stmt_list = NIL;
    ListCell* query_list = NULL;

    /* for abstimeout, transfer time to str in insert has some problem,
     *  so distinguish insert and select situation for ABSTIMEOUTFUN to avoid problem.*/
    t_thrd.time_cxt.is_abstimeout_in = true;

    /* Set initalized plan seed according to guc plan_mode_seed for random plan testing function */
    set_inital_plan_seed();

#ifndef ENABLE_MULTIPLE_NODES
    if (unlikely(list_length(querytrees) != 1)) {
        u_sess->opt_cxt.query_dop = 1;
    }
#endif

    foreach (query_list, querytrees) {
        Query* query = castNode(Query, lfirst(query_list));
        Node* stmt = NULL;
        PlannedStmt* ps = NULL;

        if (query->commandType == CMD_UTILITY) {
            /* Utility commands have no plans. */
            stmt = query->utilityStmt;

            /* output grammer error of hint */
            output_utility_hint_warning((Node*)query, DEBUG1);
        } else {
            query->boundParamsQ = boundParams;
#ifdef ENABLE_MULTIPLE_NODES
            bool is_insert_multiple_values = is_insert_multiple_values_query_in_gtmfree(query);
#endif
            /* Temporarily apply SET hint using PG_TRY for later recovery */
            int nest_level = apply_set_hint(query);
            PG_TRY();
            {
                stmt = (Node*)pg_plan_query(query, cursorOptions, boundParams);
            }
            PG_CATCH();
            {
                recover_set_hint(nest_level);
                PG_RE_THROW();
            }
            PG_END_TRY();
        
            recover_set_hint(nest_level);
#ifdef ENABLE_MULTIPLE_NODES
            /* When insert multiple values query is a generate plan,
             * we don't consider whether it can be executed on a single dn.
             * Because it can be executed on a single DN only in custom plan.
             * u_sess->pcache_cxt.query_has_params: checks whether the plan is in cachedplan.
             * boundParams: checks whether the plan is a generate plan.
             */
            if (!(is_insert_multiple_values && boundParams == NULL && u_sess->pcache_cxt.query_has_params)) {
                check_gtm_free_plan((PlannedStmt *)stmt,
                    (u_sess->attr.attr_sql.explain_allow_multinode || ClusterResizingInProgress()) ? WARNING : ERROR);
            }
#endif
            ps = (PlannedStmt*)stmt;
            check_plan_mergeinto_replicate(ps, ERROR);

            if (ps != NULL)
                u_sess->wlm_cxt->wlm_num_streams += ps->num_streams;
        }

        stmt_list = lappend(stmt_list, stmt);
    }

    t_thrd.time_cxt.is_abstimeout_in = false;

    if (!u_sess->attr.attr_common.enable_iud_fusion) {
        /* Set warning that no-analyzed relation name to log. */
        output_noanalyze_rellist_to_log(LOG);
    }

    return stmt_list;
}

/* Get the tag automatically for plan shipping.
 * For now plan shipping is used only for SELECT\INSERT\DELETE\UPDATA\MERGE.
 */
const char* CreateCommandTagForPlan(CmdType commandType)
{
    const char* tag = NULL;

    switch (commandType) {

        case CMD_SELECT:
            tag = "SELECT";
            break;

        case CMD_INSERT:
            tag = "INSERT";
            break;

        case CMD_DELETE:
            tag = "DELETE";
            break;

        case CMD_UPDATE:
            tag = "UPDATE";
            break;

        case CMD_MERGE:
            tag = "MERGE";
            break;

        default:
            ereport(WARNING, (errmsg("unrecognized command type: %d", (int)commandType)));
            tag = "?\?\?";
            break;
    }
    return tag;
}

#ifdef USE_SPQ
void ChangeDMLPlanForRONode(PlannedStmt* plan)
{
    if (IS_SPQ_EXECUTOR && !SS_PRIMARY_MODE &&
        (plan->commandType == CMD_INSERT || plan->commandType == CMD_UPDATE || plan->commandType == CMD_DELETE)) {
        if (IsA(plan->planTree, ModifyTable)) {
            ModifyTable* node = (ModifyTable*)plan->planTree;
            ListCell* l = NULL;
            foreach (l, node->plans) {
                plan->planTree = (Plan*)lfirst(l);
            }
        }
        ListCell *rtcell;
        RangeTblEntry *rte;
        AclMode removeperms = ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_SELECT_FOR_UPDATE;

        /* Just reading, so don't check INS/DEL/UPD permissions. */
        foreach(rtcell, plan->rtable) {
            rte = (RangeTblEntry *)lfirst(rtcell);
            if (rte->rtekind == RTE_RELATION &&
                0 != (rte->requiredPerms & removeperms))
                rte->requiredPerms &= ~removeperms;
        }
    }
}
#endif
/*
 * exec_simple_plan
 *
 * Execute a "simple Plan" received from coordinator
 */
void exec_simple_plan(PlannedStmt* plan)
{
    CommandDest dest = (CommandDest)t_thrd.postgres_cxt.whereToSendOutput;
    MemoryContext oldcontext;
    bool save_log_statement_stats = u_sess->attr.attr_common.log_statement_stats;
    bool isTopLevel = false;
    char msec_str[PRINTF_DST_MAX];

    if (plan == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Invaild parameter.")));
    }

    ChangeDMLPlanForRONode(plan);

    plpgsql_estate = NULL;

    /* Initialize the global variables for recursive */
    InitRecursiveCTEGlobalVariables(plan);

    /*
     * The "debug_query_string" may still be used after transaction is done,
     * say in ShowUsage. To avoid the "debug_query_string" point to a freed
     * space, premalloc a space for "debug_query_string".
     */
    if (save_log_statement_stats)
        t_thrd.postgres_cxt.debug_query_string = MemoryContextStrdup(t_thrd.mem_cxt.msg_mem_cxt, plan->query_string);
    else
        t_thrd.postgres_cxt.debug_query_string = plan->query_string;

    instr_stmt_report_start_time();
    pgstat_report_activity(STATE_RUNNING, t_thrd.postgres_cxt.debug_query_string);

    /*
     * We use save_log_statement_stats so ShowUsage doesn't report incorrect
     * results because ResetUsage wasn't called.
     */
    if (save_log_statement_stats)
        ResetUsage();

    if (ThreadIsDummy(plan->planTree)) {
        u_sess->stream_cxt.dummy_thread = true;
        u_sess->exec_cxt.executorStopFlag = true;
    }

    /* "IS_PGXC_DATANODE && StreamTopConsumerAmI()" means on the CN of the compute pool. */
    if (IS_PGXC_COORDINATOR && StreamTopConsumerAmI())
        exec_init_poolhandles();

    /* "IS_PGXC_DATANODE && plan->in_compute_pool" means on the DN of the compute pool. */
    if (IS_PGXC_DATANODE && plan->in_compute_pool)
        u_sess->exec_cxt.executorStopFlag = false;

    /*
     * Start up a transaction command.	All queries generated by the
     * query_string will be in this same command block, *unless* we find a
     * BEGIN/COMMIT/ABORT statement; we have to force a new xact command after
     * one of those, else bad things will happen in xact.c. (Note that this
     * will normally change current memory context.)
     */
    start_xact_command();

    /*
     * Zap any pre-existing unnamed statement.	(While not strictly necessary,
     * it seems best to define simple-Query mode as if it used the unnamed
     * statement and portal; this ensures we recover any storage used by prior
     * unnamed operations.)
     */
    drop_unnamed_stmt();

    /*
     * We'll tell PortalRun it's a top-level command iff there's exactly one
     * raw parsetree.  If more than one, it's effectively a transaction block
     * and we want PreventTransactionChain to reject unsafe commands. (Note:
     * we're assuming that query rewrite cannot add commands that are
     * significant to PreventTransactionChain.)
     */
    isTopLevel = true;

    {
        const char* commandTag = NULL;
        char completionTag[COMPLETION_TAG_BUFSIZE];
        Portal portal;
        DestReceiver* receiver = NULL;
        int16 format;

        /*
         * By default we do not want Datanodes or client Coordinators to contact GTM directly,
         * it should get this information passed down to it.
         */
        SetForceXidFromGTM(false);

        /*
         * Get the tag automatically for plan shipping.
         * For now plan shipping is used only for SELECT\INSERT\DELETE\UPDATA\MERGE.
         */
        commandTag = CreateCommandTagForPlan(plan->commandType);

        set_ps_display(commandTag, false);

        BeginCommand(commandTag, dest);

        /*
         * If we are in an aborted transaction, reject all commands except
         * COMMIT/ABORT.  It is important that this test occur before we try
         * to do parse analysis, rewrite, or planning, since all those phases
         * try to do database accesses, which may fail in abort state. (It
         * might be safe to allow some additional utility commands in this
         * state, but not many...)
         */
        if (IsAbortedTransactionBlockState())
            ereport(ERROR,
                (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                    errmsg("current transaction is aborted, "
                        "commands ignored until end of transaction block, firstChar[%c]",
                        u_sess->proc_cxt.firstChar),
                    errdetail_abort()));

        /* Make sure we are in a transaction command */
        start_xact_command();

        /*
         * OK to analyze, rewrite, and plan this query.
         *
         * Switch to appropriate context for constructing querytrees (again,
         * these must outlive the execution context).
         */
        oldcontext = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
        WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

        /* local dn has vcgroup */
        if (*g_instance.wlm_cxt->local_dn_ngname && *g_wlm_params->ngroup &&
            0 != strcmp(g_instance.wlm_cxt->local_dn_ngname, g_wlm_params->ngroup)) {
            /* Get the node group information */
            t_thrd.wlm_cxt.thread_node_group = g_instance.wlm_cxt->local_dn_nodegroup;

            /* check if the control group is valid and set it for foreign user */
            if (t_thrd.wlm_cxt.thread_node_group->foreignrp) {
                u_sess->wlm_cxt->local_foreign_respool = t_thrd.wlm_cxt.thread_node_group->foreignrp;

                /* update the resource pool information if it is from foreign users */
                WLMSetControlGroup(t_thrd.wlm_cxt.thread_node_group->foreignrp->cgroup);

                /* reset the resource pool name */
                char* foreignrp_name = get_resource_pool_name(t_thrd.wlm_cxt.thread_node_group->foreignrp->rpoid);

                if (foreignrp_name && *foreignrp_name) {
                    errno_t rc = snprintf_s(g_wlm_params->rpdata.rpname,
                        sizeof(g_wlm_params->rpdata.rpname),
                        sizeof(g_wlm_params->rpdata.rpname) - 1,
                        "%s",
                        foreignrp_name);
                    securec_check_ss(rc, "", "");

                    pfree(foreignrp_name);

                    /* only the foreign resource pool has rpoid in hash table */
                    g_wlm_params->rpdata.rpoid = t_thrd.wlm_cxt.thread_node_group->foreignrp->rpoid;
                } else {
                    errno_t rc = snprintf_s(g_wlm_params->rpdata.rpname,
                        sizeof(g_wlm_params->rpdata.rpname),
                        sizeof(g_wlm_params->rpdata.rpname) - 1,
                        "%s",
                        DEFAULT_POOL_NAME);
                    securec_check_ss(rc, "", "");
                }
            } else {
                WLMSetControlGroup(GSCGROUP_INVALID_GROUP);
            }

            /* reset the ngname */
            errno_t rc = snprintf_s(g_wlm_params->ngroup,
                sizeof(g_wlm_params->ngroup),
                sizeof(g_wlm_params->ngroup) - 1,
                "%s",
                g_instance.wlm_cxt->local_dn_ngname);
            securec_check_ss(rc, "", "");
        }

        GSCGROUP_ATTACH_TASK();

        /* bypass log */
        if (IS_PGXC_DATANODE && u_sess->attr.attr_sql.enable_opfusion == true &&
            u_sess->attr.attr_sql.opfusion_debug_mode == BYPASS_LOG) {
            BypassUnsupportedReason(NOBYPASS_STREAM_NOT_SUPPORT);
        }
        /*
         * Create unnamed portal to run the query or queries in. If there
         * already is one, silently drop it.
         */
        portal = CreatePortal("", true, true);
        /* Don't display the portal in pg_cursors */
        portal->visible = false;

        /*
         * We don't have to copy anything into the portal, because everything
         * we are passing here is in t_thrd.mem_cxt.msg_mem_cxt, which will outlive the
         * portal anyway.
         */
        PortalDefineQuery(portal,
            NULL,
            "DUMMY",
            commandTag,
            lappend(NULL, plan),  // vam plantree_list,
            NULL);

        /*
         * Start the portal.  No parameters here.
         */
        PortalStart(portal, NULL, 0, u_sess->spq_cxt.snapshot);

        /*
         * Select the appropriate output format: text unless we are doing a
         * FETCH from a binary cursor.	(Pretty grotty to have to do this here
         * --- but it avoids grottiness in other places.  Ah, the joys of
         * backward compatibility...)
         */
        format = 0; /* TEXT is default */
        PortalSetResultFormat(portal, 1, &format);

        /*
         * Now we can create the destination receiver object.
         */
        receiver = CreateDestReceiver(dest);
        if (dest == DestRemote)
            SetRemoteDestReceiverParams(receiver, portal);
#ifdef USE_SPQ
        if (IS_SPQ_RUNNING && dest == DestRemote)
            SetRemoteDestTupleReceiverParams(receiver);
#endif

        /*
         * Switch back to transaction context for execution.
         */
        MemoryContextSwitchTo(oldcontext);

        /*
         * Run the portal to completion, and then drop it (and the receiver).
         */
        (void)PortalRun(portal, FETCH_ALL, isTopLevel, receiver, receiver, completionTag);

        (*receiver->rDestroy)(receiver);

        PortalDrop(portal, false);

        /* Flush messages left in PqSendBuffer before entering syncQuit. */
        (void)pq_flush();

        /* obs_instr->serializeSend() must be before finish_xact_command() */
        if (StreamTopConsumerAmI() && u_sess->instr_cxt.obs_instr != NULL) {
            u_sess->instr_cxt.obs_instr->serializeSend();
        }

        finish_xact_command();

        EndCommand(completionTag, dest);

        /* Set sync point for waiting all stream threads complete. */
        StreamNodeGroup::syncQuit(STREAM_COMPLETE);
        UnRegisterStreamSnapshots();

        /*
         * send the stream instrumentation to the coordinator until all the stream thread quit.
         */
        if (IS_PGXC_DATANODE && StreamTopConsumerAmI() && u_sess->instr_cxt.global_instr != NULL) {
            u_sess->instr_cxt.global_instr->serializeSend();
            if (u_sess->instr_cxt.global_instr->needTrack()) {
                u_sess->instr_cxt.global_instr->serializeSendTrack();
            }
        }

        /*
         * send the stream instrumentation to DWS DN until all the stream thread quit.
         * just run on the CN of the compute pool.
         */
        if (IS_PGXC_COORDINATOR && StreamTopConsumerAmI() && u_sess->instr_cxt.global_instr != NULL) {
            Plan* aplan = plan->planTree->lefttree;
            int plannode_num = 0;
            while (aplan != NULL) {
                plannode_num++;

                /* plantree is always left-hand tree on the compute pool currently. */
                aplan = aplan->lefttree;
            }
            u_sess->instr_cxt.global_instr->aggregate(plannode_num);
            u_sess->instr_cxt.global_instr->serializeSend();
            if (u_sess->instr_cxt.global_instr->needTrack()) {
                u_sess->instr_cxt.global_instr->serializeSendTrack();
            }
        }
    } /* end loop over parsetrees */

    /*
     * Close down transaction statement, if one is open.
     */
    finish_xact_command();

    /*
     * Emit duration logging if appropriate.
     */
    switch (check_log_duration(msec_str, false)) {
        case 1:
            Assert(false);
            break;
        case 2:
            ereport(LOG,
                (errmsg("duration: %s ms, debug id %ld unique id %lu statement: %s", msec_str,  u_sess->debug_query_id, u_sess->slow_query_cxt.slow_query.unique_sql_id, "TODO: deparse plan"),  // vam query_string),
                    errhidestmt(true)
                    // vam errdetail_execute(parsetree_list)
                    ));
            break;
        default:
            break;
    }

    if (save_log_statement_stats)
        ShowUsage("QUERY STATISTICS");

    ereport(DEBUG2, (errmsg("exec_simple_plan() completed plan\n")));

    t_thrd.postgres_cxt.debug_query_string = NULL;
}

/*
 * @hdfs
 * spilt_querystring_sql_info
 *
 * when we got a hybirdmesage. We call this function to split query_string into two parts.
 * Sql_query_string and info_query_string, both of them will be set value in this function.
 * For example if we got a hybridmessage like "analyze table_name;information......" storing
 * in query_string, sql_query_string and info_query_string are set into "analyze table_name;"
 * and "information......", separately.
 */
static size_t split_querystring_sql_info(const char* query_string, char*& sql_query_string, char*& info_query_string)
{
    const char* position = query_string;
    int iLen = 0;
    uint32 queryStringLen = 0;
    errno_t errorno = EOK;

    /* Find the serialized Const in order to get queryStringLen. */
    position = strchr(query_string, '}');

    if (position == NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Could not find \'}\' in the query string.")));
    }

    iLen = position - query_string + 1;
    char* constNodeString = (char*)palloc0(iLen + 1);
    errorno = memcpy_s(constNodeString, iLen, query_string, iLen);
    securec_check(errorno, "\0", "\0");
    constNodeString[iLen] = '\0';
    Const* n = (Const*)stringToNode(constNodeString);
    if (n == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Invaild query string.")));
    }
    queryStringLen = DatumGetUInt32(n->constvalue);

    /* skip the end char '}' which serialized Const info of queryStringLen. */
    position += 1;

    if (strlen(position) <= queryStringLen) {
        ereport(ERROR,  (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Input queryStringLen is illegal.")));
    }

    /*
     * Get sql_query_string from query_string
     */
    sql_query_string = (char*)palloc(queryStringLen + 1);
    errorno = memcpy_s(sql_query_string, queryStringLen, position, queryStringLen);
    securec_check(errorno, "\0", "\0");
    sql_query_string[queryStringLen] = '\0';

    /*
     * Get info_query_string from query_string
     */
    position += queryStringLen;
    info_query_string = pstrdup(position);
    pfree(constNodeString);
    pfree(n);

    return size_t(position - query_string) + strlen(position);
}

/*
 * @hdfs
 * attach_info_to_plantree_list
 *
 * If we got a hybridmessage, the second part of the messaget is information string.
 * We use this function transfer information string to a struct whose type is related to
 * query string type. The struct will be add to plantree_list.
 */
static void attach_info_to_plantree_list(List* plantree_list, AttachInfoContext* attachInfoCtx)
{
#define READ_MEM_USAGE(type)                                                                                  \
    do {                                                                                                      \
        type* indexStmt = (type*)stmt;                                                                        \
        List* mem_list = (List*)stringToNode(info_query_string);                                              \
        if (unlikely(list_length(mem_list) != 2))                                                             \
            ereport(ERROR,                                                                                    \
                    (errmsg("unexpect list length %d of mem_list from info_query_string.",                    \
                     mem_list->length)));                                                                      \
        indexStmt->memUsage.work_mem = list_nth_int(mem_list, 0);                                             \
        indexStmt->memUsage.max_mem = list_nth_int(mem_list, 1);                                              \
        list_free(mem_list);                                                                                  \
        MEMCTL_LOG(DEBUG3, "DN receive (%d, %d)", indexStmt->memUsage.work_mem, indexStmt->memUsage.max_mem); \
    } while (0)

    ListCell* query_list = NULL;
    char* info_query_string = attachInfoCtx->info_query_string;

    foreach (query_list, plantree_list) {
        Node* stmt = (Node*)lfirst(query_list);

        switch (nodeTag(stmt)) {
            case T_VacuumStmt: {
                VacuumStmt* vacuumStmt = (VacuumStmt*)stmt;
                if (((unsigned int)vacuumStmt->options & VACOPT_ANALYZE) ||
                    ((unsigned int)vacuumStmt->options & VACOPT_FULL)) {
                    /*
                     * @hdfs
                     * We define HDFSTableAnalyze in hdfs_fdw.h. When we got a hybridmesage.
                     * If stmt has VACOPT_ANALYZE, we transfer info_query_string from string to
                     * HDFSTableAnalyz struct.
                     */
                    Node* analyzeNode = NULL;
                    analyzeNode = (Node*)stringToNode(info_query_string);
                    if (analyzeNode == NULL) {
                        ereport(ERROR,(errmsg("analyzeNode is null.")));
                    }

                    /*
                     * @hdfs
                     * Hybridmessage must come from a coordinator node and the length of
                     * HDFSNode->DnWorkFlow is 1. We are coordinator node or data node
                     * is not important when we got hybridmessage. If we are coordinator node,
                     * it means we must collect statistics information from some data node defined
                     * in HDFSNode->DnWorkFlow. If we are data node, it means a coordinator
                     * node told us to analyze some foreign hdfs table. We don't judge whether
                     * info_query_string belongs to me or not here. We do this in standard_ProcessUtility.
                     *
                     * We must notice that: HDFSNode->DnWorkFlow which is got from CN
                     * may be null in some situation.
                     */
                    if (IsConnFromCoord()) {
                        switch (nodeTag(analyzeNode)) {
                            case T_HDFSTableAnalyze: {
                                AnalyzeMode eAnalyzeMode;
                                HDFSTableAnalyze* HDFSNode = (HDFSTableAnalyze*)analyzeNode;

                                if (!HDFSNode->isHdfsStore)
                                    eAnalyzeMode = ANALYZENORMAL;

                                vacuumStmt->tmpSampleTblNameList = HDFSNode->tmpSampleTblNameList;
                                vacuumStmt->disttype = HDFSNode->disttype;

                                if (NULL == HDFSNode->DnWorkFlow) {
                                    vacuumStmt->HDFSDnWorkFlow = NULL;
                                    vacuumStmt->DnCnt = 0;
                                    vacuumStmt->nodeNo = 0;
                                    global_stats_set_samplerate(eAnalyzeMode, vacuumStmt, HDFSNode->sampleRate);
                                    vacuumStmt->sampleTableRequired = false;
                                } else {
                                    vacuumStmt->orgCnNodeNo = HDFSNode->orgCnNodeNo;
                                    vacuumStmt->DnCnt = HDFSNode->DnCnt;
                                    global_stats_set_samplerate(eAnalyzeMode, vacuumStmt, HDFSNode->sampleRate);
                                    vacuumStmt->sampleTableRequired = HDFSNode->sampleTableRequired;

                                    /* identify the analyze table is common table or hdfs table. */
                                    if (!HDFSNode->isHdfsForeignTbl) {
                                        vacuumStmt->HDFSDnWorkFlow = NULL;
                                    } else {
                                        /*
                                         * hdfs foreign table
                                         * we must set default node as -1, because the nodeNo of the dn1 is 0,
                                         * we can't discriminate if do analyze on dn1.
                                         */
                                        vacuumStmt->nodeNo = -1;

                                        /*
                                         * when we do global analyze for foreign table,
                                         * the list contain all files to each DN, we should get the dn task of local dn.
                                         */
                                        ListCell* splitToDnMapCell = NULL;

                                        foreach (splitToDnMapCell, HDFSNode->DnWorkFlow) {

                                            Node* data = (Node*)lfirst(splitToDnMapCell);
                                            if (IsA(data, SplitMap)) {
                                                SplitMap* dnTask = (SplitMap*)data;
                                                /* only get dn files for local dn. */
                                                if ((u_sess->pgxc_cxt.PGXCNodeId == dnTask->nodeId ||
                                                        LOCATOR_TYPE_REPLICATED == dnTask->locatorType) &&
                                                    NIL != dnTask->splits) {
                                                    vacuumStmt->HDFSDnWorkFlow = (void*)dnTask;
                                                    vacuumStmt->nodeNo = u_sess->pgxc_cxt.PGXCNodeId;
                                                    break;
                                                }
                                            } else {
                                                DistFdwDataNodeTask* dnTask = (DistFdwDataNodeTask*)data;
                                                /* only get dn files for local dn. */
                                                if (0 == pg_strcasecmp(dnTask->dnName,
                                                    g_instance.attr.attr_common.PGXCNodeName) &&
                                                    NIL != dnTask->task) {
                                                    vacuumStmt->HDFSDnWorkFlow = (void*)dnTask;
                                                    vacuumStmt->nodeNo = u_sess->pgxc_cxt.PGXCNodeId;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                                vacuumStmt->memUsage.work_mem = HDFSNode->memUsage.work_mem;
                                vacuumStmt->memUsage.max_mem = HDFSNode->memUsage.max_mem;
                                break;
                            }
                            case T_IntList: {
                                READ_MEM_USAGE(VacuumStmt);
                                break;
                            }
                            default:
                                break;
                        }
                    }
                }
                break;
            }
            case T_IndexStmt: {
                READ_MEM_USAGE(IndexStmt);
                break;
            }
            case T_ReindexStmt: {
                READ_MEM_USAGE(ReindexStmt);
                break;
            }
            case T_ClusterStmt: {
                READ_MEM_USAGE(ClusterStmt);
                break;
            }
            case T_CopyStmt: {
                READ_MEM_USAGE(CopyStmt);
                break;
            }
            case T_CreateSeqStmt: {
                Node* n = NULL;
                List* info_list = NULL;
                CreateSeqStmt* seqstmt = (CreateSeqStmt*)stmt;

                if (attachInfoCtx->info_node == NULL) {
                    n = (Node*)stringToNode(info_query_string);
                    if (n == NULL) {
                        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("node n is null.")));
                    }

                    if (IsA(n, Const)) {
                        Const* c = (Const*)n;
                        attachInfoCtx->info_node = (Node*)list_make1(c);
                    } else if (IsA(n, List)) {
                        attachInfoCtx->info_node = n;
                    } else {
                        ereport(ERROR,
                            (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Invaild UUID Message for CREATE SEQUENCE.")));
                    }
                }

                info_list = (List*)attachInfoCtx->info_node;

                if (attachInfoCtx->info_index < info_list->length) {
                    Const* c = (Const*)list_nth(info_list, attachInfoCtx->info_index);
                    seqstmt->uuid = DatumGetInt64(c->constvalue);

                    ereport(DEBUG2,
                        (errmodule(MOD_SEQ),
                            (errmsg("Create Sequence %s with UUID %ld ", seqstmt->sequence->relname, seqstmt->uuid))));
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("Not enough UUID (%d/%d) in CREATE SEQUENCE.",
                                info_list->length,
                                plantree_list->length)));
                }

                attachInfoCtx->info_index++;
                break;
            }
#ifdef ENABLE_MOT
            case T_CreateStmt:  /* fall through */
            case T_CreateForeignTableStmt:
#else
            case T_CreateStmt:
#endif
            {
                Node* l = (Node*)stringToNode(info_query_string);
                CreateStmt* cstmt = (CreateStmt*)stmt;

                if (IsA(l, List)) {
                    List* uuids = (List*)l;
                    cstmt->uuids = uuids;
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("Invaild UUID Message while CREATE SEQUENCE by serial.")));
                }
                break;
            }
            case T_CreateSchemaStmt: {
                Node* l = (Node*)stringToNode(info_query_string);
                CreateSchemaStmt* csstmt = (CreateSchemaStmt*)stmt;

                if (IsA(l, List)) {
                    List* uuids = (List*)l;
                    csstmt->uuids = uuids;
                } else {
                    ereport(
                        ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Invaild UUID Message while CREATE SCHEMA.")));
                }
                break;
            }
            default:
                ereport(WARNING, (errmsg("Wrong statment which should be without additional information.")));
                break;
        }
    }
}

void exec_init_poolhandles(void)
{
#ifdef ENABLE_MULTIPLE_NODES
    /* If this postmaster is launched from another Coord, do not initialize handles. skip it */
    if (IS_PGXC_COORDINATOR && IsPostmasterEnvironment) {
        ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;

        /* we use session memory context to remember all node info in this cluster. */
        MemoryContext old = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));

        t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(currentOwner, "ForPGXCNodes",
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));

        if (currentOwner != NULL) {
            InitMultinodeExecutor(false);
            if (!IsConnFromGTMTool() && !IsConnFromCoord() && !IsPoolHandle()) {
                PoolHandle* pool_handle = GetPoolManagerHandle();
                if (pool_handle == NULL) {
                    ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("Can not connect to pool manager")));
                    return;
                }

                /*
                 * We must switch to old memory context, if we use TopMemContext,
                 * that will cause memory leak when call elog(ERROR), because TopMemContext
                 * only is reset during thread exit. later we need refactor code section which
                 * allocate from TopMemContext for better. TopMemcontext is a session level memory
                 * context, we forbid allocate temp memory from TopMemcontext.
                 */
                MemoryContextSwitchTo(old);
                char* session_options_ptr = session_options();

                /* Pooler initialization has to be made before ressource is released */
                PoolManagerConnect(pool_handle,
                    u_sess->proc_cxt.MyProcPort->database_name,
                    u_sess->proc_cxt.MyProcPort->user_name,
                    session_options_ptr);
                if (session_options_ptr != NULL)
                    pfree(session_options_ptr);
                MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
            }
        } else {
            PG_TRY();
            {
                InitMultinodeExecutor(false);
                if (!IsConnFromGTMTool() && !IsConnFromCoord() && !IsPoolHandle()) {
                    PoolHandle* pool_handle = GetPoolManagerHandle();
                    if (pool_handle == NULL) {
                        ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("Can not connect to pool manager")));
                        PG_TRY_RETURN();
                    }

                    /*
                     * We must switch to old memory context, if we use TopMemContext,
                     * that will cause memory leak when call elog(ERROR), because TopMemContext
                     * only is reset during thread exit. later we need refactor code section which
                     * allocate from TopMemContext for better. TopMemcontext is a session level memory
                     * context, we forbid allocate temp memory from TopMemcontext.
                     */
                    MemoryContextSwitchTo(old);
                    char* session_options_ptr = session_options();

                    /* Pooler initialization has to be made before ressource is released */
                    PoolManagerConnect(pool_handle,
                        u_sess->proc_cxt.MyProcPort->database_name,
                        u_sess->proc_cxt.MyProcPort->user_name,
                        session_options_ptr);

                    if (session_options_ptr != NULL)
                        pfree(session_options_ptr);
                    MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
                }
            }
            PG_CATCH();
            {
                /* Report the error to the server log */
                EmitErrorReport();
                /*
                 * These operations are really just a minimal subset of
                 * AbortTransaction().	We don't have very many resources to worry
                 * about in checkpointer, but we do have LWLocks, buffers, and temp
                 * files.
                 */
                LWLockReleaseAll();
                AbortBufferIO();
                UnlockBuffers();

                ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
                AtEOXact_Buffers(false);
                /* If waiting, get off wait queue (should only be needed after error) */
                LockErrorCleanup();
                ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, false, true);
                ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, true);

                ResourceOwner newOwner = t_thrd.utils_cxt.CurrentResourceOwner;
                t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;

                ResourceOwnerDelete(newOwner);
                (void)MemoryContextSwitchTo(old);
                PG_RE_THROW();
            }
            PG_END_TRY();
        }

        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);

        ResourceOwner newOwner = t_thrd.utils_cxt.CurrentResourceOwner;
        t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;

        ResourceOwnerDelete(newOwner);

        (void)MemoryContextSwitchTo(old);
    }
#endif
}

bool IsRightRefState(List* plantreeList)
{
    if (!plantreeList || !list_length(plantreeList)) {
        return false;
    }

    ListCell* cell = list_head(plantreeList);
    if (!cell) {
        return false;
    }
    Node* node = (Node*)lfirst(cell);
    if (node && IsA(node, PlannedStmt)) {
        PlannedStmt* stmt = (PlannedStmt*) node;
        return stmt->planTree && IS_ENABLE_RIGHT_REF(stmt->planTree->rightRefState) && !stmt->is_flt_frame;
    }

    return false;
}

/*
 * exec_simple_query
 *
 * Execute a "simple Query" protocol message.
 *
 * @hdfs
 * Add default parameter unint16 messageType. Its default vaule is 0. If we receive
 * hybridmesage, this parameter will be set to 1 to tell us the normal query string
 * followed by information string. query_string = normal querystring + message.
 */
static void exec_simple_query(const char* query_string, MessageType messageType, StringInfo msg = NULL)
{
    OgRecordAutoController _local_opt(SRT2_SIMPLE_QUERY);
    CommandDest dest = (CommandDest)t_thrd.postgres_cxt.whereToSendOutput;
    MemoryContext oldcontext;
    MemoryContext OptimizerContext;
    AttachInfoContext attachInfoCtx = {0};
    List* parsetree_list = NULL;
    ListCell* parsetree_item = NULL;
    bool save_log_statement_stats = u_sess->attr.attr_common.log_statement_stats;
    bool was_logged = false;
    bool isTopLevel = false;
    char msec_str[PRINTF_DST_MAX];
    List* query_string_locationlist = NIL;
    int stmt_num = 0;
    size_t query_string_len = 0;
    char** query_string_single = NULL;
    bool is_multistmt = false;
    bool is_compl_sql = true;
    bool savedisAllowCommitRollback = false;
    bool needResetErrMsg = false;
    const char* querystringForLibpqsw = NULL;
    /*
     * @hdfs
     * When messageType is 1, we get hybridmessage. This message
     * can be splitted into two parts. sql_query_string will store normal
     * query string. info_query_string will store information string.
     */
    char* sql_query_string = NULL;
    char* info_query_string = NULL;
    u_sess->statement_cxt.last_row_count = u_sess->statement_cxt.current_row_count;
    u_sess->statement_cxt.current_row_count = -1;
#ifdef ENABLE_DISTRIBUTE_TEST
    if (IS_PGXC_COORDINATOR && IsConnFromCoord()) {
        if (TEST_STUB(NON_EXEC_CN_IS_DOWN, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("SUBXACT_TEST %s: non-exec cn is down.", g_instance.attr.attr_common.PGXCNodeName)));
        }

        /* white box test start */
        if (execute_whitebox(WHITEBOX_LOC, NULL, WHITEBOX_DEFAULT, DEFAULT_PROBABILITY)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("WHITE_BOX TEST %s: non-exec cn is down.", g_instance.attr.attr_common.PGXCNodeName)));
        }
    }
    /* white box test end */
#endif

    /*
     * Set query dop at the first beginning of a query.
     * Autonomous session will not support smp.
     */
    if (!u_sess->is_autonomous_session) {
        u_sess->opt_cxt.query_dop = u_sess->opt_cxt.query_dop_store;
        u_sess->opt_cxt.skew_strategy_opt = u_sess->attr.attr_sql.skew_strategy_store;
    } else {
        u_sess->opt_cxt.query_dop = 1;
    }

    /*
     * Rest PTFastQueryShippingStore.(We may set it in ExplainQuery routine).
     */
    PTFastQueryShippingStore = true;

    /*
     * Report query to various monitoring facilities.
     */
    t_thrd.explain_cxt.explain_perf_mode = u_sess->attr.attr_sql.guc_explain_perf_mode;

    plpgsql_estate = NULL;

    /*
     * @hdfs
     * 1. We compare meesageType with 1. When messageType is 1, it means
     * this message is a hybrid message. query_string is followed
     * by some information we must process. We split query_stirng into two
     * part,  normal query string and information string. Parser will parse
     * normal query string. Normal query string and information string is separated
     * by ";" in query_string, like this: "analyze table_name;information....."
     * 2. Do basic parsing of the query or queries (this should be safe even if
     * we are in aborted transaction state!
     */
    if (HYBRID_MESSAGE != messageType) {
        pgstat_report_activity(STATE_RUNNING, query_string);
        t_thrd.postgres_cxt.debug_query_string = query_string;
        query_string_len = strlen(query_string);
    } else {
        MemoryContext oldcontext_tmp = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
        query_string_len = split_querystring_sql_info(query_string, sql_query_string, info_query_string);
        attachInfoCtx.info_query_string = info_query_string;
        (void)MemoryContextSwitchTo(oldcontext_tmp);

        /* report original sql query string with no info query for analyze command. */
        pgstat_report_activity(STATE_RUNNING, sql_query_string);
        t_thrd.postgres_cxt.debug_query_string = sql_query_string;
    }

    resetErrorDataArea(true, false);
    instr_stmt_report_start_time();

    /*
     * Start up a transaction command.	All queries generated by the
     * query_string will be in this same command block, *unless* we find a
     * BEGIN/COMMIT/ABORT statement; we have to force a new xact command after
     * one of those, else bad things will happen in xact.c. (Note that this
     * will normally change current memory context.)
     */
    start_xact_command();

    if (ENABLE_WORKLOAD_CONTROL && SqlIsValid(query_string) && (IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        if (IsConnFromCoord())
            t_thrd.wlm_cxt.wlmalarm_dump_active = WLMIsDumpActive(query_string);
        else {
            u_sess->wlm_cxt->is_active_statements_reset = false;

            if (g_instance.wlm_cxt->dynamic_workload_inited) {
                dywlm_parallel_ready(query_string);
                dywlm_client_max_reserve();
            } else {
                WLMParctlReady(query_string);
                WLMParctlReserve(PARCTL_GLOBAL);
            }
        }
    }

    // Init pool handlers
    exec_init_poolhandles();

    TRACE_POSTGRESQL_QUERY_START(query_string);

    /*
     * We use save_log_statement_stats so ShowUsage doesn't report incorrect
     * results because ResetUsage wasn't called.
     */
    if (save_log_statement_stats)
        ResetUsage();

    /*
     * Zap any pre-existing unnamed statement.	(While not strictly necessary,
     * it seems best to define simple-Query mode as if it used the unnamed
     * statement and portal; this ensures we recover any storage used by prior
     * unnamed operations.)
     */
    drop_unnamed_stmt();

    /*
     * Switch to appropriate context for constructing parsetrees.
     */
    oldcontext = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

    if (HYBRID_MESSAGE == messageType) {
        parsetree_list = pg_parse_query(sql_query_string);
    } else {
        if (copy_need_to_be_reparse != NULL && g_instance.status == NoShutdown) {
            bool reparse_query = false;
            gs_stl::gs_string reparsed_query;
            do {
                parsetree_list = pg_parse_query(reparsed_query.empty() ?
                                                query_string : reparsed_query.c_str(), &query_string_locationlist);
                reparse_query = copy_need_to_be_reparse(parsetree_list, query_string, reparsed_query);
            } while (reparse_query);
        } else {
            parsetree_list = pg_parse_query(query_string, &query_string_locationlist);
        }
    }

    /* Log immediately if dictated by log_statement */
    if (check_log_statement(parsetree_list)) {
        char* mask_string = NULL;

        mask_string = maskPassword(query_string);
        if (NULL == mask_string) {
            mask_string = (char*)query_string;
        }

        ereport(LOG, (errmsg("statement: %s", mask_string), errhidestmt(true), errdetail_execute(parsetree_list)));
        if (mask_string != query_string)
            pfree(mask_string);
        was_logged = true;
    }

    /* Light proxy is only enabled for single query from 'Q' message */
    bool runLightProxyCheck = (msg != NULL) && IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
        u_sess->attr.attr_sql.enable_light_proxy &&
        (list_length(parsetree_list) == 1) && (query_string_len < SECUREC_MEM_MAX_LEN);

    bool runOpfusionCheck = (msg != NULL) && IS_PGXC_DATANODE && u_sess->attr.attr_sql.enable_opfusion &&
        (list_length(parsetree_list) == 1) && (query_string_len < SECUREC_MEM_MAX_LEN);

    /*
     * Switch back to transaction context to enter the loop.
     */
    MemoryContextSwitchTo(oldcontext);

    /*
     * Before executor, check the status of password.
     */
    if (!t_thrd.postgres_cxt.password_changed) {
        ForceModifyInitialPwd((const char*)query_string, parsetree_list);
    }

    /*
     * Read password status from pg_user_status. If is in aborted block, don't check.
     */
    Oid current_user = GetUserId();
    if (current_user != BOOTSTRAP_SUPERUSERID) {
        if (!IsAbortedTransactionBlockState() && GetAccountPasswordExpired(current_user) == EXPIRED_STATUS) {
            ForceModifyExpiredPwd((const char*)query_string, parsetree_list);
        }
    }
    /*
     * We'll tell PortalRun it's a top-level command iff there's exactly one
     * raw parsetree.  If more than one, it's effectively a transaction block
     * and we want PreventTransactionChain to reject unsafe commands. (Note:
     * we're assuming that query rewrite cannot add commands that are
     * significant to PreventTransactionChain.)
     */
    isTopLevel = (list_length(parsetree_list) == 1);
    stp_retore_old_xact_stmt_state(true);

    if (!isTopLevel)
        t_thrd.explain_cxt.explain_perf_mode = EXPLAIN_NORMAL;

    /* Apply for a new memory context for analyze and rewrite as well as pg_plan_queries */
    OptimizerContext = AllocSetContextCreate(t_thrd.mem_cxt.msg_mem_cxt,
        "OptimizerContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    bool is_multi_query_text = false;
    /* When the query is multi query like "begin;...end;" , we need update query statement for WLM collect Info */
    if (t_thrd.wlm_cxt.collect_info->sdetail.statement != NULL && list_length(parsetree_list) > 1) {
        is_multi_query_text = true;
    }

    /* just for cooperation analysis. */
    if (IS_PGXC_COORDINATOR && u_sess->pgxc_cxt.is_gc_fdw) {
        u_sess->attr.attr_sql.enable_stream_operator = false;
        u_sess->opt_cxt.qrw_inlist2join_optmode = QRW_INLIST2JOIN_DISABLE;
    }

    /* reset unique sql start time */
    u_sess->unique_sql_cxt.unique_sql_start_time = 0;

    /*
     * Run through the raw parsetree(s) and process each one.
     */
    foreach (parsetree_item, parsetree_list) {
        instr_unique_sql_handle_multi_sql(parsetree_item == list_head(parsetree_list));
        Node* parsetree = (Node*)lfirst(parsetree_item);
        bool snapshot_set = false;
        const char* commandTag = NULL;
        char completionTag[COMPLETION_TAG_BUFSIZE];
        List* querytree_list = NULL;
        List* plantree_list = NULL;
        Portal portal = NULL;
        DestReceiver* receiver = NULL;
        int16 format;
        char* randomPlanInfo = NULL;

        /* Reset the single_shard_stmt flag */
        u_sess->exec_cxt.single_shard_stmt = false;

        /*
         * When dealing with a multi-query, get the snippets of each single querys through
         * get_next_snippet which cut the multi-query by query_string_locationlist.
         */
        if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && PointerIsValid(query_string_locationlist) &&
            list_length(query_string_locationlist) > 1) {
            oldcontext = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
            is_multistmt = true;
            query_string_single =
                get_next_snippet(query_string_single, query_string, query_string_locationlist, &stmt_num);
            (void)MemoryContextSwitchTo(oldcontext);

            /* INSTR: for multi-query case, unique sql needs to use every single sql
             * to generate unique sql id & string */
            if (is_unique_sql_enabled() && !IsConnFromCoord() && query_string_single != NULL) {
                u_sess->unique_sql_cxt.is_multi_unique_sql = true;
                u_sess->unique_sql_cxt.curr_single_unique_sql = query_string_single[stmt_num - 1];
                if (stmt_num > 1 && stmt_num <= list_length(query_string_locationlist)) {
                    u_sess->unique_sql_cxt.multi_sql_offset =
                        list_nth_int(query_string_locationlist, stmt_num - 2) + 1;
                }
            }
        }

#ifdef ENABLE_MULTIPLE_NODES

        /*
         * By default we do not want Datanodes or client Coordinators to contact GTM directly,
         * it should get this information passed down to it.
         */
        if (IS_PGXC_DATANODE || IsConnFromCoord())
            SetForceXidFromGTM(false);
#endif

        /*
         * Get the command name for use in status display (it also becomes the
         * default completion tag, down inside PortalRun).	Set ps_status and
         * do any special start-of-SQL-command processing needed by the
         * destination.
         */
        commandTag = CreateCommandTag(parsetree);
        t_thrd.postgres_cxt.cur_command_tag = transform_node_tag(parsetree);

        set_ps_display(commandTag, false);
        if (libpqsw_skip_check_readonly()) {
            if (is_multistmt) {
                querystringForLibpqsw = query_string_single[stmt_num - 1];
            } else {
                querystringForLibpqsw = query_string;
            }
            // create table as / select into / insert into
            if (nodeTag(parsetree) == T_CreateTableAsStmt
                || ((nodeTag(parsetree) == T_SelectStmt) && ((SelectStmt*)parsetree)->intoClause != NULL)
                ) {
                libpqsw_set_redirect(true);
            }

        }

        if (libpqsw_get_redirect()) {
            if (libpqsw_process_query_message(commandTag, NULL, querystringForLibpqsw, is_multistmt, lnext(parsetree_item) == NULL)) {
                libpqsw_trace_q_msg(commandTag, querystringForLibpqsw);
                if (snapshot_set) {
                    PopActiveSnapshot();
                }
                finish_xact_command();
                continue;
            }
        }

        BeginCommand(commandTag, dest);

        /*
         * If we are in an aborted transaction, reject all commands except
         * COMMIT/ABORT.  It is important that this test occur before we try
         * to do parse analysis, rewrite, or planning, since all those phases
         * try to do database accesses, which may fail in abort state. (It
         * might be safe to allow some additional utility commands in this
         * state, but not many...)
         */
        if (IsAbortedTransactionBlockState() && !IsTransactionExitStmt(parsetree))
            ereport(ERROR,
                (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                    errmsg("current transaction is aborted, "
                        "commands ignored until end of transaction block, firstChar[%c]",
                        u_sess->proc_cxt.firstChar),
                    errdetail_abort()));

        /* Make sure we are in a transaction command */
        start_xact_command();

        /* If we got a cancel signal in parsing or prior command, quit */
        CHECK_FOR_INTERRUPTS();

        // We can't load system cache when transaction is TBLOCK_ABORT
        // even if it is 'commit/rollback'
        //
        u_sess->wlm_cxt->wlm_num_streams = 0;

        /*
         * Set up a snapshot if parse analysis/planning will need one.
         */
        if (analyze_requires_snapshot(parsetree)) {
            PushActiveSnapshot(GetTransactionSnapshot());
            snapshot_set = true;
        }

        /*
         * Before going into planner, set default work mode.
         */
        set_default_stream();

        /*
         * OK to analyze, rewrite, and plan this query.
         *
         * Switch to appropriate context for constructing querytrees (again,
         * these must outlive the execution context).
         */
        oldcontext = MemoryContextSwitchTo(OptimizerContext);

        /* 
         * sqladvisor check if it can be collected 
         * select into... only can be checked in parsetree, it will transfrom to insert into after rewrite.
         */
        bool isCollect = checkAdivsorState() && checkParsetreeTag(parsetree);

        /* Does not allow commit in pre setting scenario */
        is_compl_sql = CheckElementParsetreeTag(parsetree);
        if (is_compl_sql) {
            needResetErrMsg = stp_disable_xact_and_set_err_msg(&savedisAllowCommitRollback, STP_XACT_COMPL_SQL);
        }
        /*
         * @hdfs
         * If we received a hybridmessage, we use sql_query_string to analyze and rewrite.
         */
        if (HYBRID_MESSAGE != messageType)
            querytree_list = pg_analyze_and_rewrite(parsetree, query_string, NULL, 0);
        else
            querytree_list = pg_analyze_and_rewrite(parsetree, sql_query_string, NULL, 0);

        isCollect = checkCollectSimpleQuery(isCollect, querytree_list);
        if (quickPlanner(querytree_list, parsetree, query_string, dest, completionTag)) {
            CommandCounterIncrement();
            if (snapshot_set != false)
                PopActiveSnapshot();
            finish_xact_command();
            EndCommand(completionTag, dest);
            MemoryContextReset(OptimizerContext);
            break;
        }
#ifdef ENABLE_MOT
        /* check cross engine queries and transactions violation for MOT */
        StorageEngineType storageEngineType = SE_TYPE_UNSPECIFIED;
        if (querytree_list) {
            CheckTablesStorageEngine((Query*)linitial(querytree_list), &storageEngineType);
        }
        SetCurrentTransactionStorageEngine(storageEngineType);

        /* block MOT engine queries in sub-transactions */
        if (!IsTransactionExitStmt(parsetree) && IsMOTEngineUsedInParentTransaction() && IsMOTEngineUsed()) {
            ereport(ERROR, (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED), errmodule(MOD_MOT),
                    errmsg("SubTransaction is not supported for memory table")));
        }

        if (IsTransactionPrepareStmt(parsetree) && (IsMOTEngineUsed() || IsMixedEngineUsed())) {
            /* Explicit prepare transaction is not supported for memory table */
            ereport(ERROR, (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED), errmodule(MOD_MOT),
                    errmsg("Explicit prepare transaction is not supported for memory table")));
        }
#endif
        
        /* Try using light proxy to execute query */
        if (runLightProxyCheck &&
            exec_query_through_light_proxy(querytree_list, parsetree, snapshot_set, msg, OptimizerContext)) {
            (void)MemoryContextSwitchTo(oldcontext);

            /* sqladvisor collect query */
            collectSimpleQuery(query_string, isCollect);
            break;
        }

        // Mixed statement judgments
        if (libpqsw_process_query_message(commandTag, querytree_list, querystringForLibpqsw, is_multistmt, lnext(parsetree_item) == NULL)) {
            libpqsw_trace_q_msg(commandTag, querystringForLibpqsw);
            if (libpqsw_begin_command(commandTag) || libpqsw_end_command(commandTag)) {
                libpqsw_trace("libpq send sql at my side as well:%s", commandTag);
            } else {
                if (snapshot_set != false) {
                    PopActiveSnapshot();
                }
                CommandCounterIncrement();
                finish_xact_command();
                MemoryContextReset(OptimizerContext);
                continue;
            }
        }

        plantree_list = pg_plan_queries(querytree_list, CURSOR_OPT_SPQ_OK, NULL);

        /* sqladvisor collect query */
        collectSimpleQuery(query_string, isCollect);

        randomPlanInfo = get_random_plan_string();
        if (was_logged != false && randomPlanInfo != NULL) {
            ereport(LOG, (errmsg("%s", randomPlanInfo), errhidestmt(true)));
            pfree(randomPlanInfo);
        }

        (void)MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

        /*
         * @hdfs
         * If we received a hybridmessage, we attach additional information to plantree_list's node.
         * Additional information is a string which can be recovered into a struct by call string to node.
         */
        if (HYBRID_MESSAGE == messageType)
            attach_info_to_plantree_list(plantree_list, &attachInfoCtx);

        /* Done with the snapshot used for parsing/planning */
        if (snapshot_set != false)
            PopActiveSnapshot();

        /* If we got a cancel signal in analysis or planning, quit */
        CHECK_FOR_INTERRUPTS();

#ifdef PGXC
        /* PGXC_DATANODE */

        /* Flag to indicate whether vacuum needs getting Xid from GTM */
        bool vacuumForceXid = false;

        /* If the parsetree is a vacuum statement */
        if (IsA(parsetree, VacuumStmt)) {
            /* copy the parsetree */
            VacuumStmt* vacstmt = (VacuumStmt*)parsetree;

            /* Initially, vacuum forces getting Xid from GTM */
            vacuumForceXid = true;

            /* If vaccum(analyze) one relation, don't force getting xid from GTM. Instead, use Xid sent down by CN */
            if (!((uint32)vacstmt->options & VACOPT_VACUUM) && vacstmt->relation)
                vacuumForceXid = false;
        }

        /* Force getting Xid from GTM if neither an autovacuum nor a vacuum(analyze) to one relation */
        if ((IS_PGXC_DATANODE || IsConnFromCoord()) && (vacuumForceXid || IsA(parsetree, ClusterStmt)) &&
            IsPostmasterEnvironment)
            SetForceXidFromGTM(true);

        /* Commands like reindex database ..., coordinator don't send snapshot down, need to get from GTM. */
        if ((IS_PGXC_DATANODE || IsConnFromCoord()) && IsA(parsetree, ReindexStmt) &&
            ((ReindexStmt*)parsetree)->kind == OBJECT_DATABASE && IsPostmasterEnvironment)
            SetForceXidFromGTM(true);
#endif
        if (!u_sess->attr.attr_storage.phony_autocommit) {
            BeginTxnForAutoCommitOff();
        }
        /* SQL bypass */
        if (runOpfusionCheck && !IsRightRefState(plantree_list)) {
            (void)MemoryContextSwitchTo(oldcontext);
            void* opFusionObj = OpFusion::FusionFactory(
                OpFusion::getFusionType(NULL, NULL, plantree_list), oldcontext, NULL, plantree_list, NULL);
            if (opFusionObj != NULL) {
                ((OpFusion*)opFusionObj)->setCurrentOpFusionObj((OpFusion*)opFusionObj);
                if (OpFusion::process(FUSION_EXECUTE, NULL, completionTag, isTopLevel, NULL)) {
                    CommandCounterIncrement();
                    finish_xact_command();
                    EndCommand(completionTag, dest);
                    MemoryContextReset(OptimizerContext);
                    break;
                }
                Assert(0);
            }
            (void)MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
        }
        /*
         * Create unnamed portal to run the query or queries in. If there
         * already is one, silently drop it.
         */
        portal = CreatePortal("", true, true);
        /* Don't display the portal in pg_cursors */
        portal->visible = false;

        /*
         * We don't have to copy anything into the portal, because everything
         * we are passing here is in t_thrd.mem_cxt.msg_mem_cxt, which will outlive the
         * portal anyway. If we received a hybridmesage, we send sql_query_string
         * to PortalDefineQuery as the original query string.
         */
        if (HYBRID_MESSAGE != messageType) {
            if (is_multistmt && (IsConnFromApp() || IsConnFromInternalTool())) {
                PortalDefineQuery(portal, NULL, query_string_single[stmt_num - 1], commandTag, plantree_list, NULL);
            } else
                PortalDefineQuery(portal, NULL, query_string, commandTag, plantree_list, NULL);
        } else {
            PortalDefineQuery(portal, NULL, sql_query_string, commandTag, plantree_list, NULL);
        }

        /* PortalRun will reset this flag */
        portal->nextval_default_expr_type = u_sess->opt_cxt.nextval_default_expr_type;


        if (ENABLE_WORKLOAD_CONTROL && IS_PGXC_COORDINATOR && is_multi_query_text) {
            if (t_thrd.wlm_cxt.collect_info->sdetail.statement) {
                pfree_ext(t_thrd.wlm_cxt.collect_info->sdetail.statement);
            }

            AutoContextSwitch memSwitch(t_thrd.wlm_cxt.query_resource_track_mcxt);
            /* g_collectInfo.sdetail.statement will be free in WLMReleaseStmtDetailItem() */
            if (strlen(portal->sourceText) < 8 * KBYTES) {
                t_thrd.wlm_cxt.collect_info->sdetail.statement = pstrdup(portal->sourceText);
            } else {
                t_thrd.wlm_cxt.collect_info->sdetail.statement = (char*)palloc0(8 * KBYTES);
                errno_t rc = strncpy_s(
                    t_thrd.wlm_cxt.collect_info->sdetail.statement, 8 * KBYTES, portal->sourceText, 8 * KBYTES - 1);
                securec_check(rc, "\0", "\0");
            }
        }

        /*
         * Start the portal.  No parameters here.
         */
        PortalStart(portal, NULL, 0, InvalidSnapshot);

        /*
         * Select the appropriate output format: text unless we are doing a
         * FETCH from a binary cursor.	(Pretty grotty to have to do this here
         * --- but it avoids grottiness in other places.  Ah, the joys of
         * backward compatibility...)
         */
        format = 0; /* TEXT is default */
        if (IsA(parsetree, FetchStmt)) {
            FetchStmt* stmt = (FetchStmt*)parsetree;

            if (!stmt->ismove) {
                Portal fportal = GetPortalByName(stmt->portalname);

                if (PortalIsValid(fportal) && ((uint32)fportal->cursorOptions & CURSOR_OPT_BINARY))
                    format = 1; /* BINARY */
            }
        }
        PortalSetResultFormat(portal, 1, &format);

        /*
         * Now we can create the destination receiver object.
         */
        receiver = CreateDestReceiver(dest);
        if (dest == DestRemote)
            SetRemoteDestReceiverParams(receiver, portal);

        /*
         * Switch back to transaction context for execution.
         */
        (void)MemoryContextSwitchTo(oldcontext);

        if (u_sess->attr.attr_resource.use_workload_manager && g_instance.wlm_cxt->gscgroup_init_done &&
            !IsAbortedTransactionBlockState()) {
            u_sess->wlm_cxt->cgroup_last_stmt = u_sess->wlm_cxt->cgroup_stmt;
            u_sess->wlm_cxt->cgroup_stmt = WLMIsSpecialCommand(parsetree, portal);
        }

        /*
         * Run the portal to completion, and then drop it (and the receiver).
         */
        (void)PortalRun(portal, FETCH_ALL, isTopLevel, receiver, receiver, completionTag);

        (*receiver->rDestroy)(receiver);

        PortalDrop(portal, false);

        CleanHotkeyCandidates(true);

        /* restore is_allow_commit_rollback */
        if (is_compl_sql) {
            stp_reset_xact_state_and_err_msg(savedisAllowCommitRollback, needResetErrMsg);
        }

        if (IsA(parsetree, TransactionStmt)) {
            /*
             * If this was a transaction control statement, commit it. We will
             * start a new xact command for the next command (if any).
             */
            finish_xact_command();
        } else if (lnext(parsetree_item) == NULL) {
            /*
             * If this is the last parsetree of the query string, close down
             * transaction statement before reporting command-complete.  This
             * is so that any end-of-transaction errors are reported before
             * the command-complete message is issued, to avoid confusing
             * clients who will expect either a command-complete message or an
             * error, not one and then the other.  But for compatibility with
             * historical openGauss behavior, we do not force a transaction
             * boundary between queries appearing in a single query string.
             */
            finish_xact_command();
        } else {
            /*
             * We need a CommandCounterIncrement after every query, except
             * those that start or end a transaction block.
             */
            CommandCounterIncrement();
        }

        /*
         * Tell client that we're done with this query.  Note we emit exactly
         * one EndCommand report for each raw parsetree, thus one for each SQL
         * command the client sent, regardless of rewriting. (But a command
         * aborted by error will not send an EndCommand report at all.)
         */
        EndCommand(completionTag, dest);
        MemoryContextReset(OptimizerContext);
    }
    /* end loop over parsetrees */

    /* Reset the single_shard_stmt flag */
    u_sess->exec_cxt.single_shard_stmt = false;

    MemoryContextDelete(OptimizerContext);

    /*
     * Close down transaction statement, if one is open.
     */
    finish_xact_command();

    /*
     * If there were no parsetrees, return EmptyQueryResponse message.
     */
    if (parsetree_list == NULL)
        NullCommand(dest);

    /* release global active counts */
    if (ENABLE_WORKLOAD_CONTROL) {
        if (g_instance.wlm_cxt->dynamic_workload_inited) {
            if (t_thrd.wlm_cxt.parctl_state.simple == 0)
                dywlm_client_release(&t_thrd.wlm_cxt.parctl_state);
            else
                WLMReleaseGroupActiveStatement();
            dywlm_client_max_release(&t_thrd.wlm_cxt.parctl_state);
        } else {
            WLMParctlRelease(&t_thrd.wlm_cxt.parctl_state);
        }
        WLMSetCollectInfoStatusFinish();
    }

    /* Reset hint flag */
    u_sess->parser_cxt.has_hintwarning = false;

    /*
     * Emit duration logging if appropriate.
     */
    switch (check_log_duration(msec_str, was_logged)) {
        case 1:
            ereport(LOG, (errmsg("duration: %s ms, queryid %ld, unique id %lu", msec_str,
                u_sess->debug_query_id, u_sess->slow_query_cxt.slow_query.unique_sql_id), errhidestmt(true)));
            break;
        case 2: {
            char* mask_string = NULL;

            MASK_PASSWORD_START(mask_string, query_string);
            ereport(LOG, (errmsg("duration: %s ms queryid %ld unique id %ld statement: %s", msec_str,
                u_sess->debug_query_id, u_sess->slow_query_cxt.slow_query.unique_sql_id, mask_string),
                    errhidestmt(true), errdetail_execute(parsetree_list)));
            MASK_PASSWORD_END(mask_string, query_string);
            break;
        }
        default:
            break;
    }

    if (save_log_statement_stats)
        ShowUsage("QUERY STATISTICS");

    TRACE_POSTGRESQL_QUERY_DONE(query_string);

    t_thrd.postgres_cxt.debug_query_string = NULL;
    t_thrd.postgres_cxt.cur_command_tag = T_Invalid;

    /*
     * @hdfs
     * If we received a hybridmesage, we applied additional memory. At the end of
     * exec_simple_query, we free them.
     */
    if (HYBRID_MESSAGE == messageType) {
        pfree_ext(info_query_string);
        pfree_ext(sql_query_string);
    }

    /* Free the memory of query_string_single malloced in get_next_snippet. */
    if (is_multistmt) {
        for (int i = 0; i < stmt_num; i++) {
            pfree(query_string_single[i]);
            query_string_single[i] = NULL;
        }
        pfree(query_string_single);
    }
}

#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)
/*
 * exec_plan_with_params
 *
 * Execute a "extend plan with params" received from coordinator
 */
static void exec_plan_with_params(StringInfo input_message)
{
    CommandDest dest = (CommandDest)t_thrd.postgres_cxt.whereToSendOutput;
    bool save_log_statement_stats = u_sess->attr.attr_common.log_statement_stats;

    PlannedStmt* planstmt = NULL;
    Node* node = NULL;

    int numParams;
    Oid* paramTypes = NULL;
    char** paramTypeNames = NULL;

    int numPFormats;
    int16* pformats = NULL;
    int numRFormats;
    int16* rformats = NULL;

    ParamListInfo params;
    long max_rows;

    if (save_log_statement_stats)
        ResetUsage();

    /* "IS_PGXC_DATANODE && StreamTopConsumerAmI()" means on the CN of the compute pool. */
    if (IS_PGXC_COORDINATOR && StreamTopConsumerAmI())
        exec_init_poolhandles();

    start_xact_command();

    /* Switch back to message context */
    MemoryContext oldcontext = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

    /* get plan */
    const char* plan_string = pq_getmsgstring(input_message);
    if (strlen(plan_string) > SECUREC_MEM_MAX_LEN) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Too long plan_string.")));
    }
    node = (Node*)stringToNode((char*)plan_string);
    if (node != NULL && !IsA(node, PlannedStmt)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("Received unexpected node type.")));
    }

    planstmt = (PlannedStmt*) node;

    InitGlobalNodeDefinition(planstmt);

    if (planstmt == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Invaild parameter.")));
    }

    ChangeDMLPlanForRONode(planstmt);

    if (ThreadIsDummy(planstmt->planTree)) {
        u_sess->stream_cxt.dummy_thread = true;
        u_sess->exec_cxt.executorStopFlag = true;
    }

    /* nodeid in planstmt->planTree->exec_nodes->nodeList is from DWS,
     * and PGXCNodeId is from the compute pool. so ignore the result of ThreadIsDummy()
     * and set u_sess->exec_cxt.executor_stop_flag and u_sess->stream_cxt.dummy_thread to proper value.
     */
    if (planstmt->in_compute_pool) {
        u_sess->stream_cxt.dummy_thread = false;
        u_sess->exec_cxt.executorStopFlag = false;
    }

    /* get parameter numbers */
    numParams = pq_getmsgint(input_message, 2);

    /* get parameter types */
    paramTypes = (Oid*)palloc(numParams * sizeof(Oid));
    if (numParams > 0) {
        int i;
        paramTypeNames = (char**)palloc(numParams * sizeof(char*));
        for (i = 0; i < numParams; i++)
            paramTypeNames[i] = (char*)pq_getmsgstring(input_message);
    }

    if (paramTypeNames != NULL) {
        if (IS_SPQ_EXECUTOR || IsConnFromCoord() || (IS_PGXC_COORDINATOR && planstmt->in_compute_pool)) {
            int cnt_param;
            for (cnt_param = 0; cnt_param < numParams; cnt_param++)
                parseTypeString(paramTypeNames[cnt_param], &paramTypes[cnt_param], NULL);
        }
    }

    /* Get the parameter format codes */
    numPFormats = pq_getmsgint(input_message, 2);
    if (numPFormats > 0) {
        int i;

        pformats = (int16*)palloc(numPFormats * sizeof(int16));
        for (i = 0; i < numPFormats; i++)
            pformats[i] = pq_getmsgint(input_message, 2);
    }

    /* Get the parameter value count */
    numParams = pq_getmsgint(input_message, 2);

    if (numPFormats > 1 && numPFormats != numParams)
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("bind message has %d parameter formats but %d parameters", numPFormats, numParams)));

    /* Get the parameter value */
    if (numParams > 0) {
        int paramno;
        Oid param_collation = GetCollationConnection();
        int param_charset = GetCharsetConnection();

        params = (ParamListInfo)palloc(offsetof(ParamListInfoData, params) + numParams * sizeof(ParamExternData));

        /* we have static list of params, so no hooks needed */
        params->paramFetch = NULL;
        params->paramFetchArg = NULL;
        params->parserSetup = NULL;
        params->parserSetupArg = NULL;
        params->params_need_process = false;
        params->uParamInfo = DEFUALT_INFO;
        params->params_lazy_bind = false;
        params->numParams = numParams;

        for (paramno = 0; paramno < numParams; paramno++) {
            Oid ptype = paramTypes[paramno];
            int32 plength;
            Datum pval = 0;
            bool isNull = false;
            StringInfoData pbuf;
            char csave;
            int16 pformat;

            plength = pq_getmsgint(input_message, 4);
            isNull = (plength == -1);

            if (!isNull) {
                const char* pvalue = pq_getmsgbytes(input_message, plength);

                /*
                 * Rather than copying data around, we just set up a phony
                 * StringInfo pointing to the correct portion of the message
                 * buffer.	We assume we can scribble on the message buffer so
                 * as to maintain the convention that StringInfos have a
                 * trailing null.  This is grotty but is a big win when
                 * dealing with very large parameter strings.
                 */
                pbuf.data = (char*)pvalue;
                pbuf.maxlen = plength + 1;
                pbuf.len = plength;
                pbuf.cursor = 0;

                csave = pbuf.data[plength];
                pbuf.data[plength] = '\0';
            } else {
                pbuf.data = NULL; /* keep compiler quiet */
                csave = 0;
            }

            if (numPFormats > 1) {
                Assert(NULL != pformats);
                pformat = pformats[paramno];
            } else if (numPFormats > 0) {
                Assert(NULL != pformats);
                pformat = pformats[0];
            } else {
                pformat = 0; /* default = text */
            }

            if (pformat == 0) {
                /* text mode */
                Oid typinput;
                Oid typioparam;
                char* pstring = NULL;

                getTypeInputInfo(ptype, &typinput, &typioparam);

                /*
                 * We have to do encoding conversion before calling the
                 * typinput routine.
                 */
                if (isNull) {
                    pstring = NULL;
                } else if (OidIsValid(param_collation) && IsSupportCharsetType(ptype)) {
                    pstring = pg_client_to_any(pbuf.data, plength, param_charset);
                } else {
                    pstring = pg_client_to_server(pbuf.data, plength);
                }

                pval = OidInputFunctionCall(typinput, pstring, typioparam, -1);

                /* Free result of encoding conversion, if any */
                if (pstring != NULL && pstring != pbuf.data) {
                    pfree(pstring);
                }
            } else {
                ereport(
                    ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unsupported format code: %d", pformat)));
            }

            /* Restore message buffer contents */
            if (!isNull) {
                pbuf.data[plength] = csave;
            }

            params->params[paramno].value = pval;
            params->params[paramno].isnull = isNull;

            /*
             * We mark the params as CONST.  This has no effect if we already
             * did planning, but if we didn't, it licenses the planner to
             * substitute the parameters directly into the one-shot plan we
             * will generate below.
             */
            params->params[paramno].pflags = PARAM_FLAG_CONST;
            params->params[paramno].ptype = ptype;
            params->params[paramno].tabInfo = NULL;
        }
    } else
        params = NULL;

    /* Get the result format codes */
    numRFormats = pq_getmsgint(input_message, 2);
    if (numRFormats != 0) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid result format codes: %d.", numRFormats)));
    }

    /* get the fetch size */
    max_rows = pq_getmsgint(input_message, 4);

    /* End of message */
    pq_getmsgend(input_message);

    {
        const char* commandTag = NULL;
        char completionTag[COMPLETION_TAG_BUFSIZE];
        Portal portal;
        DestReceiver* receiver = NULL;

        SetForceXidFromGTM(false);

        commandTag = "SELECT";

        set_ps_display(commandTag, false);

        BeginCommand(commandTag, dest);

        if (IsAbortedTransactionBlockState()) {
            ereport(ERROR,
                (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                    errmsg("current transaction is aborted, "
                        "commands ignored until end of transaction block, firstChar[%c]",
                        u_sess->proc_cxt.firstChar),
                    errdetail_abort()));
        }

        /* Make sure we are in a transaction command */
        start_xact_command();

        /* If we got a cancel signal in parsing or prior command, quit */
        CHECK_FOR_INTERRUPTS();

        GSCGROUP_ATTACH_TASK();

        MemoryContextSwitchTo(oldcontext);

        portal = CreatePortal("", true, true);

        /* Don't display the portal in pg_cursors */
        portal->visible = false;

        /*
         * We don't have to copy anything into the portal, because everything
         * we are passing here is in t_thrd.mem_cxt.msg_mem_cxt, which will outlive the
         * portal anyway.
         */
        PortalDefineQuery(portal, NULL, "DUMMY", commandTag, lappend(NULL, planstmt), NULL);

        PortalStart(portal, params, 0, u_sess->spq_cxt.snapshot);

        PortalSetResultFormat(portal, numRFormats, rformats);

        /*
         * Now we can create the destination receiver object.
         */
        receiver = CreateDestReceiver(dest);
        if (dest == DestRemote) {
            SetRemoteDestReceiverParams(receiver, portal);
        }
#ifdef USE_SPQ
        if (IS_SPQ_RUNNING && dest == DestRemote)
            SetRemoteDestTupleReceiverParams(receiver);
#endif

        if (max_rows <= 0) {
            max_rows = FETCH_ALL;
        }

        (void)PortalRun(portal, max_rows, true, receiver, receiver, completionTag);

        (*receiver->rDestroy)(receiver);

        PortalDrop(portal, false);

        /* Flush messages left in PqSendBuffer before entering syncQuit. */
        (void)pq_flush();

        /* Set sync point for waiting all stream threads complete. */
        StreamNodeGroup::syncQuit(STREAM_COMPLETE);
        UnRegisterStreamSnapshots();

        /*
         * send the stream instrumentation to the coordinator until all the stream thread quit.
         */
        if (StreamTopConsumerAmI() && u_sess->instr_cxt.global_instr != NULL) {
            u_sess->instr_cxt.global_instr->serializeSend();
            if (u_sess->instr_cxt.global_instr->needTrack()) {
                u_sess->instr_cxt.global_instr->serializeSendTrack();
            }
        }

        if (StreamTopConsumerAmI() && u_sess->instr_cxt.obs_instr != NULL) {
            u_sess->instr_cxt.obs_instr->serializeSend();
        }

        finish_xact_command();
        EndCommand(completionTag, dest);
    }

    finish_xact_command();
}
#endif

#ifdef ENABLE_MULTIPLE_NODES
/* get param oid from paramTypeNames, and save into paramTypes. */
static void GetParamOidFromName(char** paramTypeNames, Oid* paramTypes, int numParams)
{
    // It can't be commit/rollback xact if numParams > 0
    // And if transaction is abort state, we should abort query
    //
    if (IsAbortedTransactionBlockState() && numParams > 0) {
        ereport(ERROR, (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION), errmsg("current transaction is aborted, "
                    "commands ignored until end of transaction block, firstChar[%c]",
                    u_sess->proc_cxt.firstChar), errdetail_abort()));
    }
    /* we don't expect type mod */
    for (int cnt_param = 0; cnt_param < numParams; cnt_param++) {
        parseTypeString(paramTypeNames[cnt_param], &paramTypes[cnt_param], NULL);
    }
}
#endif

/*
 * exec_parse_message
 *
 * Execute a "Parse" protocol message.
 * If paramTypeNames is specified, paraTypes is filled with corresponding OIDs.
 * The caller is expected to allocate space for the paramTypes.
 */
static void exec_parse_message(const char* query_string, /* string to execute */
    const char* stmt_name,                               /* name for prepared stmt */
    Oid* paramTypes,                                     /* parameter types */
    char** paramTypeNames,                               /* parameter type names */
    const char* paramModes,
    int numParams)                                       /* number of parameters */
{
    MemoryContext unnamed_stmt_context = NULL;
    MemoryContext oldcontext;
    List* parsetree_list = NULL;
    Node* raw_parse_tree = NULL;
    const char* commandTag = NULL;
    Query* query = NULL;
    List* querytree_list = NULL;
    CachedPlanSource* psrc = NULL;
    bool is_named = false;
    bool save_log_statement_stats = u_sess->attr.attr_common.log_statement_stats;
    char msec_str[PRINTF_DST_MAX];
    char* mask_string = NULL;
#ifdef ENABLE_MULTIPLE_NODES
    bool runOnSingleNode = false;
#endif
    ExecNodes* single_exec_node = NULL;
    bool is_read_only = false;
    bool is_ctas = false;

    gstrace_entry(GS_TRC_ID_exec_parse_message);
    /*
     * Report query to various monitoring facilities.
     */
    t_thrd.postgres_cxt.debug_query_string = query_string;

    plpgsql_estate = NULL;

    /*
     * Only support normal perf mode for PBE, as DestRemoteExecute can not send T message automatically.
     */
    t_thrd.explain_cxt.explain_perf_mode = EXPLAIN_NORMAL;

    /*
     * Set query dop at the first beginning of a query.
     */
    u_sess->opt_cxt.query_dop = u_sess->opt_cxt.query_dop_store;

    u_sess->pbe_message = PARSE_MESSAGE_QUERY;

    pgstat_report_activity(STATE_RUNNING, query_string);
    instr_stmt_report_start_time();

    // Init pool handlers
    exec_init_poolhandles();

    set_ps_display("PARSE", false);

    if (save_log_statement_stats) {
        ResetUsage();
    }

    if (log_min_messages <= DEBUG2 || client_min_messages <= DEBUG2) {
        mask_string = maskPassword(query_string);
        if (mask_string == NULL) {
            mask_string = (char*)query_string;
        }

        ereport(DEBUG2, (errmsg("parse %s: %s", *stmt_name ? stmt_name : "<unnamed>", mask_string)));

        if (mask_string != query_string) {
            pfree(mask_string);
        }
    }

    /* just for cooperation analysis. */
    if (IS_PGXC_COORDINATOR && u_sess->pgxc_cxt.is_gc_fdw) {
        u_sess->attr.attr_sql.enable_stream_operator = false;
        u_sess->opt_cxt.qrw_inlist2join_optmode = QRW_INLIST2JOIN_DISABLE;
    }

    /*
     * Start up a transaction command so we can run parse analysis etc. (Note
     * that this will normally change current memory context.) Nothing happens
     * if we are already in one.
     */
    start_xact_command();

    if (ENABLE_DN_GPC)
        CleanSessGPCPtr(u_sess);


    is_named = (stmt_name[0] != '\0');
    if (ENABLE_GPC) {
#ifdef ENABLE_MULTIPLE_NODES
        if (IsConnFromCoord() && paramTypeNames) {
            MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->temp_mem_cxt);
            GetParamOidFromName(paramTypeNames, paramTypes, numParams);
            MemoryContextSwitchTo(oldcxt);
        }
#endif
        psrc = g_instance.plan_cache->Fetch(query_string, strlen(query_string),
                                                                     numParams, paramTypes, NULL);
        if (psrc != NULL) {
            bool hasGetLock = false;
            if (is_named) {
                if (ENABLE_CN_GPC)
                    StorePreparedStatementCNGPC(stmt_name, psrc, false, true);
                else {
                    u_sess->pcache_cxt.cur_stmt_psrc = psrc;
                    if (g_instance.plan_cache->CheckRecreateCachePlan(psrc, &hasGetLock))
                        g_instance.plan_cache->RecreateCachePlan(psrc, stmt_name, NULL, NULL, NULL, hasGetLock);
                }
                goto pass_parsing;
            } else {
                drop_unnamed_stmt();
                u_sess->pcache_cxt.unnamed_stmt_psrc = psrc;
                if (ENABLE_DN_GPC)
                    u_sess->pcache_cxt.private_refcount--;
                /* don't share unnamed invalid or lightproxy plansource */
                if (!g_instance.plan_cache->CheckRecreateCachePlan(psrc, &hasGetLock) && psrc->gplan) {
                    goto pass_parsing;
                } else {
                    if (hasGetLock) {
                        AcquirePlannerLocks(psrc->query_list, false);
                        if (psrc->gplan) {
                            AcquireExecutorLocks(psrc->gplan->stmt_list, false);
                        }
                    }
                    /* not pass parsing, set psrc to NULL to prevent accidental modification. */
                    psrc = NULL;
                }
            }
        }
    }
#ifdef ENABLE_MULTIPLE_NODES
    /* in smooth open gpc status, when dn get same prepare statement saved in this session,
     * just skip it, don't throw error */
    if (IS_PGXC_DATANODE && !ENABLE_GPC && IN_GPC_GRAYRELEASE_CHANGE && is_named) {
        PreparedStatement* ps = FetchPreparedStatement(stmt_name, false, false);
        if (ps != NULL && strcmp(ps->plansource->query_string, query_string) == 0) {
            goto pass_parsing;
        } else if (ps != NULL) {
            DropPreparedStatement(stmt_name, false);
        }
    }
#endif

    /*
     * Switch to appropriate context for constructing parsetrees.
     *
     * We have two strategies depending on whether the prepared statement is
     * named or not.  For a named prepared statement, we do parsing in
     * u_sess->parser_cxt.temp_parse_message_context and copy the finished trees
     * into the prepared statement's plancache entry; then the reset of
     * u_sess->parser_cxt.temp_parse_message_context temporary space used by
     * parsing and rewriting. For an unnamed prepared statement, we assume
     * the statement isn't going to hang around long, so getting rid of temp
     * space quickly is probably not worth the costs of copying parse trees.
     * So in this case, we create the plancache entry's query_context here,
     * and do all the parsing work therein.
     */
    if (is_named) {
        /* Named prepared statement --- parse in u_sess->parser_cxt.temp_parse_message_context */
        oldcontext = MemoryContextSwitchTo(u_sess->temp_mem_cxt);
    } else {
        /* Unnamed prepared statement --- release any prior unnamed stmt */
        drop_unnamed_stmt();
        /* Create context for parsing */
        unnamed_stmt_context = AllocSetContextCreate(u_sess->temp_mem_cxt, "unnamed prepared statement",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
        oldcontext = MemoryContextSwitchTo(unnamed_stmt_context);
    }

#ifdef ENABLE_MULTIPLE_NODES
    /*
     * if we have the parameter types passed, which happens only in case of
     * connection from Coordinators, fill paramTypes with their OIDs for
     * subsequent use. We have to do name to OID conversion, in a transaction
     * context.
     */
    if (IsConnFromCoord() && paramTypeNames) {
        GetParamOidFromName(paramTypeNames, paramTypes, numParams);
    }
#endif /* PGXC */

    /*
     * Do basic parsing of the query or queries (this should be safe even if
     * we are in aborted transaction state!)
     */
    parsetree_list = pg_parse_query(query_string);

    /*
     * We only allow a single user statement in a prepared statement. This is
     * mainly to keep the protocol simple --- otherwise we'd need to worry
     * about multiple result tupdescs and things like that.
     */
    if (list_length(parsetree_list) > 1)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot insert multiple commands into a prepared statement")));

    if (parsetree_list != NIL) {
        bool snapshot_set = false;
        int i;

        raw_parse_tree = (Node*)linitial(parsetree_list);

        /*
         * Get the command name for possible use in status display.
         */
        commandTag = CreateCommandTag(raw_parse_tree);
        t_thrd.postgres_cxt.cur_command_tag = transform_node_tag(raw_parse_tree);

        /*
         * If we are in an aborted transaction, reject all commands except
         * COMMIT/ROLLBACK.  It is important that this test occur before we
         * try to do parse analysis, rewrite, or planning, since all those
         * phases try to do database accesses, which may fail in abort state.
         * (It might be safe to allow some additional utility commands in this
         * state, but not many...)
         */
        if (IsAbortedTransactionBlockState() && !IsTransactionExitStmt(raw_parse_tree))
            ereport(ERROR, (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                    errmsg("current transaction is aborted, "
                        "commands ignored until end of transaction block, firstChar[%c]",
                        u_sess->proc_cxt.firstChar), errdetail_abort()));

        /*
         * Read password status from pg_user_status.
         */
        Oid current_user = GetUserId();
        if (current_user != BOOTSTRAP_SUPERUSERID) {
            if (!IsAbortedTransactionBlockState() && GetAccountPasswordExpired(current_user) == EXPIRED_STATUS) {
                ForceModifyExpiredPwd((const char*)query_string, parsetree_list);
            }
        } 

            /*
             * Create the CachedPlanSource before we do parse analysis, since it
             * needs to see the unmodified raw parse tree.
             */
#ifdef PGXC
        psrc = CreateCachedPlan(raw_parse_tree, query_string, stmt_name, commandTag);
#else
        psrc = CreateCachedPlan(raw_parse_tree, query_string, commandTag);
#endif

        /* initialized it to false first. */
        psrc->single_shard_stmt = false;

        /*
         * Set up a snapshot if parse analysis will need one.
         */
        if (analyze_requires_snapshot(raw_parse_tree)) {
            PushActiveSnapshot(GetTransactionSnapshot());
            snapshot_set = true;
        }

        /*
         * Analyze and rewrite the query.  Note that the originally specified
         * parameter set is not required to be complete, so we have to use
         * parse_analyze_varparams().
         */
        if (u_sess->attr.attr_common.log_parser_stats)
            ResetUsage();

        query = parse_analyze_varparams(raw_parse_tree, query_string, &paramTypes, &numParams);

#ifdef ENABLE_MOT
        /* check cross engine queries  */
        StorageEngineType storageEngineType = SE_TYPE_UNSPECIFIED;
        CheckTablesStorageEngine(query, &storageEngineType);
        SetCurrentTransactionStorageEngine(storageEngineType);
        /* set the plan's storage engine */
        psrc->storageEngineType = storageEngineType;

        /* gpc does not support MOT engine */
        if (ENABLE_CN_GPC && psrc->gpc.status.IsSharePlan() &&
            (storageEngineType == SE_TYPE_MOT || storageEngineType == SE_TYPE_MIXED)) {
            psrc->gpc.status.SetKind(GPC_UNSHARED);
        }
#endif

        if (ENABLE_CN_GPC && psrc->gpc.status.IsSharePlan() && contains_temp_tables(query->rtable)) {
            /* temp table unsupport shared */
            psrc->gpc.status.SetKind(GPC_UNSHARED);
        }

        /*
         * Check all parameter types got determined.
         */
        for (i = 0; i < numParams; i++) {
            Oid ptype = paramTypes[i];

            if (ptype == InvalidOid || ptype == UNKNOWNOID)
                ereport(ERROR, (errcode(ERRCODE_INDETERMINATE_DATATYPE),
                        errmsg("could not determine data type of parameter $%d", i + 1)));
        }

        if (u_sess->attr.attr_common.log_parser_stats)
            ShowUsage("PARSE ANALYSIS STATISTICS");

#ifndef ENABLE_MULTIPLE_NODES
        /* store normalized uniquesQl text into Query in P phase of PBE, only if auto-cleanup is enabled */
        if (is_unique_sql_enabled() && g_instance.attr.attr_common.enable_auto_clean_unique_sql) {
            query->unique_sql_text = FindCurrentUniqueSQL();
        }
#endif

        /*
        * 'create table as select' is divided into 'create table' and 'insert into select', and 'create table' is
        * executed in sql rewrite. For PBE, if the plan is cached, then we will not do PARSE anymore. So we just not
        * call rewrite in PARSE. This work will be done in BIND.
        */
        if (query->utilityStmt != NULL && IsA(query->utilityStmt, CreateTableAsStmt)) {
            querytree_list = list_make1(query);
            is_ctas = true;
        } else {
            querytree_list = pg_rewrite_query(query);
        }


#ifdef ENABLE_MULTIPLE_NODES
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
            ListCell* lc = NULL;
            runOnSingleNode = u_sess->attr.attr_sql.enable_light_proxy &&
                              list_length(querytree_list) == 1 &&
                              !IsA(raw_parse_tree, CreateTableAsStmt) &&
                              !IsA(raw_parse_tree, RefreshMatViewStmt);

            foreach (lc, querytree_list) {
                Query* cur_query = (Query*)lfirst(lc);

                if (runOnSingleNode) {
                    if (ENABLE_ROUTER(cur_query->commandType)) {
                        single_exec_node = lightProxy::checkRouterQuery(cur_query);
                    } else {
                        single_exec_node = lightProxy::checkLightQuery(cur_query);
                    }

                    CleanHotkeyCandidates(true);

                    /* only deal with single node/param */
                    if (single_exec_node == NULL || list_length(single_exec_node->nodeList) +
                        list_length(single_exec_node->primarynodelist) > 1) {
                        runOnSingleNode = false;
                        FreeExecNodes(&single_exec_node);
                    } else {
                        psrc->single_shard_stmt = true;
                        is_read_only = (cur_query->commandType == CMD_SELECT && !cur_query->hasForUpdate);

                        /* do here but not bind to only process once */
                        if (is_named) {
                            (void)light_set_datanode_queries(stmt_name);
                        }

                        LPROXY_DEBUG(ereport(DEBUG2,
                            (errmsg("[LIGHT PROXY] Got Parse slim: name %s, query %s", stmt_name, query_string))));
                    }
                }

                if (cur_query->sql_statement == NULL)
                    cur_query->sql_statement = pstrdup(query_string);
            }
        } else if (IS_PGXC_DATANODE) {
            /*
             * Light proxy would send the parse request to the single DN, so if we are
             * doing PARSE at DN, then statement must be single shard
             */
            psrc->single_shard_stmt = true;
        }

#endif

        /* Done with the snapshot used for parsing */
        if (snapshot_set)
            PopActiveSnapshot();

    } else {
        /* Empty input string.	This is legal. */
        raw_parse_tree = NULL;
        commandTag = NULL;
#ifdef PGXC
        psrc = CreateCachedPlan(raw_parse_tree, query_string, stmt_name, commandTag);
#else
        psrc = CreateCachedPlan(raw_parse_tree, query_string, commandTag);
#endif
        querytree_list = NIL;
    }

    psrc->nextval_default_expr_type = u_sess->opt_cxt.nextval_default_expr_type;

    /*
     * CachedPlanSource must be a direct child of u_sess->parser_cxt.temp_parse_message_context
     * before we reparent unnamed_stmt_context under it, else we have a disconnected circular
     * subgraph.  Klugy, but less so than flipping contexts even more above.
     */
    if (unnamed_stmt_context)
        MemoryContextSetParent(psrc->context, u_sess->temp_mem_cxt);

    /* Finish filling in the CachedPlanSource */
    CompleteCachedPlan(psrc, querytree_list, unnamed_stmt_context, paramTypes, paramModes, numParams, NULL, NULL,
        0, /* default cursor options */
        true, stmt_name, single_exec_node,
        is_read_only); /* fixed result */

     /* For ctas query, rewrite is not called in PARSE, so we must set invalidation to revalidate the cached plan. */
    if (is_ctas) {
        psrc->is_valid = false;
    }


    /* If we got a cancel signal during analysis, quit */
    CHECK_FOR_INTERRUPTS();

    if (is_named) {
        /*
         * Store the query as a prepared statement.
         */
        StorePreparedStatement(stmt_name, psrc, false);
    } else {
        /*
         * We just save the CachedPlanSource into unnamed_stmt_psrc.
         */
        SaveCachedPlan(psrc);

        u_sess->pcache_cxt.unnamed_stmt_psrc = psrc;
    }

#ifdef ENABLE_MOT
    // Try MOT JIT code generation only after the plan source is saved.
    if (query != NULL && (psrc->storageEngineType == SE_TYPE_MOT || psrc->storageEngineType == SE_TYPE_UNSPECIFIED) &&
        !IS_PGXC_COORDINATOR && JitExec::IsMotCodegenEnabled()) {
        // MOT JIT code generation
        TryMotJitCodegenQuery(query_string, psrc, query);
    }
#endif

    MemoryContextSwitchTo(oldcontext);

pass_parsing:

    /*
     * We do NOT close the open transaction command here; that only happens
     * when the client sends Sync.	Instead, do CommandCounterIncrement just
     * in case something happened during parse/plan.
     */
    CommandCounterIncrement();

    /*
     * Send ParseComplete.
     */
    bool need_redirect = libpqsw_process_parse_message(psrc->commandTag, psrc->query_list);
    libpqsw_trace("we find pbe new %s cmdtag:%s, sql:%s",
                  need_redirect ? "transfer" : "select",
                  psrc->commandTag == NULL ? "" : psrc->commandTag,
                  psrc->query_string);
    if ((!need_redirect || libpqsw_is_begin()) && t_thrd.postgres_cxt.whereToSendOutput == DestRemote) {
        pq_putemptymessage('1');
    }

    /*
     * Emit duration logging if appropriate.
     */
    switch (check_log_duration(msec_str, false)) {
        case 1:
            Assert(false);
            break;
        case 2: {
            char* cur_mask_string = NULL;

            MASK_PASSWORD_START(cur_mask_string, query_string);
            ereport(LOG, (errmsg(
                     "duration: %s ms queryid %ld unique id %ld parse %s: %s", msec_str, u_sess->debug_query_id, u_sess->slow_query_cxt.slow_query.unique_sql_id, *stmt_name ? stmt_name : "<unnamed>", cur_mask_string),
                    errhidestmt(true)));
            MASK_PASSWORD_END(cur_mask_string, query_string);
            break;
        }
        default:
            break;
    }

    if (save_log_statement_stats)
        ShowUsage("PARSE MESSAGE STATISTICS");

    t_thrd.postgres_cxt.debug_query_string = NULL;
    t_thrd.postgres_cxt.cur_command_tag = T_Invalid;
    gstrace_exit(GS_TRC_ID_exec_parse_message);
}

static int getSingleNodeIdx(StringInfo input_message, CachedPlanSource* psrc, const char* stmt_name)
{
    ParamListInfo params = NULL;
    /* Get the parameter format codes */
    int numPFormats = pq_getmsgint(input_message, 2);
    int16* pformats = NULL;
    bool snapshot_set = false;
    int idx = -1;

    if (numPFormats > 0) {
        int i;

        pformats = (int16*)palloc(numPFormats * sizeof(int16));
        for (i = 0; i < numPFormats; i++)
            pformats[i] = pq_getmsgint(input_message, 2);
    }

    /* Get the parameter value count */
    int numParams = pq_getmsgint(input_message, 2);
    if (numPFormats > 1 && numPFormats != numParams)
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("bind message has %d parameter formats but %d parameters", numPFormats, numParams)));

    if (numParams != psrc->num_params)
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("bind message supplies %d parameters, but prepared statement \"%s\" requires %d",
                    numParams,
                    stmt_name,
                    psrc->num_params)));

    /*
     * If we are in aborted transaction state, the only portals we can
     * actually run are those containing COMMIT or ROLLBACK commands. We
     * disallow binding anything else to avoid problems with infrastructure
     * that expects to run inside a valid transaction.	We also disallow
     * binding any parameters, since we can't risk calling user-defined I/O
     * functions.
     */
    if (IsAbortedTransactionBlockState() && (!IsTransactionExitStmt(psrc->raw_parse_tree) || numParams != 0))
        ereport(ERROR,
            (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                errmsg("current transaction is aborted, "
                    "commands ignored until end of transaction block, firstChar[%c]",
                    u_sess->proc_cxt.firstChar),
                errdetail_abort()));

    /*
     * Set a snapshot if we have parameters to fetch (since the input
     * functions might need it) or the query isn't a utility command (and
     * hence could require redoing parse analysis and planning).  We keep the
     * snapshot active till we're done, so that plancache.c doesn't have to
     * take new ones.
     */
    if (!GTM_LITE_MODE && (numParams > 0 || analyze_requires_snapshot(psrc->raw_parse_tree))) {
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_set = true;
    }

    /*
     * Fetch parameters, if any, and store in the portal's memory context.
     */
    if (numParams > 0) {
        int paramno;
        Oid param_collation = GetCollationConnection();
        int param_charset = GetCharsetConnection();

        params = (ParamListInfo)palloc(offsetof(ParamListInfoData, params) + numParams * sizeof(ParamExternData));

        /* we have static list of params, so no hooks needed */
        params->paramFetch = NULL;
        params->paramFetchArg = NULL;
        params->parserSetup = NULL;
        params->parserSetupArg = NULL;
        params->params_need_process = false;
        params->numParams = numParams;

        for (paramno = 0; paramno < numParams; paramno++) {
            Oid ptype = psrc->param_types[paramno];
            int32 plength;
            Datum pval;
            bool isNull = false;
            StringInfoData pbuf;
            char csave;
            int16 pformat;

            plength = pq_getmsgint(input_message, 4);
            isNull = (plength == -1);
            /* add null value process for date type */
            if (((VARCHAROID == ptype  && !ACCEPT_EMPTY_STR) || TIMESTAMPOID == ptype || TIMESTAMPTZOID == ptype ||
                    TIMEOID == ptype || TIMETZOID == ptype || INTERVALOID == ptype || SMALLDATETIMEOID == ptype) &&
                0 == plength && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
                isNull = true;

            /*
             * Insert into bind values support illegal characters import,
             * and this just wroks for char type attribute.
             */
            u_sess->mb_cxt.insertValuesBind_compatible_illegal_chars = IsCharType(ptype);

            if (!isNull) {
                const char* pvalue = pq_getmsgbytes(input_message, plength);

                /*
                 * Rather than copying data around, we just set up a phony
                 * StringInfo pointing to the correct portion of the message
                 * buffer.	We assume we can scribble on the message buffer so
                 * as to maintain the convention that StringInfos have a
                 * trailing null.  This is grotty but is a big win when
                 * dealing with very large parameter strings.
                 */
                pbuf.data = (char*)pvalue;
                pbuf.maxlen = plength + 1;
                pbuf.len = plength;
                pbuf.cursor = 0;

                csave = pbuf.data[plength];
                pbuf.data[plength] = '\0';
            } else {
                pbuf.data = NULL; /* keep compiler quiet */
                csave = 0;
            }

            if (numPFormats > 1) {
                Assert(NULL != pformats);
                pformat = pformats[paramno];
            } else if (numPFormats > 0) {
                Assert(NULL != pformats);
                pformat = pformats[0];
            } else {
                pformat = 0; /* default = text */
            }

            if (pformat == 0) {
                /* text mode */
                Oid typinput;
                Oid typioparam;
                char* pstring = NULL;

                getTypeInputInfo(ptype, &typinput, &typioparam);

                /*
                 * We have to do encoding conversion before calling the
                 * typinput routine.
                 */
                if (isNull) {
                    pstring = NULL;
                } else if (OidIsValid(param_collation) && IsSupportCharsetType(ptype)) {
                    pstring = pg_client_to_any(pbuf.data, plength, param_charset);
                } else {
                    pstring = pg_client_to_server(pbuf.data, plength);
                }

                pval = OidInputFunctionCall(typinput, pstring, typioparam, -1);

                /* Free result of encoding conversion, if any */
                if (pstring != NULL && pstring != pbuf.data)
                    pfree(pstring);
            } else if (pformat == 1) {
                /* binary mode */
                Oid typreceive;
                Oid typioparam;
                StringInfo bufptr;

                /*
                 * Call the parameter type's binary input converter
                 */
                getTypeBinaryInputInfo(ptype, &typreceive, &typioparam);

                if (isNull)
                    bufptr = NULL;
                else
                    bufptr = &pbuf;

                pval = OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);

                /* Trouble if it didn't eat the whole buffer */
                if (!isNull && pbuf.cursor != pbuf.len)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                            errmsg("incorrect binary data format in bind parameter %d", paramno + 1)));
            } else {
                ereport(
                    ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unsupported format code: %d", pformat)));
                pval = 0; /* keep compiler quiet */
            }

            /* Restore message buffer contents */
            if (!isNull)
                pbuf.data[plength] = csave;

            params->params[paramno].value = pval;
            params->params[paramno].isnull = isNull;

            /*
             * We mark the params as CONST.  This ensures that any custom plan
             * makes full use of the parameter values.
             */
            params->params[paramno].pflags = PARAM_FLAG_CONST;
            params->params[paramno].ptype = ptype;
            params->params[paramno].tabInfo = NULL;

            /* Reset the compatible illegal chars import flag */
            u_sess->mb_cxt.insertValuesBind_compatible_illegal_chars = false;
        }
    }

    /* Make sure the querytree list is valid and we have parse-time locks */
    RevalidateCachedQuery(psrc);

    if (!psrc->single_exec_node) {
        ereport(LOG, (errmsg("[LIGHT PROXY] distribute key of table in cached plan is changed")));
        if (snapshot_set)
            PopActiveSnapshot();
        pfree_ext(pformats);
        pfree_ext(params);
        return -1;
    }

    /* Only when dynamically one datanode pushdwn is ok, we find single node */
    if (pgxc_check_dynamic_param(psrc->single_exec_node->dynamic_en_expr, params))
        idx = getSingleNodeIdx_internal(psrc->single_exec_node, params);

    /* Done with the snapshot used for parameter I/O and parsing/planning */
    if (snapshot_set)
        PopActiveSnapshot();
    pfree_ext(pformats);
    pfree_ext(params);
    return idx;
}

static ExecNodes* GetSingleNodeForEqualExpr(ExecNodes* execNodes, ParamListInfo params, bool* singleNodeFlag)
{
    ExecNodes* singleNode = NULL;
    RelationLocInfo* relLocInfo = GetRelationLocInfo(execNodes->en_relid);
    if (relLocInfo == NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("rel_loc_info is NULL.")));
    }

    int len = list_length(relLocInfo->partAttrNum);
    Datum* distcolValue = (Datum*)palloc(len * sizeof(Datum));
    bool* distcolIsnull = (bool*)palloc(len * sizeof(bool));
    Oid* distcolType = (Oid*)palloc(len * sizeof(Oid));
    List* idxDist = NULL;
    int i = 0;
    ListCell* cell = NULL;

    *singleNodeFlag = true;
    foreach (cell, execNodes->en_expr) {
        Expr* distcolExpr = (Expr*)lfirst(cell);
        distcolExpr = (Expr*)eval_const_expressions_params(NULL, (Node*)distcolExpr, params);
        if (distcolExpr && IsA(distcolExpr, Const)) {
            Const* constExpr = (Const*)distcolExpr;
            distcolValue[i] = constExpr->constvalue;
            distcolIsnull[i] = constExpr->constisnull;
            distcolType[i] = constExpr->consttype;
            idxDist = lappend_int(idxDist, i);
            i++;
        } else {
            Assert(distcolExpr != NULL);
            ereport(LOG, (errmsg("[LIGHT PROXY] param of distribute key is not const")));
            *singleNodeFlag = false;
            return NULL;
        }
    }

    singleNode = GetRelationNodes(relLocInfo, distcolValue, distcolIsnull,
        distcolType, idxDist, execNodes->accesstype);

    pfree(distcolValue);
    pfree(distcolIsnull);
    pfree(distcolType);
    list_free(idxDist);
    FreeRelationLocInfo(relLocInfo);

    return singleNode;
}

static int getSingleNodeIdx_internal(ExecNodes* exec_nodes, ParamListInfo params)
{
    int idx = -1;
    ExecNodes* single_node = NULL;

    Assert(exec_nodes->nodeList != NIL || exec_nodes->en_expr != NIL);

    if (exec_nodes->en_expr != NIL) {
        if (exec_nodes->need_range_prune) {
            single_node = makeNode(ExecNodes);
            *single_node = *exec_nodes;
            PruningDatanode(single_node, params);
        } else {
            bool singleNodeFlag = false;
            single_node = GetSingleNodeForEqualExpr(exec_nodes, params, &singleNodeFlag);
            if (!singleNodeFlag) {
                return -1;
            }
        }
    } else {
        single_node = exec_nodes;
    }

    /* make sure it is one dn */
    if (list_length(single_node->nodeList) != 1) {
        ereport(LOG, (errmsg("[LIGHT PROXY] nodelist is computed to be not single node")));
        return -1;
    }

    idx = linitial_int(single_node->nodeList);
    return idx;
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * exec_get_ddl_params
 *     just get params info from CN
 */
#ifndef ENABLE_UT
static void exec_get_ddl_params(StringInfo input_message)
#else
void exec_get_ddl_params(StringInfo input_message)
#endif
{
    int numPFormats;
    int16* pformats = NULL;
    int numParams;
    int numRFormats;
    int16* rformats = NULL;
    ParamListInfo params = NULL;

    if (unlikely(!((IS_PGXC_COORDINATOR || IS_PGXC_DATANODE) && IsConnFromCoord()))) {
        ereport(ERROR,
            (errcode(ERRCODE_IO_ERROR),
                errmsg("The current node should not receive z messages")));
    }

    if (u_sess->top_transaction_mem_cxt == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                errmsg("current transaction is not start")));
    }

    if (u_sess->parser_cxt.ddl_pbe_context == NULL) {
        u_sess->parser_cxt.ddl_pbe_context = AllocSetContextCreate(u_sess->top_mem_cxt,
            "DDLPBEContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    }
    MemoryContext old_context = MemoryContextSwitchTo(u_sess->parser_cxt.ddl_pbe_context);

    /*
     * Get the fixed part of the message, This information is not useful to us, but we still have to process it,
     * because the cursor will change during the processing of the information, otherwise the subsequent information
     * cannot be parsed.
     */
    (void)pq_getmsgstring(input_message);
    (void)pq_getmsgstring(input_message);

    /* Get the parameter format codes */
    numPFormats = pq_getmsgint(input_message, 2);
    if (numPFormats > 0) {
        int i;
        pformats = (int16*)palloc(numPFormats * sizeof(int16));
        for (i = 0; i < numPFormats; i++)
            pformats[i] = pq_getmsgint(input_message, 2);
    }

    /* Get the parameter value count */
    numParams = pq_getmsgint(input_message, 2);
    if (numPFormats > 1 && numPFormats != numParams)
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("bind message has %d parameter formats but %d parameters", numPFormats, numParams)));

    if (numParams > 0) {
        int paramno;
        Oid param_collation = GetCollationConnection();
        int param_charset = GetCharsetConnection();
        params = (ParamListInfo)palloc(offsetof(ParamListInfoData, params) + numParams * sizeof(ParamExternData));
        params->paramFetch = NULL;
        params->paramFetchArg = NULL;
        params->parserSetup = NULL;
        params->parserSetupArg = NULL;
        params->params_need_process = false;
        params->uParamInfo = DEFUALT_INFO;
        params->params_lazy_bind = false;
        params->numParams = numParams;

        for (paramno = 0; paramno < numParams; paramno++) {
            Oid ptype = TEXTOID;
            int32 plength;
            Datum pval;
            bool isNull = false;
            StringInfoData pbuf;
            char csave;
            int16 pformat;

            plength = pq_getmsgint(input_message, 4);
            isNull = (plength == -1);

            if (!isNull) {
                const char* pvalue = pq_getmsgbytes(input_message, plength);

                pbuf.data = (char*)pvalue;
                pbuf.maxlen = plength + 1;
                pbuf.len = plength;
                pbuf.cursor = 0;
                csave = pbuf.data[plength];
                pbuf.data[plength] = '\0';
            } else {
                pbuf.data = NULL; /* keep compiler quiet */
                csave = 0;
            }

            if (numPFormats > 1) {
                Assert(NULL != pformats);
                pformat = pformats[paramno];
            } else if (numPFormats > 0) {
                Assert(NULL != pformats);
                pformat = pformats[0];
            } else {
                pformat = 0; /* default = text */
            }
            if (pformat == 0) /* text mode */
            {
                Oid typinput;
                Oid typioparam;
                char* pstring = NULL;

                getTypeInputInfo(ptype, &typinput, &typioparam);

                if (isNull) {
                    pstring = NULL;
                } else if (OidIsValid(param_collation) && IsSupportCharsetType(ptype)) {
                    pstring = pg_client_to_any(pbuf.data, plength, param_charset);
                } else {
                    pstring = pg_client_to_server(pbuf.data, plength);
                }

                pval = OidInputFunctionCall(typinput, pstring, typioparam, -1);

                /* Free result of encoding conversion, if any */
                if (pstring != NULL && pstring != pbuf.data)
                    pfree(pstring);
            } else if (pformat == 1) /* binary mode */
            {
                Oid typreceive;
                Oid typioparam;
                StringInfo bufptr;

                /*
                 * Call the parameter type's binary input converter
                 */
                getTypeBinaryInputInfo(ptype, &typreceive, &typioparam);

                bufptr = isNull ? NULL : &pbuf;
                pval = OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);

                /* Trouble if it didn't eat the whole buffer */
                if (!isNull && pbuf.cursor != pbuf.len)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                            errmsg("incorrect binary data format in bind parameter %d", paramno + 1)));
            } else {
                ereport(
                    ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unsupported format code: %d", pformat)));
                pval = 0; /* keep compiler quiet */
            }

            /* Restore message buffer contents */
            if (!isNull)
                pbuf.data[plength] = csave;

            params->params[paramno].value = pval;
            params->params[paramno].isnull = isNull;

            /*
             * We mark the params as CONST.  This ensures that any custom plan
             * makes full use of the parameter values.
             */
            params->params[paramno].pflags = PARAM_FLAG_CONST;
            params->params[paramno].ptype = ptype;
            params->params[paramno].tabInfo = NULL;
        }
    } else {
        params = NULL;
    }
    u_sess->parser_cxt.param_info = (void*)params;

    MemoryContextSwitchTo(old_context);

    /* Get the result format codes */
    numRFormats = pq_getmsgint(input_message, 2);
    if (numRFormats > 0) {
        int i;
        rformats = (int16*)palloc(numRFormats * sizeof(int16));
        for (i = 0; i < numRFormats; i++)
            rformats[i] = pq_getmsgint(input_message, 2);
    }

    if (pformats != NULL)
        pfree_ext(pformats);
    if (rformats != NULL)
        pfree_ext(rformats);

    pq_getmsgend(input_message);
    pq_putemptymessage('z');
}
#endif

/*
 * exec_bind_message
 *
 * Process a "Bind" message to create a portal from a prepared statement
 */
static void exec_bind_message(StringInfo input_message)
{
    const char* portal_name = NULL;
    const char* stmt_name = NULL;
    int numPFormats;
    int16* pformats = NULL;
    int numParams;
    int numRFormats;
    int16* rformats = NULL;
    CachedPlanSource* psrc = NULL;
    CachedPlan* cplan = NULL;
    Portal portal;
    char* query_string = NULL;
    char* saved_stmt_name = NULL;
    ParamListInfo params = NULL;
    MemoryContext oldContext;
    bool save_log_statement_stats = u_sess->attr.attr_common.log_statement_stats;
    bool snapshot_set = false;
    char msec_str[PRINTF_DST_MAX];
    u_sess->parser_cxt.param_info = NULL;
    u_sess->parser_cxt.param_message = NULL;

    StringInfo temp_message = makeStringInfo();
    copyStringInfo(temp_message, input_message);
    gstrace_entry(GS_TRC_ID_exec_bind_message);

    /* Instrumentation: PBE - reset unique sql elapsed start time */
    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && is_unique_sql_enabled())
        u_sess->unique_sql_cxt.unique_sql_start_time = GetCurrentTimestamp();

    /* Get the fixed part of the message */
    portal_name = pq_getmsgstring(input_message);
    stmt_name = pq_getmsgstring(input_message);
    /* Check not NULL */
    if (portal_name == NULL || stmt_name == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("portal_name or stmt_name is null.")));
    }
    if (strlen(portal_name) > SECUREC_MEM_MAX_LEN || strlen(stmt_name) > SECUREC_MEM_MAX_LEN)
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("Too long portal_name and stmt_name.")));

    /*
     * Only support normal perf mode for PBE, as DestRemoteExecute can not send T message automatically.
     */
    t_thrd.explain_cxt.explain_perf_mode = EXPLAIN_NORMAL;
    plpgsql_estate = NULL;
    u_sess->xact_cxt.pbe_execute_complete = true;
    u_sess->pbe_message = BIND_MESSAGE_QUERY;

    if (SHOW_DEBUG_MESSAGE()) {
        ereport(DEBUG2,
            (errmsg("bind %s to %s", *portal_name ? portal_name : "<unnamed>", *stmt_name ? stmt_name : "<unnamed>")));
    }

    /* Find prepared statement */
    PreparedStatement *pstmt = NULL;
    if (stmt_name[0] != '\0') {
        if (ENABLE_DN_GPC) {
            psrc = u_sess->pcache_cxt.cur_stmt_psrc;
            if (SECUREC_UNLIKELY(psrc == NULL))
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                        errmsg("dn gpc's prepared statement %s does not exist", stmt_name)));
        } else {
            pstmt = FetchPreparedStatement(stmt_name, true, true);
            psrc = pstmt->plansource;
        }
    } else {
        /* special-case the unnamed statement */
        psrc = u_sess->pcache_cxt.unnamed_stmt_psrc;
        if (SECUREC_UNLIKELY(psrc == NULL))
            ereport(
                ERROR, (errcode(ERRCODE_UNDEFINED_PSTATEMENT), errmsg("unnamed prepared statement does not exist")));
    }

    Assert(NULL != psrc);

    /*
     * Report query to various monitoring facilities.
     */
    t_thrd.postgres_cxt.debug_query_string = psrc->query_string;
    t_thrd.postgres_cxt.cur_command_tag = transform_node_tag(psrc->raw_parse_tree);

    pgstat_report_activity(STATE_RUNNING, psrc->query_string);
    instr_stmt_report_start_time();

    set_ps_display("BIND", false);

    if (save_log_statement_stats)
        ResetUsage();

    /*
     * Start up a transaction command so we can call functions etc. (Note that
     * this will normally change current memory context.) Nothing happens if
     * we are already in one.
     */
    start_xact_command();

#ifdef ENABLE_MOT
    /* set transaction storage engine and check for cross transaction violation */
    SetCurrentTransactionStorageEngine(psrc->storageEngineType);

    /* block MOT engine queries in sub-transactions */
    if (!IsTransactionExitStmt(psrc->raw_parse_tree) && IsMOTEngineUsedInParentTransaction() && IsMOTEngineUsed()) {
        ereport(ERROR, (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED), errmodule(MOD_MOT),
            errmsg("SubTransaction is not supported for memory table")));
    }

    if (IsTransactionPrepareStmt(psrc->raw_parse_tree) && (IsMOTEngineUsed() || IsMixedEngineUsed())) {
        /* Explicit prepare transaction is not supported for memory table */
        ereport(ERROR, (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED), errmodule(MOD_MOT),
            errmsg("Explicit prepare transaction is not supported for memory table")));
    }

    /*
     * MOT JIT Execution:
     * Assist in distinguishing query boundaries in case of range query when client uses batches. This allows us to
     * know a new query started, and in case a previous execution did not fetch all records (since user is working in
     * batch-mode, and can decide to quit fetching in the middle), using this information we can infer this is a new
     * scan, and old scan state should be discarded.
     */
    if (psrc->mot_jit_context != NULL) {
        JitResetScan(psrc->mot_jit_context);
    }
#endif

    /* set unique sql id to current context */
    if (!u_sess->attr.attr_common.track_stmt_parameter) {
        SetUniqueSQLIdFromCachedPlanSource(psrc);
    }

    OpFusion::clearForCplan((OpFusion*)psrc->opFusionObj, psrc);

    if (psrc->opFusionObj != NULL) {
        Assert(psrc->cplan == NULL);
        (void)RevalidateCachedQuery(psrc);

        OpFusion *opFusionObj = (OpFusion *)(psrc->opFusionObj);
        if (opFusionObj != NULL) {
            if (opFusionObj->IsGlobal()) {
                opFusionObj = (OpFusion *)OpFusion::FusionFactory(opFusionObj->m_global->m_type,
                                                      u_sess->cache_mem_cxt, psrc, NULL, params);
                Assert(opFusionObj != NULL);
            }
            opFusionObj->clean();
            opFusionObj->updatePreAllocParamter(input_message);
            opFusionObj->setCurrentOpFusionObj(opFusionObj);
            opFusionObj->storeFusion(portal_name);

            CachedPlanSource* cps = opFusionObj->m_global->m_psrc;
            if (cps != NULL && cps->gplan) {
                setCachedPlanBucketId(cps->gplan, opFusionObj->m_local.m_params);
            }
            if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote)
                pq_putemptymessage('2');
            gstrace_exit(GS_TRC_ID_exec_bind_message);
            return;
        }
    }

    if (ENABLE_WORKLOAD_CONTROL && SqlIsValid(t_thrd.postgres_cxt.debug_query_string) &&
        (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) &&
        !IsConnFromCoord()) {
        u_sess->wlm_cxt->is_active_statements_reset = false;

        if (g_instance.wlm_cxt->dynamic_workload_inited) {
            dywlm_parallel_ready(t_thrd.postgres_cxt.debug_query_string);
            dywlm_client_max_reserve();
        } else {
            WLMParctlReady(t_thrd.postgres_cxt.debug_query_string);
            WLMParctlReserve(PARCTL_GLOBAL);
        }
    }

    /* Switch back to message context */
    MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

    /* light proxy and not set fetch size */
    if (psrc->single_exec_node && (unsigned int)input_message->len <= SECUREC_MEM_MAX_LEN) {
        /* save the cursor in case of error */
        int msg_cursor = input_message->cursor;
        int nodeIdx = getSingleNodeIdx(input_message, psrc, stmt_name);
        if (nodeIdx != -1) {
            lightProxy* scn = NULL;
            bool enable_gpc = (ENABLE_CN_GPC && stmt_name[0] != '\0');
            bool enable_unamed_gpc = psrc->gpc.status.InShareTable() && stmt_name[0] == '\0';
            if (enable_gpc)
                scn = lightProxy::locateLpByStmtName(stmt_name);
            else
                scn = (lightProxy *)psrc->lightProxyObj;

            if (scn == NULL) {
                /* initialize session cache context; typically it won't store much */
                MemoryContext context = AllocSetContextCreate(u_sess->cache_mem_cxt, "LightProxyMemory",
                    ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
                scn = New(context)lightProxy(context, psrc, portal_name, stmt_name);
                if (enable_gpc) {
                    scn->storeLpByStmtName(stmt_name);
                    psrc->lightProxyObj = NULL;
                } else if (enable_unamed_gpc) {
                    Assert(u_sess->pcache_cxt.unnamed_gpc_lp == NULL);
                    Assert(psrc->lightProxyObj == NULL);
                    u_sess->pcache_cxt.unnamed_gpc_lp = scn;
                } else {
                    psrc->lightProxyObj = scn;
                }
            } else {
                Assert(!enable_unamed_gpc);
                if (portal_name[0] != '\0') {
                    scn->storeLightProxy(portal_name);
                }
            }
            if (enable_gpc) {
                /* cngpc need fill gpc msg just like BuildCachedPlan. */
                GPCFillMsgForLp(psrc);
            }
            /* set nodeidx to router node if in router */
            if (HAS_ROUTER) {
                scn->m_nodeIdx = u_sess->exec_cxt.CurrentRouter->GetRouterNodeId();
            } else {
                scn->m_nodeIdx = nodeIdx;
            }
            lightProxy::setCurrentProxy(scn);
            lightProxy::processMsg(BIND_MESSAGE, input_message);

            LPROXY_DEBUG(ereport(DEBUG2,
                (errmsg("[LIGHT PROXY] Got Bind slim: name %s, query %s", psrc->stmt_name, psrc->query_string))));
            gstrace_exit(GS_TRC_ID_exec_bind_message);
            return;
        } else {
            /* gpc's shared plan used lightproxy before but cannot go through in current sessioin because of param */
            if (unlikely(psrc->gpc.status.InShareTable())) {
                psrc->gpc.status.SetStatus(GPC_INVALID);
                Assert (pstmt != NULL);
                g_instance.plan_cache->RecreateCachePlan(psrc, stmt_name, pstmt, NULL, NULL, true);
                psrc = pstmt->plansource;
            }
            /* fail to get node idx */
            ExecNodes* tmp_en = psrc->single_exec_node;
            psrc->single_exec_node = NULL;
            lightProxy::setCurrentProxy(NULL);
            input_message->cursor = msg_cursor;

            if (tmp_en != NULL)
                FreeExecNodes(&tmp_en);
            lightProxy* lp = NULL;
            if (ENABLE_CN_GPC && stmt_name[0] != '\0')
                lp = lightProxy::locateLpByStmtName(stmt_name);
            else
                lp = (lightProxy *)psrc->lightProxyObj;

            /* clean lightProxyObj if exists */
            if (lp != NULL) {
                lightProxy::tearDown(lp);
                psrc->lightProxyObj = NULL;
            }
        }
    } else
        /* it may be not NULL if last time report error */
        lightProxy::setCurrentProxy(NULL);

    /* Get the parameter format codes */
    numPFormats = pq_getmsgint(input_message, 2);
    if (numPFormats > 0) {
        int i;

        pformats = (int16*)palloc(numPFormats * sizeof(int16));
        for (i = 0; i < numPFormats; i++)
            pformats[i] = pq_getmsgint(input_message, 2);
    }

    /* Get the parameter value count */
    numParams = pq_getmsgint(input_message, 2);

    if (numPFormats > 1 && numPFormats != numParams)
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("bind message has %d parameter formats but %d parameters", numPFormats, numParams)));

    if (numParams != psrc->num_params)
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("bind message supplies %d parameters, but prepared statement \"%s\" requires %d",
                    numParams,
                    stmt_name,
                    psrc->num_params)));

    /*
     * If we are in aborted transaction state, the only portals we can
     * actually run are those containing COMMIT or ROLLBACK commands. We
     * disallow binding anything else to avoid problems with infrastructure
     * that expects to run inside a valid transaction.	We also disallow
     * binding any parameters, since we can't risk calling user-defined I/O
     * functions.
     */
    if (IsAbortedTransactionBlockState() && (!IsTransactionExitStmt(psrc->raw_parse_tree) || numParams != 0))
        ereport(ERROR,
            (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                errmsg("current transaction is aborted, "
                    "commands ignored until end of transaction block, firstChar[%c]",
                    u_sess->proc_cxt.firstChar),
                errdetail_abort()));

    /*
     * Create the portal.  Allow silent replacement of an existing portal only
     * if the unnamed portal is specified.
     */
    if (portal_name[0] == '\0')
        portal = CreatePortal(portal_name, true, true, false, true);
    else
        portal = CreatePortal(portal_name, false, false, false, true);

    /*
     * Prepare to copy stuff into the portal's memory context.  We do all this
     * copying first, because it could possibly fail (out-of-memory) and we
     * don't want a failure to occur between GetCachedPlan and
     * PortalDefineQuery; that would result in leaking our plancache refcount.
     */
    oldContext = MemoryContextSwitchTo(PortalGetHeapMemory(portal));

    /* Version control for DDL PBE */
    if (t_thrd.proc->workingVersionNum >= DDL_PBE_VERSION_NUM) {
        u_sess->parser_cxt.param_message = makeStringInfo();
        copyStringInfo(u_sess->parser_cxt.param_message, temp_message);
    }
    if (temp_message != NULL) {
        if (temp_message->data != NULL)
            pfree_ext(temp_message->data);
        pfree_ext(temp_message);
    }
    /* Copy the plan's query string into the portal */
    query_string = pstrdup(psrc->query_string);

    /* Likewise make a copy of the statement name, unless it's unnamed */
    if (stmt_name[0])
        saved_stmt_name = pstrdup(stmt_name);
    else
        saved_stmt_name = NULL;

    /*
     * Set a snapshot if we have parameters to fetch (since the input
     * functions might need it) or the query isn't a utility command (and
     * hence could require redoing parse analysis and planning).  We keep the
     * snapshot active till we're done, so that plancache.c doesn't have to
     * take new ones.
     */
    if (numParams > 0 || analyze_requires_snapshot(psrc->raw_parse_tree)) {
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_set = true;
    }

    /*
     * Fetch parameters, if any, and store in the portal's memory context.
     */
    if (numParams > 0) {
        int paramno;
        Oid param_collation = GetCollationConnection();
        int param_charset = GetCharsetConnection();

        params = (ParamListInfo)palloc(offsetof(ParamListInfoData, params) + numParams * sizeof(ParamExternData));
        /* we have static list of params, so no hooks needed */
        params->paramFetch = NULL;
        params->paramFetchArg = NULL;
        params->parserSetup = NULL;
        params->parserSetupArg = NULL;
        params->params_need_process = false;
        params->numParams = numParams;
        params->uParamInfo = DEFUALT_INFO;
        params->params_lazy_bind = false;

        for (paramno = 0; paramno < numParams; paramno++) {
            Oid ptype = psrc->param_types[paramno];
#ifndef ENABLE_MULTIPLE_NODES			
            char* pmode = NULL;
            if (psrc->param_modes != NULL) {
                pmode = &(psrc->param_modes[paramno]);
            }
#endif

            int32 plength;
            Datum pval;
            bool isNull = false;
            StringInfoData pbuf;
            char csave;
            int16 pformat;

            plength = pq_getmsgint(input_message, 4);
            isNull = (plength == -1);
            /* add null value process for date type */
            if (((VARCHAROID == ptype  && !ACCEPT_EMPTY_STR) || TIMESTAMPOID == ptype || TIMESTAMPTZOID == ptype ||
                    TIMEOID == ptype || TIMETZOID == ptype || INTERVALOID == ptype || SMALLDATETIMEOID == ptype) &&
                0 == plength && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
                isNull = true;

            /*
             * Insert into bind values support illegal characters import,
             * and this just wroks for char type attribute.
             */
            u_sess->mb_cxt.insertValuesBind_compatible_illegal_chars = IsCharType(ptype);

            if (!isNull) {
                const char* pvalue = pq_getmsgbytes(input_message, plength);

                /*
                 * Rather than copying data around, we just set up a phony
                 * StringInfo pointing to the correct portion of the message
                 * buffer.	We assume we can scribble on the message buffer so
                 * as to maintain the convention that StringInfos have a
                 * trailing null.  This is grotty but is a big win when
                 * dealing with very large parameter strings.
                 */
                pbuf.data = (char*)pvalue;
                pbuf.maxlen = plength + 1;
                pbuf.len = plength;
                pbuf.cursor = 0;

                csave = pbuf.data[plength];
                pbuf.data[plength] = '\0';
            } else {
                pbuf.data = NULL; /* keep compiler quiet */
                csave = 0;
            }

            if (numPFormats > 1) {
                Assert(NULL != pformats);
                pformat = pformats[paramno];
            } else if (numPFormats > 0) {
                Assert(NULL != pformats);
                pformat = pformats[0];
            } else {
                pformat = 0; /* default = text */
            }

            if (pformat == 0) {
                /* text mode */
                Oid typinput;
                Oid typioparam;
                char* pstring = NULL;

                getTypeInputInfo(ptype, &typinput, &typioparam);

                /*
                 * We have to do encoding conversion before calling the
                 * typinput routine.
                 */
                if (isNull) {
                    pstring = NULL;
                } else if (OidIsValid(param_collation) && IsSupportCharsetType(ptype)) {
                    pstring = pg_client_to_any(pbuf.data, plength, param_charset);
                } else {
                    pstring = pg_client_to_server(pbuf.data, plength);
                }

#ifndef ENABLE_MULTIPLE_NODES
                if (pmode == NULL || *pmode != PROARGMODE_OUT || !enable_out_param_override()) {
                    pval = OidInputFunctionCall(typinput, pstring, typioparam, -1);
                } else {
                    pval = (Datum)0;
                }
#else
                pval = OidInputFunctionCall(typinput, pstring, typioparam, -1);
#endif
                /* Free result of encoding conversion, if any */
                if (pstring != NULL && pstring != pbuf.data)
                    pfree(pstring);
            } else if (pformat == 1) {
                /* binary mode */
                Oid typreceive;
                Oid typioparam;
                StringInfo bufptr;

                /*
                 * Call the parameter type's binary input converter
                 */
                getTypeBinaryInputInfo(ptype, &typreceive, &typioparam);

                if (isNull)
                    bufptr = NULL;
                else
                    bufptr = &pbuf;


#ifndef ENABLE_MULTIPLE_NODES
                if (pmode == NULL || *pmode != PROARGMODE_OUT || !enable_out_param_override()) {
                    pval = OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);
                } else {
                    pval = (Datum)0;
                }
#else
                pval = OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);
#endif               

                /* Trouble if it didn't eat the whole buffer */
                if (!isNull && pbuf.cursor != pbuf.len)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                            errmsg("incorrect binary data format in bind parameter %d", paramno + 1)));
            } else {
                ereport(
                    ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unsupported format code: %d", pformat)));
                pval = 0; /* keep compiler quiet */
            }

            /* Restore message buffer contents */
            if (!isNull) {
                pbuf.data[plength] = csave;
            }

            params->params[paramno].value = pval;
#ifndef ENABLE_MULTIPLE_NODES
            isNull = isNull || (enable_out_param_override() && pmode != NULL && *pmode == PROARGMODE_OUT);
#endif
            params->params[paramno].isnull = isNull;

            /*
             * We mark the params as CONST.  This ensures that any custom plan
             * makes full use of the parameter values.
             */
            params->params[paramno].pflags = PARAM_FLAG_CONST;
            params->params[paramno].ptype = ptype;
            params->params[paramno].tabInfo = NULL;

            /* Reset the compatible illegal chars import flag */
            u_sess->mb_cxt.insertValuesBind_compatible_illegal_chars = false;
        }
    } else {
        params = NULL;
    }
    /* u_sess->parser_cxt.param_info is for ddl pbe. If the logic is not ddl pbe, It will not be used*/
    if (t_thrd.proc->workingVersionNum >= DDL_PBE_VERSION_NUM) {
        u_sess->parser_cxt.param_info = (void*)params;
    }

    /* Done storing stuff in portal's context */
    MemoryContextSwitchTo(oldContext);

    /* Get the result format codes */
    numRFormats = pq_getmsgint(input_message, 2);
    if (numRFormats > 0) {
        int i;

        rformats = (int16*)palloc(numRFormats * sizeof(int16));
        for (i = 0; i < numRFormats; i++)
            rformats[i] = pq_getmsgint(input_message, 2);
    }

    pq_getmsgend(input_message);

    psrc->cursor_options |= CURSOR_OPT_SPQ_OK;
    /*
     * Obtain a plan from the CachedPlanSource.  Any cruft from (re)planning
     * will be generated in t_thrd.mem_cxt.msg_mem_cxt.  The plan refcount will be
     * assigned to the Portal, so it will be released at portal destruction.
     */
    if (ENABLE_CACHEDPLAN_MGR) {
        cplan = GetWiseCachedPlan(psrc, params, false);
    } else {
        cplan = GetCachedPlan(psrc, params, false);
    }

    /*
     * copy the single_shard info from plan source into plan.
     * With this, we can determine if we should use global snapshot or local snapshot after.
     */
    cplan->single_shard_stmt = psrc->single_shard_stmt;

    /*
     * Now we can define the portal.
     *
     * DO NOT put any code that could possibly throw an error between the
     * above GetCachedPlan call and here.
     */
    PortalDefineQuery(portal, saved_stmt_name, query_string, psrc->commandTag, cplan->stmt_list, cplan);

    portal->nextval_default_expr_type = psrc->nextval_default_expr_type;

    if (OpFusion::IsSqlBypass(psrc, cplan->stmt_list)) {
        psrc->opFusionObj = OpFusion::FusionFactory(OpFusion::getFusionType(cplan, params, NULL),
                                                    u_sess->cache_mem_cxt, psrc, NULL, params);
        psrc->is_checked_opfusion = true;
        if (psrc->opFusionObj != NULL) {
            ((OpFusion*)psrc->opFusionObj)->clean();
            ((OpFusion*)psrc->opFusionObj)->useOuterParameter(params);
            ((OpFusion*)psrc->opFusionObj)->setCurrentOpFusionObj((OpFusion*)psrc->opFusionObj);
            ((OpFusion*)psrc->opFusionObj)->CopyFormats(rformats, numRFormats);
            ((OpFusion*)psrc->opFusionObj)->storeFusion(portal_name);

            if (snapshot_set)
                PopActiveSnapshot();

            if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote)
                pq_putemptymessage('2');
            gstrace_exit(GS_TRC_ID_exec_bind_message);
            return;
        }
    }

    if (ENABLE_GPC && psrc->gplan) {
        portal->stmts = CopyLocalStmt(cplan->stmt_list, u_sess->top_portal_cxt, &portal->copyCxt);
    }

    /* Done with the snapshot used for parameter I/O and parsing/planning */
    if (snapshot_set)
        PopActiveSnapshot();

    /*
     * And we're ready to start portal execution.
     */
    PortalStart(portal, params, 0, InvalidSnapshot);

    /*
     * Apply the result format requests to the portal.
     */
    PortalSetResultFormat(portal, numRFormats, rformats);

    if (u_sess->attr.attr_resource.use_workload_manager && g_instance.wlm_cxt->gscgroup_init_done &&
        !IsAbortedTransactionBlockState()) {
        u_sess->wlm_cxt->cgroup_last_stmt = u_sess->wlm_cxt->cgroup_stmt;
        u_sess->wlm_cxt->cgroup_stmt = WLMIsSpecialCommand(psrc->raw_parse_tree, portal);
    }

    /*
     * Send BindComplete.
     */
    if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote && !libpqsw_is_end())
        pq_putemptymessage('2');

    /*
     * Emit duration logging if appropriate.
     */
    switch (check_log_duration(msec_str, false)) {
        case 1:
            Assert(false);
            break;
        case 2: {
            char* mask_string = NULL;

            MASK_PASSWORD_START(mask_string, psrc->query_string);
            ereport(LOG,
                (errmsg("duration: %s ms queryid %ld unique id %ld bind %s%s%s: %s",
                     msec_str,
                     u_sess->debug_query_id,
                     u_sess->slow_query_cxt.slow_query.unique_sql_id,
                     *stmt_name ? stmt_name : "<unnamed>",
                     *portal_name ? "/" : "",
                     *portal_name ? portal_name : "",
                     mask_string),
                    errhidestmt(true),
                    errdetail_params(params)));
            MASK_PASSWORD_END(mask_string, psrc->query_string);
            break;
        }
        default:
            break;
    }

    if (save_log_statement_stats)
        ShowUsage("BIND MESSAGE STATISTICS");

    t_thrd.postgres_cxt.debug_query_string = NULL;
    t_thrd.postgres_cxt.cur_command_tag = T_Invalid;
    gstrace_exit(GS_TRC_ID_exec_bind_message);
}

/*
 * exec_execute_message
 *
 * Process an "Execute" message for a portal
 */
static void exec_execute_message(const char* portal_name, long max_rows)
{
    CommandDest dest;
    DestReceiver* receiver = NULL;
    Portal portal;
    bool completed = false;
    char completionTag[COMPLETION_TAG_BUFSIZE];
    const char* sourceText = NULL;
    const char* prepStmtName = NULL;
    ParamListInfo portalParams;
    bool save_log_statement_stats = u_sess->attr.attr_common.log_statement_stats;
    bool is_xact_command = false;
    bool execute_is_fetch = false;
    bool was_logged = false;
    char msec_str[PRINTF_DST_MAX];

    gstrace_entry(GS_TRC_ID_exec_execute_message);
    /* Adjust destination to tell printtup.c what to do */
    dest = (CommandDest)t_thrd.postgres_cxt.whereToSendOutput;
    if (dest == DestRemote)
        dest = DestRemoteExecute;

    /*
     * Only support normal perf mode for PBE, as DestRemoteExecute can not send T message automatically.
     */
    t_thrd.explain_cxt.explain_perf_mode = EXPLAIN_NORMAL;
    u_sess->pbe_message = EXECUTE_MESSAGE_QUERY;

    portal = GetPortalByName(portal_name);
    if (!PortalIsValid(portal))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CURSOR), errmsg("portal \"%s\" does not exist", portal_name)));

    /*
     * If the original query was a null string, just return
     * EmptyQueryResponse.
     */
    if (portal->commandTag == NULL) {
        if (portal->stmts != NIL) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("stmts is not NULL")));
        }
        NullCommand(dest);

        if (ENABLE_WORKLOAD_CONTROL) {
            if (g_instance.wlm_cxt->dynamic_workload_inited) {
                dywlm_client_max_release(&t_thrd.wlm_cxt.parctl_state);
            } else {
                WLMParctlRelease(&t_thrd.wlm_cxt.parctl_state);
            }
        }
        gstrace_exit(GS_TRC_ID_exec_execute_message);
        return;
    }


#ifndef ENABLE_MULTIPLE_NODES
    portal->streamInfo.AttachToSession();
    if (u_sess->stream_cxt.global_obj != NULL) {
        StreamTopConsumerIam();
    }
#endif

    /* Does the portal contain a transaction command? */
    is_xact_command = IsTransactionStmtList(portal->stmts);

    /*
     * We must copy the sourceText and prepStmtName into t_thrd.mem_cxt.msg_mem_cxt in
     * case the portal is destroyed during finish_xact_command. Can avoid the
     * copy if it's not an xact command, though.
     */
    sourceText = pstrdup(portal->sourceText);
    if (is_xact_command) {
        if (portal->prepStmtName)
            prepStmtName = pstrdup(portal->prepStmtName);
        else
            prepStmtName = "<unnamed>";

        /*
         * An xact command shouldn't have any parameters, which is a good
         * thing because they wouldn't be around after finish_xact_command.
         */
        portalParams = NULL;
    } else {
        if (portal->prepStmtName)
            prepStmtName = portal->prepStmtName;
        else
            prepStmtName = "<unnamed>";
        portalParams = portal->portalParams;
    }

    /*
     * Report query to various monitoring facilities.
     */
    t_thrd.postgres_cxt.debug_query_string = sourceText;
    if (strncmp(portal->commandTag, "SELECT", strlen("SELECT")) == 0) {
        /* SELECT INTO is select stmt too */
        t_thrd.postgres_cxt.cur_command_tag = T_SelectStmt;
    } else if (strcmp(portal->commandTag, "SHOW") == 0) {
        t_thrd.postgres_cxt.cur_command_tag = T_VariableShowStmt;
    } else if (strcmp(portal->commandTag, "CALL") == 0) {
        t_thrd.postgres_cxt.cur_command_tag = T_DolphinCallStmt;
    } else {
        t_thrd.postgres_cxt.cur_command_tag = T_CreateStmt;
    }

    pgstat_report_activity(STATE_RUNNING, sourceText);

    set_ps_display(portal->commandTag, false);

    if (save_log_statement_stats)
        ResetUsage();

    BeginCommand(portal->commandTag, dest);

    /*
     * Create dest receiver in t_thrd.mem_cxt.msg_mem_cxt (we don't want it in transaction
     * context, because that may get deleted if portal contains VACUUM).
     */
    receiver = CreateDestReceiver(dest);
    if (dest == DestRemoteExecute)
        SetRemoteDestReceiverParams(receiver, portal);

    /*
     * Ensure we are in a transaction command (this should normally be the
     * case already due to prior BIND).
     */
    start_xact_command();

    /* set unique sql id */
    SetUniqueSQLIdFromPortal(portal, u_sess->pcache_cxt.unnamed_stmt_psrc);

    /*
     * If we re-issue an Execute protocol request against an existing portal,
     * then we are only fetching more rows rather than completely re-executing
     * the query from the start. atStart is never reset for a v3 portal, so we
     * are safe to use this check.
     */
    execute_is_fetch = !portal->atStart;

    /*
     * instr unique sql: fetch portal case will call exec_execute_message multi times,
     * and the message from JDBC driver looks like 'P/BDES/ES/ES/.../ES', we update sql
     * elapse time/n_calls in message 'E', so in fetch case, we need update sql start
     * time in B/E message.
     */
    instr_unique_sql_set_start_time(execute_is_fetch, GetCurrentStatementLocalStartTimestamp(),
        u_sess->unique_sql_cxt.unique_sql_start_time);

    /* Log immediately if dictated by log_statement */
    if (check_log_statement(portal->stmts)) {
        char* mask_string = NULL;

        MASK_PASSWORD_START(mask_string, sourceText);
        ereport(LOG,
            (errmsg("%s %s%s%s: %s",
                 execute_is_fetch ? _("execute fetch from") : _("execute"),
                 prepStmtName,
                 *portal_name ? "/" : "",
                 *portal_name ? portal_name : "",
                 mask_string),
                errhidestmt(true),
                errdetail_params(portalParams)));
        MASK_PASSWORD_END(mask_string, sourceText);

        was_logged = true;
    }

    /*
     * If we are in aborted transaction state, the only portals we can
     * actually run are those containing COMMIT or ROLLBACK commands.
     */
    if (IsAbortedTransactionBlockState() && !IsTransactionExitStmtList(portal->stmts))
        ereport(ERROR,
            (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                errmsg("current transaction is aborted, "
                    "commands ignored until end of transaction block, firstChar[%c]",
                    u_sess->proc_cxt.firstChar),
                errdetail_abort()));

    /* Check for cancel signal before we start execution */
    CHECK_FOR_INTERRUPTS();

    /*
     * Okay to run the portal.
     */
    if (max_rows <= 0)
        max_rows = FETCH_ALL;

    bool savedisAllowCommitRollback = false;
    savedisAllowCommitRollback = stp_enable_and_get_old_xact_stmt_state();

    completed = PortalRun(portal,
        max_rows,
        true, /* always top level */
        receiver,
        receiver,
        completionTag);

    // for fetch size, free stringinfo each time
    if (!completed && dest == DestRemoteExecute) {
        DR_printtup* myState = (DR_printtup*)receiver;
        StringInfo buf = &myState->buf;
        pfree_ext(buf->data);
    }

    stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
    (*receiver->rDestroy)(receiver);

    if (completed) {
        if (is_xact_command) {
            /*
             * If this was a transaction control statement, commit it.	We
             * will start a new xact command for the next command (if any).
             */
            finish_xact_command();
        } else {
            /*
             * We need a CommandCounterIncrement after every query, except
             * those that start or end a transaction block.
             */
            CommandCounterIncrement();
        }

        /* Send appropriate CommandComplete to client */
        EndCommand(completionTag, dest);
        u_sess->xact_cxt.pbe_execute_complete = true;
    } else {
        /* Portal run not complete, so send PortalSuspended */
        if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote)
            pq_putemptymessage('s');

        u_sess->xact_cxt.pbe_execute_complete = false;
        /* when only set maxrows, we don't need to set pbe_execute_complete flag. */
        if ((portal_name == NULL || portal_name[0] == '\0') &&
            max_rows != FETCH_ALL && IsConnFromApp()) {
            u_sess->xact_cxt.pbe_execute_complete = true;
        }
#ifndef ENABLE_MULTIPLE_NODES
        /* reset stream info for session */
        if (u_sess->stream_cxt.global_obj != NULL) {
            portal->streamInfo.ResetEnv();
        }
#endif
    }

    if (ENABLE_WORKLOAD_CONTROL) {
        if (g_instance.wlm_cxt->dynamic_workload_inited) {
            if (t_thrd.wlm_cxt.parctl_state.simple == 0)
                dywlm_client_release(&t_thrd.wlm_cxt.parctl_state);
            else
                WLMReleaseGroupActiveStatement();
            dywlm_client_max_release(&t_thrd.wlm_cxt.parctl_state);
        } else
            WLMParctlRelease(&t_thrd.wlm_cxt.parctl_state);
    }

    /*
     * Emit duration logging if appropriate.
     */
    switch (check_log_duration(msec_str, was_logged)) {
        case 1:
            ereport(LOG, (errmsg("duration: %s ms queryid %ld unique id %ld", msec_str,
                u_sess->debug_query_id, u_sess->slow_query_cxt.slow_query.unique_sql_id), errhidestmt(true)));
            break;
        case 2: {
            char* mask_string = NULL;

            MASK_PASSWORD_START(mask_string, sourceText);
            ereport(LOG,
                (errmsg("duration: %s ms queryid %ld unique id %ld %s %s%s%s: %s",
                     msec_str,
                     u_sess->debug_query_id,
                     u_sess->slow_query_cxt.slow_query.unique_sql_id,
                     execute_is_fetch ? _("execute fetch from") : _("execute"),
                     prepStmtName,
                     *portal_name ? "/" : "",
                     *portal_name ? portal_name : "",
                     mask_string),
                    errhidestmt(true),
                    errdetail_params(portalParams)));
            MASK_PASSWORD_END(mask_string, sourceText);
            break;
        }
        default:
            break;
    }

    if (MEMORY_TRACKING_QUERY_PEAK)
        ereport(LOG, (errmsg("execute portal %s, peak memory %ld(kb)", sourceText,
                             (int64)(t_thrd.utils_cxt.peakedBytesInQueryLifeCycle/1024))));

    if (save_log_statement_stats)
        ShowUsage("EXECUTE MESSAGE STATISTICS");

    t_thrd.postgres_cxt.debug_query_string = NULL;
    t_thrd.postgres_cxt.cur_command_tag = T_Invalid;
    gstrace_exit(GS_TRC_ID_exec_execute_message);
}

/*
 * check_log_statement
 *		Determine whether command should be logged because of log_statement
 *
 * parsetree_list can be either raw grammar output or a list of planned
 * statements
 */
bool check_log_statement(List* stmt_list)
{
    ListCell* stmt_item = NULL;

    if (u_sess->attr.attr_common.log_statement == LOGSTMT_NONE)
        return false;
    if (u_sess->attr.attr_common.log_statement == LOGSTMT_ALL)
        return true;

    /* Else we have to inspect the statement(s) to see whether to log */
    foreach (stmt_item, stmt_list) {
        Node* stmt = (Node*)lfirst(stmt_item);

        if (GetCommandLogLevel(stmt) <= u_sess->attr.attr_common.log_statement)
            return true;
    }

    return false;
}

/*
 * check_log_duration
 *		Determine whether current command's duration should be logged
 *
 * Returns:
 *		0 if no logging is needed
 *		1 if just the duration should be logged
 *		2 if duration and query details should be logged
 *
 * If logging is needed, the duration in msec is formatted into msec_str[],
 * which must be a 32-byte buffer.
 *
 * was_logged should be TRUE if caller already logged query details (this
 * essentially prevents 2 from being returned).
 */
int check_log_duration(char* msec_str, bool was_logged)
{
    if (u_sess->attr.attr_sql.log_duration || u_sess->attr.attr_storage.log_min_duration_statement >= 0) {
        long secs;
        int usecs;
        int msecs;
        bool exceeded = false;

        u_sess->slow_query_cxt.slow_query.debug_query_sql_id = u_sess->debug_query_id;

        TimestampDifference(GetCurrentStatementLocalStartTimestamp(), GetCurrentTimestamp(), &secs, &usecs);
        msecs = usecs / 1000;

        /*
         * This odd-looking test for log_min_duration_statement being exceeded
         * is designed to avoid integer overflow with very long durations:
         * don't compute secs * 1000 until we've verified it will fit in int.
         */
        exceeded = (u_sess->attr.attr_storage.log_min_duration_statement == 0 ||
                    (u_sess->attr.attr_storage.log_min_duration_statement > 0 &&
                        (secs > u_sess->attr.attr_storage.log_min_duration_statement / 1000 ||
                            secs * 1000 + msecs >= u_sess->attr.attr_storage.log_min_duration_statement)));

        /*
         * Only record the time, which is larger than log_min_duration_statement.
         * This condition can reduce the impactation on performance.
         */
        if (exceeded) {
            if (u_sess->attr.attr_sql.log_duration) {
                errno_t rc =
                    snprintf_s(msec_str, PRINTF_DST_MAX, PRINTF_DST_MAX - 1, "%ld.%03d", secs * 1000 + msecs, usecs % 1000);
                securec_check_ss(rc, "", "");
                if (exceeded && !was_logged) {
                    return 2;
                } else {
                    return 1;
                }
            }
        }
    }

    msec_str[0] = '\0';
    return 0;
}

/*
 * errdetail_execute
 *
 * Add an errdetail() line showing the query referenced by an EXECUTE, if any.
 * The argument is the raw parsetree list.
 */
static int errdetail_execute(List* raw_parsetree_list)
{
    ListCell* parsetree_item = NULL;

    foreach (parsetree_item, raw_parsetree_list) {
        Node* parsetree = (Node*)lfirst(parsetree_item);

        if (IsA(parsetree, ExecuteStmt)) {
            ExecuteStmt* stmt = (ExecuteStmt*)parsetree;
            PreparedStatement *pstmt = NULL;

            pstmt = FetchPreparedStatement(stmt->name, false, false);
            if (pstmt != NULL) {
                errdetail("prepare: %s", pstmt->plansource->query_string);
                return 0;
            }
        }
    }

    return 0;
}

/*
 * get_param_str
 *
 * Forming param string info from ParamListInfo.
 */
static void get_param_str(StringInfo param_str, ParamListInfo params)
{
    Assert(params != NULL);
    for (int paramno = 0; paramno < params->numParams; paramno++) {
        ParamExternData* prm = &params->params[paramno];
        Oid typoutput;
        bool typisvarlena = false;
        char* pstring = NULL;

        appendStringInfo(param_str, "%s$%d = ", (paramno > 0) ? ", " : "", paramno + 1);

        if (prm->isnull || !OidIsValid(prm->ptype)) {
            appendStringInfoString(param_str, "NULL");
            continue;
        }

        getTypeOutputInfo(prm->ptype, &typoutput, &typisvarlena);
        pstring = OidOutputFunctionCall(typoutput, prm->value);

        appendStringInfoCharMacro(param_str, '\'');
        char* chunk_search_start = pstring;
        char* chunk_copy_start = pstring;
        char* chunk_end = NULL;
        while ((chunk_end = strchr(chunk_search_start, '\'')) != NULL) {
            /* copy including the found delimiting ' */
            appendBinaryStringInfoNT(param_str,
                                     chunk_copy_start,
                                     chunk_end - chunk_copy_start + 1);
            /* in order to double it, include this ' into the next chunk as well */
            chunk_copy_start = chunk_end;
            chunk_search_start = chunk_end + 1;
        }
        appendStringInfo(param_str, "%s'", chunk_copy_start);
        pfree(pstring);
    }

}

/*
 * errdetail_params
 *
 * Add an errdetail() line showing bind-parameter data, if available.
 */
int errdetail_params(ParamListInfo params)
{
    /* We mustn't call user-defined I/O functions when in an aborted xact */
    if (params && params->numParams > 0 && !IsAbortedTransactionBlockState()) {
        StringInfoData param_str;
        MemoryContext oldcontext;

        /* Make sure any trash is generated in t_thrd.mem_cxt.msg_mem_cxt */
        oldcontext = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

        initStringInfo(&param_str);

        get_param_str(&param_str, params);

        errdetail("parameters: %s", param_str.data);

        pfree(param_str.data);

        MemoryContextSwitchTo(oldcontext);
    }

    return 0;
}

/*
 * errdetail_batch_params
 *
 * Add an errdetail() line showing bind-batch-parameter data, if available.
 */
int errdetail_batch_params(int batchCount, int numParams, ParamListInfo* params_set)
{
    /* We mustn't call user-defined I/O functions when in an aborted xact */
    if (batchCount <= 0 || params_set == NULL || numParams <= 0 || IsAbortedTransactionBlockState()) {
        return 0;
    }
    MemoryContext oldContext;
    /* Make sure any trash is generated in t_thrd.mem_cxt.msg_mem_cxt */
    oldContext = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

    StringInfoData param_str;
    initStringInfo(&param_str);

    for (int i = 0; i < batchCount; i++) {
        appendStringInfo(&param_str, "(");
        ParamListInfo params = params_set[i];
        get_param_str(&param_str, params);
        appendStringInfo(&param_str, ")");
        if (i < batchCount - 1) {
            appendStringInfo(&param_str, ",");
        }
    }

    errdetail("batch parameters: %s", param_str.data);
    FreeStringInfo(&param_str);

    (void)MemoryContextSwitchTo(oldContext);

    return 0;
}
/*
 * errdetail_abort
 *
 * Add an errdetail() line showing abort reason, if any.
 */
int errdetail_abort(void)
{
    if (t_thrd.proc->recoveryConflictPending)
        errdetail("abort reason: recovery conflict");

    return 0;
}

/*
 * errdetail_recovery_conflict
 *
 * Add an errdetail() line showing conflict source.
 */
static int errdetail_recovery_conflict(void)
{
    switch (t_thrd.postgres_cxt.RecoveryConflictReason) {
        case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:
            errdetail("User was holding shared buffer pin for too long.");
            break;
        case PROCSIG_RECOVERY_CONFLICT_LOCK:
            errdetail("User was holding a relation lock for too long.");
            break;
        case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
            errdetail("User was or might have been using tablespace that must be dropped.");
            break;
        case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:
            errdetail("User query might have needed to see row versions that must be removed.");
            break;
        case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:
            errdetail("User transaction caused buffer deadlock with recovery.");
            break;
        case PROCSIG_RECOVERY_CONFLICT_DATABASE:
            errdetail("User was connected to a database that must be dropped.");
            break;
        default:
            break;
            /* no errdetail */
    }

    return 0;
}

/*
 * exec_describe_statement_message
 *
 * Process a "Describe" message for a prepared statement
 */
#ifndef ENABLE_UT
static void exec_describe_statement_message(const char* stmt_name)
#else
void exec_describe_statement_message(const char* stmt_name)
#endif
{
    CachedPlanSource* psrc = NULL;
    int i;

    /*
     * Start up a transaction command. (Note that this will normally change
     * current memory context.) Nothing happens if we are already in one.
     */
    start_xact_command();

    /* Switch back to message context */
    MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

    /* Find prepared statement */
    if (stmt_name[0] != '\0') {
        if (ENABLE_DN_GPC) {
            psrc = u_sess->pcache_cxt.cur_stmt_psrc;
            if (SECUREC_UNLIKELY(psrc == NULL))
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                        errmsg("dn gpc's prepared statement %s does not exist", stmt_name)));
        } else {
            PreparedStatement *pstmt = NULL;
            pstmt = FetchPreparedStatement(stmt_name, true, true);
            psrc = pstmt->plansource;
        }
    } else {
        /* special-case the unnamed statement */
        psrc = u_sess->pcache_cxt.unnamed_stmt_psrc;
        if (psrc == NULL)
            ereport(
                ERROR, (errcode(ERRCODE_UNDEFINED_PSTATEMENT), errmsg("unnamed prepared statement does not exist")));
    }

    Assert(NULL != psrc);

    /* Prepared statements shouldn't have changeable result descs */
    Assert(psrc->fixed_result);

#ifdef ENABLE_MOT
    /* set current transaction storage engine */
    SetCurrentTransactionStorageEngine(psrc->storageEngineType);
#endif

    /*
     * If we are in aborted transaction state, we can't run
     * SendRowDescriptionMessage(), because that needs catalog accesses.
     * Hence, refuse to Describe statements that return data.  (We shouldn't
     * just refuse all Describes, since that might break the ability of some
     * clients to issue COMMIT or ROLLBACK commands, if they use code that
     * blindly Describes whatever it does.)  We can Describe parameters
     * without doing anything dangerous, so we don't restrict that.
     */
    if (IsAbortedTransactionBlockState() && psrc->resultDesc != NULL)
        ereport(ERROR,
            (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                errmsg("current transaction is aborted, "
                    "commands ignored until end of transaction block, firstChar[%c]",
                    u_sess->proc_cxt.firstChar),
                errdetail_abort()));

    if (t_thrd.postgres_cxt.whereToSendOutput != DestRemote)
        return; /* can't actually do anything... */

    /*
     * First describe the parameters...
     */
    pq_beginmessage_reuse(&(*t_thrd.postgres_cxt.row_description_buf), 't'); /* parameter description message type */
    pq_sendint16(&(*t_thrd.postgres_cxt.row_description_buf), psrc->num_params);

    for (i = 0; i < psrc->num_params; i++) {
        Oid ptype = psrc->param_types[i];

        pq_sendint32(&(*t_thrd.postgres_cxt.row_description_buf), (int)ptype);
    }
    pq_endmessage_reuse(&(*t_thrd.postgres_cxt.row_description_buf));

    /*
     * Next send RowDescription or NoData to describe the result...
     */
    if (psrc->resultDesc) {
        List* tlist = NIL;

        /* Get the plan's primary targetlist */
        tlist = CachedPlanGetTargetList(psrc);

        SendRowDescriptionMessage(&(*t_thrd.postgres_cxt.row_description_buf), psrc->resultDesc, tlist, NULL);
    } else
        pq_putemptymessage('n'); /* NoData */
}

/*
 * exec_describe_portal_message
 *
 * Process a "Describe" message for a portal
 */
static void exec_describe_portal_message(const char* portal_name)
{
    Portal portal;

    /*
     * Start up a transaction command. (Note that this will normally change
     * current memory context.) Nothing happens if we are already in one.
     */
    start_xact_command();

    /* Switch back to message context */
    MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

    portal = GetPortalByName(portal_name);
    if (!PortalIsValid(portal))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CURSOR), errmsg("portal \"%s\" does not exist", portal_name)));

    Assert(NULL != portal);

    /*
     * If we are in aborted transaction state, we can't run
     * SendRowDescriptionMessage(), because that needs catalog accesses.
     * Hence, refuse to Describe portals that return data.	(We shouldn't just
     * refuse all Describes, since that might break the ability of some
     * clients to issue COMMIT or ROLLBACK commands, if they use code that
     * blindly Describes whatever it does.)
     */
    if (IsAbortedTransactionBlockState() && portal->tupDesc)
        ereport(ERROR,
            (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                errmsg("current transaction is aborted, "
                    "commands ignored until end of transaction block, firstChar[%c]",
                    u_sess->proc_cxt.firstChar),
                errdetail_abort()));

    if (t_thrd.postgres_cxt.whereToSendOutput != DestRemote)
        return; /* can't actually do anything... */

    if (portal->tupDesc)
        SendRowDescriptionMessage(&(*t_thrd.postgres_cxt.row_description_buf),
            portal->tupDesc,
            FetchPortalTargetList(portal),
            portal->formats);
    else
        pq_putemptymessage('n'); /* NoData */
}

/*
 * Convenience routines for starting/committing a single command.
 */
void start_xact_command(void)
{
    if (!t_thrd.postgres_cxt.xact_started) {
        if (SHOW_DEBUG_MESSAGE()) {
            ereport(DEBUG3, (errmsg_internal("StartTransactionCommand")));
        }

        StartTransactionCommand();

        /* Set statement timeout running, if any */
        /* NB: this mustn't be enabled until we are within an xact */
        if (u_sess->attr.attr_common.StatementTimeout > 0)
            enable_sig_alarm(u_sess->attr.attr_common.StatementTimeout, true);
        else
            t_thrd.storage_cxt.cancel_from_timeout = false;

        t_thrd.postgres_cxt.xact_started = true;
    }
}

void finish_xact_command(void)
{
    if (t_thrd.postgres_cxt.xact_started) {
        /* Cancel any active statement timeout before committing */
        disable_sig_alarm(true);

        /* Now commit the command */
        if (SHOW_DEBUG_MESSAGE()) {
            ereport(DEBUG3, (errmsg_internal("CommitTransactionCommand")));
        }

#ifdef ENABLE_MOT
        CallXactCallbacks(XACT_EVENT_STMT_FINISH);
#endif

        CommitTransactionCommand();

#ifdef MEMORY_CONTEXT_CHECKING
        /* Check all memory contexts that weren't freed during commit */
        /* (those that were, were checked before being deleted) */
        MemoryContextCheck(t_thrd.top_mem_cxt, false);

        if (IS_THREAD_POOL_WORKER && u_sess && u_sess->session_id > 0)
            MemoryContextCheck(u_sess->top_mem_cxt, true);
#endif

#ifdef SHOW_MEMORY_STATS
        /* Print mem stats after each commit for leak tracking */
        MemoryContextStats(t_thrd.top_mem_cxt);
#endif

        t_thrd.postgres_cxt.xact_started = false;
    }
}

/*
 * Convenience routines for checking whether a statement is one of the
 * ones that we allow in transaction-aborted state.
 */

/* Test a bare parsetree */
bool IsTransactionExitStmt(Node* parsetree)
{
    if (parsetree && IsA(parsetree, TransactionStmt)) {
        TransactionStmt* stmt = (TransactionStmt*)parsetree;

        if (stmt->kind == TRANS_STMT_COMMIT || stmt->kind == TRANS_STMT_PREPARE || stmt->kind == TRANS_STMT_ROLLBACK ||
            stmt->kind == TRANS_STMT_ROLLBACK_TO)
            return true;
    }
    return false;
}

/* Test a list that might contain Query nodes or bare parsetrees */
static bool IsTransactionExitStmtList(List* parseTrees)
{
    if (list_length(parseTrees) == 1) {
        Node* stmt = (Node*)linitial(parseTrees);

        if (IsA(stmt, Query)) {
            Query* query = (Query*)stmt;

            if (query->commandType == CMD_UTILITY && IsTransactionExitStmt(query->utilityStmt))
                return true;
        } else if (IsTransactionExitStmt(stmt))
            return true;
    }
    return false;
}

/* Test a list that might contain Query nodes or bare parsetrees */
static bool IsTransactionStmtList(List* parseTrees)
{
    if (list_length(parseTrees) == 1) {
        Node* stmt = (Node*)linitial(parseTrees);

        if (IsA(stmt, Query)) {
            Query* query = (Query*)stmt;

            if (query->commandType == CMD_UTILITY && IsA(query->utilityStmt, TransactionStmt))
                return true;
        } else if (IsA(stmt, TransactionStmt))
            return true;
    }
    return false;
}

#ifdef ENABLE_MOT
static bool IsTransactionPrepareStmt(const Node* parsetree)
{
    if (parsetree && IsA(parsetree, TransactionStmt)) {
        TransactionStmt* stmt = (TransactionStmt*)parsetree;
        if (stmt->kind == TRANS_STMT_PREPARE) {
            return true;
        }
    }
    return false;
}
#endif

/* Release any existing unnamed prepared statement */
static void drop_unnamed_stmt(void)
{
    /* paranoia to avoid a dangling pointer in case of error */
    if (u_sess->pcache_cxt.unnamed_stmt_psrc) {
        CachedPlanSource* psrc = u_sess->pcache_cxt.unnamed_stmt_psrc;
        u_sess->pcache_cxt.unnamed_stmt_psrc = NULL;
        if (psrc->gpc.status.InShareTable()) {
            /* drop unnamed lightproxy */
            if (u_sess->pcache_cxt.unnamed_gpc_lp) {
                lightProxy* lp = u_sess->pcache_cxt.unnamed_gpc_lp;
                u_sess->pcache_cxt.unnamed_gpc_lp = NULL;
                Assert(lp->m_cplan == psrc);
                Assert(lp->m_stmtName == NULL || lp->m_stmtName[0] == '\0');
                if (lp->m_portalName == NULL || lightProxy::locateLightProxy(lp->m_portalName) == NULL) {
                    lightProxy::tearDown(lp);
                }
            }
            GPC_LOG("delete shared unnamed plansource", psrc, psrc->stmt_name);
            psrc->gpc.status.SubRefCount();
        } else {
            Assert(u_sess->pcache_cxt.unnamed_gpc_lp == NULL);
            if (ENABLE_GPC) {
                GPC_LOG("drop private plansource", psrc, psrc->stmt_name);
            }
            DropCachedPlan(psrc);
            if (ENABLE_GPC) {
                GPC_LOG("drop private plansource end", psrc, 0);
            }
        }
    }
}


/* --------------------------------
 *		signal handler routines used in PostgresMain()
 * --------------------------------
 */

/*
 * quickdie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */
void quickdie(SIGNAL_ARGS)
{
    if (t_thrd.int_cxt.ignoreBackendSignal) {
        return;
    }
    sigaddset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT); /* prevent nested calls */
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    /*
     * If we're aborting out of client auth, don't risk trying to send
     * anything to the client; we will likely violate the protocol, not to
     * mention that we may have interrupted the guts of OpenSSL or some
     * authentication library.
     */
    if (u_sess->ClientAuthInProgress && t_thrd.postgres_cxt.whereToSendOutput == DestRemote)
        t_thrd.postgres_cxt.whereToSendOutput = DestNone;

    /*
     * Ideally this should be ereport(FATAL), but then we'd not get control
     * back...
     */
    ereport(WARNING,
        (errcode(ERRCODE_CRASH_SHUTDOWN),
            errmsg("terminating connection because of crash of another server process"),
            errdetail("The postmaster has commanded this server process to roll back"
                      " the current transaction and exit, because another"
                      " server process exited abnormally and possibly corrupted"
                      " shared memory."),
            errhint("In a moment you should be able to reconnect to the"
                    " database and repeat your command.")));

    /*
     * We DO NOT want to run proc_exit() callbacks -- we're here because
     * shared memory may be corrupted, so we don't want to try to clean up our
     * transaction.  Just nail the windows shut and get out of town.  Now that
     * there's an atexit callback to prevent third-party code from breaking
     * things by calling exit() directly, we have to reset the callbacks
     * explicitly to make this work as intended.
     */
    on_exit_reset();

    /*
     * Note we do exit(2) not exit(0).	This is to force the postmaster into a
     * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
     * backend.  This is necessary precisely because we don't clean up our
     * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
     * should ensure the postmaster sees this as a crash, too, but no harm in
     * being doubly sure.)
     */
    exit(2);
}

/*
 * Shutdown signal from postmaster: abort transaction and exit
 * at soonest convenient time
 */
void die(SIGNAL_ARGS)
{
    if (t_thrd.int_cxt.ignoreBackendSignal) {
        return;
    }
    int save_errno = errno;

    /* Don't joggle the elbow of proc_exit */
    if (!t_thrd.proc_cxt.proc_exit_inprogress) {
        instrSnapshotCancel();

        InterruptPending = true;
        t_thrd.int_cxt.ProcDiePending = true;

        /*  Set flag to handle SIGUSER2 and SIGTERM during connection */
        if (t_thrd.proc_cxt.pooler_connection_inprogress) {
            g_pq_interrupt_happened = true;
        }

        /*
         * in libcomm interrupt is not allow,
         * gs_r_cancel will signal libcomm and
         * libcomm will then check for interrupt.
         */
        gs_r_cancel();

        /*
         * If it's safe to interrupt, and we're waiting for input or a lock,
         * service the interrupt immediately
         */
        if (t_thrd.int_cxt.ImmediateInterruptOK && t_thrd.int_cxt.InterruptHoldoffCount == 0 &&
            t_thrd.int_cxt.CritSectionCount == 0) {
            /* bump holdoff count to make ProcessInterrupts() a no-op */
            /* until we are done getting ready for it */
            t_thrd.int_cxt.InterruptHoldoffCount++;
            LockErrorCleanup(); /* prevent CheckDeadLock from running */
            t_thrd.int_cxt.InterruptHoldoffCount--;
            ProcessInterrupts();
        }
    }

    /* If we're still here, waken anything waiting on the process latch */
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    if (g_instance.attr.attr_common.light_comm == TRUE &&
        t_thrd.postgres_cxt.DoingCommandRead &&
        t_thrd.postgres_cxt.whereToSendOutput != DestRemote) {
        ProcessInterrupts();
    }
    errno = save_errno;
}

/*
 * Query-cancel signal from postmaster: abort current transaction
 * at soonest convenient time
 */
void StatementCancelHandler(SIGNAL_ARGS)
{
    if (t_thrd.int_cxt.ignoreBackendSignal) {
        return;
    }
    int save_errno = errno;

    /*
     * Don't joggle the elbow of proc_exit
     */
    if (!t_thrd.proc_cxt.proc_exit_inprogress) {
        InterruptPending = true;
        t_thrd.int_cxt.QueryCancelPending = true;

        /*
         * in libcomm interrupt is not allow,
         * gs_r_cancel will signal libcomm and
         * libcomm will then check for interrupt.
         */
        gs_r_cancel();

        /*
         * If it's safe to interrupt, and we're waiting for input or a lock,
         * service the interrupt immediately
         */
        if (t_thrd.int_cxt.ImmediateInterruptOK && t_thrd.int_cxt.InterruptHoldoffCount == 0 &&
            t_thrd.int_cxt.CritSectionCount == 0) {
            /* bump holdoff count to make ProcessInterrupts() a no-op */
            /* until we are done getting ready for it */
            t_thrd.int_cxt.InterruptHoldoffCount++;
            LockErrorCleanup(); /* prevent CheckDeadLock from running */
            t_thrd.int_cxt.InterruptHoldoffCount--;
            ProcessInterrupts();
        }
    }

    /* If we're still here, waken anything waiting on the process latch */
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/*
 * Query-cancel signal from pg_pool_validate: abort current transaction
 * at soonest convenient time
 */
void PoolValidateCancelHandler(SIGNAL_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (SignalCancelGTMConnection()) {
        /* If it is going to cancel GTM connection, return immediately. */
        return;
    }
#endif

    int save_errno = errno;
    t_thrd.int_cxt.InterruptByCN = true;

    /*
     * Don't joggle the elbow of proc_exit
     */
    if (!t_thrd.proc_cxt.proc_exit_inprogress) {
        InterruptPending = true;
        t_thrd.int_cxt.QueryCancelPending = true;
        t_thrd.int_cxt.PoolValidateCancelPending = true;

        /*  Set flag to handle SIGUSER2 and SIGTERM during connection */
        if (t_thrd.proc_cxt.pooler_connection_inprogress) {
            g_pq_interrupt_happened = true;
        }

        /*
         * in libcomm interrupt is not allow,
         * gs_r_cancel will signal libcomm and
         * libcomm will then check for interrupt.
         */
        gs_r_cancel();

        /*
         * If it's safe to interrupt, and we're waiting for input or a lock,
         * service the interrupt immediately
         */
        if (t_thrd.int_cxt.ImmediateInterruptOK && t_thrd.int_cxt.InterruptHoldoffCount == 0 &&
            t_thrd.int_cxt.CritSectionCount == 0) {
            /* bump holdoff count to make ProcessInterrupts() a no-op */
            /* until we are done getting ready for it */
            t_thrd.int_cxt.InterruptHoldoffCount++;
            LockErrorCleanup(); /* prevent CheckDeadLock from running */
            t_thrd.int_cxt.InterruptHoldoffCount--;
            ProcessInterrupts();
        }
    }

    /* If we're still here, waken anything waiting on the process latch */
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* signal handler for floating point exception */
void FloatExceptionHandler(SIGNAL_ARGS)
{
    Assert(0);
    /* We're not returning, so no need to save errno */
    ereport(ERROR,
        (errcode(ERRCODE_FLOATING_POINT_EXCEPTION),
            errmsg("floating-point exception"),
            errdetail("An invalid floating-point operation was signaled. "
                      "This probably means an out-of-range result or an "
                      "invalid operation, such as division by zero.")));
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void SigHupHandler(SIGNAL_ARGS)
{
    if (IS_THREAD_POOL_WORKER) {
        return;
    }

    int save_errno = errno;
    t_thrd.int_cxt.InterruptByCN = true;

    u_sess->sig_cxt.got_SIGHUP = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

// HandlePoolerReload
// 	set the flag got_PoolReload, and do pooler reload in the main loop.
//
void HandlePoolerReload(void)
{
    /* A Datanode has no pooler active, so do not bother about that */
    if (IS_PGXC_DATANODE)
        return;

    ResetGotPoolReload(true);
    u_sess->sig_cxt.cp_PoolReload = true;
}

// HandleMemoryContextDump
// 	call memory dump function directly in aset.cpp to dump all memory info.
//
void HandleMemoryContextDump(void)
{
#ifdef MEMORY_CONTEXT_CHECKING
    DumpMemoryContext(STANDARD_DUMP);
#endif
}

void HandleExecutorFlag(void)
{
    if (IS_PGXC_DATANODE)
        u_sess->exec_cxt.executorStopFlag = true;
}
/*
 * RecoveryConflictInterrupt: out-of-line portion of recovery conflict
 * handling following receipt of SIGUSR1. Designed to be similar to die()
 * and StatementCancelHandler(). Called only by a normal user backend
 * that begins a transaction during recovery.
 */
void RecoveryConflictInterrupt(ProcSignalReason reason)
{
    int save_errno = errno;

    /*
     * Don't joggle the elbow of proc_exit
     */
    if (!t_thrd.proc_cxt.proc_exit_inprogress) {
        t_thrd.postgres_cxt.RecoveryConflictReason = reason;
        switch (reason) {
            case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:

                /*
                 * If we aren't waiting for a lock we can never deadlock.
                 */
                if (!IsWaitingForLock())
                    return;

                /* Intentional drop through to check wait for pin */
                /* fall through */
            case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:

                /*
                 * If we aren't blocking the Startup process there is nothing
                 * more to do.
                 */
                if (!HoldingBufferPinThatDelaysRecovery())
                    return;

                t_thrd.proc->recoveryConflictPending = true;

                /* Intentional drop through to error handling */
                /* fall through */
            case PROCSIG_RECOVERY_CONFLICT_LOCK:
            case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
            case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:
                /*
                 * If we aren't in a transaction any longer then ignore.
                 */
                if (!IsTransactionOrTransactionBlock())
                    return;

                /*
                 * If we can abort just the current subtransaction then we are
                 * OK to throw an ERROR to resolve the conflict. Otherwise
                 * drop through to the FATAL case.
                 *
                 * XXX other times that we can throw just an ERROR *may* be
                 * PROCSIG_RECOVERY_CONFLICT_LOCK if no locks are held in
                 * parent transactions
                 *
                 * PROCSIG_RECOVERY_CONFLICT_SNAPSHOT if no snapshots are held
                 * by parent transactions and the transaction is not
                 * transaction-snapshot mode
                 *
                 * PROCSIG_RECOVERY_CONFLICT_TABLESPACE if no temp files or
                 * cursors open in parent transactions
                 */
                if (!IsSubTransaction()) {
                    /*
                     * If we already aborted then we no longer need to cancel.
                     * We do this here since we do not wish to ignore aborted
                     * subtransactions, which must cause FATAL, currently.
                     */
                    if (IsAbortedTransactionBlockState())
                        return;

                    t_thrd.postgres_cxt.RecoveryConflictPending = true;
                    t_thrd.int_cxt.QueryCancelPending = true;
                    InterruptPending = true;
                    break;
                }

                /* Intentional drop through to session cancel */
                /* fall through */
            case PROCSIG_RECOVERY_CONFLICT_DATABASE:
                t_thrd.postgres_cxt.RecoveryConflictPending = true;
                t_thrd.int_cxt.ProcDiePending = true;
                InterruptPending = true;
                break;

            default:
                ereport(FATAL, (errmsg("unrecognized conflict mode: %d", (int)reason)));
                break;
        }

        Assert(t_thrd.postgres_cxt.RecoveryConflictPending &&
               (t_thrd.int_cxt.QueryCancelPending || t_thrd.int_cxt.ProcDiePending));

        /*
         * All conflicts apart from database cause dynamic errors where the
         * command or transaction can be retried at a later point with some
         * potential for success. No need to reset this, since non-retryable
         * conflict errors are currently FATAL.
         */
        if (reason == PROCSIG_RECOVERY_CONFLICT_DATABASE)
            t_thrd.postgres_cxt.RecoveryConflictRetryable = false;

        /*
         * If it's safe to interrupt, and we're waiting for input or a lock,
         * service the interrupt immediately
         */
        if (t_thrd.int_cxt.ImmediateInterruptOK && t_thrd.int_cxt.InterruptHoldoffCount == 0 &&
            t_thrd.int_cxt.CritSectionCount == 0) {
            /* bump holdoff count to make ProcessInterrupts() a no-op */
            /* until we are done getting ready for it */
            t_thrd.int_cxt.InterruptHoldoffCount++;
            LockErrorCleanup(); /* prevent CheckDeadLock from running */
            t_thrd.int_cxt.InterruptHoldoffCount--;
            ProcessInterrupts();
        }
    }

    /*
     * Set the process latch. This function essentially emulates signal
     * handlers like die() and StatementCancelHandler() and it seems prudent
     * to behave similarly as they do. Alternatively all plain backend code
     * waiting on that latch, expecting to get interrupted by query cancels et
     * al., would also need to set set_latch_on_sigusr1.
     */
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

static void PrintWaitEvent()
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    if (beentry && beentry->st_waitevent != 0) {
        uint32 raw_wait_event = beentry->st_waitevent;
        const char* waitStatus = pgstat_get_waitstatusdesc(raw_wait_event);
        const char* waitEvent = pgstat_get_wait_event(raw_wait_event);
        ereport(LOG, (errmodule(MOD_PGSTAT),
                errmsg("Received interrupt signal, current wait status:%s, wait event:%s.", waitStatus, waitEvent)));
    }
}

/*
 * ProcessInterrupts: out-of-line portion of CHECK_FOR_INTERRUPTS() macro
 *
 * If an interrupt condition is pending, and it's safe to service it,
 * then clear the flag and accept the interrupt.  Called only when
 * InterruptPending is true.
 */
void ProcessInterrupts(void)
{
    /* OK to accept interrupt now? */
    if (t_thrd.int_cxt.InterruptHoldoffCount != 0 || t_thrd.int_cxt.CritSectionCount != 0)
        return;

    if (t_thrd.bn && ((unsigned int)(t_thrd.bn->flag) & THRD_SIGTERM)) {
        t_thrd.int_cxt.ProcDiePending = true;
        t_thrd.bn->flag = ((unsigned int)(t_thrd.bn->flag)) & ~THRD_SIGTERM;
    }

    // The 'u_sess->stream_cxt.in_waiting_quit' flag is set to true to enable signal handling when waiting sub stream
    // threads quit. At the same time, if we get a SIGTERM signal, this signal should be held and the 'InterruptPending'
    // flag should not be set to false immediately. After all sub thread quit and the top consumer goes back to
    // ReadCommand again, the pending interrupt can be safely handled in function prepare_for_client_read.
    //
    if (t_thrd.int_cxt.ProcDiePending && u_sess->stream_cxt.in_waiting_quit) {
        // It's more efficient to notify all stream threads to cancel the query first
        // and then top consumer can quit quickly.
        //
        StreamNodeGroup::cancelStreamThread();
        return;
    }

    if (StreamThreadAmI() && u_sess->debug_query_id == 0) {
        Assert(0);
    }

    InterruptPending = false;

    if (t_thrd.wlm_cxt.wlmalarm_pending) {
        t_thrd.wlm_cxt.wlmalarm_pending = false;
        (void)WLMProcessWorkloadManager();
    }

    if (t_thrd.int_cxt.ProcDiePending && !u_sess->stream_cxt.in_waiting_quit) {
        t_thrd.int_cxt.ProcDiePending = false;
        t_thrd.int_cxt.QueryCancelPending = false;   /* ProcDie trumps QueryCancel */
        t_thrd.int_cxt.ImmediateInterruptOK = false; /* not idle anymore */
	t_thrd.int_cxt.ClientConnectionLost = false; /* omit ClientConnectionLost, otherwise it will be rehandle in proc_exit */

        if (u_sess->stream_cxt.global_obj != NULL)
            u_sess->stream_cxt.global_obj->signalStreamThreadInNodeGroup(SIGTERM);

        if (u_sess->ClientAuthInProgress) {
            if (t_thrd.storage_cxt.cancel_from_timeout) {
                force_backtrace_messages = true;
            } else if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote) {
                t_thrd.postgres_cxt.whereToSendOutput = DestNone;
            }
        }
        if (IsAutoVacuumWorkerProcess()) {
            ereport(FATAL,
                (errcode(ERRCODE_ADMIN_SHUTDOWN),
                    errmsg("terminating autovacuum process due to administrator command")));
#ifndef ENABLE_MULTIPLE_NODES
        } else if (IsLogicalLauncher()) {
            ereport(DEBUG1, (errmsg("logical replication launcher shutting down")));

            /* The logical replication launcher can be stopped at any time. */
            proc_exit(0);
        } else if (IsLogicalWorker()) {
            ereport(FATAL, (errcode(ERRCODE_ADMIN_SHUTDOWN),
                errmsg("terminating logical replication worker due to administrator command")));
#endif
        } else if (IsTxnSnapCapturerProcess()) {
            ereport(FATAL,
                (errcode(ERRCODE_ADMIN_SHUTDOWN),
                    errmsg("terminating txnsnapcapturer process due to administrator command")));
        }
        else if (IsTxnSnapWorkerProcess()) {
            ereport(FATAL,
                (errcode(ERRCODE_ADMIN_SHUTDOWN),
                    errmsg("terminating txnsnapcapturer worker process due to administrator command")));
        }
        else if (IsCfsShrinkerProcess()) {
            ereport(FATAL,
                (errcode(ERRCODE_ADMIN_SHUTDOWN),
                    errmsg("terminating csf shrinker worker process due to administrator command")));
        }
#ifdef ENABLE_MULTIPLE_NODES
        else if (IsRedistributionWorkerProcess())
            ereport(FATAL,
                (errcode(ERRCODE_ADMIN_SHUTDOWN),
                    errmsg("terminating data redistribution process due to administrator command")));
#endif
        else if (t_thrd.postgres_cxt.RecoveryConflictPending && t_thrd.postgres_cxt.RecoveryConflictRetryable) {
            pgstat_report_recovery_conflict(t_thrd.postgres_cxt.RecoveryConflictReason);
            ereport(FATAL,
                (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                    errmsg("terminating connection due to conflict with recovery"),
                    errdetail_recovery_conflict()));
        } else if (t_thrd.postgres_cxt.RecoveryConflictPending) {
            /* Currently there is only one non-retryable recovery conflict */
            if (LWLockHeldByMe(ProcArrayLock)) {
                LWLockRelease(ProcArrayLock);
            }
            Assert(t_thrd.postgres_cxt.RecoveryConflictReason == PROCSIG_RECOVERY_CONFLICT_DATABASE);
            pgstat_report_recovery_conflict(t_thrd.postgres_cxt.RecoveryConflictReason);
            ereport(FATAL,
                (errcode(ERRCODE_DATABASE_DROPPED),
                    errmsg("terminating connection due to conflict with recovery"),
                    errdetail_recovery_conflict()));
        } else if (IsJobSnapshotProcess()) {
            pgstat_report_activity(STATE_IDLE, NULL);
            ereport(FATAL,
                (errcode(ERRCODE_ADMIN_SHUTDOWN), errmsg("terminating snapshot process due to administrator command")));
        } else
            ereport(FATAL,
                (errcode(ERRCODE_ADMIN_SHUTDOWN), errmsg("terminating connection due to administrator command")));
    }
    if (t_thrd.int_cxt.ClientConnectionLost && !u_sess->stream_cxt.in_waiting_quit) {
        t_thrd.int_cxt.QueryCancelPending = false;   /* lost connection trumps QueryCancel */
        t_thrd.int_cxt.ImmediateInterruptOK = false; /* not idle anymore */
        /* don't send to client, we already know the connection to be dead. */
        t_thrd.postgres_cxt.whereToSendOutput = DestNone;

        if (StreamThreadAmI() == false && !WLMProcessExiting) {
            // for thread pool, it's too waste to shutdown worker for session close.
            if (IS_THREAD_POOL_WORKER && t_thrd.threadpool_cxt.worker) {
                t_thrd.int_cxt.ClientConnectionLost = false;
                t_thrd.threadpool_cxt.reaper_dead_session = true;
                ereport(ERROR,
                    (errcode(ERRCODE_QUERY_CANCELED),
                        errmsg("detach and reaper session from thread due to session connection lost")));
            } else
                // if in the stream thread, we do not abort transaction,
                // it means the consumer thread has close the fd, so we got connection lost
                // error,if we abort transaction here, it may result a consistency transaction
                // status.
                // Neither we can set u_sess->exec_cxt.executor_stop_flag to true for now, as it only means
                // some consumer has close the fd, not all of the consumer
                // we can only set this if we are sure all the consumer has gone.
                // we need further optimize the logic here
                ereport(FATAL, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("connection to client lost")));
        }
    }
    if (t_thrd.int_cxt.QueryCancelPending) {
        int pool_validate_cancel_pending = t_thrd.int_cxt.PoolValidateCancelPending;
        t_thrd.int_cxt.QueryCancelPending = false;
        t_thrd.int_cxt.PoolValidateCancelPending = false;

        PrintWaitEvent();

        if (u_sess->ClientAuthInProgress) {
            t_thrd.int_cxt.ImmediateInterruptOK = false;        /* not idle anymore */
            int errlevel = ERROR;
            if (ENABLE_DMS && IS_THREAD_POOL_WORKER) {
                errlevel = FATAL;
            }

            if (t_thrd.storage_cxt.cancel_from_timeout) {
                force_backtrace_messages = true;
                ereport(errlevel,
                        (errcode(ERRCODE_QUERY_CANCELED),
                         errmsg("terminate because authentication timeout(%ds)",
                             u_sess->attr.attr_network.PoolerConnectTimeout)));
            } else {
                if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote) {
                    t_thrd.postgres_cxt.whereToSendOutput = DestNone;
                }
                ereport(errlevel,
                        (errcode(ERRCODE_QUERY_CANCELED),
                         errmsg("terminate because cancel interrupts")));
            }
        }

        if (t_thrd.storage_cxt.cancel_from_timeout || u_sess->wlm_cxt->cancel_from_wlm ||
            u_sess->wlm_cxt->cancel_from_space_limit || u_sess->wlm_cxt->cancel_from_defaultXact_readOnly ||
            g_pq_interrupt_happened) {
            t_thrd.int_cxt.ImmediateInterruptOK = false; /* not idle anymore */
            StreamNodeGroup::cancelStreamThread();
            char* str = NULL;
            int err_code = ERRCODE_QUERY_CANCELED;
            if (t_thrd.storage_cxt.cancel_from_timeout) {
                if (IsAutoVacuumWorkerProcess())
                    str = "autovacuum task timeout";
                else if (IsTxnSnapCapturerProcess()) {
                    str = "txnsnapcapturer task timeout";
                }
                else if (IsTxnSnapWorkerProcess()) {
                    str = "txnsnapworker task timeout";
                }
                else if (IsCfsShrinkerProcess()) {
                    str = "csf shrinker task timeout";
                } else
                    str = "statement timeout";
            } else if (u_sess->wlm_cxt->cancel_from_wlm) {
                str = "workload manager exception";
                u_sess->wlm_cxt->cancel_from_wlm = false;
            } else if (u_sess->wlm_cxt->cancel_from_space_limit) {
                str = "space limit exceed";
                u_sess->wlm_cxt->cancel_from_space_limit = false;
            } else if (u_sess->wlm_cxt->cancel_from_defaultXact_readOnly) {
                str = "default_transaction_read_only is on.";
                u_sess->wlm_cxt->cancel_from_defaultXact_readOnly = false;
            } else if (g_pq_interrupt_happened) {
                str = "connecting interrupted by pool validation.";
                g_pq_interrupt_happened = false;
                err_code = ERRCODE_CONNECTION_FAILURE;
            }
            CancelAutoAnalyze();
            lightProxy::setCurrentProxy(NULL);

            if (!u_sess->stream_cxt.in_waiting_quit) {
                if (t_thrd.wlm_cxt.collect_info->sdetail.msg)
                    ereport(ERROR,
                        (errcode(err_code),
                            errmsg("canceling statement due to %s.%s", str, t_thrd.wlm_cxt.collect_info->sdetail.msg)));
                else
                    ereport(ERROR, (errcode(err_code), errmsg("canceling statement due to %s", str)));
            }
        }

        if (IsAutoVacuumWorkerProcess()) {
            t_thrd.int_cxt.ImmediateInterruptOK = false; /* not idle anymore */
            ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("canceling autovacuum task")));
        }
        if (IsTxnSnapCapturerProcess()) {
            t_thrd.int_cxt.ImmediateInterruptOK = false; /* not idle anymore */
            ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("canceling txnsnapcapturer task")));
        }
        if (IsTxnSnapWorkerProcess()) {
            t_thrd.int_cxt.ImmediateInterruptOK = false; /* not idle anymore */
            ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("canceling txnsnapworker task")));
        }
        if (IsCfsShrinkerProcess()) {
            t_thrd.int_cxt.ImmediateInterruptOK = false; /* not idle anymore */
            ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("canceling csf shrinker task")));
        }
#ifdef ENABLE_MULTIPLE_NODES
        if (IsRedistributionWorkerProcess()) {

            t_thrd.int_cxt.ImmediateInterruptOK = false; /* not idle anymore */
            ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("canceling data redistribution task")));
        }
#endif
        if (t_thrd.postgres_cxt.RecoveryConflictPending && !u_sess->stream_cxt.in_waiting_quit) {
            t_thrd.int_cxt.ImmediateInterruptOK = false; /* not idle anymore */
            t_thrd.postgres_cxt.RecoveryConflictPending = false;
            pgstat_report_recovery_conflict(t_thrd.postgres_cxt.RecoveryConflictReason);
            if (t_thrd.postgres_cxt.DoingCommandRead)
                ereport(FATAL,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg("terminating connection due to conflict with recovery"),
                        errdetail_recovery_conflict(),
                        errhint("In a moment you should be able to reconnect to the"
                                " database and repeat your command.")));
            else
                ereport(ERROR,
                    (errcode(ERRCODE_SR_RECOVERY_CONFLICT),
                        errmsg("canceling statement due to conflict with recovery"),
                        errdetail_recovery_conflict()));
        }

        /*
         * If we are reading a command from the client, just ignore the cancel
         * request --- sending an extra error message won't accomplish
         * anything.  Otherwise, go ahead and throw the error.
         */
        if (!t_thrd.postgres_cxt.DoingCommandRead) {
            t_thrd.int_cxt.ImmediateInterruptOK = false; /* not idle anymore */
            StreamNodeGroup::cancelStreamThread();
            CancelAutoAnalyze();
            lightProxy::setCurrentProxy(NULL);

            if (!u_sess->stream_cxt.in_waiting_quit) {
                /*
                 * User direct cancel and error cancel are both driven by Coordinator. Internal cancel
                 * (including canceling due to coordinator request and canceling child stream thread)
                 * can be ignored in Coordinator. So we distinguish it from user request one by setting
                 * different error code.
                 *
                 * After pg_pool_validate, we should use ERRCODE_CONNECTION_EXCEPTION error code to
                 * realize cn retry.
                 */

                /* If a single node, always user request */
                bool is_datanode = IS_PGXC_DATANODE && !IS_SINGLE_NODE;
                int error_code = is_datanode ? ERRCODE_QUERY_INTERNAL_CANCEL : ERRCODE_QUERY_CANCELED;

                if (pool_validate_cancel_pending && IS_PGXC_COORDINATOR) {
                    ereport(ERROR,
                        (errcode(ERRCODE_CONNECTION_EXCEPTION),
                            errmsg("canceling statement due to failover, pending")));
                } else {
                    ereport(ERROR,
                        (errcode(error_code),
                            errmsg("canceling statement due to %s request", is_datanode ? "coordinator" : "user")));
                }
            }
        } else if (pool_validate_cancel_pending && IS_PGXC_COORDINATOR) {
            InterruptPending = true;
            t_thrd.int_cxt.QueryCancelPending = true;
            t_thrd.int_cxt.PoolValidateCancelPending = true;
            ereport(WARNING, (errmsg("thread receive SIGUSR2 signal but can not INTERRUPT while in DoingCommandRead. "
                "Set validate and interrupt flag for checking next time.")));
        }
        if (IsJobSnapshotProcess()) {
            ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("canceling snapshot task")));
        }
    }
    /* If we get here, do nothing (probably, t_thrd.int_cxt.QueryCancelPending was reset) */
}

/*
 * IA64-specific code to fetch the AR.BSP register for stack depth checks.
 *
 * We currently support gcc, icc, and HP-UX inline assembly here.
 */
#if defined(__ia64__) || defined(__ia64)

#if defined(__hpux) && !defined(__GNUC__) && !defined __INTEL_COMPILER
#include <ia64/sys/inline.h>
#define ia64_get_bsp() ((char*)(_Asm_mov_from_ar(_AREG_BSP, _NO_FENCE)))
#else

#ifdef __INTEL_COMPILER
#include <asm/ia64regs.h>
#endif

static __inline__ char* ia64_get_bsp(void)
{
    char* ret = NULL;

#ifndef __INTEL_COMPILER
    /* the ;; is a "stop", seems to be required before fetching BSP */
    __asm__ __volatile__(";;\n"
                         "	mov	%0=ar.bsp	\n"
                         : "=r"(ret));
#else
    ret = (char*)__getReg(_IA64_REG_AR_BSP);
#endif
    return ret;
}
#endif
#endif /* IA64 */

/*
 * set_stack_base: set up reference point for stack depth checking
 *
 * Returns the old reference point, if any.
 */
pg_stack_base_t set_stack_base(void)
{
    char stack_base;
    pg_stack_base_t old;

#if defined(__ia64__) || defined(__ia64)
    old.t_thrd.postgres_cxt.stack_base_ptr = t_thrd.postgres_cxt.stack_base_ptr;
    old.register_stack_base_ptr = register_stack_base_ptr;
#else
    old = t_thrd.postgres_cxt.stack_base_ptr;
#endif

    /* Set up reference point for stack depth checking */
    t_thrd.postgres_cxt.stack_base_ptr = &stack_base;
#if defined(__ia64__) || defined(__ia64)
    register_stack_base_ptr = ia64_get_bsp();
#endif

    return old;
}

/*
 * restore_stack_base: restore reference point for stack depth checking
 *
 * This can be used after set_stack_base() to restore the old value. This
 * is currently only used in PL/Java. When PL/Java calls a backend function
 * from different thread, the thread's stack is at a different location than
 * the main thread's stack, so it sets the base pointer before the call, and
 * restores it afterwards.
 */
void restore_stack_base(pg_stack_base_t base)
{
#if defined(__ia64__) || defined(__ia64)
    t_thrd.postgres_cxt.stack_base_ptr = base.t_thrd.postgres_cxt.stack_base_ptr;
    register_stack_base_ptr = base.register_stack_base_ptr;
#else
    t_thrd.postgres_cxt.stack_base_ptr = base;
#endif
}

/*
 * check_stack_depth/stack_is_too_deep: check for excessively deep recursion
 *
 * This should be called someplace in any recursive routine that might possibly
 * recurse deep enough to overflow the stack.  Most Unixen treat stack
 * overflow as an unrecoverable SIGSEGV, so we want to error out ourselves
 * before hitting the hardware limit.
 *
 * check_stack_depth() just throws an error summarily.  stack_is_too_deep()
 * can be used by code that wants to handle the error condition itself.
 */
void check_stack_depth(void)
{
    if (stack_is_too_deep()) {
        ereport(ERROR,
            (errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
                errmsg("stack depth limit exceeded"),
                errhint("Increase the configuration parameter \"max_stack_depth\" (currently %dkB), "
                        "after ensuring the platform's stack depth limit is adequate.",
                    u_sess->attr.attr_common.max_stack_depth)));
    }
}

bool stack_is_too_deep(void)
{
    char stack_top_loc;
    long stack_depth;

    /*
     * Compute distance from reference point to my local variables
     */
    stack_depth = (long)(t_thrd.postgres_cxt.stack_base_ptr - &stack_top_loc);

    /*
     * Take abs value, since stacks grow up on some machines, down on others
     */
    if (stack_depth < 0) {
        stack_depth = -stack_depth;
    }

    /*
     * Trouble?
     *
     * The test on stack_base_ptr prevents us from erroring out if called
     * during process setup or in a non-backend process.  Logically it should
     * be done first, but putting it here avoids wasting cycles during normal
     * cases.
     */
    if (stack_depth > t_thrd.postgres_cxt.max_stack_depth_bytes && t_thrd.postgres_cxt.stack_base_ptr != NULL)
        return true;

        /*
         * On IA64 there is a separate "register" stack that requires its own
         * independent check.  For this, we have to measure the change in the
         * "BSP" pointer from PostgresMain to here.  Logic is just as above,
         * except that we know IA64's register stack grows up.
         *
         * Note we assume that the same max_stack_depth applies to both stacks.
         */
#if defined(__ia64__) || defined(__ia64)
    stack_depth = (long)(ia64_get_bsp() - register_stack_base_ptr);
    if (stack_depth > t_thrd.postgres_cxt.max_stack_depth_bytes && register_stack_base_ptr != NULL)
        return true;
#endif /* IA64 */

    return false;
}

/* GUC check hook for max_stack_depth */
bool check_max_stack_depth(int* newval, void** extra, GucSource source)
{
    long newval_bytes = *newval * 1024L;
    long stack_rlimit = get_stack_depth_rlimit();

    if (stack_rlimit > 0 && newval_bytes > stack_rlimit - STACK_DEPTH_SLOP) {
        GUC_check_errdetail("\"max_stack_depth\" must not exceed %ldkB.", (stack_rlimit - STACK_DEPTH_SLOP) / 1024L);
        GUC_check_errhint("Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent.");
        return false;
    }
    return true;
}

/* GUC assign hook for max_stack_depth */
void assign_max_stack_depth(int newval, void* extra)
{
    long newval_bytes = newval * 1024L;

    t_thrd.postgres_cxt.max_stack_depth_bytes = newval_bytes;
}

/*
 * set_debug_options --- apply "-d N" command line option
 *
 * -d is not quite the same as setting log_min_messages because it enables
 * other output options.
 */
void set_debug_options(int debug_flag, GucContext context, GucSource source)
{
    int rcs = 0;
    if (debug_flag > 0) {
        char debugstr[PRINFT_DST_MAX_DOUBLE];

        rcs = snprintf_s(debugstr, PRINFT_DST_MAX_DOUBLE, PRINFT_DST_MAX_DOUBLE - 1, "debug%d", debug_flag);
        securec_check_ss(rcs, "\0", "\0");
        SetConfigOption("log_min_messages", debugstr, context, source);
    } else {
        SetConfigOption("log_min_messages", "notice", context, source);
    }

    if (debug_flag >= 1 && context == PGC_POSTMASTER) {
        SetConfigOption("log_connections", "true", context, source);
        SetConfigOption("log_disconnections", "true", context, source);
    }
    if (debug_flag >= 2)
        SetConfigOption("log_statement", "all", context, source);
    if (debug_flag >= 3)
        SetConfigOption("debug_print_parse", "true", context, source);
    if (debug_flag >= 4)
        SetConfigOption("debug_print_plan", "true", context, source);
    if (debug_flag >= 5)
        SetConfigOption("debug_print_rewritten", "true", context, source);
}

bool set_plan_disabling_options(const char* arg, GucContext context, GucSource source)
{
    const char* tmp = NULL;

    switch (arg[0]) {
        case 's': /* seqscan */
            tmp = "enable_seqscan";
            break;
        case 'i': /* indexscan */
            tmp = "enable_indexscan";
            break;
        case 'o': /* indexonlyscan */
            tmp = "enable_indexonlyscan";
            break;
        case 'b': /* bitmapscan */
            tmp = "enable_bitmapscan";
            break;
        case 't': /* tidscan */
            tmp = "enable_tidscan";
            break;
        case 'n': /* nestloop */
            tmp = "enable_nestloop";
            break;
        case 'm': /* mergejoin */
            tmp = "enable_mergejoin";
            break;
        case 'h': /* hashjoin */
            tmp = "enable_hashjoin";
            break;
        default:
            break;
    }
    if (tmp != NULL) {
        SetConfigOption(tmp, "false", context, source);
        return true;
    } else
        return false;
}

const char* get_stats_option_name(const char* arg)
{
    switch (arg[0]) {
        case 'p':
            if (arg[1] == 'a') { /* "parser" */
                return "log_parser_stats";
            } else if (arg[1] == 'l') { /* "planner" */
                return "log_planner_stats";
            }
            break;

        case 'e': /* "executor" */
            return "log_executor_stats";
        default:
            break;
    }

    return NULL;
}

/* ----------------------------------------------------------------
 * process_postgres_switches
 *	   Parse command line arguments for PostgresMain
 *
 * This is called twice, once for the "secure" options coming from the
 * postmaster or command line, and once for the "insecure" options coming
 * from the client's startup packet.  The latter have the same syntax but
 * may be restricted in what they can do.
 *
 * argv[0] is ignored in either case (it's assumed to be the program name).
 *
 * ctx is PGC_POSTMASTER for secure options, PGC_BACKEND for insecure options
 * coming from the client, or PGC_SUSET for insecure options coming from
 * a superuser client.
 *
 * If a database name is present in the command line arguments, it's
 * returned into *dbname (this is allowed only if *dbname is initially NULL).
 * ----------------------------------------------------------------
 */
void process_postgres_switches(int argc, char* argv[], GucContext ctx, const char** dbname)
{
    bool secure = (ctx == PGC_POSTMASTER);
    int errs = 0;
    GucSource gucsource;
    int flag;
#ifdef PGXC
    bool singleuser = false;
#endif
    OptParseContext optCtxt;
    bool needCheckUuid = need_check_repl_uuid(ctx);
    bool gotUuidOpt = false;

    if (secure) {
        gucsource = PGC_S_ARGV; /* switches came from command line */

        /* Ignore the initial --single argument, if present */
        if (argc > 1 && strcmp(argv[1], "--single") == 0) {
            argv++;
            argc--;
#ifdef PGXC
            singleuser = true;
#endif
        }
    } else {
        gucsource = PGC_S_CLIENT; /* switches came from client */
    }

#ifdef HAVE_INT_OPTERR

    /*
     * Turn this off because it's either printed to stderr and not the log
     * where we'd want it, or argv[0] is now "--single", which would make for
     * a weird error message.  We print our own error message below.
     */
    opterr = 0;
#endif

    /*
     * Parse command-line options.	CAUTION: keep this in sync with
     * postmaster/postmaster.c (the option sets should not conflict) and with
     * the common help() function in main/main.c.
     */
    initOptParseContext(&optCtxt);
    while ((flag = getopt_r(argc, argv, "A:B:bc:C:D:d:EeFf:Gh:ijk:lN:nOo:Pp:r:S:sTt:v:W:g:Q:-:", &optCtxt)) != -1) {
        switch (flag) {
            case 'A':
                SetConfigOption("debug_assertions", optCtxt.optarg, ctx, gucsource);
                break;

            case 'B':
                SetConfigOption("shared_buffers", optCtxt.optarg, ctx, gucsource);
                break;

            case 'b':
                /* Undocumented flag used for binary upgrades */
                if (secure)
                    u_sess->proc_cxt.IsBinaryUpgrade = true;
                break;

            case 'C':
                /* ignored for consistency with the postmaster */
                break;

            case 'D':
                if (secure)
                    t_thrd.postgres_cxt.userDoption = MemoryContextStrdup(
                        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), optCtxt.optarg);
                break;

            case 'd':
                set_debug_options(atoi(optCtxt.optarg), ctx, gucsource);
                break;

            case 'E':
                if (secure)
                    t_thrd.postgres_cxt.EchoQuery = true;
                break;

            case 'e':
                SetConfigOption("datestyle", "euro", ctx, gucsource);
                break;

            case 'F':
                SetConfigOption("fsync", "false", ctx, gucsource);
                break;

            case 'f':
                if (!set_plan_disabling_options(optCtxt.optarg, ctx, gucsource))
                    errs++;
                break;

            case 'G':
                if (!singleuser) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("-G can be used only in single user or bootstrap mode")));
                }
                EnableInitDBSegment = true;
                break;

            case 'g':
                SetConfigOption("xlog_file_path", optCtxt.optarg, ctx, gucsource);
                break;
            case 'h':
                SetConfigOption("listen_addresses", optCtxt.optarg, ctx, gucsource);
                break;

            case 'i':
                SetConfigOption("listen_addresses", "*", ctx, gucsource);
                break;

            case 'j':
                if (secure)
                    t_thrd.postgres_cxt.UseNewLine = 0;
                break;

            case 'k':
                SetConfigOption("unix_socket_directory", optCtxt.optarg, ctx, gucsource);
                break;

            case 'l':
                SetConfigOption("ssl", "true", ctx, gucsource);
                break;

            case 'N':
                SetConfigOption("max_connections", optCtxt.optarg, ctx, gucsource);
                break;

            case 'n':
                /* ignored for consistency with postmaster */
                break;

            case 'O':
                SetConfigOption("allow_system_table_mods", "true", ctx, gucsource);
                break;

            case 'o':
                errs++;
                break;

            case 'P':
                SetConfigOption("ignore_system_indexes", "true", ctx, gucsource);
                break;

            case 'p':
                SetConfigOption("port", optCtxt.optarg, ctx, gucsource);
                break;
            case 'Q':
                {
                    uint32 high = 0;
                    uint32 low = 0;
                    if (optCtxt.optarg != NULL &&
                        strncmp(optCtxt.optarg, "copy_from_local", strlen("copy_from_local")) == 0) {
                        xlogCopyMode = XLOG_COPY_FROM_LOCAL;
                        if (sscanf_s(optCtxt.optarg, "copy_from_local(%08X/%08X)", &high, &low) == 2) {
                            xlogCopyStart = low + ((XLogRecPtr)high * XLogSegmentsPerXLogId * XLogSegSize);
                        }
                    } else if (optCtxt.optarg != NULL &&
                        strncmp(optCtxt.optarg, "force_copy_from_local", strlen("force_copy_from_local")) == 0) {
                        xlogCopyMode = XLOG_FORCE_COPY_FROM_LOCAL;
                        if (sscanf_s(optCtxt.optarg, "force_copy_from_local(%08X/%08X)", &high, &low) == 2) {
                            xlogCopyStart = low + ((XLogRecPtr)high * XLogSegmentsPerXLogId * XLogSegSize);
                        }
                    } else if (optCtxt.optarg != NULL && strcmp(optCtxt.optarg, "copy_from_share") == 0) {
                        xlogCopyMode = XLOG_COPY_FROM_SHARE;
                    }
                }
                break;
            case 'r':
                /* send output (stdout and stderr) to the given file */
                if (secure)
                    (void)strlcpy(t_thrd.proc_cxt.OutputFileName, optCtxt.optarg, MAXPGPATH);
                break;

            case 'S':
                SetConfigOption("work_mem", optCtxt.optarg, ctx, gucsource);
                break;

            case 's':
                SetConfigOption("log_statement_stats", "true", ctx, gucsource);
                break;

            case 'T':
                /* ignored for consistency with the postmaster */
                break;

            case 't': {
                const char* tmp = get_stats_option_name(optCtxt.optarg);

                if (tmp != NULL)
                    SetConfigOption(tmp, "true", ctx, gucsource);
                else
                    errs++;
                break;
            }

            case 'v':

                /*
                 * -v is no longer used in normal operation, since
                 * FrontendProtocol is already set before we get here. We keep
                 * the switch only for possible use in standalone operation,
                 * in case we ever support using normal FE/BE protocol with a
                 * standalone backend.
                 */
                if (secure)
                    FrontendProtocol = (ProtocolVersion)atoi(optCtxt.optarg);
                break;

            case 'W':
                SetConfigOption("post_auth_delay", optCtxt.optarg, ctx, gucsource);
                break;

            case 'c':
            case '-': {
                char* name = NULL;
                char* value = NULL;

                ParseLongOption(optCtxt.optarg, &name, &value);
#ifdef PGXC
#ifndef ENABLE_MULTIPLE_NODES
                if (flag == '-' && (strcmp(name, "coordinator") == 0 || strcmp(name, "datanode") == 0)) {
                    ereport(FATAL, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("Single node mode: must start as single node (--single_node)\n")));
                }
#endif
                /* A Coordinator is being activated */
                if (strcmp(name, "coordinator") == 0 && value == NULL)
                    g_instance.role = VCOORDINATOR;
                /* A Datanode is being activated */
                else if (strcmp(name, "datanode") == 0 && value == NULL)
                    g_instance.role = VDATANODE;
                /* A SingleDN mode is being activated */
                else if (strcmp(name, "single_node") == 0 && value == NULL) {
                    g_instance.role = VSINGLENODE;
                    useLocalXid = true;
                } else if (strcmp(name, "localxid") == 0 && value == NULL) {
                    if (!singleuser)
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("local xids can be used only in single user mode")));
                    useLocalXid = true;
                } else /* default case */
                {
#endif /* PGXC */
                    if (value == NULL) {
                        if (flag == '-')
                            ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("--%s requires a value", optCtxt.optarg)));
                        else
                            ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("-c %s requires a value", optCtxt.optarg)));
                    }
#ifndef ENABLE_MULTIPLE_NODES
#ifndef USE_SPQ
                    /* Only support 'internaltool' and 'application' for remotetype in single-node mode */
                    if (strcmp(name, "remotetype") == 0 && strcmp(value, "application") != 0 &&
                        strcmp(value, "internaltool") != 0) {
                        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Invalid remote type:%s", value)));
                    }
#endif
#endif
                    if (CheckReplUuid(name, value, needCheckUuid)) {
                        gotUuidOpt = true;
                    } else {
                        SetConfigOption(name, value, ctx, gucsource);
                    }
                }
                pfree(name);
                pfree_ext(value);
                break;
            }

            default:
                errs++;
                break;
        }

        if (errs)
            break;
    }

#ifdef PGXC
    /*
     * Make sure we specified the mode if Coordinator or Datanode.
     * Allow for the exception of initdb by checking config option
     */
    if (!IS_PGXC_COORDINATOR && !IS_PGXC_DATANODE && IsUnderPostmaster) {
        ereport(FATAL, (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("openGauss: must start as either a Coordinator (--coordinator) or Datanode (--datanode)\n")));
    }
    if (!IsPostmasterEnvironment) {
        /* Treat it as a Datanode for initdb to work properly */
        g_instance.role = VDATANODE;
        isSingleMode = true;
        useLocalXid = true;
    }
#endif

    if (needCheckUuid && !gotUuidOpt) {
        /* If UUID auth is required, standby's UUID should be passed with startup packet -c option. */
        ereport(FATAL,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("UUID replication auth is required."),
                errhint("Please pass standby's replication UUID within startup packet -c option.")));
    }

    /*
     * Optional database name should be there only if *dbname is NULL.
     */
    if (errs == 0 && dbname != NULL && *dbname == NULL && argc - optCtxt.optind >= 1) {
        char* tmp_str = argv[optCtxt.optind++];
        *dbname = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), tmp_str);
    }

    if (errs || argc != optCtxt.optind) {
        if (errs)
            optCtxt.optind--; /* complain about the previous argument */

        /* spell the error message a bit differently depending on context */
        if (IsUnderPostmaster)
            ereport(FATAL, (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("invalid command-line argument for server process: %s", argv[optCtxt.optind]),
                    errhint("Try \"%s --help\" for more information.", progname)));
        else
            ereport(FATAL, (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("%s: invalid command-line argument: %s", progname, argv[optCtxt.optind]),
                    errhint("Try \"%s --help\" for more information.", progname)));
    }

    /*
     * Reset getopt(3) library so that it will work correctly in subprocesses
     * or when this function is called a second time with another array.
     */
#ifdef HAVE_INT_OPTRESET
    optreset = 1; /* some systems need this too */
#endif
}

/* clear key message that may appear in core file for security */
static void clear_memory(char* qstr, char* msg, int maxlen)
{
    if (qstr == NULL || msg == NULL)
        return;

    if (t_thrd.postgres_cxt.clear_key_memory) {
        errno_t errorno = EOK;
        PgBackendStatus* beentry = NULL;

        errorno = memset_s(qstr, strlen(qstr), 0, strlen(qstr));
        securec_check(errorno, "", "");

        errorno = memset_s(msg, maxlen, 0, maxlen);
        securec_check(errorno, "", "");

        beentry = GetMyBEEntry();
        if (beentry != NULL) {
            errorno = memset_s(beentry->st_activity,
                g_instance.attr.attr_common.pgstat_track_activity_query_size,
                0,
                g_instance.attr.attr_common.pgstat_track_activity_query_size);
            securec_check(errorno, "", "");
        }
        t_thrd.postgres_cxt.clear_key_memory = false;
    }
}

/* read and process the configuration file, just for openGauss backend workers */
void reload_configfile(void)
{
    if (u_sess->sig_cxt.got_SIGHUP) {
        ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
        if (currentOwner == NULL) {
            /* we use t_thrd.mem_cxt.msg_mem_cxt to remember config info in this cluster. */
            MemoryContext old = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
            t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "ForReloadConfig",
                THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
            (void)MemoryContextSwitchTo(old);
        }
        u_sess->sig_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
        most_available_sync = (volatile bool) u_sess->attr.attr_storage.guc_most_available_sync;
        SyncRepUpdateSyncStandbysDefined();
        if (currentOwner == NULL) {
            ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
            ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
            ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
            ResourceOwner newOwner = t_thrd.utils_cxt.CurrentResourceOwner;
            t_thrd.utils_cxt.CurrentResourceOwner = NULL;
            ResourceOwnerDelete(newOwner);
        }
    }
}

/* reload pooler for online business in expansion. */
void reload_online_pooler()
{
    if (IsGotPoolReload() && !IsConnFromGTMTool()) {
        if (!IsTransactionBlock()) {
            processPoolerReload();
            ResetGotPoolReload(false);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                    errmsg("Cannot execute DDL in a transaction block when need reconnect pooler")));
        }
    }
}

/* reload pooler if necessary, just pass while in transaction block */
void ReloadPoolerWithoutTransaction()
{
    if (IsGotPoolReload()) {
        if (!IsTransactionBlock()) {
            processPoolerReload();
            ResetGotPoolReload(false);
        } else {
            ereport(LOG, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                errmsg("receive signal to execute pooler reload, "
                    "just pass and keep flag while in transaction block.")));
        }
    }
}

#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)
/*
 * @Description: Initialize or refresh global node definition
 *
 * @param[IN] planstmt:  PlannedStmt node which holds the "one time" information needed by the executor
 * @return: void
 */
static void InitGlobalNodeDefinition(PlannedStmt* planstmt)
{
    if (planstmt == NULL)
        return;

    if ((global_node_definition != NULL && global_node_definition->num_nodes == planstmt->num_nodes)
		|| planstmt->nodesDefinition == NULL)
        return;

    AutoMutexLock copyLock(&nodeDefCopyLock);
    copyLock.lock();

    /* first initialization or need update when cluster size changes */
    if (global_node_definition == NULL || global_node_definition->num_nodes != planstmt->num_nodes) {
        MemoryContext oldMemory = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));
        Size nodeDefSize;
        errno_t rc = EOK;

        /* free the old one before refresh */
        if (global_node_definition != NULL) {
            if (global_node_definition->nodesDefinition)
                pfree(global_node_definition->nodesDefinition);
            pfree(global_node_definition);
        }

        global_node_definition = (GlobalNodeDefinition*)palloc(sizeof(GlobalNodeDefinition));
        if ((planstmt->num_nodes > 0) && ((uint)(INT_MAX / planstmt->num_nodes) > sizeof(NodeDefinition))) {
            global_node_definition->num_nodes = planstmt->num_nodes;
            nodeDefSize = mul_size(sizeof(NodeDefinition), (Size)planstmt->num_nodes);
            global_node_definition->nodesDefinition = (NodeDefinition*)palloc(nodeDefSize);
            rc = memcpy_s(global_node_definition->nodesDefinition, nodeDefSize, planstmt->nodesDefinition, nodeDefSize);
            securec_check(rc, "\0", "\0");
        } else {
            copyLock.unLock();
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("invalid number of data nodes when initializing global node definition.")));
        }

        (void)MemoryContextSwitchTo(oldMemory);
    }

    copyLock.unLock();
}
#endif

void InitThreadLocalWhenSessionExit()
{
    t_thrd.postgres_cxt.xact_started = false;
    if (u_sess != NULL) {
        if (u_sess->libsw_cxt.redirect_manager == NULL) {
            u_sess->libsw_cxt.redirect_manager = New(CurrentMemoryContext) RedirectManager();
        }
    }
}

/*
 * Remove temp namespace.
 */
void RemoveTempNamespace()
{
    /*
     * isSingleMode means we are doing initdb. Some temp tables
     * will be created to store intermediate result, so should do cleaning
     * when finished.
     * xc_maintenance_mode is for cluster resize, temp table created
     * during it should be clean too.
     * Drop temp schema if IS_SINGLE_NODE.
     */
    bool needRemoveTempNsp =
        (IS_PGXC_COORDINATOR || isSingleMode || u_sess->attr.attr_common.xc_maintenance_mode || IS_SINGLE_NODE)
        && u_sess->catalog_cxt.deleteTempOnQuiting
        && u_sess->catalog_cxt.myTempNamespace;

    bool needRemoveLobTempNsp = u_sess->catalog_cxt.myLobTempToastNamespace != 0;

    if (needRemoveTempNsp || needRemoveLobTempNsp) {
        MemoryContext current_context = CurrentMemoryContext;
        ResourceOwner oldOwner = t_thrd.utils_cxt.CurrentResourceOwner;
        ResourceOwner newOwner = ResourceOwnerCreate(oldOwner, "ForTempTableDrop",
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
        volatile bool need_release_owner = true;
        PG_TRY();
        {
            if (!IsTransactionOrTransactionBlock()) {
                t_thrd.proc_cxt.PostInit->InitLoadLocalSysCache(u_sess->proc_cxt.MyDatabaseId,
                    u_sess->proc_cxt.MyDatabaseId == TemplateDbOid ? NULL : u_sess->proc_cxt.MyProcPort->database_name);
            }

            StringInfoData str;
            initStringInfo(&str);

            t_thrd.utils_cxt.CurrentResourceOwner = newOwner;
            if (needRemoveTempNsp) {
                char* nspname = get_namespace_name(u_sess->catalog_cxt.myTempNamespace);
                if (nspname) {
                    appendStringInfo(&str, "DROP SCHEMA %s, pg_toast_temp_%s CASCADE;\n", nspname, &nspname[8]);
                    ereport(LOG, (errmsg("Session quiting, drop temp schema %s", nspname)));
                }
            }
            if (needRemoveLobTempNsp) {
                char* nspname = get_namespace_name(u_sess->catalog_cxt.myLobTempToastNamespace);
                if (nspname) {
                    appendStringInfo(&str, "DROP SCHEMA %s CASCADE;\n", nspname);
                    ereport(LOG, (errmsg("Session quiting, drop temp toast schema %s", nspname)));
                }
            }
            need_release_owner = false;
            ResourceOwnerRelease(newOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
            ResourceOwnerRelease(newOwner, RESOURCE_RELEASE_LOCKS, true, true);
            ResourceOwnerRelease(newOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
            t_thrd.utils_cxt.CurrentResourceOwner = oldOwner;
            ResourceOwnerDelete(newOwner);

            pgstatCountSQL4SessionLevel();
            t_thrd.postgres_cxt.whereToSendOutput = DestNone;

#ifndef ENABLE_MULTIPLE_NODES
            /* Do not use smp while dropping temp schema.
             * While finish this operator, the session will be close, so no need to reset query_dop.
             */
            u_sess->opt_cxt.query_dop_store = 1;
#endif

            exec_simple_query(str.data, QUERY_MESSAGE);

            if (needRemoveTempNsp) {
                u_sess->catalog_cxt.myTempNamespace = InvalidOid;
                u_sess->catalog_cxt.myTempToastNamespace = InvalidOid;
            }
            if (needRemoveLobTempNsp) {
                u_sess->catalog_cxt.myLobTempToastNamespace = InvalidOid;
            }
            pfree_ext(str.data);
        }
        PG_CATCH();
        {
            if (need_release_owner) {
                ResourceOwnerRelease(newOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
                ResourceOwnerRelease(newOwner, RESOURCE_RELEASE_LOCKS, true, true);
                ResourceOwnerRelease(newOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
                t_thrd.utils_cxt.CurrentResourceOwner = oldOwner;
                ResourceOwnerDelete(newOwner);
            } else {
                t_thrd.utils_cxt.CurrentResourceOwner = oldOwner;
            }
            MemoryContextSwitchTo(current_context);
            EmitErrorReport();
            FlushErrorState();
            ereport(WARNING, (errmsg("Drop temp schema failed. The temp schema will be drop by TwoPhaseCleanner.")));
        }
        PG_END_TRY();
    }
}

#if (!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS))
#define INITIAL_USER_ID 10
/*
 * IMPORTANT:
 * 1. load plugin should call after process is normal, cause heap_create_with_catalog will check it.
 * 2. load plugin should call after mask_password_mem_cxt is created, cause maskPassword is called
 *      when create extension, which need mask_password_mem_cxt.
 */
void LoadSqlPlugin()
{
    if (u_sess->proc_cxt.MyDatabaseId != InvalidOid && DB_IS_CMPT(B_FORMAT) && IsFileExisted(DOLPHIN)) {
        /* start_xact_command will change CurrentResourceOwner, so save it here */
        ResourceOwner save = t_thrd.utils_cxt.CurrentResourceOwner;
        if (!u_sess->attr.attr_sql.dolphin && u_sess->attr.attr_common.upgrade_mode == 0) {
            Oid userId = GetUserId();
            if (userId != INITIAL_USER_ID) {
                ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("Please use the original role to connect B-compatibility database first, to load extension dolphin")));
            }
            /* recheck and load dolphin within lock */
            t_thrd.utils_cxt.holdLoadPluginLock[DB_CMPT_B] = true;
            pthread_mutex_lock(&g_instance.loadPluginLock[DB_CMPT_B]);

            PG_TRY();
            {
                start_xact_command();
                u_sess->attr.attr_sql.dolphin = CheckIfExtensionExists("dolphin");
                finish_xact_command();

                if (!u_sess->attr.attr_sql.dolphin) {
                    LoadDolphinIfNeeded();
                }
                InitBSqlPluginHookIfNeeded();
            }
            PG_CATCH();
            {
                pthread_mutex_unlock(&g_instance.loadPluginLock[DB_CMPT_B]);
                t_thrd.utils_cxt.holdLoadPluginLock[DB_CMPT_B] = false;
                t_thrd.utils_cxt.CurrentResourceOwner = save;
                PG_RE_THROW();
            }
            PG_END_TRY();
            pthread_mutex_unlock(&g_instance.loadPluginLock[DB_CMPT_B]);
            t_thrd.utils_cxt.holdLoadPluginLock[DB_CMPT_B] = false;
        } else if (u_sess->attr.attr_sql.dolphin) {
            InitBSqlPluginHookIfNeeded();
        }
        t_thrd.utils_cxt.CurrentResourceOwner = save;
    } else if (u_sess->proc_cxt.MyDatabaseId != InvalidOid && DB_IS_CMPT(A_FORMAT) && u_sess->attr.attr_sql.whale) {
        InitASqlPluginHookIfNeeded();
    }

     /* load age if the age extension been created and the age.o file exists */
    if(u_sess->proc_cxt.MyDatabaseId != InvalidOid ){
        InitAGESqlPluginHookIfNeeded();
    }
}
#endif

/* ----------------------------------------------------------------
 * PostgresMain
 *	   openGauss main loop -- all backends, interactive or otherwise start here
 *
 * argc/argv are the command line arguments to be used.  (When being forked
 * by the postmaster, these are not the original argv array of the process.)
 * dbname is the name of the database to connect to, or NULL if the database
 * name should be extracted from the command line arguments or defaulted.
 * username is the openGauss user name to be used for the session.
 * ----------------------------------------------------------------
 */
int PostgresMain(int argc, char* argv[], const char* dbname, const char* username)
{
    int firstchar;
    StringInfoData input_message = {NULL, 0, 0, 0};
    sigjmp_buf local_sigjmp_buf;
    volatile bool send_ready_for_query = true;

#ifdef ENABLE_MULTIPLE_NODES /* PGXC_DATANODE */
    /* Snapshot info */
    uint64 csn;
    bool cn_xc_maintain_mode = false;
    bool remote_gtm_mode = false;
    /* Timestamp info */
    TimestampTz gtmstart_timestamp;
    TimestampTz stmtsys_timestamp;
    int ss_need_sync_wait_all = 0;
    errno_t rc = EOK;
    CsnType csn_type;
#endif

    CommandDest saved_whereToSendOutput = DestNone;

    gstrace_entry(GS_TRC_ID_PostgresMain);

    /*
     * Initialize globals (already done if under postmaster, but not if
     * standalone).
     */
    if (!IsUnderPostmaster) {
        PostmasterPid = gs_thread_self();

        t_thrd.proc_cxt.MyProcPid = gs_thread_self();

        t_thrd.proc_cxt.MyStartTime = time(NULL);

        t_thrd.proc_cxt.MyProgName = "gaussdb";

        /*
         * Initialize random() for the first time, like PostmasterMain()
         * would.  In a regular IsUnderPostmaster backend, BackendRun()
         * computes a high-entropy seed before any user query.  Fewer distinct
         * initial seeds can occur here.
         */
        srandom((unsigned int)(t_thrd.proc_cxt.MyProcPid ^ (unsigned int)t_thrd.proc_cxt.MyStartTime));
    } else {
        t_thrd.proc_cxt.MyProgName = "postgres";
#ifdef DEBUG_UHEAP
        UHeapStatInit(UHeapStat_local);
#endif
    }

    /*
     * Fire up essential subsystems: error and memory management
     *
     * If we are running under the postmaster, this is done already.
     */
    if (!IsUnderPostmaster) {
        MemoryContextInit();
        init_plog_global_mem();
    }

    SetProcessingMode(InitProcessing);

    /* Compute paths, if we didn't inherit them from postmaster */
    if (my_exec_path[0] == '\0') {
        if (find_my_exec(argv[0], my_exec_path) < 0) {
            gstrace_exit(GS_TRC_ID_PostgresMain);
            ereport(FATAL, (errmsg("%s: could not locate my own executable path", argv[0])));
        }
    }

    if (t_thrd.proc_cxt.pkglib_path[0] == '\0')
        get_pkglib_path(my_exec_path, t_thrd.proc_cxt.pkglib_path);

    /*
     * Set default values for command-line options.
     */
    if (!IsUnderPostmaster) {
        InitializeGUCOptions();
    }

    /* set user-defined params */
    init_set_user_params_htab();

    /*
     * Parse command-line options.
     */
    process_postgres_switches(argc, argv, PGC_POSTMASTER, &dbname);

    if (!IS_THREAD_POOL_WORKER) {
        /* Must have gotten a database name, or have a default (the username) */
        if (dbname == NULL) {
            gstrace_exit(GS_TRC_ID_PostgresMain);
            ereport(FATAL,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("%s: no database nor user name specified", progname)));
        }
    }

    /* Acquire configuration parameters, unless inherited from postmaster */
    if (!IsUnderPostmaster) {
        if (!SelectConfigFiles(t_thrd.postgres_cxt.userDoption, progname)) {
            gstrace_exit(GS_TRC_ID_PostgresMain);
            proc_exit(1);
        }
        InitializeNumLwLockPartitions();
    }

    /* initialize guc variables which need to be sended to stream threads */
    if (IS_PGXC_DATANODE && IsUnderPostmaster)
        init_sync_guc_variables();

    /*
     * You might expect to see a setsid() call here, but it's not needed,
     * because if we are under a postmaster then BackendInitialize() did it.
     */

    /*
     * Set up signal handlers and masks.
     *
     * Note that postmaster blocked all signals before forking child process,
     * so there is no race condition whereby we might receive a signal before
     * we have set up the handler.
     *
     * Also note: it's best not to use any signals that are SIG_IGNored in the
     * postmaster.	If such a signal arrives before we are able to change the
     * handler to non-SIG_IGN, it'll get dropped.  Instead, make a dummy
     * handler in the postmaster to reserve the signal. (Of course, this isn't
     * an issue for signals that are locally generated, such as SIGALRM and
     * SIGPIPE.)
     */
    if (t_thrd.datasender_cxt.am_datasender)
        DataSndSignals();
    else if (AM_WAL_SENDER)
        WalSndSignals();
    else {
        if (!IsUnderPostmaster) {
            (void)gs_signal_createtimer();

            (void)pqsignal(SIGALRM, SIG_IGN); /* ignored */
            (void)pqsignal(SIGPIPE, SIG_IGN); /* ignored */
            (void)pqsignal(SIGFPE, FloatExceptionHandler);
        }

        (void)gspqsignal(SIGHUP, SigHupHandler);
        (void)gspqsignal(SIGURG, print_stack);
        /* set flag to read config
         * file */
        (void)gspqsignal(SIGINT, StatementCancelHandler); /* cancel current query */
        (void)gspqsignal(SIGTERM, die);                   /* cancel current query and exit */

        /*
         * In a standalone backend, SIGQUIT can be generated from the keyboard
         * easily, while SIGTERM cannot, so we make both signals do die()
         * rather than quickdie().
         */
        if (IsUnderPostmaster)
            (void)gspqsignal(SIGQUIT, quickdie); /* hard crash time */
        else
            (void)gspqsignal(SIGQUIT, die);          /* cancel current query and exit */
        (void)gspqsignal(SIGALRM, handle_sig_alarm); /* timeout conditions */

        /*
         * Ignore failure to write to frontend. Note: if frontend closes
         * connection, we will notice it and exit cleanly when control next
         * returns to outer loop.  This seems safer than forcing exit in the
         * midst of output during who-knows-what operation...
         */
        (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
        (void)gspqsignal(SIGUSR2, PoolValidateCancelHandler);

        /*
         * Reset some signals that are accepted by postmaster but not by
         * backend
         */
        (void)gspqsignal(SIGCHLD, SIG_DFL); /* system() requires this on some
                                             * platforms */
    }
    (void)gs_signal_unblock_sigusr2();

    if (IsUnderPostmaster) {
        /* We allow SIGQUIT (quickdie) at all times */
        (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);
    }

    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL); /* block everything except SIGQUIT */

    if (IsInitdb) {
        Assert(!IsUnderPostmaster);
        g_instance.global_sysdbcache.Init(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
        CreateLocalSysDBCache();
    }

    if (!IsUnderPostmaster) {
        /*
         * Validate we have been given a reasonable-looking t_thrd.proc_cxt.DataDir (if under
         * postmaster, assume postmaster did this already).
         */
        Assert(t_thrd.proc_cxt.DataDir);
        ValidatePgVersion(t_thrd.proc_cxt.DataDir);

        /* Change into t_thrd.proc_cxt.DataDir (if under postmaster, was done already) */
        ChangeToDataDir();

        /*
         * Create lockfile for data directory.
         */
        CreateDataDirLockFile(false);
    }

    /* Callback function for dss operator */
    if (dss_device_init(g_instance.attr.attr_storage.dss_attr.ss_dss_conn_path,
                    g_instance.attr.attr_storage.dss_attr.ss_enable_dss) != DSS_SUCCESS) {
        ereport(FATAL, (errmsg("failed to init dss device")));
        proc_exit(1);
    }
    initDSSConf();

    /* Early initialization */
    BaseInit();

    if (!IsUnderPostmaster) {
        ShareStorageInit();
    }

    if (!IsUnderPostmaster && xlogCopyMode != XLOG_COPY_NOT) {
        if (g_instance.attr.attr_storage.xlog_file_path == NULL) {
            proc_exit(1);
        }
        bool copySuccess = false;
        if (xlogCopyMode == XLOG_COPY_FROM_LOCAL) {
            copySuccess = XLogOverwriteFromLocal(false, xlogCopyStart);
        } else if (xlogCopyMode == XLOG_FORCE_COPY_FROM_LOCAL) {
            copySuccess = XLogOverwriteFromLocal(true, xlogCopyStart);
        } else if (xlogCopyMode == XLOG_COPY_FROM_SHARE) {
            copySuccess = XLogOverwriteFromShare();
        }
        if (copySuccess) {
            proc_exit(0);
        } else {
            proc_exit(1);
        }
    }

#ifdef ENABLE_MULTIPLE_NODES /* PGXC_COORD */
    if (IS_PGXC_COORDINATOR && IsPostmasterEnvironment) {
        /* If we exit, first try and clean connections and send to pool */
        /*
         * pooler thread does NOT exist any more, PoolerLock of LWlock is used instead.
         *
         * PoolManagerDisconnect() which is called by PGXCNodeCleanAndRelease()
         * is the last call to pooler in the openGauss thread, and PoolerLock is
         * used in PoolManagerDisconnect(), but it is called after ProcKill()
         * when postgres thread exits.
         * ProcKill() releases any of its held LW locks. So Assert(!(proc == NULL ...))
         * will fail in LWLockAcquire() which is called by PoolManagerDisconnect().
         *
         * All exit functions in "on_shmem_exit_list" will be called before those functions
         * in "on_proc_exit_list", so move PGXCNodeCleanAndRelease() to "on_shmem_exit_list"
         * and registers it after ProcKill(), and PGXCNodeCleanAndRelease() will
         * be called before ProcKill().
         *
         * the register sequence of exit functions on openGauss thread:
         * on_proc_exit_list:
         *     pq_close,                            ^
         *     AtProcExit_Files,                    |
         *     smgrshutdown,                        |
         *     AtProcExit_SnapshotData,             |
         *     audit_processlogout,                 |
         *     PGXCNodeCleanAndRelease, --+         |
         *                                |         ^
         * on_shmem_exit_list:            |         |
         *     ProcKill,                  |         |
         *     PGXCNodeCleanAndRelease, <-+         |
         *     RemoveProcFromArray,                 |
         *     CleanupInvalidationState,            |
         *     CleanupProcSignalState,              ^  called on openGauss thread
         *     AtProcExit_Buffers,                  |  exit.
         *     pgstat_beshutdown_hook,              |
         *     ShutdownPostgres,                    |
         *     endMySessionTimeEntry,               |
         *     endMySessionStatEntry,               |
         */
        on_shmem_exit(PGXCNodeCleanAndRelease, 0);
    }
#endif

#ifndef ENABLE_MULTIPLE_NODES
    if (!IS_THREAD_POOL_WORKER) {
        on_shmem_exit(PlDebugerCleanUp, 0);
    }

#endif

    if (ENABLE_GPC) {
        on_shmem_exit(cleanGPCPlanProcExit, 0);
    }
    /*
     * Create a per-backend PGPROC struct in shared memory, except in the
     * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
     * this before we can use LWLocks (and in the EXEC_BACKEND case we already
     * had to do some stuff with LWLocks).
     */
#ifdef EXEC_BACKEND
    if (!IsUnderPostmaster)
        InitProcess();
#else
    InitProcess();
#endif

    if (t_thrd.proc) {
        errno_t rc = snprintf_s(t_thrd.proc->myProgName,
            sizeof(t_thrd.proc->myProgName),
            sizeof(t_thrd.proc->myProgName) - 1,
            "%s",
            t_thrd.proc_cxt.MyProgName);
        securec_check_ss(rc, "", "");
        t_thrd.proc->myStartTime = t_thrd.proc_cxt.MyStartTime;
        t_thrd.proc->sessMemorySessionid = (IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid);
    }

    /* We need to allow SIGINT, etc during the initial transaction */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    /* Initialize the memory tracking information */
    MemoryTrackingInit();

    /*
     * General initialization.
     *
     * NOTE: if you are tempted to add code in this vicinity, consider putting
     * it inside InitPostgres() instead.  In particular, anything that
     * involves database access should be there, not here.
     */
    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(dbname, InvalidOid, username);

    /*
     * PostgresMain thread can be user for wal sender, which will call
     * DataSenderMain() or WalSenderMain() later.
     */
    if (unlikely(AM_WAL_SENDER))
        t_thrd.proc_cxt.PostInit->InitWAL();
    else
        t_thrd.proc_cxt.PostInit->InitBackendWorker();

    /*
     * If the t_thrd.mem_cxt.postmaster_mem_cxt is still around, recycle the space; we don't
     * need it anymore after InitPostgres completes.  Note this does not trash
     * *u_sess->proc_cxt.MyProcPort, because ConnCreate() allocated that space with malloc()
     * ... else we'd need to copy the Port data first.  Also, subsidiary data
     * such as the username isn't lost either; see ProcessStartupPacket().
     */
    if (t_thrd.mem_cxt.postmaster_mem_cxt) {
        MemoryContextDelete(t_thrd.mem_cxt.postmaster_mem_cxt);
        t_thrd.mem_cxt.postmaster_mem_cxt = NULL;
    }

    SetProcessingMode(NormalProcessing);

    if (!IS_THREAD_POOL_WORKER) {
        /*
         * Now all GUC states are fully set up.  Report them to client if
         * appropriate.
         */
        BeginReportingGUCOptions();
        /* Registering backend_version */
        if (t_thrd.proc && contain_backend_version(t_thrd.proc->workingVersionNum)) {
            register_backend_version(t_thrd.proc->workingVersionNum);
        }
    }

    /*
     * Also set up handler to log session end; we have to wait till now to be
     * sure Log_disconnections has its final value.
     */
    if (IsUnderPostmaster && !IS_THREAD_POOL_WORKER && u_sess->attr.attr_common.Log_disconnections)
        on_proc_exit(log_disconnections, 0);

    /* Data sender process */
    if (t_thrd.datasender_cxt.am_datasender)
        proc_exit(DataSenderMain());

    /* If this is a WAL sender process, we're done with initialization. */
    if (AM_WAL_SENDER)
        proc_exit(WalSenderMain());

    if (IsUnderPostmaster && !IS_THREAD_POOL_WORKER) {
        on_proc_exit(audit_processlogout, 0);
    }

    /*
     * process any libraries that should be preloaded at backend start (this
     * likewise can't be done until GUC settings are complete)
     */
    process_local_preload_libraries();

    /*
     * Send this backend's cancellation info to the frontend.
     */
    if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote && PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2 &&
        !IS_THREAD_POOL_WORKER) {

        Port* MyProcPort = u_sess->proc_cxt.MyProcPort;
        if (MyProcPort && MyProcPort->protocol_config->fn_send_cancel_key) {
            MyProcPort->protocol_config->fn_send_cancel_key((int32)t_thrd.proc->pgprocno,
                                                            (int32)t_thrd.proc_cxt.MyCancelKey);
        }

        /* DN send thread pid to CN */
        if (IsConnFromCoord() && IS_PGXC_DATANODE && (t_thrd.proc && t_thrd.proc->workingVersionNum >= 92060)) {
            StringInfoData buf_pid;
            pq_beginmessage(&buf_pid, 'k');
            pq_sendint64(&buf_pid, t_thrd.proc_cxt.MyProcPid);
            pq_endmessage(&buf_pid);
        }
        /* Need not flush since ReadyForQuery will do it. */
    }

    /* Welcome banner for standalone case */
    if (t_thrd.postgres_cxt.whereToSendOutput == DestDebug)
        printf("\nPostgreSQL stand-alone backend %s\n", PG_VERSION);

    /*
     * Create the memory context we will use in the main loop.
     *
     * t_thrd.mem_cxt.msg_mem_cxt is reset once per iteration of the main loop, ie, upon
     * completion of processing of each command message from the client.
     */
    t_thrd.mem_cxt.msg_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "MessageContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * Create memory context and buffer used for RowDescription messages. As
     * SendRowDescriptionMessage(), via exec_describe_statement_message(), is
     * frequently executed for ever single statement, we don't want to
     * allocate a separate buffer every time.
     */
    t_thrd.mem_cxt.row_desc_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "RowDescriptionContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext old_mc = MemoryContextSwitchTo(t_thrd.mem_cxt.row_desc_mem_cxt);
    initStringInfo(&(*t_thrd.postgres_cxt.row_description_buf));
    MemoryContextSwitchTo(old_mc);

    t_thrd.mem_cxt.mask_password_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "MaskPasswordCtx",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    InitVecFuncMap();

    /* init param hash table for sending set message */
    if (IS_PGXC_COORDINATOR)
        init_set_params_htab();

#if (!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS))
    if (!IsInitdb) {
        LoadSqlPlugin();
    }
#endif

    /*
     * Remember stand-alone backend startup time
     */
    if (!IsUnderPostmaster)
        t_thrd.time_cxt.pg_start_time = GetCurrentTimestamp();

    t_thrd.wlm_cxt.thread_node_group = &g_instance.wlm_cxt->MyDefaultNodeGroup;  // initialize the default value
    t_thrd.wlm_cxt.thread_climgr = &t_thrd.wlm_cxt.thread_node_group->climgr;
    t_thrd.wlm_cxt.thread_srvmgr = &t_thrd.wlm_cxt.thread_node_group->srvmgr;

    /*
     * Initialize key pair to be used as object id while using advisory lock
     * for backup
     */
    t_thrd.postmaster_cxt.xc_lockForBackupKey1 = Int32GetDatum(XC_LOCK_FOR_BACKUP_KEY_1);
    t_thrd.postmaster_cxt.xc_lockForBackupKey2 = Int32GetDatum(XC_LOCK_FOR_BACKUP_KEY_2);

#ifdef ENABLE_MULTIPLE_NODES 
    if (IS_PGXC_DATANODE) {
        /* If we exit, first try and clean connection to GTM */
        on_proc_exit(DataNodeShutdown, 0);
    }
#endif

    /* make sure that each module name is unique */
    Assert(check_module_name_unique());

    if (IS_THREAD_POOL_WORKER) {
        u_sess->proc_cxt.MyProcPort->sock = PGINVALID_SOCKET;
        t_thrd.threadpool_cxt.worker->NotifyReady();
    }

    /*
     * openGauss main processing loop begins here
     *
     * If an exception is encountered, processing resumes here so we abort the
     * current transaction and start a new one.
     *
     * You might wonder why this isn't coded as an infinite loop around a
     * PG_TRY construct.  The reason is that this is the bottom of the
     * exception stack, and so with PG_TRY there would be no exception handler
     * in force at all during the CATCH part.  By leaving the outermost setjmp
     * always active, we have at least some chance of recovering from an error
     * during error recovery.  (If we get into an infinite loop thereby, it
     * will soon be stopped by overflow of elog.c's internal state stack.)
     */
    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        /* reset signal block flag for threadpool worker */
        ResetInterruptCxt();
        if (g_threadPoolControler) {
            g_threadPoolControler->GetSessionCtrl()->releaseLockIfNecessary();
        }
        gstrace_tryblock_exit(true, oldTryCounter);
        Assert(t_thrd.proc->dw_pos == -1);

        /* Reset hint flag */
        u_sess->parser_cxt.has_hintwarning = false;

        volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
        if ((beentry->st_changecount & 1) != 0) {
            pgstat_increment_changecount_after(beentry);
        }

        (void)pgstat_report_waitstatus(STATE_WAIT_UNDEFINED);
        t_thrd.pgxc_cxt.GlobalNetInstr = NULL;
        /* output the memory tracking information when error happened */
        MemoryTrackingOutputFile();

        /*
         * NOTE: if you are tempted to add more code in this if-block,
         * consider the high probability that it should be in
         * AbortTransaction() instead.	The only stuff done directly here
         * should be stuff that is guaranteed to apply *only* for outer-level
         * error recovery, such as adjusting the FE/BE protocol status.
         */

        /* Since not using PG_TRY, must reset error stack by hand */
        u_sess->plsql_cxt.cur_exception_cxt = NULL;
        u_sess->plsql_cxt.is_exec_autonomous = false;
        t_thrd.log_cxt.error_context_stack = NULL;
        t_thrd.log_cxt.call_stack = NULL;
        /* reset buffer strategy flag */
        t_thrd.storage_cxt.is_btree_split = false;

        SetForceXidFromGTM(false);
        /* reset timestamp_from_cn if error happen between 't' message and start transaction */
        t_thrd.xact_cxt.timestamp_from_cn = false;
        /* reset create schema flag if error happen */
        u_sess->catalog_cxt.setCurCreateSchema = false;
        pfree_ext(u_sess->catalog_cxt.curCreateSchema);

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /*
         * Forget any pending QueryCancel request, since we're returning to
         * the idle loop anyway, and cancel the statement timer if running.
         */
        t_thrd.int_cxt.QueryCancelPending = false;
        disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false; /* again in case timeout occurred */

        /*
         * Turn off these interrupts too.  This is only needed here and not in
         * other exception-catching places since these interrupts are only
         * enabled while we wait for client input.
         */
        t_thrd.postgres_cxt.DoingCommandRead = false;

        /* Make sure libpq is in a good state */
        Port* MyProcPort = u_sess->proc_cxt.MyProcPort;
        if (MyProcPort && MyProcPort->protocol_config->fn_comm_reset) {
            MyProcPort->protocol_config->fn_comm_reset();
        }

        /* statement retry phase : long jump */
        if (IsStmtRetryEnabled()) {
            bool is_extended_query = u_sess->postgres_cxt.doing_extended_query_message;
            if (!is_extended_query && (u_sess->exec_cxt.RetryController->MessageOnExecuting() == S_MESSAGE)) {
                /*
                 * if ereport error when processing a sync message, then doing_extended_query_message will be false,
                 * but we want it to be retried as a pbe message.
                 */
                is_extended_query = true;
            }

            if (IsStmtRetryCapable(u_sess->exec_cxt.RetryController, is_extended_query)) {
                u_sess->exec_cxt.RetryController->TriggerRetry(is_extended_query);
            } else {
                u_sess->exec_cxt.RetryController->DisableRetry();
                if (!pq_disk_is_temp_file_created())
                    pq_disk_disable_temp_file();
            }
        }

        /* Report the error to the client and/or server log */
        EmitErrorReport();

        /* reset global values of perm space */
        perm_space_value_reset();

#ifdef USE_RETRY_STUB
        if (IsStmtRetryEnabled())
            u_sess->exec_cxt.RetryController->stub_.CloseOneStub();
#endif
        /*
         * when clearing the BCM encounter ERROR, we should ResetBCMArray, or it
         * will enter ClearBCMArray infinite loop, then coredump.
         */
        ResetBCMArray();

        /* Now release the active statement reserved. */
        if (ENABLE_WORKLOAD_CONTROL) {
            /* save error to history info */
            save_error_message();

            if (g_instance.wlm_cxt->dynamic_workload_inited) {
                t_thrd.wlm_cxt.parctl_state.errjmp = 1;

                if (t_thrd.wlm_cxt.parctl_state.simple == 0)
                    dywlm_client_release(&t_thrd.wlm_cxt.parctl_state);
                else
                    WLMReleaseGroupActiveStatement();
                dywlm_client_max_release(&t_thrd.wlm_cxt.parctl_state);
            } else
                WLMParctlRelease(&t_thrd.wlm_cxt.parctl_state);

            if (IS_PGXC_COORDINATOR && t_thrd.wlm_cxt.collect_info->sdetail.msg) {
                pfree_ext(t_thrd.wlm_cxt.collect_info->sdetail.msg);
            }
        }
        gsplsql_unlock_func_pkg_dependency_all();
        u_sess->plsql_cxt.pragma_autonomous = false;
        u_sess->plsql_cxt.curr_compile_context = NULL;
        u_sess->plsql_cxt.isCreateFunction = false;
        u_sess->plsql_cxt.isCreatePkg = false;
        u_sess->plsql_cxt.is_alter_compile_stmt = false;
        u_sess->plsql_cxt.during_compile = false;
        u_sess->pcache_cxt.gpc_in_batch = false;
        u_sess->pcache_cxt.gpc_in_try_store = false;
        u_sess->plsql_cxt.have_error = false;
        u_sess->parser_cxt.isPerform = false;
        u_sess->parser_cxt.in_userset = false;
        u_sess->parser_cxt.has_set_uservar = false;
        u_sess->parser_cxt.has_equal_uservar = false;
        u_sess->parser_cxt.stmt = NULL;
        OpFusion::tearDown(u_sess->exec_cxt.CurrentOpFusionObj);
        /* init pbe execute status when long jump */
        u_sess->xact_cxt.pbe_execute_complete = true;

        /* init row trigger shipping status when long jump */
        u_sess->tri_cxt.exec_row_trigger_on_datanode = false;

        u_sess->opt_cxt.xact_modify_sql_patch = false;
        u_sess->opt_cxt.nextval_default_expr_type = 0;

        u_sess->statement_cxt.executer_run_level = 0;

        /* reset stream for-loop flag when long jump */
        u_sess->SPI_cxt.has_stream_in_cursor_or_forloop_sql = false;

        /* release operator-level hash table in memory */
        releaseExplainTable();

        if (StreamTopConsumerAmI() && u_sess->debug_query_id != 0) {
            gs_close_all_stream_by_debug_id(u_sess->debug_query_id);
        }

        /* Mark recursive vfd is invalid before aborting transaction. */
        StreamNodeGroup::MarkRecursiveVfdInvalid();

        BgworkerListSyncQuit();
        
        /* clean autonomous session */
        DestoryAutonomousSession(true);
        /*
         * Abort the current transaction in order to recover.
         */

        if (u_sess->is_autonomous_session) {
            AbortOutOfAnyTransaction();
        } else {
            AbortCurrentTransaction();
        }

        ReleaseResownerOutOfTransaction();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);
        /* Notice: at the most time it isn't necessary to call because
         *   all the LWLocks are released in AbortCurrentTransaction().
         *   but in some rare exception not in one transaction (for
         *   example the following InitMultinodeExecutor() calling )
         *   maybe hold LWLocks unused.
         */
        LWLockReleaseAll();
        AbortBufferIO();

#ifndef ENABLE_MULTIPLE_NODES
        gsplsql_unlock_func_pkg_dependency_all();
#endif

        /* We should syncQuit after LWLockRelease to avoid dead lock of LWLocks. */
        RESUME_INTERRUPTS();
        StreamNodeGroup::syncQuit(STREAM_ERROR);
        StreamNodeGroup::destroy(STREAM_ERROR);

#ifndef ENABLE_MULTIPLE_NODES
        clean_up_debug_client(true);
        DebugInfo* debug_server = u_sess->plsql_cxt.cur_debug_server;
        u_sess->plsql_cxt.has_step_into = false;
        clean_up_debug_server(debug_server, false, true);
        u_sess->plsql_cxt.cur_debug_server = NULL;
#endif
        u_sess->plsql_cxt.during_compile = false;
#ifdef ENABLE_MULTIPLE_NODES
        /* reset send role flag */
        if (InSendingLocalUserIdChange()) {
            u_sess->misc_cxt.SecurityRestrictionContext &= (~SENDER_LOCAL_USERID_CHANGE);
        }
        /* reset receive role flag */
        if (InReceivingLocalUserIdChange()) {
            SetUserIdAndSecContext(GetOldUserId(true),
                u_sess->misc_cxt.SecurityRestrictionContext & (~RECEIVER_LOCAL_USERID_CHANGE));
        }
#endif
        HOLD_INTERRUPTS();
        ForgetRegisterStreamSnapshots();

        /* reset query_id after sync quit */
        pgstat_report_queryid(0);
        pgstat_report_unique_sql_id(true);

        /*
         * Make sure debug_query_string gets reset before we possibly clobber
         * the storage it points at.
         */
        t_thrd.postgres_cxt.debug_query_string = NULL;
        t_thrd.postgres_cxt.cur_command_tag = T_Invalid;

        if (u_sess->unique_sql_cxt.need_update_calls &&
            is_unique_sql_enabled() && is_local_unique_sql()) {
            instr_unique_sql_report_elapse_time(u_sess->unique_sql_cxt.unique_sql_start_time);
        }
        /* reset unique sql */
        ResetCurrentUniqueSQL(true);
        SetIsTopUniqueSQL(false);

        PortalErrorCleanup();
        SPICleanup();

        /*
         * We can't release replication slots inside AbortTransaction() as we
         * need to be able to start and abort transactions while having a slot
         * acquired. But we never need to hold them across top level errors,
         * so releasing here is fine. There's another cleanup in ProcKill()
         * ensuring we'll correctly cleanup on FATAL errors as well.
         */
        CleanMyReplicationSlot();

        if (AlignMemoryContext != NULL)
            MemoryContextReset(AlignMemoryContext);

        // reinit record time class and related memory.
        og_record_time_reinit();

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
        FlushErrorState();

        /*
         * If we were handling an extended-query-protocol message, initiate
         * skip till next Sync.  This also causes us not to issue
         * ReadyForQuery (until we get Sync).
         */
        if (u_sess->postgres_cxt.doing_extended_query_message)
            u_sess->postgres_cxt.ignore_till_sync = true;

        /* We don't have a transaction command open anymore */
        t_thrd.postgres_cxt.xact_started = false;

        t_thrd.xact_cxt.isSelectInto = false;
        t_thrd.xact_cxt.callPrint = false;

        u_sess->pcache_cxt.cur_stmt_name = NULL;
        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /* Now we not allow pool_validate interrupts again */
        PREVENT_POOL_VALIDATE_SIGUSR2();
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    gs_signal_unblock_sigusr2();

    if (!u_sess->postgres_cxt.ignore_till_sync)
        send_ready_for_query = true; /* initially, or after error */

    t_thrd.proc_cxt.postgres_initialized = true;

    PG_TRY();
    {
        pq_disk_discard_temp_file();
    }
    PG_CATCH();
    {
        elog(LOG, "catch error while discard temp file");
        FlushErrorState();
    }
    PG_END_TRY();
    /* statement retry phase : RI */
    if (IsStmtRetryEnabled() && u_sess->exec_cxt.RetryController->IsQueryRetrying()) {
        /*
         * if stmt is retring, we can't send ready for query
         * if we are retrying a extend query message, we can't ignore message before
         * sync message is received
         */
        u_sess->debug_query_id = 0;
        send_ready_for_query = false;
        ++u_sess->exec_cxt.RetryController->retry_times;

        /*
         * here are some actions can't be done in long jump
         */

        /* clean send buffer and temp file */
        pq_abandon_sendbuffer();

        u_sess->exec_cxt.RetryController->cached_msg_context.Reset();

        if (u_sess->exec_cxt.RetryController->IsExtendQueryRetrying()) {
            u_sess->postgres_cxt.ignore_till_sync = false;
            u_sess->exec_cxt.RetryController->CleanPreparedStmt();
        }
    }

    bool template0_locked = false;
    OgRecordOperator _local_tmp_opt(false, SRT13_BEFORE_QUERY);
    OgRecordOperator _local_tmp_opt1(false, SRT14_AFTER_QUERY);
    /*
     * Non-error queries loop here.
     */
    for (;;) {
        if (og_time_record_is_started()) {
            _local_tmp_opt.enter();
        }
        /*
         * Since max_query_rerty_times is a USERSET GUC, so must check Statement retry
         * in each query loop here.
         */
        StmtRetryValidate(u_sess->exec_cxt.RetryController);

        /* Add the pg_delete_audit operation to audit log */
        t_thrd.audit.Audit_delete = false;

        /* clear key message that may appear in core file for security */
        clear_memory((char*)t_thrd.postgres_cxt.clobber_qstr, input_message.data, input_message.maxlen);
        t_thrd.postgres_cxt.clobber_qstr = NULL;

        /*
         * Release storage left over from prior query cycle, and create a new
         * query input buffer in the cleared t_thrd.mem_cxt.msg_mem_cxt.
         */
        MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
        MemoryContextResetAndDeleteChildren(t_thrd.mem_cxt.msg_mem_cxt);
        MemoryContextResetAndDeleteChildren(u_sess->temp_mem_cxt);
        MemoryContextResetAndDeleteChildren(u_sess->stat_cxt.hotkeySessContext);
        u_sess->stat_cxt.hotkeyCandidates = NIL;
        u_sess->plsql_cxt.pass_func_tupdesc = NULL;

        /* reset plpgsql compile flag */
        u_sess->plsql_cxt.compile_context_list = NULL;
        u_sess->plsql_cxt.curr_compile_context = NULL;

        u_sess->plsql_cxt.compile_status = NONE_STATUS;
        u_sess->plsql_cxt.func_tableof_index = NULL;
        u_sess->plsql_cxt.portal_depth = 0;
        u_sess->plsql_cxt.isCreateFunction = false;
        u_sess->plsql_cxt.isCreatePkg = false;
        u_sess->plsql_cxt.is_alter_compile_stmt = false;
        u_sess->plsql_cxt.during_compile = false;
        t_thrd.utils_cxt.STPSavedResourceOwner = NULL;
        if (u_sess->plsql_cxt.depend_mem_cxt != NULL) {
            MemoryContextDelete(u_sess->plsql_cxt.depend_mem_cxt);
            u_sess->plsql_cxt.depend_mem_cxt = NULL;
        }
        u_sess->statement_cxt.executer_run_level = 0;

        initStringInfo(&input_message);
        t_thrd.postgres_cxt.debug_query_string = NULL;
        t_thrd.postgres_cxt.cur_command_tag = T_Invalid;
        t_thrd.postgres_cxt.g_NoAnalyzeRelNameList = NIL;
        u_sess->analyze_cxt.is_under_analyze = false;
        u_sess->exec_cxt.isLockRows = false;
        t_thrd.postgres_cxt.mark_explain_analyze = false;
        t_thrd.postgres_cxt.mark_explain_only = false;
        u_sess->SPI_cxt.has_stream_in_cursor_or_forloop_sql = false;
        if (unlikely(t_thrd.log_cxt.msgbuf->data != NULL)) {
            pfree_ext(t_thrd.log_cxt.msgbuf->data);
        }
        t_thrd.log_cxt.msgbuf->cursor = 0;
        t_thrd.log_cxt.msgbuf->len = 0;
        lc_replan_nodegroup = InvalidOid;
        /* reset xmin before ReadCommand, in case blocking redo */
        if (RecoveryInProgress()) {
            
        }

        /*
         * (1) If we've reached idle state, tell the frontend we're ready for
         * a new query.
         *
         * Note: this includes fflush()'ing the last of the prior output.
         *
         * This is also a good time to send collected statistics to the
         * collector, and to update the PS stats display.  We avoid doing
         * those every time through the message loop because it'd slow down
         * processing of batched messages, and because we don't want to report
         * uncommitted updates (that confuses autovacuum).	The notification
         * processor wants a call too, if we are not in a transaction block.
         */
        if (send_ready_for_query) {
            /*
             * Instrumentation: should update unique sql stat here
             *
             * when send_ready_for_query is true, each SQL should be run finished.
             * we don't care abort transaction status, we focus on unique sql
             * row activity and Cache/IO on DN nodes.
             *
             * when shutdown, will call pgstat_report_stat,
             * we maybe need to hanlde this case
             */
            if (is_unique_sql_enabled()) {
                if (need_update_unique_sql_row_stat())
                    UpdateUniqueSQLStatOnRemote();
                UniqueSQLStatCountResetReturnedRows();
                UniqueSQLStatCountResetParseCounter();
            }

            if (IsStmtRetryEnabled()) {
#ifdef USE_RETRY_STUB
                u_sess->exec_cxt.RetryController->stub_.FinishStubTest(u_sess->exec_cxt.RetryController->PBEFlowStr());
#endif
                u_sess->exec_cxt.RetryController->FinishRetry();
            }

            if (IsAbortedTransactionBlockState()) {
                set_ps_display("idle in transaction (aborted)", false);
                pgstat_report_activity(STATE_IDLEINTRANSACTION_ABORTED, NULL);
            } else if (IsTransactionOrTransactionBlock()) {
                set_ps_display("idle in transaction", false);
                pgstat_report_activity(STATE_IDLEINTRANSACTION, NULL);
            } else {
                ProcessCompletedNotifies();
                pgstat_report_stat(false);

                set_ps_display("idle", false);
                pgstat_report_activity(STATE_IDLE, NULL);
            }

            /* We're ready for a new query, reset wait status and u_sess->debug_query_id */
            pgstat_report_waitstatus(STATE_WAIT_UNDEFINED);
            pgstat_report_queryid(0);
            pgstat_report_unique_sql_id(true);

            u_sess->trace_cxt.trace_id[0] = '\0';
            /*
             * If connection to client is lost, we do not need to send message to client.
             */
            Port* MyProcPort = u_sess->proc_cxt.MyProcPort;
            if (IS_CLIENT_CONN_VALID(MyProcPort) && (!t_thrd.int_cxt.ClientConnectionLost)) {
                if (libpqsw_need_end()) {
                    /*
                     * before send 'Z' to frontend, we should check if INTERRUPTS happends or not.
                     * In SyncRepWaitForLSN() function, take HOLD_INTERRUPTS() to prevent interrupt happending and set
                     * t_thrd.postgres_cxt.whereToSendOutput to 'DestNone' when t_thrd.int_cxt.ProcDiePending is true. Then
                     * this session will not send 'Z' message to frontend and will not report error if it not check
                     * interrupt. So frontend will be reveiving message forever and do nothing, which is wrong.
                     */
                    CHECK_FOR_INTERRUPTS();
                    MyProcPort->protocol_config->fn_send_ready_for_query((CommandDest)t_thrd.postgres_cxt.whereToSendOutput);
               }
            }
            libpqsw_set_end(true);
#ifdef ENABLE_MULTIPLE_NODES
            /*
             * Helps us catch any problems where we did not send down a snapshot
             * when it was expected. However if any deferred trigger is supposed
             * to be fired at commit time we need to preserve the snapshot sent previously
             */
            if ((IS_PGXC_DATANODE || IsConnFromCoord()) && !IsAnyAfterTriggerDeferred()) {
                UnsetGlobalSnapshotData();
            }
#endif
            /* update our elapsed time statistics. */
            if (og_time_record_is_started()) {
                _local_tmp_opt.exit();
            }
            timeInfoRecordEnd();

            /* reset unique_sql_id & stat
             *
             * cannot reset unique_sql_cn_id here, as unique_sql_cn_id
             * is generated in parser hook, and it related to current
             * node, so can be reused, if reset here, PBE will
             * lost the unique sql entry.
             */
            ReportQueryStatus();
            statement_commit_metirc_context();
            ResetCurrentUniqueSQL();

            send_ready_for_query = false;
        } else {
            /* update our elapsed time statistics. */
            if (og_time_record_is_started()) {
                _local_tmp_opt.exit();
            }
            timeInfoRecordEnd();
        }
        /*
         * INSTR: when track type is TOP, we reset is_top_unique_sql to false,
         * for P messages,
         *   - call SetIsTopUniqueSQL(false), so each P message can generate unique
         *   sql id
         */
        if (IS_UNIQUE_SQL_TRACK_TOP)
            SetIsTopUniqueSQL(false);

        /* disable tempfile anyway */
        pq_disk_disable_temp_file();

        if (IS_THREAD_POOL_WORKER) {
            t_thrd.threadpool_cxt.worker->WaitMission();
            Assert(CheckMyDatabaseMatch());
            if (!g_instance.archive_obs_cxt.in_switchover && !g_instance.streaming_dr_cxt.isInSwitchover) {
                Assert(u_sess->status != KNL_SESS_FAKE);
            }
        } else {
            /* if we do alter db, reinit syscache */
            ReLoadLSCWhenWaitMission();
        }

        char *name = NULL;
        if (!template0_locked && u_sess->attr.attr_common.IsInplaceUpgrade &&
            u_sess->attr.attr_common.upgrade_mode > 0 && (name = get_database_name(u_sess->proc_cxt.MyDatabaseId)) &&
            strcmp(name, "template0") == 0) {
            if (IsTransactionOrTransactionBlock()) {
                LockSharedObjectForSession(DatabaseRelationId, u_sess->proc_cxt.MyDatabaseId, 0, ShareLock);
            } else {
                StartTransactionCommand();
                LockSharedObjectForSession(DatabaseRelationId, u_sess->proc_cxt.MyDatabaseId, 0, ShareLock);
                CommitTransactionCommand();
            }
            template0_locked = true;
        }

        if (isRestoreMode && !IsAbortedTransactionBlockState()) {
            ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;

            /* we use t_thrd.top_mem_cxt to remember all node info in this cluster. */
            MemoryContext old = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

            t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "ForPGXCNodes",
                THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));

            /* Update node table in the shared memory */
            PgxcNodeListAndCount();

            /* Get classified list of node Oids */
            PgxcNodeGetOids(NULL, NULL, &u_sess->pgxc_cxt.NumCoords, &u_sess->pgxc_cxt.NumDataNodes, true);

            ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
            ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
            ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);

            ResourceOwner newOwner = t_thrd.utils_cxt.CurrentResourceOwner;
            t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;

            ResourceOwnerDelete(newOwner);

            (void)MemoryContextSwitchTo(old);
        }

        /*
         * Record client connection establish time, which start on incommining resuest arrives e.g. poll()
         * invoked to accept() and end on returning message by server side clientfd.
         * One session records only once.
         */
        u_sess->clientConnTime_cxt.checkOnlyInConnProcess = false;

        /*
         * Check cache size to see if we need to AcceptInvalidationMessages.
         */
        CleanSystemCaches(true);

        CHECK_FOR_INTERRUPTS();

        /*
         * (2) Allow asynchronous signals to be executed immediately if they
         * come in while we are waiting for client input. (This must be
         * conditional since we don't want, say, reads on behalf of COPY FROM
         * STDIN doing the same thing.)
         */
        t_thrd.postgres_cxt.DoingCommandRead = true;

#ifdef MEMORY_CONTEXT_CHECKING
        MemoryContextCheck(t_thrd.top_mem_cxt, false);
#endif
        /*
         * (3) read a command (loop blocks here)
         */
        if (saved_whereToSendOutput != DestNone)
            t_thrd.postgres_cxt.whereToSendOutput = saved_whereToSendOutput;

        firstchar = ReadCommand(&input_message);
#ifdef USE_SPQ
        t_thrd.spq_ctx.spq_role = ROLE_UTILITY; 
#endif
        /* update our elapsed time statistics. */
        timeInfoRecordStart();
        _local_tmp_opt1.enter();

        /* stmt retry routine phase : pack input_message */
        if (IsStmtRetryEnabled()) {
#ifdef USE_ASSERT_CHECKING
            if (u_sess->exec_cxt.RetryController->IsExtendQueryRetrying()) {
                u_sess->exec_cxt.RetryController->ValidateExecuting(firstchar);
            }
#endif
            if (IsQueryMessage(firstchar)) {
                u_sess->exec_cxt.RetryController->CacheCommand(firstchar, &input_message);
                pq_disk_enable_temp_file();
            }
            u_sess->exec_cxt.RetryController->TrackMessageOnExecuting(firstchar);
            u_sess->exec_cxt.RetryController->LogTraceInfo(MessageInfo(firstchar, input_message.len));
            ereport(DEBUG2, (errmodule(MOD_CN_RETRY), errmsg("%s cache command.", PRINT_PREFIX_TYPE_PATH)));
        }

        /*
         * (4) disable async signal conditions again.
         */
        t_thrd.postgres_cxt.DoingCommandRead = false;

        CHECK_FOR_INTERRUPTS();
        /*
         * (5) check for any other interesting events that happened while we
         * slept.
         */
        reload_configfile();

        // (6) process pooler reload before the next transaction begin.
        //
        if (IsGotPoolReload() &&
            !IsTransactionOrTransactionBlock() && !IsConnFromGTMTool()) {
            processPoolerReload();
            ResetGotPoolReload(false);
        }

        /*
         * call the protocol hook to process the request.
         */
        if (firstchar != EOF && u_sess->proc_cxt.MyProcPort &&
            u_sess->proc_cxt.MyProcPort->protocol_config->fn_process_command) {
            firstchar = u_sess->proc_cxt.MyProcPort->protocol_config->fn_process_command(&input_message);
            send_ready_for_query = true;
            _local_tmp_opt1.exit();
            continue;
        }

        /*
         * (7) process the command.  But ignore it if we're skipping till
         * Sync.
         */
        if (u_sess->postgres_cxt.ignore_till_sync && firstchar != EOF) {
            _local_tmp_opt1.exit();
            continue;
        }
#ifdef ENABLE_MULTIPLE_NODES
        // reset some flag related to stream
        ResetSessionEnv();
#else
        t_thrd.subrole = NO_SUBROLE;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->initMemInChunks = t_thrd.utils_cxt.trackedMemChunks;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->queryMemInChunks = t_thrd.utils_cxt.trackedMemChunks;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->peakChunksQuery = t_thrd.utils_cxt.trackedMemChunks;
#endif
        t_thrd.utils_cxt.peakedBytesInQueryLifeCycle = 0;
        t_thrd.utils_cxt.basedBytesInQueryLifeCycle = 0;

        t_thrd.codegen_cxt.codegen_IRload_thr_count = 0;
        IsExplainPlanStmt = false;
        t_thrd.codegen_cxt.g_runningInFmgr = false;
        PTFastQueryShippingStore = true;
        u_sess->opt_cxt.is_under_append_plan = false;
        u_sess->opt_cxt.xact_modify_sql_patch = false;
        u_sess->opt_cxt.nextval_default_expr_type = 0;

        u_sess->attr.attr_sql.explain_allow_multinode = false;
        u_sess->pbe_message = NO_QUERY;

        /* Reset store procedure's session variables. */
        stp_reset_stmt();
        MemoryContext oldMemory =
            MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
#ifdef ENABLE_LLVM_COMPILE
        CodeGenThreadInitialize();
#endif
        (void)MemoryContextSwitchTo(oldMemory);
        u_sess->exec_cxt.single_shard_stmt = false;
        /* Set statement_timestamp */
        SetCurrentStatementStartTimestamp();

        if (u_sess->proc_cxt.MyProcPort && u_sess->proc_cxt.MyProcPort->is_logic_conn)
            LIBCOMM_DEBUG_LOG("postgres to node[nid:%d,sid:%d] with msg:%c.",
                u_sess->proc_cxt.MyProcPort->gs_sock.idx,
                u_sess->proc_cxt.MyProcPort->gs_sock.sid,
                firstchar);

        if (libpqsw_process_message(firstchar, &input_message)) {
            _local_tmp_opt1.exit();
            continue;
        }
        _local_tmp_opt1.exit();

        switch (firstchar) {
#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)
            case 'Z':  // exeute plan directly.
            {
                char* plan_string = NULL;
                PlannedStmt* planstmt = NULL;
                int oLen_msg = 0;
                int cLen_msg = 0;
                t_thrd.spq_ctx.spq_role = ROLE_QUERY_EXECUTOR;

                /* Set top consumer at the very beginning. */
                StreamTopConsumerIam();
                /* Build stream context for stream plan. */
                InitStreamContext();

                u_sess->exec_cxt.under_stream_runtime = true;

                // get the node id.
                u_sess->pgxc_cxt.PGXCNodeId = pq_getmsgint(&input_message, 4);

                // Get original length and length of compressed plan.
                //
                oLen_msg = pq_getmsgint(&input_message, 4);
                cLen_msg = pq_getmsgint(&input_message, 4);

                if (unlikely(oLen_msg <= 0 || cLen_msg <= 0 || cLen_msg > oLen_msg)) {
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg(
                                "unexpected original length %d and length of compressed plan %d", oLen_msg, cLen_msg)));
                }

                // Copy compressed data from message buffer and then decompress it.
                //
                plan_string = (char*)palloc0(cLen_msg);
                pq_copymsgbytes(&input_message, plan_string, cLen_msg);
                plan_string = DecompressSerializedPlan(plan_string, cLen_msg, oLen_msg);

                pq_getmsgend(&input_message);
                ereport(DEBUG2, (errmsg("PLAN END RECEIVED : %s", plan_string)));

                // Starting the transaction early to initialize ResourceOwner,
                // else, Plan de-serialization code path fails
                start_xact_command();

                MemoryContext old_cxt = MemoryContextSwitchTo(u_sess->stream_cxt.stream_runtime_mem_cxt);
                planstmt = (PlannedStmt*)stringToNode(plan_string);

                /* It is safe to free plan_string after deserializing the message */
                if (plan_string != NULL)
                    pfree(plan_string);

                InitGlobalNodeDefinition(planstmt);
                t_thrd.spq_ctx.spq_session_id = planstmt->spq_session_id;
                t_thrd.spq_ctx.current_id = planstmt->current_id;

                statement_init_metric_context();
                exec_simple_plan(planstmt);

                MemoryContextSwitchTo(old_cxt);

                // After query done, producer container is not usable anymore.
                StreamNodeGroup::destroy(STREAM_COMPLETE);
                u_sess->debug_query_id = 0;
                send_ready_for_query = true;
            } break;

            case 'Y': /* plan with params */
            {
                t_thrd.spq_ctx.spq_role = ROLE_QUERY_EXECUTOR;
                if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && (!IS_SPQ_EXECUTOR))
                    ereport(ERROR,
                        (errcode(ERRCODE_PROTOCOL_VIOLATION),
                            errmsg("invalid frontend message type '%c'.", firstchar)));

                /* Set top consumer at the very beginning. */
                StreamTopConsumerIam();
                /* Build stream context for stream plan. */
                InitStreamContext();

                u_sess->exec_cxt.under_stream_runtime = true;

                u_sess->pgxc_cxt.PGXCNodeId = pq_getmsgint(&input_message, 4);

                exec_plan_with_params(&input_message);

                /* After query done, producer container is not usable anymore */
                StreamNodeGroup::destroy(STREAM_COMPLETE);
                u_sess->debug_query_id = 0;
                send_ready_for_query = true;
            } break;
#endif

            case 'u': /* Autonomous transaction */
            {
                u_sess->is_autonomous_session = true; 
                Oid currentUserId = pq_getmsgint(&input_message, 4);
                u_sess->autonomous_parent_sessionid = pq_getmsgint64(&input_message);
                if (currentUserId != GetCurrentUserId()) {
                    /* Set Session Authorization */
                    ResourceOwner currentOwner = NULL;
                    currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
                    /* we use session memory context to remember all node info in this cluster. */
                    MemoryContext old = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
                    
                    ResourceOwner tmpOwner =
                            ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, "CheckUserOid",
                                THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY));
                    t_thrd.utils_cxt.CurrentResourceOwner = tmpOwner;

                    SetSessionAuthorization(currentUserId, superuser_arg(currentUserId));
                    
                    if (u_sess->proc_cxt.MyProcPort->user_name)
                        pfree(u_sess->proc_cxt.MyProcPort->user_name);

                    u_sess->proc_cxt.MyProcPort->user_name = pstrdup(GetUserNameFromId(currentUserId));
                    u_sess->misc_cxt.CurrentUserName = u_sess->proc_cxt.MyProcPort->user_name;
#ifdef ENABLE_MULTIPLE_NODES
                    InitMultinodeExecutor(false);
                    PoolHandle* pool_handle = GetPoolManagerHandle();
                    if (pool_handle == NULL) {
                        ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("Can not connect to pool manager")));
                        break;
                    }

                    /*
                     * We must switch to old memory context, if we use TopMemContext,
                     * that will cause memory leak when call elog(ERROR), because TopMemContext
                     * only is reset during thread exit. later we need refactor code section which
                     * allocate from TopMemContext for better. TopMemcontext is a session level memory
                     * context, we forbid allocate temp memory from TopMemcontext.
                     */
                    MemoryContextSwitchTo(old);
                    char* session_options_ptr = session_options();

                    /* Pooler initialization has to be made before ressource is released */
                    PoolManagerConnect(pool_handle,
                        u_sess->proc_cxt.MyProcPort->database_name,
                        u_sess->proc_cxt.MyProcPort->user_name,
                        session_options_ptr);
                    if (session_options_ptr != NULL)
                        pfree(session_options_ptr);
                    MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
#endif                    
                    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
                    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_LOCKS, true, true);
                    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
                    t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
                    ResourceOwnerDelete(tmpOwner);
                    (void)MemoryContextSwitchTo(old); /* fall through */
                }
            }
            case 'Q': /* simple query */
            {
                const char* query_string = NULL;
                OgRecordAutoController _local_opt(SRT1_Q);

                pgstat_report_trace_id(&u_sess->trace_cxt, true);
                query_string = pq_getmsgstring(&input_message);
                if (query_string == NULL) {
                    ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                    errmsg("query_string is NULL.")));
                }
                if (strlen(query_string) > SECUREC_MEM_MAX_LEN) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("Too long query_string.")));
                }

                t_thrd.postgres_cxt.clobber_qstr = query_string;

                pq_getmsgend(&input_message);

                pgstatCountSQL4SessionLevel();
                statement_init_metric_context();
#ifdef USE_RETRY_STUB
                if (IsStmtRetryEnabled()) {
                    u_sess->exec_cxt.RetryController->stub_.StartOneStubTest(firstchar);
                    u_sess->exec_cxt.RetryController->stub_.ECodeStubTest();
                }
#endif
                exec_simple_query(query_string, QUERY_MESSAGE, &input_message); /* @hdfs Add the second parameter */

                if (MEMORY_TRACKING_QUERY_PEAK)
                    ereport(LOG, (errmsg("query_string %s, peak memory %ld(kb)", query_string,
                                         (int64)(t_thrd.utils_cxt.peakedBytesInQueryLifeCycle/1024))));
                u_sess->debug_query_id = 0;
                send_ready_for_query = true;
            } break;
#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)
            case 'O': /* In pooler stateless resue mode reset connection params */
            {
                const char* query_string = NULL;
                char sql[PARAMS_LEN] = {0};
                char* sql_strtok_r = NULL;
                char* slot_session_reset = NULL;
                char* user_name_reset = NULL;
                char* pgoptions_reset = NULL;
                char* connection_params = NULL;
                char* connection_temp_namespace = NULL;
                int err = -1;
                MemoryContext oldcontext;

                /* Only pooler stateless reuse mode in the cluster uses 'O' packets. Other paths are invalid. */
                if(!IsConnFromCoord()) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Invalid packet path, remoteConnType[%d], remote_host[%s], remote_port[%s].",
                                u_sess->attr.attr_common.remoteConnType,
                                u_sess->proc_cxt.MyProcPort->remote_host,
                                u_sess->proc_cxt.MyProcPort->remote_port)));
                }

                query_string = pq_getmsgstring(&input_message);

                t_thrd.postgres_cxt.clobber_qstr = query_string;

                pq_getmsgend(&input_message);

                pgstatCountSQL4SessionLevel();

                if (strlen(query_string) + 1 > sizeof(sql)) {
                    PrintUnexpectedBufferContent(query_string, sizeof(sql));
                    ereport(ERROR,
                        (errcode(ERRCODE_SYSTEM_ERROR),
                            errmsg("Acceptter in pooler stateless resue mode reset connection params %d > sql[%d].",
                                (int)strlen(query_string) + 1,
                                (int)sizeof(sql))));
                }

                err = sprintf_s(sql, sizeof(sql), "%s", query_string);
                securec_check_ss(err, "", "");

                slot_session_reset = strtok_r(sql, "@", &sql_strtok_r);
                user_name_reset = strtok_r(NULL, "@", &sql_strtok_r);
                pgoptions_reset = strtok_r(NULL, "@", &sql_strtok_r);
                connection_temp_namespace = strtok_r(NULL, "@", &sql_strtok_r);
                connection_params = sql_strtok_r;

                /* To reset slot session */
                if (slot_session_reset != NULL) {
                    exec_simple_query(slot_session_reset, QUERY_MESSAGE);
                }

                /* Reset user_name pgoptions */
                if (user_name_reset != NULL && pgoptions_reset != NULL) {
                    oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));

                    /* check if role exits */
                    ResourceOwner currentOwner = NULL;
                    currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
                    ResourceOwner tmpOwner =
                            ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, "CheckUserOid",
                                THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY));
                    t_thrd.utils_cxt.CurrentResourceOwner = tmpOwner;
                    if (!OidIsValid(get_role_oid(user_name_reset, false))) {
                        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("role \"%s\" does not exist", user_name_reset)));
                    }
                    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
                    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_LOCKS, true, true);
                    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
                    t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
                    ResourceOwnerDelete(tmpOwner);

                    if (u_sess->proc_cxt.MyProcPort->user_name)
                        pfree(u_sess->proc_cxt.MyProcPort->user_name);
                    if (u_sess->proc_cxt.MyProcPort->cmdline_options)
                        pfree(u_sess->proc_cxt.MyProcPort->cmdline_options);

                    u_sess->proc_cxt.MyProcPort->user_name = pstrdup(user_name_reset);
                    u_sess->proc_cxt.MyProcPort->cmdline_options = pstrdup(pgoptions_reset);

                    (void)MemoryContextSwitchTo(oldcontext);

                    t_thrd.postgres_cxt.isInResetUserName = true;
                    PostgresResetUsernamePgoption(u_sess->proc_cxt.MyProcPort->user_name, true);
                    t_thrd.postgres_cxt.isInResetUserName = false;
                }

                /* set connection sesseion params , if sender params is sent "null" not exec. */
                char params_null[] = "null;";
                if (connection_params != NULL && strcmp(params_null, connection_params) != 0) {
                    exec_simple_query(connection_params, QUERY_MESSAGE);
                }

                /* set temp_namespace , if sender temp_namespace is sent "null" not exec. */
                if (connection_temp_namespace != NULL && strcmp(params_null, connection_temp_namespace) != 0) {
                    exec_simple_query(connection_temp_namespace, QUERY_MESSAGE);
                }
                u_sess->debug_query_id = 0;
                send_ready_for_query = true;
            } break;

            case 'o': {
                // switch role
                if (unlikely(!IsConnFromCoord())) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Unsupport receive role msg from remote type[%d], remote host[%s], remote port[%s].",
                                   u_sess->attr.attr_common.remoteConnType,
                                   u_sess->proc_cxt.MyProcPort->remote_host,
                                   u_sess->proc_cxt.MyProcPort->remote_port)));
                }
                bool is_reset = (bool)pq_getmsgbyte(&input_message);
                const char* role_name = pq_getmsgstring(&input_message);
                if (role_name == NULL) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("wrong role name.")));
                }
                Oid role_oid = GetRoleOid(role_name);
                Oid save_userid = InvalidOid;
                int save_sec_context = 0;
                int sec_context = 0;
                GetUserIdAndSecContext(&save_userid, &save_sec_context);

                if (is_reset) {
                    /* reset to origin role */
                    sec_context = save_sec_context & (~RECEIVER_LOCAL_USERID_CHANGE);
                } else {
                    /* set to cn's role */
                    sec_context = save_sec_context | RECEIVER_LOCAL_USERID_CHANGE;
                    SetOldUserId(GetCurrentUserId(), true);
                }
                SetUserIdAndSecContext(role_oid, sec_context);
            } break;

            case 'I': {
                // Procedure overrideStack
                int pushtype;
                const char* schema_name = NULL;
                pushtype = pq_getmsgbyte(&input_message);
                schema_name = pq_getmsgstring(&input_message);
                if (strlen(schema_name) > SECUREC_MEM_MAX_LEN) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("Too long schema_name.")));
                }

                pq_getmsgend(&input_message);

                switch (pushtype) {
                    case 'P': {
                        // check the list is override max depth
                        if (list_length(u_sess->catalog_cxt.overrideStack) > OVERRIDE_STACK_LENGTH_MAX) {
                            ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                            errmsg("The overrideStack list has been reach the max length.")));
                        }
                        // push
                        ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
                        MemoryContext old = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
                        ResourceOwner tmpOwner =
                            ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, "ForPushProcedure",
                                THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
                        t_thrd.utils_cxt.CurrentResourceOwner = tmpOwner;

                        Oid namespaceOid = get_namespace_oid(schema_name, false);

                        ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
                        ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_LOCKS, true, true);
                        ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
                        t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
                        ResourceOwnerDelete(tmpOwner);

                        MemoryContextSwitchTo(old);

                        // construct OverrideSearchPath struct
                        OverrideSearchPath* search_path = (OverrideSearchPath*)palloc0(sizeof(OverrideSearchPath));
                        search_path->addCatalog = true;
                        search_path->addTemp = true;
                        search_path->schemas = list_make1_oid(namespaceOid);

                        ereport(DEBUG2, (errmodule(MOD_SCHEMA), errmsg("recv pushschema:%s", schema_name)));

                        if (SUPPORT_BIND_SEARCHPATH) {
                            if (!u_sess->catalog_cxt.baseSearchPathValid) {
                                recomputeNamespacePath();
                            }
                            /*
                             * If SUPPORT_BIND_SEARCHPATH is true,
                             * add system's search_path.
                             * When objects cannot be found in the
                             * namespace of current function, find
                             * them in search_path list.
                             * Otherwise, we can only find objects in
                             * the namespace of current function.
                             */
                            ListCell* l = NULL;
                            /* Use baseSearchPath not activeSearchPath. */
                            foreach (l, u_sess->catalog_cxt.baseSearchPath) {
                                Oid namespaceId = lfirst_oid(l);
                                /*
                                 * Append namespaceId to searchpath.
                                 */
                                if (!list_member_oid(search_path->schemas, namespaceId)) {
                                    search_path->schemas = lappend_oid(search_path->schemas, namespaceId);
                                }
                            }
                        }
                        PushOverrideSearchPath(search_path, true);
                        pfree(search_path);
                    } break;
                    case 'p': {
                        // pop
                        if (u_sess->catalog_cxt.overrideStack)
                            PopOverrideSearchPath();
                    } break;
                    case 'C': {
                        /* set create command schema */
                        u_sess->catalog_cxt.setCurCreateSchema = true;
                        u_sess->catalog_cxt.curCreateSchema = MemoryContextStrdup(u_sess->top_transaction_mem_cxt, 
                            schema_name);
                    } break;
                    case 'F': {
                        u_sess->catalog_cxt.setCurCreateSchema = false;
                        pfree_ext(u_sess->catalog_cxt.curCreateSchema);
                    } break;
                    default:
                        ereport(ERROR,
                            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                errmsg("Invalid message type %d for procedure overrideStack.", pushtype)));
                        break;
                }
            } break;
            case 'i': /* used for instrumentation */
            {
                int sub_command = 0;
                sub_command = pq_getmsgbyte(&input_message);

                switch (sub_command) {
                    case 'q': /* recv unique sql id from CN, and set it to DN session */
                    {
                        u_sess->unique_sql_cxt.unique_sql_cn_id = (uint32)pq_getmsgint(&input_message, sizeof(uint32));
                        u_sess->unique_sql_cxt.unique_sql_user_id = (Oid)pq_getmsgint(&input_message, sizeof(uint32));
                        u_sess->unique_sql_cxt.unique_sql_id = (uint64)pq_getmsgint64(&input_message);
                        u_sess->slow_query_cxt.slow_query.unique_sql_id = u_sess->unique_sql_cxt.unique_sql_id;
                        pgstat_report_unique_sql_id(false);
                        Oid procId = 0;
                        uint64 queryId = 0;
                        int64 stamp = 0;
                        if (t_thrd.proc->workingVersionNum >= SLOW_QUERY_VERSION || input_message.cursor < input_message.len ) {
                            procId = (Oid)pq_getmsgint(&input_message, sizeof(uint32));
                            queryId = (uint64)pq_getmsgint64(&input_message);
                            stamp = (int64)pq_getmsgint64(&input_message);
                            u_sess->wlm_cxt->wlm_params.qid.procId = procId;
                            u_sess->wlm_cxt->wlm_params.qid.queryId = queryId;
                            u_sess->wlm_cxt->wlm_params.qid.stamp = stamp;
                        }

                        ereport(DEBUG1,
                            (errmodule(MOD_INSTR),
                                errmsg("[UniqueSQL] "
                                       "Received new unique cn_id: %u, user_id: %u, sql id: %lu, procId %u, queryId %lu, stamp %ld",
                                    u_sess->unique_sql_cxt.unique_sql_cn_id,
                                    u_sess->unique_sql_cxt.unique_sql_user_id,
                                    u_sess->unique_sql_cxt.unique_sql_id,
                                    procId,
                                    queryId,
                                    stamp)));
                    } break;

                    case 'S': {
                        errno_t rc = memcpy_s(&u_sess->globalSessionId.sessionId, sizeof(uint64),
                            pq_getmsgbytes(&input_message, sizeof(uint64)), sizeof(uint64));
                        securec_check(rc, "\0", "\0");
                        u_sess->globalSessionId.nodeId = (uint32)pq_getmsgint(&input_message, sizeof(uint32));
                        u_sess->globalSessionId.seq = pg_atomic_add_fetch_u64(&g_instance.global_session_seq, 1);
                        pgstat_report_global_session_id(u_sess->globalSessionId);
                        t_thrd.proc->globalSessionId = u_sess->globalSessionId;
                        ereport(DEBUG5, (errmodule(MOD_INSTR),
                            errmsg("[Global session id] node:%u, session id: %lu, seq id:%lu",
                            u_sess->globalSessionId.nodeId,
                            u_sess->globalSessionId.sessionId,
                            u_sess->globalSessionId.seq)));
                    } break;

                    case 's': {
                        /* get unique sql ids, then reply the unique sqls stat */
                        uint32 count = pq_getmsgint(&input_message, sizeof(uint32));
                        ReplyUniqueSQLsStat(&input_message, count);
                    } break;

                    case 'K': /* msg type for get sql-RT count */
                    {
                        pgstat_reply_percentile_record_count();
                        pq_getmsgend(&input_message);
                    } break;

                    case 'k': /* msg type for replay sql-RT info */
                    {
                        pgstat_reply_percentile_record();
                        pq_getmsgend(&input_message);
                    } break;

                    default:
                        break;
                }
                pq_getmsgend(&input_message);
                /* q - for unique sql id */
            } break;

            case 'h': /* @hdfs hybridmessage query */
            {
                const char* query_string = NULL;

                /* get the node id. */
                u_sess->pgxc_cxt.PGXCNodeId = pq_getmsgint(&input_message, 4);

                /* get hybridmesage */
                query_string = pq_getmsgstring(&input_message);
                t_thrd.postgres_cxt.clobber_qstr = query_string;
                pq_getmsgend(&input_message);

                pgstatCountSQL4SessionLevel();
                statement_init_metric_context();

                /*
                 * @hdfs
                 * exec_simpel_query. We set the second paramter to 1
                 * when we get the hybirmessage. It's default value is 0.
                 */
                exec_simple_query(query_string, HYBRID_MESSAGE);
                u_sess->debug_query_id = 0;
                send_ready_for_query = true;
            } break;
#endif
            case 'P': /* parse */
            {
                OgRecordAutoController _local_opt(SRT6_P);
                const char* stmt_name = NULL;
                const char* query_string = NULL;
                int numParams;
                Oid* paramTypes = NULL;
                char* paramModes = NULL;
                char** paramTypeNames = NULL;

                /* DN: get the node id. */
                if (IS_PGXC_DATANODE && IsConnFromCoord())
                    u_sess->pgxc_cxt.PGXCNodeId = pq_getmsgint(&input_message, 4);

                stmt_name = pq_getmsgstring(&input_message);
                query_string = pq_getmsgstring(&input_message);
                if (strlen(query_string) > SECUREC_MEM_MAX_LEN) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("Too long query_string.")));
                }

                numParams = pq_getmsgint(&input_message, 2);
                paramTypes = (Oid*)palloc(numParams * sizeof(Oid));
                if (numParams > 0) {
                    int i;
#ifdef ENABLE_MULTIPLE_NODES
                    if (IsConnFromCoord()) {
                        paramTypeNames = (char**)palloc(numParams * sizeof(char*));
                        for (i = 0; i < numParams; i++) {
                            paramTypeNames[i] = (char*)pq_getmsgstring(&input_message);
                        }
                    } else
#endif /* ENABLE_MULTIPLE_NODES */
                    {
                        for (i = 0; i < numParams; i++)
                            paramTypes[i] = pq_getmsgint(&input_message, 4);
                    }
                }

#ifdef USE_RETRY_STUB
                if (IsStmtRetryEnabled())
                    u_sess->exec_cxt.RetryController->stub_.StartOneStubTest(firstchar);
#endif

#ifndef ENABLE_MULTIPLE_NODES
                if (PROC_OUTPARAM_OVERRIDE) {
                    int numModes = 0;
                    if (input_message.len - input_message.cursor > 0) {
                        numModes = pq_getmsgint(&input_message, 2);
                    } else {
                        ereport(DEBUG2, (errmodule(MOD_PLSQL), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("the guc proc_outparam_override is open but message not contains modes.")));
                    }
                    if (numModes > 0) {
                        if (numModes != numParams) {
                            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("param type num %d does't equal to param mode num %d.", numParams, numModes)));
                        }

                        paramModes = (char*)palloc(numModes * sizeof(char));
                        for (int i = 0; i < numParams; i++) {
                            const char *mode = pq_getmsgbytes(&input_message, 1);
                            if (*mode != PROARGMODE_IN
                                && *mode != PROARGMODE_OUT
                                && *mode != PROARGMODE_INOUT) {
                                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("The %dth param mode %s does't exist.", i, mode)));
                            }
                            paramModes[i] = *mode;
                        }
                    }
                    if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
                        /*
                         * PROC_OUTPARAM_OVERRIDE only valid in A_FORMAT,
                         * but message contains paramModes in jdbc when set PROC_OUTPARAM_OVERRIDE,
                         * so we just free paramModes and set it to null in other sql_compatibility.
                         */
                        pfree_ext(paramModes);
                    }
                }
#endif

                pq_getmsgend(&input_message);
                statement_init_metric_context();
                instr_stmt_report_trace_id(u_sess->trace_cxt.trace_id);
                exec_parse_message(query_string, stmt_name, paramTypes, paramTypeNames, paramModes, numParams);
                if ((libpqsw_redirect() || libpqsw_get_set_command()) && !libpqsw_only_localrun()) {
                    get_redirect_manager()->push_message(firstchar,
                        &input_message,
                        true,
                        libpqsw_get_set_command() ? RT_SET : RT_NORMAL
                        );
                }
                statement_commit_metirc_context();

                /*
                 * since AbortTransaction can't clean named prepared statement, we need to
                 * cache prepared statement name here, and clean it later in long jump routine.
                 * only need to cache it if parse successfully, then cleaning is necessary if retry
                 * happens.
                 */
                if (IsStmtRetryEnabled() && stmt_name[0] != '\0') {
                    u_sess->exec_cxt.RetryController->CacheStmtName(stmt_name);
                }

            resetErrorDataArea(true, false);
            } break;

            case 'B': /* bind */
            {
                OgRecordAutoController _local_opt(SRT7_B);
#ifdef USE_RETRY_STUB
                if (IsStmtRetryEnabled())
                    u_sess->exec_cxt.RetryController->stub_.StartOneStubTest(firstchar);
#endif

                statement_init_metric_context();
                /*
                 * this message is complex enough that it seems best to put
                 * the field extraction out-of-line
                 */
                instr_stmt_report_trace_id(u_sess->trace_cxt.trace_id);
                exec_bind_message(&input_message);
            }  break;

            case 'E': /* execute */
            {
                OgRecordAutoController _local_opt(SRT8_E);
                const char* portal_name = NULL;
                int max_rows;

                statement_init_metric_context_if_needs();
                pgstat_report_trace_id(&u_sess->trace_cxt, true);
                if ((unsigned int)input_message.len > SECUREC_MEM_MAX_LEN)
                    ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid execute message")));

                u_sess->pgxc_cxt.DisasterReadArrayInit = false;
                if (lightProxy::processMsg(EXEC_MESSAGE, &input_message)) {
                    break;
                }

                char* completionTag = (char*)palloc0(COMPLETION_TAG_BUFSIZE * sizeof(char));
                if (u_sess->exec_cxt.CurrentOpFusionObj != NULL && IS_SINGLE_NODE) {
                    if (IS_UNIQUE_SQL_TRACK_TOP) {
                        SetIsTopUniqueSQL(true);
                    }
                }
                bool isQueryCompleted = false;
                if (!u_sess->attr.attr_storage.phony_autocommit) {
                    BeginTxnForAutoCommitOff();
                }
                if (OpFusion::process(FUSION_EXECUTE, &input_message, completionTag, true, &isQueryCompleted)) {
                    if(isQueryCompleted) {
                        CommandCounterIncrement();
                        EndCommand(completionTag, (CommandDest)t_thrd.postgres_cxt.whereToSendOutput);
                    } else {
                        if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote)
                            pq_putemptymessage('s');
                    }
                    pfree_ext(completionTag);
                    t_thrd.postgres_cxt.debug_query_string = NULL;
                    t_thrd.postgres_cxt.cur_command_tag = T_Invalid;
                    if (MEMORY_TRACKING_QUERY_PEAK)
                        ereport(LOG, (errmsg("execute opfusion,  peak memory %ld(kb)",
                                             (int64)(t_thrd.utils_cxt.peakedBytesInQueryLifeCycle/1024))));
                    break;
                }
                pfree_ext(completionTag);

                /* Set statement_timestamp() */
                SetCurrentStatementStartTimestamp();

                portal_name = pq_getmsgstring(&input_message);
                max_rows = pq_getmsgint(&input_message, 4);
                pq_getmsgend(&input_message);

                pgstatCountSQL4SessionLevel();
#ifdef USE_RETRY_STUB
                if (IsStmtRetryEnabled())
                    u_sess->exec_cxt.RetryController->stub_.StartOneStubTest(firstchar);
#endif
                exec_execute_message(portal_name, max_rows);
            } break;

#ifdef ENABLE_MULTIPLE_NODES
            case 'k':
                {
                    /* no k msg from cn now, but keep these code for gray upgrade */
                    if (!ENABLE_DN_GPC)
                        ereport(ERROR, (errmodule(MOD_GPC), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                          errmsg("cn has enabled global plan cache but dn %s disabled", g_instance.attr.attr_common.PGXCNodeName)));
                    if (t_thrd.proc->workingVersionNum >= 92128) {
                        rc = memcpy_s(&u_sess->sess_ident.cn_sessid, sizeof(uint64), pq_getmsgbytes(&input_message, sizeof(uint64)),
                                sizeof(uint64));
                        securec_check(rc,"\0","\0");

                        rc = memcpy_s(&u_sess->sess_ident.cn_timeline, sizeof(uint32), pq_getmsgbytes(&input_message, sizeof(uint32)),
                                sizeof(uint32));
                        securec_check(rc,"","");

                        rc = memcpy_s(&u_sess->sess_ident.cn_nodeid, sizeof(uint32), pq_getmsgbytes(&input_message, sizeof(uint32)),
                                sizeof(uint32));
                        securec_check(rc,"","");

                        PGXCNode_HandleGPC handle_type;
                        rc = memcpy_s(&handle_type, sizeof(PGXCNode_HandleGPC), pq_getmsgbytes(&input_message, sizeof(PGXCNode_HandleGPC)),
                                sizeof(PGXCNode_HandleGPC));
                        securec_check(rc,"","");
                        u_sess->pgxc_cxt.PGXCNodeId = pq_getmsgint(&input_message, 4);
                        pq_getmsgend(&input_message);
                    } else {
                        uint64 origin_global_session_id;
                        rc = memcpy_s(&origin_global_session_id, sizeof(uint64), pq_getmsgbytes(&input_message, sizeof(uint64)),
                                sizeof(uint64));
                        securec_check(rc,"","");
                        
                        rc = memcpy_s(&u_sess->sess_ident.cn_timeline, sizeof(uint32), pq_getmsgbytes(&input_message, sizeof(uint32)),
                                sizeof(uint32));
                        securec_check(rc,"","");
                        u_sess->sess_ident.cn_nodeid = (uint32)(origin_global_session_id >> 48);
                        u_sess->sess_ident.cn_sessid = (uint64)(origin_global_session_id & 0xffffffffffff);
                        PGXCNode_HandleGPC handle_type;
                        rc = memcpy_s(&handle_type, sizeof(PGXCNode_HandleGPC), pq_getmsgbytes(&input_message, sizeof(PGXCNode_HandleGPC)),
                                sizeof(PGXCNode_HandleGPC));
                        securec_check(rc,"","");
                        pq_getmsgend(&input_message);
                    }
                }
                break;
#endif
            case 'F': /* fastpath function call */

                /* Report query to various monitoring facilities. */
                pgstat_report_activity(STATE_FASTPATH, NULL);
                set_ps_display("<FASTPATH>", false);

                exec_init_poolhandles();

                /* start an xact for this function invocation */
                start_xact_command();

                /*
                 * Note: we may at this point be inside an aborted
                 * transaction.  We can't throw error for that until we've
                 * finished reading the function-call message, so
                 * HandleFunctionRequest() must check for it after doing so.
                 * Be careful not to do anything that assumes we're inside a
                 * valid transaction here.
                 */
                /* switch back to message context */
                MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

                if (HandleFunctionRequest(&input_message) == EOF) {
                    /* lost frontend connection during F message input */

                    /*
                     * Reset whereToSendOutput to prevent ereport from
                     * attempting to send any more messages to client.
                     */
                    if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote)
                        t_thrd.postgres_cxt.whereToSendOutput = DestNone;

                    proc_exit(0);
                }

                /* commit the function-invocation transaction */
                finish_xact_command();
                u_sess->debug_query_id = 0;
                send_ready_for_query = true;
                break;

            case 'C': /* close */
            {
                OgRecordAutoController _local_opt(SRT11_C);
                int close_type;
                const char* closeTarget = NULL;

                close_type = pq_getmsgbyte(&input_message);
                closeTarget = pq_getmsgstring(&input_message);
                pq_getmsgend(&input_message);

                if (closeTarget && strlen(closeTarget) > SECUREC_MEM_MAX_LEN) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("Too long closeTarget.")));
                }

                exec_init_poolhandles();

                switch (close_type) {
                    case 'S':
                        if (closeTarget[0] != '\0') {
                            DropPreparedStatement(closeTarget, false);
                        } else {
                            /* special-case the unnamed statement */
                            drop_unnamed_stmt();
                        }
                        break;
                    case 'P': {
                        Portal portal;
                        if (IS_PGXC_COORDINATOR && closeTarget[0] != '\0') {
                            lightProxy::removeLightProxy(closeTarget);
                        }

                        if (IS_PGXC_DATANODE && closeTarget[0] != '\0') {
                            OpFusion* curr = OpFusion::locateFusion(closeTarget);
                            if (curr != NULL) {
                                if (curr->IsGlobal()) {
                                    curr->clean();
                                    OpFusion::tearDown(curr);
                                } else {
                                    curr->clean();
                                    OpFusion::removeFusionFromHtab(closeTarget);
                                }
                            }
                        }

                        portal = GetPortalByName(closeTarget);
                        if (PortalIsValid(portal)) {
                            PortalDrop(portal, false);
                        }
                    } break;
                    default:
                        ereport(ERROR,
                            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                errmsg("invalid CLOSE message subtype %d", close_type)));
                        break;
                }

                if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote
                    && !libpqsw_skip_close_command()) {
                    pq_putemptymessage('3'); /* CloseComplete */
                }
            } break;

            case 'D': /* describe */
            {
                OgRecordAutoController _local_opt(SRT9_D);
                int describe_type;
                const char* describe_target = NULL;
                if ((unsigned int)input_message.len > SECUREC_MEM_MAX_LEN) {
                    ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid describe message")));
                }

                if (lightProxy::processMsg(DESC_MESSAGE, &input_message)) {
                    break;
                }

                if (OpFusion::process(FUSION_DESCRIB, &input_message, NULL, false, NULL)) {
                    break;
                }

                /* Set statement_timestamp() (needed for xact) */
                SetCurrentStatementStartTimestamp();

                describe_type = pq_getmsgbyte(&input_message);
                describe_target = pq_getmsgstring(&input_message);
                pq_getmsgend(&input_message);
#ifdef USE_RETRY_STUB
                if (IsStmtRetryEnabled()) {
                    u_sess->exec_cxt.RetryController->stub_.StartOneStubTest(firstchar);
                }
#endif
                exec_init_poolhandles();

                switch (describe_type) {
                    case 'S':
                        exec_describe_statement_message(describe_target);
                        break;
                    case 'P':
                        exec_describe_portal_message(describe_target);
                        break;
                    default:
                        ereport(ERROR,
                            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                errmsg("invalid DESCRIBE message subtype %d", describe_type)));
                        break;
                }
            } break;

            case 'H': /* flush */
                pq_getmsgend(&input_message);
                if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote) {
                    StmtRetrySetQuerytUnsupportedFlag();

                    pq_flush();
                }
                break;

            case 'S': /* sync */
            {
                OgRecordAutoController _local_opt(SRT10_S);
                pq_getmsgend(&input_message);
#ifdef USE_RETRY_STUB
                if (IsStmtRetryEnabled()) {
                    u_sess->exec_cxt.RetryController->stub_.StartOneStubTest(firstchar);
                }
#endif
                if (u_sess->xact_cxt.pbe_execute_complete == true) {
                    finish_xact_command();
                } else {
                    u_sess->xact_cxt.pbe_execute_complete = true;
                }

                u_sess->debug_query_id = 0;
                send_ready_for_query = true;

                if (IsStmtRetryEnabled()) {
                    t_thrd.log_cxt.flush_message_immediately = false;
                }
            } break;

                /*
                 * 'X' means that the frontend is closing down the socket. EOF
                 * means unexpected loss of frontend connection. Either way,
                 * perform normal shutdown.
                 */
            case 'X':
            case EOF:
                /* unified auditing logout */
                audit_processlogout_unified();

                /*
                 * isSingleMode means we are doing initdb. Some temp tables
                 * will be created to store intermediate result, so should do cleaning
                 * when finished.
                 * xc_maintenance_mode is for cluster resize, temp table created
                 * during it should be clean too.
                 * Drop temp schema if IS_SINGLE_NODE.
                 */
                RemoveTempNamespace();

                InitThreadLocalWhenSessionExit();

                if (IS_THREAD_POOL_WORKER) {
                    (void)gs_signal_block_sigusr2();
                    t_thrd.threadpool_cxt.worker->CleanUpSession(false);
                    (void)gs_signal_unblock_sigusr2();
                    break;
                } else {
                    /*
                     * Reset whereToSendOutput to prevent ereport from attempting
                     * to send any more messages to client.
                     */
                    if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote)
                        t_thrd.postgres_cxt.whereToSendOutput = DestNone;

                    /*
                     * NOTE: if you are tempted to add more code here, DON'T!
                     * Whatever you had in mind to do should be set up as an
                     * on_proc_exit or on_shmem_exit callback, instead. Otherwise
                     * it will fail to be called during other backend-shutdown
                     * scenarios.
                     */
                    proc_exit(0);
                }
                /* fall through */
            case 'd': /* copy data */
            case 'c': /* copy done */
            case 'f': /* copy fail */

                /*
                 * Accept but ignore these messages, per protocol spec; we
                 * probably got here because a COPY failed, and the frontend
                 * is still sending data.
                 */
                break;
#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)
            case 'M': /* Command ID */
            {
                CommandId cid = (CommandId)pq_getmsgint(&input_message, 4);
                ereport(DEBUG1, (errmsg("Received cmd id %u", cid)));
                SaveReceivedCommandId(cid);
            } break;

            case 'q': /* query id */
            {
                errno_t rc = EOK;

                /* Set the query id we were passed down */
                rc = memcpy_s(&u_sess->debug_query_id,
                    sizeof(uint64),
                    pq_getmsgbytes(&input_message, sizeof(uint64)),
                    sizeof(uint64));
                securec_check(rc, "\0", "\0");
                ereport(DEBUG1, (errmsg("Received new query id %lu", u_sess->debug_query_id)));
                pq_getmsgend(&input_message);
                pgstat_report_queryid(u_sess->debug_query_id);
            } break;
            case 'e': /* threadid */
            {
                /* Set the thread id we were passed down */
                u_sess->instr_cxt.gs_query_id->procId = (Oid)pq_getmsgint(&input_message, 4);
                u_sess->exec_cxt.need_track_resource = (bool)pq_getmsgbyte(&input_message);
                pq_getmsgend(&input_message);
            } break;

            case 'r': /* query id with sync */
            {
#ifndef USE_SPQ
                /* We only process 'r' message on PGCX_DATANODE. */
                if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE)
                    ereport(ERROR,
                        (errcode(ERRCODE_PROTOCOL_VIOLATION),
                            errmsg("invalid frontend message type '%c'.", firstchar)));
#endif

                /* Set top consumer at the very beginning. */
                StreamTopConsumerIam();
                List* oidlist = NIL;
                int oidcount = 0;

                /* Set the query id we were passed down */
                errno_t rc = memcpy_s(&u_sess->debug_query_id,
                    sizeof(uint64),
                    pq_getmsgbytes(&input_message, sizeof(uint64)),
                    sizeof(uint64));
                securec_check(rc, "\0", "\0");
                ereport(DEBUG1, (errmsg("Received new query id %lu", u_sess->debug_query_id)));
                rc = memcpy_s(&oidcount, sizeof(int),
                    pq_getmsgbytes(&input_message, sizeof(int)), sizeof(int));
                securec_check(rc,"\0","\0");
                for (int i = 0; i < oidcount; i++) {
                    Oid tmp = InvalidOid;
                    rc = memcpy_s(&tmp, sizeof(Oid),
                        pq_getmsgbytes(&input_message, sizeof(Oid)), sizeof(Oid));
                    securec_check(rc,"\0","\0");
                    oidlist = lappend_oid(oidlist, tmp);
                }

                pq_getmsgend(&input_message);

                /*
                 * Set top consumer for stream plan to ensure debug_query_id
                 * being removed when errors occurs between 'r' message and 'Z' message.
                 * The flag isStreamTopConsumer will be reset in StreamNodeGroup::destroy
                 * 'Z' and 'Y' messages.
                 */
                StreamNodeGroup::grantStreamConnectPermission();

                StringInfoData buf;
                pq_beginmessage(&buf, 'l');
                pq_sendint16(&buf, g_instance.attr.attr_network.comm_control_port);
                pq_sendint16(&buf, g_instance.attr.attr_network.comm_sctp_port);
                uint16 len = strlen(g_instance.attr.attr_common.PGXCNodeName) + 1;
                pq_sendint16(&buf, len);
                pq_sendbytes(&buf, g_instance.attr.attr_common.PGXCNodeName, len);
                pq_endmessage(&buf);
                if (SS_PRIMARY_MODE && oidcount > 0) {
                    StringInfoData bufoid;
                    pq_beginmessage(&bufoid, 'i');
                    pq_sendint(&bufoid, oidcount, 4);
                    ListCell *oid;
                    foreach (oid, oidlist) {
                        Oid relOid = lfirst_oid(oid);
                        Relation rel = heap_open(relOid, AccessShareLock);
                        BlockNumber ReadBlkNum = RelationGetNumberOfBlocks(rel);
                        heap_sync(rel);
                        heap_close(rel, AccessShareLock);
                        pq_sendint(&bufoid, (int)relOid, 4);
                        pq_sendint(&bufoid, (int)ReadBlkNum, 4);
                    }
                    pq_endmessage(&bufoid);
                }
                pq_putemptymessage('O'); /* PlanIdComplete */
                pq_flush();
            } break;
#endif
#ifdef ENABLE_MULTIPLE_NODES
            case 'g': /* gxid */
            {
                errno_t rc = EOK;

                /* Set the GXID we were passed down */
                TransactionId gxid;
                bool is_check_xid = false;

                rc = memcpy_s(&gxid,
                    sizeof(TransactionId),
                    pq_getmsgbytes(&input_message, sizeof(TransactionId)),
                    sizeof(TransactionId));
                securec_check(rc, "\0", "\0");

                /* get the tag indicates if it's a special xid for check, true(xid for check) */
                rc = memcpy_s(&is_check_xid, sizeof(bool), pq_getmsgbytes(&input_message, sizeof(bool)), sizeof(bool));
                securec_check(rc, "\0", "\0");

                ereport(DEBUG1, (errmsg("Received new gxid %lu", gxid)));
                /* CN function may use gxid from CN to create tmp lib name */
                t_thrd.xact_cxt.cn_xid = gxid;

                pq_getmsgend(&input_message);
            } break;
#endif
#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)
            case 's': /* snapshot */
            {
#ifdef USE_SPQ
                errno_t rc = EOK;
                Snapshot cursnap = u_sess->spq_cxt.snapshot;
                int satisfies;
                rc = memcpy_s(&satisfies, sizeof(int),
                                  pq_getmsgbytes(&input_message, sizeof(int)), sizeof(int));
                    securec_check(rc,"\0","\0");
                cursnap->satisfies = (SnapshotSatisfiesMethod)satisfies;
                rc = memcpy_s(&cursnap->xmin, sizeof(TransactionId),
                                  pq_getmsgbytes(&input_message, sizeof(TransactionId)), sizeof(TransactionId));
                    securec_check(rc,"\0","\0");
                rc = memcpy_s(&cursnap->xmax, sizeof(TransactionId),
                                  pq_getmsgbytes(&input_message, sizeof(TransactionId)), sizeof(TransactionId));
                    securec_check(rc,"\0","\0");
                rc = memcpy_s(&cursnap->snapshotcsn, sizeof(CommitSeqNo),
                                  pq_getmsgbytes(&input_message, sizeof(CommitSeqNo)), sizeof(CommitSeqNo));
                    securec_check(rc,"\0","\0");
                break;
            }
#else
                int gtm_snapshot_type = -1;

                if (GTM_LITE_MODE) { /* gtm lite mode */
                    rc = memcpy_s(&ss_need_sync_wait_all, sizeof(int),
                                  pq_getmsgbytes(&input_message, sizeof(bool)), sizeof(bool));
                    securec_check(rc,"\0","\0");
                    rc = memcpy_s(&cn_xc_maintain_mode, sizeof(bool),
                                  pq_getmsgbytes(&input_message, sizeof(bool)), sizeof(bool));
                    securec_check(rc,"\0","\0");
                    rc = memcpy_s(&csn, sizeof(uint64),
                                  pq_getmsgbytes(&input_message, sizeof(uint64)), sizeof(uint64));
                    securec_check(rc,"\0","\0");
                    rc = memcpy_s(&gtm_snapshot_type, sizeof(int),
                                  pq_getmsgbytes(&input_message, sizeof(int)), sizeof(int));
                    securec_check(rc,"\0","\0");
                    remote_gtm_mode = pq_getmsgbyte(&input_message);
                    pq_getmsgend(&input_message);
                    /* if message length is correct, set u_sess variables */
                    u_sess->utils_cxt.cn_xc_maintain_mode = cn_xc_maintain_mode;
                    if (gtm_snapshot_type == GTM_SNAPSHOT_TYPE_LOCAL) {
                        UnsetGlobalSnapshotData();
                    } else {
                        u_sess->utils_cxt.is_autovacuum_snapshot =
                            (gtm_snapshot_type == GTM_SNAPSHOT_TYPE_AUTOVACUUM) ? true : false;
                        ereport(DEBUG1, (errmodule(MOD_DISASTER_READ), errmsg("dn receive snapshot csn %lu", csn)));
                        SetGlobalSnapshotData(InvalidTransactionId, InvalidTransactionId, csn,
                                              InvalidTransactionTimeline, ss_need_sync_wait_all);
                        /* quickly set my recent global xmin */
                        u_sess->utils_cxt.RecentGlobalXmin = GetOldestXmin(NULL, true);
                        u_sess->utils_cxt.RecentGlobalCatalogXmin = GetOldestCatalogXmin();
                    }
                }
                /* check gtm mode, remote should be false, local cannot be true */
                if (remote_gtm_mode != g_instance.attr.attr_storage.enable_gtm_free &&
                    (t_thrd.proc->workingVersionNum >= 92012))
                    ereport(FATAL,
                        (errcode(ERRCODE_SYSTEM_ERROR),
                            errmsg("gtm mode unconsistency, remote mode is %s, local mode is %s.",
                                remote_gtm_mode ? "on" : "off",
                                g_instance.attr.attr_storage.enable_gtm_free ? "on" : "off")));
                /* Should not do any distributed operation when CN u_sess->attr.attr_common.xc_maintenance_mode is true
                 */
                if (u_sess->utils_cxt.cn_xc_maintain_mode != u_sess->attr.attr_common.xc_maintenance_mode)
                    ereport(WARNING,
                        (errmsg("cn_xc_maintain_mode: %s, xc_maintain_mode: %s",
                            u_sess->utils_cxt.cn_xc_maintain_mode ? "on" : "off",
                            u_sess->attr.attr_common.xc_maintenance_mode ? "on" : "off")));
                break;
            }
#endif
#endif
#ifdef ENABLE_MULTIPLE_NODES
            case 't': /* timestamp */
                /* Set statement_timestamp() */
                gtmstart_timestamp = (TimestampTz)pq_getmsgint64(&input_message);
                stmtsys_timestamp = (TimestampTz)pq_getmsgint64(&input_message);
                pq_getmsgend(&input_message);
                /*
                 * Set in xact.x the static Timestamp difference value with GTM
                 * and the timestampreceivedvalues for Datanode reference
                 */
                SetCurrentGTMTimestamp(gtmstart_timestamp);
                t_thrd.xact_cxt.timestamp_from_cn = true;

                SetCurrentStmtTimestamp(stmtsys_timestamp);

                SetCurrentGTMDeltaTimestamp();

                break;

            case 'b': /* barrier */
            {
                int command;
                char* id = NULL;
                command = pq_getmsgbyte(&input_message);
                id = (char*)pq_getmsgstring(&input_message);
                pq_getmsgend(&input_message);

                switch (command) {
                    case CREATE_BARRIER_PREPARE:
                        ProcessCreateBarrierPrepare(id, false);
                        break;

                    case CREATE_SWITCHOVER_BARRIER_PREPARE:
                        ProcessCreateBarrierPrepare(id, true);
                        break;

                    case CREATE_BARRIER_END:
                        ProcessCreateBarrierEnd(id);
                        break;

                    case CREATE_BARRIER_COMMIT:
                        ProcessCreateBarrierCommit(id);
                        break;

                    case CREATE_BARRIER_EXECUTE:
                        ProcessCreateBarrierExecute(id);
                        break;

                    case CREATE_SWITCHOVER_BARRIER_EXECUTE:
                        ProcessCreateBarrierExecute(id, true);
                        break;

                    case BARRIER_QUERY_ARCHIVE:
                        ProcessBarrierQueryArchive(id);
                        break;
                    default:
                        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("Invalid command received")));
                        break;
                }
            }
            break;

            case 'W': {
                WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

                g_wlm_params->qid.procId = (Oid)pq_getmsgint(&input_message, 4);
                g_wlm_params->qid.queryId = (uint64)pq_getmsgint64(&input_message);

                int flags[2];

                flags[0] = (int)pq_getmsgint(&input_message, 4);

                g_wlm_params->cpuctrl = *((unsigned char*)&flags[0]);
                g_wlm_params->memtrack = *((unsigned char*)&flags[0] + sizeof(char));
                g_wlm_params->iostate = *((unsigned char*)&flags[0] + 2 * sizeof(char));
                g_wlm_params->iotrack = *((unsigned char*)&flags[0] + 3 * sizeof(char));

                flags[1] = (int)pq_getmsgint(&input_message, 4);
                g_wlm_params->iocontrol = *((unsigned char*)&flags[1]);

                g_wlm_params->complicate = *((unsigned char*)&flags[1] + sizeof(char)) ? 0 : 1;

                g_wlm_params->dopvalue = (unsigned char)pq_getmsgint(&input_message, 4);
                g_wlm_params->io_priority = pq_getmsgint(&input_message, 4);
                g_wlm_params->iops_limits = pq_getmsgint(&input_message, 4);
                g_wlm_params->qid.stamp = (TimestampTz)pq_getmsgint64(&input_message);

                /* get the name of cgroup */
                const char* cgname = pq_getmsgstring(&input_message);

                /* get the name of respool */
                const char* respool = pq_getmsgstring(&input_message);

                if (StringIsValid(respool)) {
                    rc = snprintf_s(g_wlm_params->rpdata.rpname,
                        sizeof(g_wlm_params->rpdata.rpname),
                        sizeof(g_wlm_params->rpdata.rpname) - 1,
                        "%s",
                        respool);
                    securec_check_ss(rc, "", "");
                }

                /* get the node group name */
                const char* ngname = pq_getmsgstring(&input_message);

                if (StringIsValid(ngname)) {
                    rc = snprintf_s(g_wlm_params->ngroup,
                        sizeof(g_wlm_params->ngroup),
                        sizeof(g_wlm_params->ngroup) - 1,
                        "%s",
                        ngname);
                    securec_check_ss(rc, "", "");
                }

                /* local dn has vcgroup */
                if (*g_instance.wlm_cxt->local_dn_ngname && *g_wlm_params->ngroup &&
                    0 != strcmp(g_instance.wlm_cxt->local_dn_ngname, g_wlm_params->ngroup)) {
                    /* Get the node group information */
                    t_thrd.wlm_cxt.thread_node_group = g_instance.wlm_cxt->local_dn_nodegroup;

                    /* check if the control group is valid and set it for foreign user */
                    if (t_thrd.wlm_cxt.thread_node_group->foreignrp) {
                        u_sess->wlm_cxt->local_foreign_respool = t_thrd.wlm_cxt.thread_node_group->foreignrp;
                    } else {
                        WLMSetControlGroup(GSCGROUP_INVALID_GROUP);
                    }
                } else {
                    /* Get the node group information */
                    t_thrd.wlm_cxt.thread_node_group = WLMMustGetNodeGroupFromHTAB(g_wlm_params->ngroup);

                    WLMSetControlGroup(cgname);
                }

                t_thrd.wlm_cxt.thread_climgr = &t_thrd.wlm_cxt.thread_node_group->climgr;
                t_thrd.wlm_cxt.thread_srvmgr = &t_thrd.wlm_cxt.thread_node_group->srvmgr;

                pq_getmsgend(&input_message);
            } break;
            case 'w': {
                Assert(StringIsValid(g_instance.attr.attr_common.PGXCNodeName));

                exec_init_poolhandles();

                /* start an xact for this function invocation */
                start_xact_command();

                if (is_pgxc_central_nodename(g_instance.attr.attr_common.PGXCNodeName)) {
                    dywlm_server_receive(&input_message);
                } else {
                    dywlm_client_receive(&input_message);
                }

                finish_xact_command();
            } break;
            case 'R': /* reply collect info */
            {
                /* start an xact for this function invocation */
                start_xact_command();

                WLMLocalInfoCollector(&input_message);

                finish_xact_command();
            } break;

            case 'A': /* msg type for compute pool  */
                process_request(&input_message);
                break;

#ifdef ENABLE_MULTIPLE_NODES
            case 'L': /* link gc_fdw  */
            {
                start_xact_command();

                PgFdwRemoteReply(&input_message);

                finish_xact_command();
            } break;
#endif
            case 'n': /* commiting */
            {
                /* Get the csn passed down */
                rc = memcpy_s(&csn, sizeof(uint64), pq_getmsgbytes(&input_message, sizeof(uint64)), sizeof(uint64));
                securec_check(rc, "", "");
                pq_getmsgend(&input_message);

                int nchildren;
                TransactionId *children = NULL;
                TransactionId xid = GetTopTransactionIdIfAny();
                Assert(TransactionIdIsValid(xid));
                nchildren = xactGetCommittedChildren(&children);
                /* Set the commit csn to commit_in_progress */
                SetXact2CommitInProgress(InvalidTransactionId, csn);
                XLogInsertStandbyCSNCommitting(xid, csn, children, nchildren);

                /* Send back response */
                pq_putemptymessage('m');
                pq_flush();
                break;
            }
            case 'N': /* commit csn */
                /* Set the commit csn passed down */
                rc = memcpy_s(&csn, sizeof(uint64), pq_getmsgbytes(&input_message, sizeof(uint64)), sizeof(uint64));
                securec_check(rc, "", "");
                pq_getmsgend(&input_message);
                if (!COMMITSEQNO_IS_COMMITTED(csn)) {
                    ereport(ERROR, (errmsg("Received an invalid commit csn: %lu.", csn)));
                }
                setCommitCsn(csn);
                ereport(DEBUG1,
                    (errmsg(
                        "proc %lu %d set csn %lu", t_thrd.proc->pid, t_thrd.proc->pgprocno, t_thrd.proc->commitCSN)));
                break;

#ifdef ENABLE_MULTIPLE_NODES
            case 'l': /* get and handle csn for csnminsync */
                rc = memcpy_s(&csn, sizeof(uint64),
                    pq_getmsgbytes(&input_message, sizeof(uint64)), sizeof(uint64));
                securec_check(rc, "", "");
                rc = memcpy_s(&csn_type, sizeof(CsnType),
                    pq_getmsgbytes(&input_message, sizeof(CsnType)), sizeof(CsnType));
                securec_check(rc, "", "");
                pq_getmsgend(&input_message);
                if (module_logging_is_on(MOD_TRANS_SNAPSHOT)) {
                    ereport(LOG, (errmodule(MOD_TRANS_SNAPSHOT),
                        errmsg("get csn : %lu, csn_type: %d", csn, (int)csn_type)));
                }
                if (!COMMITSEQNO_IS_COMMITTED(csn)) {
                    ereport(ERROR, (errmsg("Received an invalid commit csn: %lu.", csn)));
                }
                if (csn_type == FETCH_CSN) {
                    if (csn >= COMMITSEQNO_FIRST_NORMAL) {
                        UpdateNextMaxKnownCSN(csn);
                    }
                    /* calculate and send local_csn_min to Center Cn */
                    t_thrd.xact_cxt.ShmemVariableCache->local_csn_min = calculate_local_csn_min();
                    ereport(DEBUG1, (errmsg("report local_csn_min %lu to CCN",
                        t_thrd.xact_cxt.ShmemVariableCache->local_csn_min)));
                    send_local_csn_min_to_ccn();
                } else { /* get global_csn_min */
                    CommitSeqNo currentNextCommitSeqNo =
                        pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);
                    t_thrd.xact_cxt.ShmemVariableCache->cutoff_csn_min =
                        (csn < currentNextCommitSeqNo) ?
                        csn : currentNextCommitSeqNo;
                    ereport(DEBUG1, (errmsg("set the cutoff_csn_min %lu",
                        t_thrd.xact_cxt.ShmemVariableCache->cutoff_csn_min)));
                    forward_recent_global_xmin();
                }
                break;
#endif

            case 'j': /* check gtm mode */
                remote_gtm_mode = pq_getmsgbyte(&input_message);
                pq_getmsgend(&input_message);
                /* check gtm mode, remote should be true, local cannot be false */
                if (remote_gtm_mode != g_instance.attr.attr_storage.enable_gtm_free)
                    ereport(FATAL,
                        (errcode(ERRCODE_SYSTEM_ERROR),
                            errmsg("gtm mode unconsistency, remote mode is %s, local mode is %s.",
                                remote_gtm_mode ? "on" : "off",
                                g_instance.attr.attr_storage.enable_gtm_free ? "on" : "off")));
                break;
#endif

            case 'U': /* msg type for batch Bind-Execute for PBE */
            {
                OgRecordAutoController _local_opt(SRT12_U);
                if (!u_sess->attr.attr_common.support_batch_bind)
                    ereport(ERROR,
                        (errcode(ERRCODE_SYSTEM_ERROR),
                            errmsg("Need to set support_batch_bind=true if executing batch")));

                /*
                 * reset unique sql start time, otherwise fusionExecute will repeatly report
                 * elapsed time more.
                 */
                u_sess->unique_sql_cxt.unique_sql_start_time = 0;

                pgstatCountSQL4SessionLevel();
                statement_init_metric_context();
#ifdef USE_RETRY_STUB
                if (IsStmtRetryEnabled())
                    u_sess->exec_cxt.RetryController->stub_.StartOneStubTest(firstchar);
#endif
                exec_init_poolhandles();

                /*
                 * Enable pbe optimization in batch mode, cause it may generate too many cplan
                 * when enable_pbe_optimization is false, which may consume lots of memory and
                 * lead to 'memory alloc failed'. To avoid this problem, enable pbe optimization
                 * to use gplan in this batch.
                 */
                bool original = u_sess->attr.attr_sql.enable_pbe_optimization;
                u_sess->attr.attr_sql.enable_pbe_optimization = true;
                exec_batch_bind_execute(&input_message);
                u_sess->attr.attr_sql.enable_pbe_optimization = original;
                if (is_unique_sql_enabled() && is_local_unique_sql()) {
                    instr_unique_sql_report_elapse_time(GetCurrentStatementLocalStartTimestamp());
                }
            } break;

#ifdef ENABLE_MULTIPLE_NODES
            case 'z': /* pbe for ddl */
                exec_get_ddl_params(&input_message);
                break;

            case 'G': /* MSG_TYPE_PGXC_BUCKET_MAP for PGXCBucketMap and PGXCNodeId */
            {
                if (u_sess->top_transaction_mem_cxt == NULL) {
                    ereport(
                        ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                        errmsg("current transaction is not start")));
                }

                if (!IS_PGXC_DATANODE)
                    ereport(
                        ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Only dn can receive the bucket map")));
                t_thrd.xact_cxt.PGXCNodeId = pq_getmsgint(&input_message, 4);
                if (t_thrd.xact_cxt.PGXCNodeId != MAX_DN_NODE_NUM) {
                    int len = input_message.len - 4;
                    if (t_thrd.xact_cxt.PGXCBucketCnt != (int)(len / sizeof(uint2))) {
                        pfree_ext(t_thrd.xact_cxt.PGXCBucketMap);
                        t_thrd.xact_cxt.PGXCBucketCnt = len / sizeof(uint2);
                        t_thrd.xact_cxt.PGXCBucketMap =
                            (uint2*)MemoryContextAlloc(u_sess->top_transaction_mem_cxt, len);

                    }
                    rc = memcpy_s(t_thrd.xact_cxt.PGXCBucketMap, len, pq_getmsgbytes(&input_message, len), len);
                    securec_check(rc, "", "");
                }
                pq_getmsgend(&input_message);
            } break;
#endif
            case 'y': /* sequence from cn 2 dn */
            {
                if (!IS_PGXC_DATANODE)
                    ereport(
                        ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
                            errmsg("Only cn can receive the sequence update")));
                
                int num = pq_getmsgint(&input_message, 4);
                for (int i = 0; i < num; i++) {
                    int64 result;
                    char* dbname = NULL;
                    char* schemaname = NULL;
                    char* seqname = NULL;
                    int result_signed;
                    result_signed = pq_getmsgbyte(&input_message);
                    result = pq_getmsgint64(&input_message);
                    if (result_signed == '-') {
                        result *= -1;
                    }

                    dbname = (char*)pq_getmsgstring(&input_message);
                    schemaname = (char*)pq_getmsgstring(&input_message);
                    seqname = (char*)pq_getmsgstring(&input_message);
                    List* namelist = NIL;

                    namelist = lappend(namelist, makeString(dbname));
                    namelist = lappend(namelist, makeString(schemaname));
                    namelist = lappend(namelist, makeString(seqname));
                    processUpdateSequenceMsg(namelist, result);
                }
                flushSequenceMsg();
            }
            break;
            case 'J':
            {
                char* trace_id = NULL;
                trace_id = (char*)pq_getmsgstring(&input_message);
                pq_getmsgend(&input_message);
                if (strlen(trace_id) > MAX_TRACE_ID_SIZE -1) {
                    trace_id[MAX_TRACE_ID_SIZE - 1] = '\0';
                    ereport(WARNING, (errmsg("trace_id length cannot exceed %d", MAX_TRACE_ID_SIZE - 1)));
                }
                errno_t rc =
                    memcpy_s(u_sess->trace_cxt.trace_id, MAX_TRACE_ID_SIZE, trace_id, strlen(trace_id) + 1);
                securec_check(rc, "\0", "\0");
                elog(DEBUG1, "trace_id:%s start", u_sess->trace_cxt.trace_id);
            }
            break;
#ifdef ENABLE_MULTIPLE_NODES
            case 'T':
            {
                LWLockAcquire(XLogMaxCSNLock, LW_SHARED);
                CommitSeqNo maxCSN = t_thrd.xact_cxt.ShmemVariableCache->xlogMaxCSN;
                LWLockRelease(XLogMaxCSNLock);
                ereport(DEBUG1, (errmodule(MOD_DISASTER_READ), errmsg("dn response csn %lu", maxCSN)));

                StringInfoData buf;
                pq_beginmessage(&buf, 'J');
                pq_sendint64(&buf, maxCSN);
                pq_sendint8(&buf, t_thrd.postmaster_cxt.HaShmData->is_hadr_main_standby);
                pq_endmessage(&buf);
                pq_flush();
            }
            break;
#endif
            default:
                ereport(FATAL,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid frontend message type %c", firstchar)));
                break;
        }
    } /* end of input-reading loop */

    gstrace_exit(GS_TRC_ID_PostgresMain);
    /* can't get here because the above loop never exits */
    Assert(false);

    return 1; /* keep compiler quiet */
}

/*
 * Obtain platform stack depth limit (in bytes)
 *
 * Return -1 if unknown
 */
long get_stack_depth_rlimit(void)
{
#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_STACK)
    /* This won't change after process launch, so check just once */
    if (t_thrd.postgres_cxt.val == 0) {
        struct rlimit rlim;

        if (getrlimit(RLIMIT_STACK, &rlim) < 0)
            t_thrd.postgres_cxt.val = -1;
        else if (rlim.rlim_cur == RLIM_INFINITY)
            t_thrd.postgres_cxt.val = LONG_MAX;
        /* rlim_cur is probably of an unsigned type, so check for overflow */
        else if (rlim.rlim_cur >= LONG_MAX)
            t_thrd.postgres_cxt.val = LONG_MAX;
        else
            t_thrd.postgres_cxt.val = rlim.rlim_cur;
    }
    return t_thrd.postgres_cxt.val;
#else /* no getrlimit */
#if defined(WIN32) || defined(__CYGWIN__)
    /* On Windows we set the backend stack size in src/backend/Makefile */
    return WIN32_STACK_RLIMIT;
#else /* not windows ... give up */
    return -1;
#endif
#endif
}

static THR_LOCAL struct rusage Save_r;

void ResetUsage(void)
{
    getrusage(RUSAGE_THREAD, &Save_r);
    gettimeofday(&t_thrd.postgres_cxt.Save_t, NULL);
}

void ShowUsage(const char* title)
{
    StringInfoData str;
    struct timeval user, sys;
    struct timeval elapse_t;
    struct rusage r;
    errno_t errorno = EOK;
    errorno = memset_s(&elapse_t, sizeof(elapse_t), 0, sizeof(struct timeval));
    securec_check(errorno, "\0", "\0");

    getrusage(RUSAGE_THREAD, &r);
    gettimeofday(&elapse_t, NULL);
    errorno = memcpy_s((char*)&user, sizeof(user), (char*)&r.ru_utime, sizeof(user));
    securec_check(errorno, "\0", "\0");
    errorno = memcpy_s((char*)&sys, sizeof(sys), (char*)&r.ru_stime, sizeof(sys));
    securec_check(errorno, "\0", "\0");
    if (elapse_t.tv_usec < t_thrd.postgres_cxt.Save_t.tv_usec) {
        elapse_t.tv_sec--;
        elapse_t.tv_usec += 1000000;
    }
    if (r.ru_utime.tv_usec < Save_r.ru_utime.tv_usec) {
        r.ru_utime.tv_sec--;
        r.ru_utime.tv_usec += 1000000;
    }
    if (r.ru_stime.tv_usec < Save_r.ru_stime.tv_usec) {
        r.ru_stime.tv_sec--;
        r.ru_stime.tv_usec += 1000000;
    }

    /*
     * the only stats we don't show here are for memory usage -- i can't
     * figure out how to interpret the relevant fields in the rusage struct,
     * and they change names across o/s platforms, anyway. if you can figure
     * out what the entries mean, you can somehow extract resident set size,
     * shared text size, and unshared data and stack sizes.
     */
    initStringInfo(&str);

    appendStringInfo(&str, "! system usage stats:\n");
    appendStringInfo(&str,
        "!\t%ld.%06ld elapsed %ld.%06ld user %ld.%06ld system sec\n",
        (long)(elapse_t.tv_sec - t_thrd.postgres_cxt.Save_t.tv_sec),
        (long)(elapse_t.tv_usec - t_thrd.postgres_cxt.Save_t.tv_usec),
        (long)(r.ru_utime.tv_sec - Save_r.ru_utime.tv_sec),
        (long)(r.ru_utime.tv_usec - Save_r.ru_utime.tv_usec),
        (long)(r.ru_stime.tv_sec - Save_r.ru_stime.tv_sec),
        (long)(r.ru_stime.tv_usec - Save_r.ru_stime.tv_usec));
    appendStringInfo(&str,
        "!\t[%ld.%06ld user %ld.%06ld sys total]\n",
        (long)user.tv_sec,
        (long)user.tv_usec,
        (long)sys.tv_sec,
        (long)sys.tv_usec);
#if defined(HAVE_GETRUSAGE)
    appendStringInfo(&str,
        "!\t%ld/%ld [%ld/%ld] filesystem blocks in/out\n",
        r.ru_inblock - Save_r.ru_inblock,
        /* they only drink coffee at dec */
        r.ru_oublock - Save_r.ru_oublock,
        r.ru_inblock,
        r.ru_oublock);
    appendStringInfo(&str,
        "!\t%ld/%ld [%ld/%ld] page faults/reclaims, %ld [%ld] swaps\n",
        r.ru_majflt - Save_r.ru_majflt,
        r.ru_minflt - Save_r.ru_minflt,
        r.ru_majflt,
        r.ru_minflt,
        r.ru_nswap - Save_r.ru_nswap,
        r.ru_nswap);
    appendStringInfo(&str,
        "!\t%ld [%ld] signals rcvd, %ld/%ld [%ld/%ld] messages rcvd/sent\n",
        r.ru_nsignals - Save_r.ru_nsignals,
        r.ru_nsignals,
        r.ru_msgrcv - Save_r.ru_msgrcv,
        r.ru_msgsnd - Save_r.ru_msgsnd,
        r.ru_msgrcv,
        r.ru_msgsnd);
    appendStringInfo(&str,
        "!\t%ld/%ld [%ld/%ld] voluntary/involuntary context switches\n",
        r.ru_nvcsw - Save_r.ru_nvcsw,
        r.ru_nivcsw - Save_r.ru_nivcsw,
        r.ru_nvcsw,
        r.ru_nivcsw);
#endif /* HAVE_GETRUSAGE */

    /* remove trailing newline */
    if (str.data[str.len - 1] == '\n')
        str.data[--str.len] = '\0';

    ereport(LOG, (errmsg_internal("%s", title), errdetail_internal("%s", str.data)));

    pfree(str.data);
}

/*
 * on_proc_exit handler to log end of session
 */
void log_disconnections(int code, Datum arg)
{
    if (!u_sess->attr.attr_common.Log_disconnections) {
        return;
    }
    Port* port = u_sess->proc_cxt.MyProcPort;
    long secs;
    int usecs;
    int msecs;
    int hours, minutes, seconds;

    TimestampDifference(port->SessionStartTime, GetCurrentTimestamp(), &secs, &usecs);
    msecs = usecs / 1000;

    hours = secs / SECS_PER_HOUR;
    secs %= SECS_PER_HOUR;
    minutes = secs / SECS_PER_MINUTE;
    seconds = secs % SECS_PER_MINUTE;
    if (port->user_name != NULL && port->database_name != NULL && port->remote_host != NULL && 
        port->remote_port != NULL) {
        ereport(LOG,
                (errmsg("disconnection: session time: %d:%02d:%02d.%03d "
                "user=%s database=%s host=%s%s%s",
                hours,
                minutes,
                seconds,
                msecs,
                port->user_name,
                port->database_name,
                port->remote_host,
                port->remote_port[0] ? " port=" : "",
                port->remote_port)));
    } else {
        ereport(LOG,
                (errmsg("disconnection: session time: %d:%02d:%02d.%03d ", hours, minutes, seconds, msecs)));
    }
    
}

void cleanGPCPlanProcExit(int code, Datum arg)
{
    /* clear prepare statements first, in case pstmt hold reference of plan */
    DropAllPreparedStatements();

    GPCCleanUpSessionSavedPlan();
}

/* Aduit user logout */
/*
 * Brief		    : audit_processlogout
 * Description	    : audit logout
 */
void audit_processlogout(int code, Datum arg)
{
    pgaudit_user_logout();
    return;
}

/* Aduit user logout */
/*
 * Brief		    : audit_processlogout_unified
 * Description	    : audit logout
 */
void audit_processlogout_unified()
{
    /* it's unsafe to deal with plugins hooks as dynamic lib may be released */
    if (!(g_instance.status > NoShutdown) && user_login_hook) {
        user_login_hook(u_sess->proc_cxt.MyProcPort->database_name,
                        u_sess->proc_cxt.MyProcPort->user_name, true, false);
    }

    return;
}

/*
 * Brief : Check if the initial password of the user has been changed or not.
 *         If not, all the commands from APP should be forbidden except the
 *         "ALTER USER ***".
 *
 * Description : Firstly, we only need to take care of the sqls from client.
 * 				 For synchronization between coordinators, remoteConnType should be
 *               introduced to distinct different status. Secondly, we should not
 *               impact the process of initdb. We make a difference here with
 *         		 'IsUnderPostmaster' status. Thirdly, we only support the gsql
 *               client, the sqls from jdbc && odbc should be passed.
 *
 * Notes : In default, we only deal with the initial user(UserOid == 10).
 */
static void ForceModifyInitialPwd(const char* query_string, List* parsetree_list)
{
    Oid current_user = GetUserId();

    if (current_user != BOOTSTRAP_SUPERUSERID || parsetree_list == NIL) {
        return;
    }
    
    char* current_user_name = GetUserNameFromId(current_user);
    password_info pass_info = {NULL, 0, 0, false, false};
    /* return when the initial user's password is not empty */
    if (get_stored_password(current_user_name, &pass_info)) {
        t_thrd.postgres_cxt.password_changed = true;
        pfree_ext(pass_info.shadow_pass);
        return;
    }
    pfree_ext(pass_info.shadow_pass);

    if (IsUnderPostmaster && IsConnFromApp() && strcasecmp(u_sess->attr.attr_common.application_name, "gsql") == 0) {
        if (strcmp(query_string, "SELECT pg_catalog.intervaltonum(pg_catalog.gs_password_deadline())") != 0 &&
            strcmp(query_string, "SELECT pg_catalog.gs_password_notifytime()") != 0 &&
            strcmp(query_string, "SELECT VERSION()") != 0 && strcasecmp(query_string, "delete from pgxc_node;") != 0 &&
            strcasecmp(query_string, "delete from pgxc_group;") != 0 &&
            strncasecmp(query_string, "CREATE NODE", 11) != 0) {
        /*
         * Just focuse on the first parsetree_list element, since before
         * each operation, the initial password should be changed.
         */
        Node* parsetree = (Node*)linitial(parsetree_list);
        char* current_user_name = GetUserNameFromId(current_user);
        if (IsA(parsetree, AlterRoleStmt)) {
            /*
             * Check if the role in "AlterRoleStmt" matches the current_user.
             */
            char* alter_name = ((AlterRoleStmt*)parsetree)->role;
            if (strcasecmp(current_user_name, alter_name) != 0)
                ereport(ERROR,
                    (errcode(ERRCODE_INITIAL_PASSWORD_NOT_MODIFIED),
                        errmsg("Please use \"ALTER ROLE \"%s\" PASSWORD 'password';\" "
                               "to set the password of the user before other operations!",
                               current_user_name)));
        } else
            ereport(ERROR,
                (errcode(ERRCODE_INITIAL_PASSWORD_NOT_MODIFIED),
                    errmsg("Please use \"ALTER ROLE \"%s\" PASSWORD 'password';\" "
                           "to set the password of the user before other operations!",
                           current_user_name)));
        }
    }
}

/*
 * Require user to modify password since password is expired, 
 * all the commands from APP should be forbidden except the
 * "ALTER USER ***".
 */
static void ForceModifyExpiredPwd(const char* queryString, const List* parsetreeList)
{
    if (parsetreeList == NIL) {
        return;
    }

    if (!IsUnderPostmaster || !IsConnFromApp()) {
        return;
    }
    
    /* check if gsql use -U -W */ 
    if (strcmp(queryString, "SELECT pg_catalog.intervaltonum(pg_catalog.gs_password_deadline())") == 0 ||
        strcmp(queryString, "SELECT pg_catalog.gs_password_notifytime()") == 0 ||
        strcmp(queryString, "SELECT VERSION()") == 0) {
        return;    
    }

    /* Check if the role in "AlterRoleStmt" matches the current_user. */
    Oid current_user = GetUserId();
    Node* parsetree = (Node*)linitial(parsetreeList);
    char* current_user_name = GetUserNameFromId(current_user);
    if (IsA(parsetree, AlterRoleStmt)) {
        char* alter_name = ((AlterRoleStmt*)parsetree)->role;
        ListCell* option = NULL;
        foreach (option, ((AlterRoleStmt*)parsetree)->options) {
            DefElem* defel = (DefElem*)lfirst(option);
            DefElem* dpassword = NULL;

            if ((strcmp(defel->defname, "encryptedPassword") == 0 || 
                strcmp(defel->defname, "unencryptedPassword") == 0 ||
                strcmp(defel->defname, "password") == 0) && 
                strcasecmp(current_user_name, alter_name) == 0) {
                dpassword = defel;
            }

            if (dpassword != NULL && dpassword->arg != NULL) {
                return;
            }            
        }   
    }

    ereport(ERROR,
        (errcode(ERRCODE_INITIAL_PASSWORD_NOT_MODIFIED),
            errmsg("Please use \"ALTER ROLE user_name IDENTIFIED BY 'password' REPLACE 'old "
                "password';\" to modify the expired password of user %s before operation!",
                current_user_name)));
}

/*
 * merge_one_relation() will run in a new transaction, so it is necessary to
 * finish the previous transaction before running it, and restart a new transaction
 * after running it;
 */
void do_delta_merge(List* infos, VacuumStmt* stmt)
{
    ListCell* cell = NULL;

    /*
     * match StartTransactionCommand() outside
     */
    if (ActiveSnapshotSet())
        PopActiveSnapshot();
    finish_xact_command();

    foreach (cell, infos) {
        void* info = lfirst(cell);
        Assert(!((MergeInfo*)info)->is_hdfs);
        merge_cu_relation(info, stmt);
    }

    /*
     * match CommitTransactionCommand() outside
     */
    start_xact_command();
    PushActiveSnapshot(GetTransactionSnapshot());
}

/* Job worker Process, execute procedure */
void execute_simple_query(const char* query_string)
{
    exec_simple_query(query_string, QUERY_MESSAGE);
}

/*
 * exec_one_in_batch
 *	main entry of execute one in bind-execute message for not light cn
 *
 * Parameters:
 *	@in psrc: CachedPlanSource
 *	@in params: input params
 *	@in numRFormats: num of result format codes
 *	@in rformats: result format codes
 *	@in send_DP_msg: if send the DP msg
 *	@in dest: command dest
 *	@in completionTag: used in PortalRun and to compute num of processed tuples later.
 *   NULL value means the query is not SELECT/INSERT/UPDATE/DELETE,
 *   and will not count the num of processed tuples.
 *
 * Returns: const char *
 *   If query is not SELECT/INSERT/UPDATE/DELETE, return completionTag, else NULL.
 */
static void exec_one_in_batch(CachedPlanSource* psrc, ParamListInfo params, int numRFormats, int16* rformats,
    bool send_DP_msg, CommandDest dest, char* completionTag,
    const char* stmt_name, List** gpcCopyStmts, MemoryContext* tmpCxt, PreparedStatement* pstmt)
{
    CachedPlan* cplan = NULL;
    Portal portal;

    DestReceiver* receiver = NULL;
    bool completed = false;
    bool hasGetLock = false;

    if (ENABLE_GPC && psrc->stmt_name && psrc->stmt_name[0] != '\0' &&
        g_instance.plan_cache->CheckRecreateCachePlan(psrc, &hasGetLock)) {
        g_instance.plan_cache->RecreateCachePlan(psrc, stmt_name, pstmt, NULL, NULL, hasGetLock);
#ifdef ENABLE_MULTIPLE_NODES
        psrc = IS_PGXC_DATANODE ? u_sess->pcache_cxt.cur_stmt_psrc : pstmt->plansource;
#else
        psrc = pstmt->plansource;
#endif
        t_thrd.postgres_cxt.debug_query_string = psrc->query_string;
    }

    int generation = psrc->generation;

    OpFusion::clearForCplan((OpFusion*)psrc->opFusionObj, psrc);

#ifdef ENABLE_MOT
    /*
     * MOT JIT Execution:
     * Assist in distinguishing query boundaries in case of range query when client uses batches. This allows us to
     * know a new query started, and in case a previous execution did not fetch all records (since user is working in
     * batch-mode, and can decide to quit fetching in the middle), using this information we can infer this is a new
     * scan, and old scan state should be discarded.
     */
    if (psrc->mot_jit_context != NULL) {
        JitResetScan(psrc->mot_jit_context);
    }
#endif

    t_thrd.postgres_cxt.cur_command_tag = transform_node_tag(psrc->raw_parse_tree);
    if (psrc->opFusionObj != NULL) {
        (void)RevalidateCachedQuery(psrc);
        OpFusion *opFusionObj = (OpFusion *)(psrc->opFusionObj);
        if (psrc->opFusionObj != NULL) {
            Assert(psrc->cplan == NULL);
            if (opFusionObj->IsGlobal()) {
                opFusionObj = (OpFusion *)OpFusion::FusionFactory(opFusionObj->m_global->m_type,
                                                      u_sess->cache_mem_cxt, psrc, NULL, params);
                Assert(opFusionObj != NULL);
            }
            opFusionObj->bindClearPosition();
            opFusionObj->useOuterParameter(params);
            opFusionObj->setCurrentOpFusionObj(opFusionObj);
            opFusionObj->CopyFormats(rformats, numRFormats);
            opFusionObj->storeFusion("");

            CachedPlanSource* cps = opFusionObj->m_global->m_psrc;
            if (cps != NULL && cps->gplan) {
                setCachedPlanBucketId(cps->gplan, params);
            }

            if (OpFusion::process(FUSION_EXECUTE, NULL, completionTag, true, NULL)) {
                CommandCounterIncrement();
                return;
            }

            Assert(0);
            return;
        }
    }

    portal = CreatePortal("", true, true);

    MemoryContext oldContext = MemoryContextSwitchTo(PortalGetHeapMemory(portal));
    const char* saved_stmt_name = NULL;
    const char* cur_stmt_name = NULL;
    if (ENABLE_CN_GPC) {
        saved_stmt_name = (stmt_name[0] != '\0') ? pstrdup(stmt_name) : NULL;
        cur_stmt_name = psrc->gpc.status.IsPrivatePlan() ? psrc->stmt_name : saved_stmt_name;
    } else {
        cur_stmt_name = psrc->stmt_name;
    }
    (void)MemoryContextSwitchTo(oldContext);

    /*
     * Obtain a plan from the CachedPlanSource.  Any cruft from (re)planning
     * will be generated in t_thrd.mem_cxt.msg_mem_cxt.  The plan refcount will be
     * assigned to the Portal, so it will be released at portal destruction.
     */
    cplan = GetCachedPlan(psrc, params, false);

    /*
     * copy the single_shard info from plan source into plan.
     * With this, we can determine if we should use global snapshot or local snapshot after.
     */
    cplan->single_shard_stmt = psrc->single_shard_stmt;

    /*
     * Now we can define the portal.
     *
     * DO NOT put any code that could possibly throw an error between the
     * above GetCachedPlan call and here.
     */
    PortalDefineQuery(portal,
        (psrc != u_sess->pcache_cxt.unnamed_stmt_psrc) ? cur_stmt_name : NULL,
        psrc->query_string,
        psrc->commandTag,
        cplan->stmt_list,
        cplan);

    portal->nextval_default_expr_type = psrc->nextval_default_expr_type;

    if (ENABLE_GPC) {
        /* generated new gplan, copy it incase someone change it */
        if (generation != psrc->generation && psrc->gplan) {
            if (*tmpCxt != NULL)
                MemoryContextDelete(*tmpCxt);
            *gpcCopyStmts = CopyLocalStmt(psrc->gplan->stmt_list, u_sess->temp_mem_cxt, tmpCxt);
        } else if (*gpcCopyStmts == NULL && psrc->gpc.status.InShareTable() && psrc->gplan) {
            /* copy for shared plan */
            if (*tmpCxt != NULL)
                MemoryContextDelete(*tmpCxt);
            *gpcCopyStmts = CopyLocalStmt(psrc->gplan->stmt_list, u_sess->temp_mem_cxt, tmpCxt);
        }
        if (*gpcCopyStmts != NULL)
            portal->stmts = *gpcCopyStmts;
    }

    if (OpFusion::IsSqlBypass(psrc, cplan->stmt_list)) {
        psrc->opFusionObj = OpFusion::FusionFactory(OpFusion::getFusionType(cplan, params, NULL),
                                                    u_sess->cache_mem_cxt, psrc, NULL, params);
        psrc->is_checked_opfusion = true;
        if (psrc->opFusionObj != NULL) {
            ((OpFusion*)psrc->opFusionObj)->bindClearPosition();
            ((OpFusion*)psrc->opFusionObj)->useOuterParameter(params);
            ((OpFusion*)psrc->opFusionObj)->setCurrentOpFusionObj((OpFusion*)psrc->opFusionObj);
            ((OpFusion*)psrc->opFusionObj)->CopyFormats(rformats, numRFormats);
            ((OpFusion*)psrc->opFusionObj)->storeFusion("");

            if (OpFusion::process(FUSION_EXECUTE, NULL, completionTag, true, NULL)) {
                CommandCounterIncrement();
                return;
            }

            Assert(0);
        }
    }


    /*
     * And we're ready to start portal execution.
     */
    PortalStart(portal, params, 0, InvalidSnapshot);

    /*
     * Apply the result format requests to the portal.
     */
    PortalSetResultFormat(portal, numRFormats, rformats);

    /* send DP message if necessary */
    if (send_DP_msg) {
        if (portal->tupDesc)
            SendRowDescriptionMessage(&(*t_thrd.postgres_cxt.row_description_buf),
                portal->tupDesc,
                FetchPortalTargetList(portal),
                portal->formats);
        else
            pq_putemptymessage('n'); /* NoData */
    }

    /*
     * Create dest receiver in t_thrd.mem_cxt.msg_mem_cxt (we don't want it in transaction
     * context, because that may get deleted if portal contains VACUUM).
     */
    receiver = CreateDestReceiver(dest);
    if (dest == DestRemoteExecute)
        SetRemoteDestReceiverParams(receiver, portal);

    /* Check for cancel signal before we start execution */
    CHECK_FOR_INTERRUPTS();

    completed = PortalRun(portal,
        FETCH_ALL,
        true, /* always top level */
        receiver,
        receiver,
        completionTag);

    (*receiver->rDestroy)(receiver);

    if (completed) {
        /*
         * We need a CommandCounterIncrement after every query, except
         * those that start or end a transaction block.
         */
        CommandCounterIncrement();
    } else {
        /* Portal run not complete, maybe something wrong */
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("Portal run not complete for one in Batch bind-execute: name %s, query %s",
                    psrc->stmt_name,
                    psrc->query_string)));
    }
    t_thrd.postgres_cxt.cur_command_tag = T_Invalid;
}

/*
 * light_preprocess_batchmsg_set
 *	do preprocessing work before constructing batch message for each dn
 *
 * Parameters:
 *	@in psrc: plan
 *	@in params_set: params used to compute dn index
 *	@in params_set_end: params position for computing params size for each dn
 *	@in batch_count: batch count
 *
 *	@out node_idx_set: node index for each bind-execute
 *	@out batch_count_dnset: batch count for each dn
 *	@out params_size_dnset: params size for each dn
 *
 * Returns: void
 */
static void light_preprocess_batchmsg_set(CachedPlanSource* psrc, const ParamListInfo* params_set,
    const int* params_set_end, int batch_count, int* node_idx_set, int* batch_count_dnset, int* params_size_dnset)
{
    int idx = -1;
    ExecNodes* exec_nodes = psrc->single_exec_node;
    Assert(exec_nodes->nodeList != NIL || exec_nodes->en_expr != NIL);

    ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    ResourceOwner tmpOwner = ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, "BatchLightProxy",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
    t_thrd.utils_cxt.CurrentResourceOwner = tmpOwner;

    if (exec_nodes->en_expr != NIL) {
        /* use my own context to store tmp info */
        MemoryContext old_context = CurrentMemoryContext;
        MemoryContext my_context = AllocSetContextCreate(u_sess->top_portal_cxt,
            "BatchLightPorxyMemory",
            ALLOCSET_SMALL_MINSIZE,
            ALLOCSET_SMALL_INITSIZE,
            ALLOCSET_SMALL_MAXSIZE);
        MemoryContextSwitchTo(my_context);

        RelationLocInfo* rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
        if (rel_loc_info == NULL) {
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("rel_loc_info is NULL.")));
        }

        int len = list_length(rel_loc_info->partAttrNum);
        Datum* distcol_value = (Datum*)palloc0(len * sizeof(Datum));
        bool* distcol_isnull = (bool*)palloc0(len * sizeof(bool));
        Oid* distcol_type = (Oid*)palloc0(len * sizeof(Oid));
        errno_t ss_rc = 0;

        for (int j = 0; j < batch_count; j++) {
            List* idx_dist = NULL;
            int i = 0;
            ExecNodes* single_node = NULL;
            ListCell* cell = NULL;

            foreach (cell, exec_nodes->en_expr) {
                Expr* distcol_expr = (Expr*)lfirst(cell);
                distcol_expr = (Expr*)eval_const_expressions_params(NULL, (Node*)distcol_expr, params_set[j]);

                if (distcol_expr && IsA(distcol_expr, Const)) {
                    Const* const_expr = (Const*)distcol_expr;
                    distcol_value[i] = const_expr->constvalue;
                    distcol_isnull[i] = const_expr->constisnull;
                    distcol_type[i] = const_expr->consttype;
                    idx_dist = lappend_int(idx_dist, i);
                    i++;
                } else
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Batch param of distribute key only support const")));
            }

            single_node = GetRelationNodes(
                rel_loc_info, distcol_value, distcol_isnull, distcol_type, idx_dist, exec_nodes->accesstype);

            /* make sure it is one dn */
            if (single_node == NULL || list_length(single_node->nodeList) != 1)
                ereport(ERROR,
                    (errcode(ERRCODE_SYSTEM_ERROR),
                        errmsg("Failed to get DataNode id for Batch bind-execute: name %s, query %s",
                            psrc->stmt_name,
                            psrc->query_string)));

            idx = linitial_int(single_node->nodeList);
            node_idx_set[j] = idx;
            batch_count_dnset[idx]++;
            params_size_dnset[idx] += (params_set_end[j + 1] >= params_set_end[j] ?
                                       params_set_end[j + 1] - params_set_end[j] : 0);

            /* reset */
            ss_rc = memset_s(distcol_value, len * sizeof(Datum), 0, len * sizeof(Datum));
            securec_check(ss_rc, "\0", "\0");
            ss_rc = memset_s(distcol_isnull, len * sizeof(bool), 0, len * sizeof(bool));
            securec_check(ss_rc, "\0", "\0");
            ss_rc = memset_s(distcol_type, len * sizeof(Oid), 0, len * sizeof(Oid));
            securec_check(ss_rc, "\0", "\0");
        }

        MemoryContextSwitchTo(old_context);
        MemoryContextDelete(my_context);
    } else {
        /* make sure it is one dn */
        if (list_length(exec_nodes->nodeList) != 1)
            ereport(ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("Failed to get DataNode id for Batch bind-execute: name %s, query %s",
                        psrc->stmt_name,
                        psrc->query_string)));

        /* all same node index in this case */
        idx = linitial_int(exec_nodes->nodeList);
        batch_count_dnset[idx] = batch_count;
        for (int j = 0; j < batch_count; j++) {
            node_idx_set[j] = idx;
            params_size_dnset[idx] += (params_set_end[j + 1] >= params_set_end[j] ?
                                       params_set_end[j + 1] - params_set_end[j] : 0);
        }
    }

    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
    t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
    ResourceOwnerDelete(tmpOwner);
}

/*
 * light_construct_batchmsg_set
 *	construct batch messages for light cn for each dn
 *
 * Parameters:
 *	@in input: message from client
 *	@in params_set_end: position index of params in input message
 *	@in node_idx_set: node index set
 *	@in batch_count_dnset: batch count for each dn
 *	@in params_size_dnset: params size for each dn
 *	@in desc_msg: describe message
 *	@in exec_msg: execute message
 *
 * Returns: batch message for each dn
 */
static StringInfo light_construct_batchmsg_set(StringInfo input, const int* params_set_end, const int* node_idx_set,
    const int* batch_count_dnset, const int* params_size_dnset, StringInfo desc_msg, StringInfo exec_msg)
{
    int batch_count;
    int fix_part_size;
    int before_size;
    bool send_DP_msg = (desc_msg != NULL);
    errno_t ss_rc = 0;

    int idx;
    int batch_count_dn;
    int tmp_batch_count_dn;
    int tmp_params_size;
    StringInfo batch_msg;
    StringInfo batch_msg_dnset;

    /* parse it again */
    input->cursor = 0;
    batch_count = pq_getmsgint(input, 4);
    if (unlikely(batch_count <= 0)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("unexpected batch_count %d get from inputmessage", batch_count)));
    }
    /* size of common part before params */
    before_size = params_set_end[0] - input->cursor;
    Assert(before_size > 0);

    /* 1.construct common part before params */
    fix_part_size = 4 + before_size + exec_msg->len;
    batch_msg_dnset = (StringInfo)palloc0(u_sess->pgxc_cxt.NumDataNodes * sizeof(StringInfoData));
    for (int i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++) {
        batch_count_dn = batch_count_dnset[i];
        /* not related to this dn */
        if (batch_count_dn == 0)
            continue;

        batch_msg = &batch_msg_dnset[i];
        /* only send DP message for the first dn */
        if (send_DP_msg) {
            batch_msg->len = fix_part_size + desc_msg->len + params_size_dnset[i];
            send_DP_msg = false;
        } else
            batch_msg->len = fix_part_size + params_size_dnset[i];

        batch_msg->maxlen = batch_msg->len + 1;
        batch_msg->data = (char*)palloc0(batch_msg->maxlen);

        /* batch_count */
        tmp_batch_count_dn = htonl(batch_count_dn);
        ss_rc = memcpy_s(batch_msg->data, MEMCPY_DST_NUM, &tmp_batch_count_dn, MEMCPY_DST_NUM);
        securec_check(ss_rc, "\0", "\0");
        batch_msg->cursor = 4;

        /* before params */
        ss_rc = memcpy_s(batch_msg->data + batch_msg->cursor, before_size, input->data + input->cursor, before_size);
        securec_check(ss_rc, "\0", "\0");
        batch_msg->cursor += before_size;
    }

    /* 2.construct the params part */
    input->cursor += before_size;
    /* has params */
    if (params_set_end[1] > 0) {
        for (int i = 0; i < batch_count; i++) {
            idx = node_idx_set[i];
            batch_msg = &batch_msg_dnset[idx];
            if (unlikely(batch_msg->maxlen <= 0)) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("unexpected maxlen %d ", batch_msg->maxlen)));
            }

            /* append params */
            tmp_params_size = params_set_end[i + 1] - params_set_end[i];
            ss_rc = memcpy_s(
                batch_msg->data + batch_msg->cursor, tmp_params_size, input->data + input->cursor, tmp_params_size);
            securec_check(ss_rc, "\0", "\0");
            batch_msg->cursor += tmp_params_size;
            input->cursor += tmp_params_size;
        }
    }

    /* 3.construct common part after params */
    send_DP_msg = (desc_msg != NULL);
    for (int i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++) {
        batch_count_dn = batch_count_dnset[i];
        /* not related to this dn */
        if (batch_count_dn == 0)
            continue;

        batch_msg = &batch_msg_dnset[i];
        if (send_DP_msg) {
            ss_rc = memcpy_s(batch_msg->data + batch_msg->cursor, desc_msg->len, desc_msg->data, desc_msg->len);
            securec_check(ss_rc, "\0", "\0");
            batch_msg->cursor += desc_msg->len;
            send_DP_msg = false;
        }

        ss_rc = memcpy_s(batch_msg->data + batch_msg->cursor, exec_msg->len, exec_msg->data, exec_msg->len);
        securec_check(ss_rc, "\0", "\0");
        batch_msg->cursor += exec_msg->len;

        /* check finished and reset cursor */
        Assert(batch_msg->cursor == batch_msg->len);
        batch_msg->cursor = 0;
    }

    return batch_msg_dnset;
}

/*
 * light_execute_batchmsg_set
 *	execute batch for light cn for each dn
 *
 * Parameters:
 *	@in scn: Light proxy
 *	@in batch_msg_dnset: batch message for each dn
 *	@in batch_count_dnset: batch count for each dn
 *	@in send_DP_msg: mark if send DP message
 *
 * Returns: process_count
 */
static int light_execute_batchmsg_set(
    lightProxy* scn, const StringInfo batch_msg_dnset, const int* batch_count_dnset, bool send_DP_msg)
{
    int process_count = 0;
    int tmp_count = 0;
    bool sendDMsg = send_DP_msg;
    int batch_count_dn;

    /* set statement start timestamp */
    SetCurrentStmtTimestamp();

    for (int i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++) {
        batch_count_dn = batch_count_dnset[i];
        /* not related to this dn */
        if (batch_count_dn == 0)
            continue;

        /* Check for cancel signal before we start execution */
        CHECK_FOR_INTERRUPTS();
        /* in router, only execute batch msg on router node */
        if (HAS_ROUTER)
            scn->m_nodeIdx = u_sess->exec_cxt.CurrentRouter->GetRouterNodeId();
        else
            scn->m_nodeIdx = i;

        tmp_count = scn->runBatchMsg(&batch_msg_dnset[i], sendDMsg, batch_count_dn);
        process_count += tmp_count;
        if (sendDMsg)
            sendDMsg = false;
    }

    return process_count;
}

/*
 * exec_batch_bind_execute
 *	main entry of execute batch bind-execute message
 *
 * Parameters:
 *	@in input_message: message from client
 *
 * Returns: void
 */
static void exec_batch_bind_execute(StringInfo input_message)
{
    /* special for U message */
    int batch_count;
    CmdType cmd_type;
    int* params_set_end = NULL;
    bool send_DP_msg = false;
    int process_count = 0;
    CommandDest dest;
    StringInfoData process_result;
    /* use original logic if not SELECT/INSERT/UPDATE/DELETE */
    bool use_original_logic = false;

    /* like B message */
    const char* portal_name = NULL;
    const char* stmt_name = NULL;
    int numPFormats;
    int16* pformats = NULL;
    int numParams;
    int numRFormats;
    int16* rformats = NULL;
    CachedPlanSource* psrc = NULL;
    ParamListInfo* params_set = NULL;
    MemoryContext oldContext;
    bool save_log_statement_stats = u_sess->attr.attr_common.log_statement_stats;
    bool snapshot_set = false;
    char msec_str[PRINTF_DST_MAX];
    Oid param_collation = GetCollationConnection();
    int param_charset = GetCharsetConnection();
    FmgrInfo convert_finfo;

    int msg_type;
    /* D message */
    int describe_type = 0;
    const char* describe_target = NULL;
    /* E message */
    const char* exec_portal_name = NULL;
    int max_rows;
    /* reset gpc batch flag */
    u_sess->pcache_cxt.gpc_in_batch = false;

    /* set PBE message */
    u_sess->pbe_message = EXECUTE_BATCH_MESSAGE_QUERY;

    /*
     * Only support normal perf mode for PBE, as DestRemoteExecute can not send T message automatically.
     */
    t_thrd.explain_cxt.explain_perf_mode = EXPLAIN_NORMAL;
    instr_stmt_report_start_time();

    /* 'U' Message format: many B-like and one D and one E message together
     * batchCount
     * portal_name
     * stmt_name
     * numPFormats
     * PFormats
     * numRFormats
     * RFormats
     * numParams
     * Params1
     * Params2...
     * 'D'
     * describe_type
     * describe_target
     * 'E'
     * portal_name
     * max_rows
     */
    /* A. Parse message */
    /* 1.specila for U message */
    /* batchCount: count of bind */
    batch_count = pq_getmsgint(input_message, 4);
    if (batch_count <= 0 || batch_count > PG_INT32_MAX / (int)sizeof(ParamListInfo))
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("Batch bind-execute message with invalid batch count: %d", batch_count)));

    /* 2.B-like message */
    /* Get the fixed part of the message */
    portal_name = pq_getmsgstring(input_message);
    stmt_name = pq_getmsgstring(input_message);
    /* Check not NULL */
    AssertEreport(portal_name != NULL && stmt_name != NULL, MOD_OPT, "portal_name and stmt_name can not be NULL");
    if (strlen(portal_name) > SECUREC_MEM_MAX_LEN || strlen(stmt_name) > SECUREC_MEM_MAX_LEN)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Too long portal_name and stmt_name.")));
    if (portal_name[0] != '\0')
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Not support portal_name %s for Batch bind-execute.", portal_name)));

    ereport(DEBUG2,
        (errmsg("Batch bind-execute %s to %s, batch count %d",
            *portal_name ? portal_name : "<unnamed>",
            *stmt_name ? stmt_name : "<unnamed>",
            batch_count)));
    PreparedStatement *pstmt = NULL;

    /* Find prepared statement */
    if (stmt_name[0] != '\0') {
        if (ENABLE_DN_GPC) {
            psrc = u_sess->pcache_cxt.cur_stmt_psrc;
            if (SECUREC_UNLIKELY(psrc == NULL))
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                        errmsg("dn gpc's prepared statement %s does not exist", stmt_name)));
        } else {
            pstmt = FetchPreparedStatement(stmt_name, true, true);
            psrc = pstmt->plansource;
        }
    } else {
        /* special-case the unnamed statement */
        psrc = u_sess->pcache_cxt.unnamed_stmt_psrc;
        if (psrc == NULL)
            ereport(
                ERROR, (errcode(ERRCODE_UNDEFINED_PSTATEMENT), errmsg("unnamed prepared statement does not exist")));
    }

    Assert(NULL != psrc);

    /* Check command type: only support IUD */
    initStringInfo(&process_result);
    cmd_type = set_cmd_type(psrc->commandTag);
    switch (cmd_type) {
        case CMD_INSERT:
            appendStringInfo(&process_result, "INSERT 0 ");
            break;
        case CMD_UPDATE:
            appendStringInfo(&process_result, "UPDATE ");
            break;
        case CMD_DELETE:
            appendStringInfo(&process_result, "DELETE ");
            break;
        case CMD_SELECT:
            appendStringInfo(&process_result, "SELETE ");
            break;
        case CMD_MERGE:
            appendStringInfo(&process_result, "MERGE ");
            break;
        default:
            use_original_logic = true;
            ereport(LOG,
                (errmsg("Not support Batch bind-execute for %s: stmt_name %s, query %s",
                    psrc->commandTag,
                    psrc->stmt_name,
                    psrc->query_string)));
            break;
    }

    /*
     * Report query to various monitoring facilities.
     */
    t_thrd.postgres_cxt.debug_query_string = psrc->query_string;
    t_thrd.postgres_cxt.cur_command_tag = transform_node_tag(psrc->raw_parse_tree);
    pgstat_report_activity(STATE_RUNNING, psrc->query_string);

    set_ps_display(psrc->commandTag, false);

    if (save_log_statement_stats) {
        ResetUsage();
    }

    /*
     * Start up a transaction command so we can call functions etc. (Note that
     * this will normally change current memory context.) Nothing happens if
     * we are already in one.
     */
    start_xact_command();
    if (!u_sess->attr.attr_storage.phony_autocommit) {
        BeginTxnForAutoCommitOff();
    }

    if (ENABLE_WORKLOAD_CONTROL && SqlIsValid(t_thrd.postgres_cxt.debug_query_string) &&
        (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) &&
        !IsConnFromCoord()) {
        u_sess->wlm_cxt->is_active_statements_reset = false;

        if (g_instance.wlm_cxt->dynamic_workload_inited) {
            dywlm_parallel_ready(t_thrd.postgres_cxt.debug_query_string);
            dywlm_client_max_reserve();
        } else {
            WLMParctlReady(t_thrd.postgres_cxt.debug_query_string);
            WLMParctlReserve(PARCTL_GLOBAL);
        }
    }

    /*
     * Begin to parse the big B-like message.
     * First, common part:
     * numPFormats, PFormats
     * numRFormats, RFormats
     * numParams
     */

    /* Switch back to message context */
    oldContext = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

    /* Get the parameter format codes */
    numPFormats = pq_getmsgint(input_message, 2);
    if (numPFormats > PG_UINT16_MAX) {
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("Batch bind-execute message with invalid parameter number: %d", numPFormats)));
    }
    if (numPFormats > 0) {
        pformats = (int16*)palloc0(numPFormats * sizeof(int16));
        for (int i = 0; i < numPFormats; i++) {
            pformats[i] = pq_getmsgint(input_message, 2);
        }
    }

    /* Get the result format codes */
    numRFormats = pq_getmsgint(input_message, 2);
    if (numRFormats > PG_UINT16_MAX) {
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("Batch bind-execute message with invalid parameter number: %d", numRFormats)));
    }
    if (numRFormats > 0) {
        rformats = (int16*)palloc0(numRFormats * sizeof(int16));
        for (int i = 0; i < numRFormats; i++) {
            rformats[i] = pq_getmsgint(input_message, 2);
        }
    }

    /* Get the parameter value count */
    numParams = pq_getmsgint(input_message, 2);
    if (numPFormats > 1 && numPFormats != numParams)
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("bind message has %d parameter formats but %d parameters", numPFormats, numParams)));
    if (numParams != psrc->num_params)
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("bind message supplies %d parameters, but prepared statement \"%s\" requires %d",
                    numParams,
                    stmt_name,
                    psrc->num_params)));

    /*
     * If we are in aborted transaction state, the only portals we can
     * actually run are those containing COMMIT or ROLLBACK commands. We
     * disallow binding anything else to avoid problems with infrastructure
     * that expects to run inside a valid transaction.	We also disallow
     * binding any parameters, since we can't risk calling user-defined I/O
     * functions.
     */
    if (IsAbortedTransactionBlockState() && (!IsTransactionExitStmt(psrc->raw_parse_tree) || numParams != 0))
        ereport(ERROR,
            (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                errmsg("current transaction is aborted, "
                    "commands ignored until end of transaction block, firstChar[%c]",
                    u_sess->proc_cxt.firstChar),
                errdetail_abort()));

    /*
     * Set a snapshot if we have parameters to fetch (since the input
     * functions might need it) or the query isn't a utility command (and
     * hence could require redoing parse analysis and planning).  We keep the
     * snapshot active till we're done, so that plancache.c doesn't have to
     * take new ones.
     */
    if (!GTM_LITE_MODE && (numParams > 0 || analyze_requires_snapshot(psrc->raw_parse_tree))) {
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_set = true;
    }

    /* Make sure the querytree list is valid and we have parse-time locks */
    if (psrc->single_exec_node != NULL)
        RevalidateCachedQuery(psrc);

    /* record the params set position for light cn to contruct batch message */
    if (psrc->single_exec_node != NULL) {
        params_set_end = (int*)palloc0((batch_count + 1) * sizeof(int));
        /* keep the end pos of message before params at last */
        params_set_end[0] = input_message->cursor;
    }

    /*
     * There is a fast path for transcoding from the client to the server.
     * If the characterset_connection is different from the server_encoding,
     * Fmgrinfo should be constructed here to avoid poor construction performance during each conversion.
     */
    if (OidIsValid(param_collation) && param_charset != GetDatabaseEncoding()) {
        construct_conversion_fmgr_info(pg_get_client_encoding(), param_charset, (void*)&convert_finfo);
    } else {
        convert_finfo.fn_oid = InvalidOid;
    }

    /* Second, process each set of params */
    params_set = (ParamListInfo*)palloc0(batch_count * sizeof(ParamListInfo));
    if (numParams > 0) {
        for (int i = 0; i < batch_count; i++) {
            ParamListInfo params =
                (ParamListInfo)palloc0(offsetof(ParamListInfoData, params) + numParams * sizeof(ParamExternData));
            /* we have static list of params, so no hooks needed */
            params->paramFetch = NULL;
            params->paramFetchArg = NULL;
            params->parserSetup = NULL;
            params->parserSetupArg = NULL;
            params->params_need_process = false;
            params->uParamInfo = DEFUALT_INFO;
            params->params_lazy_bind = false;
            params->numParams = numParams;

            for (int paramno = 0; paramno < numParams; paramno++) {
                Oid ptype = psrc->param_types[paramno];
                int32 plength;
                Datum pval;
                bool isNull = false;
                StringInfoData pbuf;
                char csave;
                int16 pformat;

                plength = pq_getmsgint(input_message, 4);
                isNull = (plength == -1);
                /* add null value process for date type */
                if (((VARCHAROID == ptype  && !ACCEPT_EMPTY_STR) || TIMESTAMPOID == ptype ||
                        TIMESTAMPTZOID == ptype || TIMEOID == ptype || TIMETZOID == ptype ||
                        INTERVALOID == ptype || SMALLDATETIMEOID == ptype) &&
                    0 == plength && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
                    isNull = true;

                /*
                 * Insert into bind values support illegal characters import,
                 * and this just wroks for char type attribute.
                 */
                u_sess->mb_cxt.insertValuesBind_compatible_illegal_chars = IsCharType(ptype);

                if (!isNull) {
                    const char* pvalue = pq_getmsgbytes(input_message, plength);

                    /*
                     * Rather than copying data around, we just set up a phony
                     * StringInfo pointing to the correct portion of the message
                     * buffer.	We assume we can scribble on the message buffer so
                     * as to maintain the convention that StringInfos have a
                     * trailing null.  This is grotty but is a big win when
                     * dealing with very large parameter strings.
                     */
                    pbuf.data = (char*)pvalue;
                    pbuf.maxlen = plength + 1;
                    pbuf.len = plength;
                    pbuf.cursor = 0;

                    csave = pbuf.data[plength];
                    pbuf.data[plength] = '\0';
                } else {
                    pbuf.data = NULL; /* keep compiler quiet */
                    csave = 0;
                }

                if (numPFormats > 1) {
                    Assert(NULL != pformats);
                    pformat = pformats[paramno];
                } else if (numPFormats > 0) {
                    Assert(NULL != pformats);
                    pformat = pformats[0];
                } else {
                    pformat = 0; /* default = text */
                }

                if (pformat == 0) {
                    /* text mode */
                    Oid typinput;
                    Oid typioparam;
                    char* pstring = NULL;

                    getTypeInputInfo(ptype, &typinput, &typioparam);

                    /*
                     * We have to do encoding conversion before calling the
                     * typinput routine.
                     */
                    if (isNull) {
                        pstring = NULL;
                    } else if (OidIsValid(param_collation) && IsSupportCharsetType(ptype)) {
                        pstring = pg_client_to_any(pbuf.data, plength, param_charset, (void*)&convert_finfo);
                    } else {
                        pstring = pg_client_to_server(pbuf.data, plength);
                    }

                    pval = OidInputFunctionCall(typinput, pstring, typioparam, -1);

                    /* Free result of encoding conversion, if any */
                    if (pstring != NULL && pstring != pbuf.data)
                        pfree(pstring);
                } else if (pformat == 1) {
                    /* binary mode */
                    Oid typreceive;
                    Oid typioparam;
                    StringInfo bufptr;

                    /*
                     * Call the parameter type's binary input converter
                     */
                    getTypeBinaryInputInfo(ptype, &typreceive, &typioparam);

                    if (isNull)
                        bufptr = NULL;
                    else
                        bufptr = &pbuf;

                    pval = OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);

                    /* Trouble if it didn't eat the whole buffer */
                    if (!isNull && pbuf.cursor != pbuf.len)
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                                errmsg("incorrect binary data format in bind parameter %d", paramno + 1)));
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unsupported format code: %d", pformat)));
                    pval = 0; /* keep compiler quiet */
                }

                /* Restore message buffer contents */
                if (!isNull)
                    pbuf.data[plength] = csave;

                params->params[paramno].value = pval;
                params->params[paramno].isnull = isNull;
                /*
                 * We mark the params as CONST.  This ensures that any custom plan
                 * makes full use of the parameter values.
                 */
                params->params[paramno].pflags = PARAM_FLAG_CONST;
                params->params[paramno].ptype = ptype;
                params->params[paramno].tabInfo = NULL;

                /* Reset the compatible illegal chars import flag */
                u_sess->mb_cxt.insertValuesBind_compatible_illegal_chars = false;
            }

            /* assign to the set */
            params_set[i] = params;
            if (params_set_end != NULL)
                params_set_end[i + 1] = input_message->cursor;
        }
    }

    /* Log immediately if dictated by log_statement */
    List* parse_list = list_make1(psrc->raw_parse_tree);
    if (check_log_statement(parse_list)) {
        char* mask_string = NULL;
        mask_string = maskPassword(psrc->query_string);
        if (mask_string == NULL) {
            mask_string = (char*)psrc->query_string;
        }
        MASK_PASSWORD_START(mask_string, psrc->query_string);
        ereport(LOG,
            (errmsg("execute batch %s%s%s: %s",
                 psrc->stmt_name,
                 *portal_name ? "/" : "",
                 *portal_name ? portal_name : "",
                 mask_string),
                errhidestmt(true),
                errdetail_batch_params(batch_count, numParams, params_set)));
        MASK_PASSWORD_END(mask_string, psrc->query_string);
    }
    list_free(parse_list);

    /* Set unique SQLID in batch bind execute */
    SetUniqueSQLIdInBatchBindExecute(psrc, params_set, batch_count);

    /* msg_type: maybe D or E */
    msg_type = pq_getmsgbyte(input_message);

    /* 3.D message */
    if (msg_type == 'D') {
        describe_type = pq_getmsgbyte(input_message);
        describe_target = pq_getmsgstring(input_message);

        if (describe_type == 'S') {
            if (strcmp(stmt_name, describe_target))
                ereport(ERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                        errmsg("conflict stmt name in Batch bind-execute message: bind %s, describe %s",
                            stmt_name,
                            describe_target)));
        } else if (describe_type == 'P') {
            if (strcmp(portal_name, describe_target))
                ereport(ERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                        errmsg("conflict portal name in Batch bind-execute message: bind %s, describe %s",
                            portal_name,
                            describe_target)));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                    errmsg("invalid DESCRIBE message subtype in Batch bind-execute message: %d", describe_type)));
        }

        /* next should be E */
        msg_type = pq_getmsgbyte(input_message);
    }

    /* 4.E message */
    if (msg_type == 'E') {
        exec_portal_name = pq_getmsgstring(input_message);
        if (strcmp(portal_name, exec_portal_name))
            ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                    errmsg("conflict portal name in Batch bind-execute message: bind %s, execute %s",
                        portal_name,
                        exec_portal_name)));

        max_rows = pq_getmsgint(input_message, 4);
        if (max_rows > 0)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Not support max_row in Batch bind-execute message: %d", max_rows)));

        max_rows = 0;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid value in Batch bind-execute message: %d", msg_type)));
    }

    /* 5.Finish the message */
    pq_getmsgend(input_message);

    /* B.Set message for bind and describe */
    if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote) {
        /* 1.Send BindComplete */
        pq_putemptymessage('2');

        /* 2.Send Describe */
        if (describe_type != 0) {
            switch (describe_type) {
                case 'S': {
                    StringInfoData buf;
                    /* Prepared statements shouldn't have changeable result descs */
                    Assert(psrc->fixed_result);

                    /*
                     * First describe the parameters...
                     */
                    pq_beginmessage(&buf, 't'); /* parameter description message type */
                    pq_sendint(&buf, psrc->num_params, 2);
                    for (int i = 0; i < psrc->num_params; i++) {
                        Oid ptype = psrc->param_types[i];
                        pq_sendint(&buf, (int)ptype, 4);
                    }
                    pq_endmessage(&buf);

                    /*
                     * Next send RowDescription or NoData to describe the result...
                     */
                    if (psrc->resultDesc) {
                        /* Get the plan's primary targetlist */
                        List* tlist = CachedPlanGetTargetList(psrc);
                        SendRowDescriptionMessage(
                            &(*t_thrd.postgres_cxt.row_description_buf), psrc->resultDesc, tlist, NULL);
                    } else
                        pq_putemptymessage('n'); /* NoData */
                } break;

                case 'P':
                    /* set the message later before execute */
                    send_DP_msg = true;
                    break;
                default:
                    /* should not be here */
                    break;
            }
        }
    }

    /* Adjust destination to tell printtup.c what to do */
    dest = (CommandDest)t_thrd.postgres_cxt.whereToSendOutput;
    if (dest == DestRemote)
        dest = DestRemoteExecute;

    /* C.Execute each one */
    if (psrc->single_exec_node != NULL) {
        StringInfo desc_msg = NULL;
        StringInfoData describe_body;
        StringInfoData execute_msg;
        int pnameLen = strlen(portal_name) + 1;
        errno_t ss_rc = 0;

        StringInfo batch_msg_dnset;
        int* node_idx_set = NULL;
        int* batch_count_dnset = NULL;
        int* params_size_dnset = NULL;

        /* No nodes found ?? */
        if (u_sess->pgxc_cxt.NumDataNodes == 0)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("No Datanode defined in cluster")));

        node_idx_set = (int*)palloc0(batch_count * sizeof(int));
        batch_count_dnset = (int*)palloc0(u_sess->pgxc_cxt.NumDataNodes * sizeof(int));
        params_size_dnset = (int*)palloc0(u_sess->pgxc_cxt.NumDataNodes * sizeof(int));

        /* 1.get necessary info before construct batch message for each dn */
        light_preprocess_batchmsg_set(
            psrc, params_set, params_set_end, batch_count, node_idx_set, batch_count_dnset, params_size_dnset);

        /* 2.construct batch message for each dn */
        /* construct D message if necessary */
        if (send_DP_msg) {
            /* 'D' + 'P' + portal_name(describe_target) */
            describe_body.len = 1 + 1 + pnameLen;
            describe_body.maxlen = describe_body.len + 1;
            describe_body.cursor = 0;
            describe_body.data = (char*)palloc0(describe_body.maxlen);

            describe_body.data[0] = 'D';
            describe_body.data[1] = 'P';
            ss_rc = memcpy_s(describe_body.data + 2, pnameLen, portal_name, pnameLen);
            securec_check(ss_rc, "\0", "\0");

            desc_msg = &describe_body;
        }

        /* construct E message (all same): 'E', portal name, 0 max_row */
        execute_msg.len = 1 + pnameLen + 4;
        execute_msg.maxlen = execute_msg.len + 1;
        execute_msg.cursor = 0;
        execute_msg.data = (char*)palloc0(execute_msg.maxlen);

        execute_msg.data[0] = 'E';
        ss_rc = memcpy_s(execute_msg.data + 1, pnameLen, portal_name, pnameLen);
        securec_check(ss_rc, "\0", "\0");

        batch_msg_dnset = light_construct_batchmsg_set(
            input_message, params_set_end, node_idx_set, batch_count_dnset, params_size_dnset, desc_msg, &execute_msg);

        /* 3.run for each dn */
        lightProxy *scn = NULL;
        bool enable_gpc = (ENABLE_CN_GPC && stmt_name[0] != '\0');
        bool enable_unamed_gpc = psrc->gpc.status.InShareTable() && stmt_name[0] == '\0';
        if (enable_gpc)
            scn = lightProxy::locateLpByStmtName(stmt_name);
        else
            scn = (lightProxy *)psrc->lightProxyObj;

        if (scn == NULL) {
            /* initialize session cache context; typically it won't store much */
            MemoryContext context = AllocSetContextCreate(u_sess->cache_mem_cxt, "LightPorxyMemory",
                ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
            scn = New(context)lightProxy(context, psrc, portal_name, stmt_name);
            if (enable_gpc) {
                scn->storeLpByStmtName(stmt_name);
                psrc->lightProxyObj = NULL;
            } else if (enable_unamed_gpc) {
                Assert(u_sess->pcache_cxt.unnamed_gpc_lp == NULL);
                Assert(psrc->lightProxyObj == NULL);
                u_sess->pcache_cxt.unnamed_gpc_lp = scn;
            } else {
                psrc->lightProxyObj = scn;
            }
        } else {
            Assert(!enable_unamed_gpc);
        }
        if (enable_gpc) {
            /* cngpc need fill gpc msg just like BuildCachedPlan. */
            GPCFillMsgForLp(psrc);
        }

        Assert(scn != NULL);
        process_count = light_execute_batchmsg_set(scn, batch_msg_dnset, batch_count_dnset, send_DP_msg);
    } else {
        char* completionTag = (char*)palloc0(COMPLETION_TAG_BUFSIZE * sizeof(char));
        List* copyedStmts = NULL;
        MemoryContext tmpCxt = NULL;

        if (u_sess->attr.attr_resource.use_workload_manager && g_instance.wlm_cxt->gscgroup_init_done &&
            !IsAbortedTransactionBlockState()) {
            u_sess->wlm_cxt->cgroup_last_stmt = u_sess->wlm_cxt->cgroup_stmt;
            u_sess->wlm_cxt->cgroup_stmt = WLMIsSpecialCommand(psrc->raw_parse_tree, NULL);
        }

        if (use_original_logic) {
            for (int i = 0; i < batch_count; i++) {
                exec_one_in_batch(psrc, params_set[i], numRFormats, rformats,
                                  (i == 0) ? send_DP_msg : false, dest, completionTag,
                                  stmt_name, &copyedStmts, &tmpCxt, pstmt);
                u_sess->pcache_cxt.gpc_in_batch = true;
                if (ENABLE_GPC && stmt_name[0] != '\0') {
#ifdef ENABLE_MULTIPLE_NODES
                    psrc = IS_PGXC_DATANODE ? u_sess->pcache_cxt.cur_stmt_psrc : pstmt->plansource;
#else
                    psrc = pstmt->plansource;
#endif
                }
            }

            /* only send the last commandTag */
            EndCommand(completionTag, dest);
        } else {
            int tmp_count = 0;

            for (int i = 0; i < batch_count; i++) {
                exec_one_in_batch(psrc, params_set[i], numRFormats, rformats,
                                  (i == 0) ? send_DP_msg : false, dest, completionTag,
                                  stmt_name, &copyedStmts, &tmpCxt, pstmt);
                u_sess->pcache_cxt.gpc_in_batch = true;
                if (ENABLE_GPC && stmt_name[0] != '\0') {
#ifdef ENABLE_MULTIPLE_NODES
                    psrc = IS_PGXC_DATANODE ? u_sess->pcache_cxt.cur_stmt_psrc : pstmt->plansource;
#else
                    psrc = pstmt->plansource;
#endif
                }

                /* Get process_count (X) from completionTag */
                if (completionTag[0] == 'I') {
                    /* INSERT 0 X */
                    tmp_count = pg_atoi(&completionTag[9], sizeof(int32), '\0');
                } else if (completionTag[0] == 'M') {
                    /* MERGE X */
                    tmp_count = pg_atoi(&completionTag[6], sizeof(int32), '\0');
                } else {
                    /* DELETE X / UPDATE X / SELECT X */
                    Assert(completionTag[0] == 'U' || completionTag[0] == 'D' || completionTag[0] == 'S');
                    tmp_count = pg_atoi(&completionTag[7], sizeof(int32), '\0');
                }

                process_count += tmp_count;
            }
        }

        pfree(completionTag);
    }
    /* end batch, reset gpc batch flag */
    u_sess->pcache_cxt.gpc_in_batch = false;
    
    /* Reset hint flag */
    u_sess->parser_cxt.has_hintwarning = false;
    
    /* Done with the snapshot used */
    if (snapshot_set)
        PopActiveSnapshot();

    if (!use_original_logic) {
        /* Send appropriate CommandComplete to client */
        appendStringInfo(&process_result, "%d", process_count);
        EndCommand(process_result.data, dest);
    }

    MemoryContextSwitchTo(oldContext);

    /* release global active counts */
    if (ENABLE_WORKLOAD_CONTROL) {
        if (g_instance.wlm_cxt->dynamic_workload_inited) {
            if (t_thrd.wlm_cxt.parctl_state.simple == 0)
                dywlm_client_release(&t_thrd.wlm_cxt.parctl_state);
            else
                WLMReleaseGroupActiveStatement();
            dywlm_client_max_release(&t_thrd.wlm_cxt.parctl_state);
        } else
            WLMParctlRelease(&t_thrd.wlm_cxt.parctl_state);
    }

    /*
     * Emit duration logging if appropriate.
     */
    switch (check_log_duration(msec_str, false)) {
        case 1:
            Assert(false);
            break;
        case 2: {
            char* mask_string = NULL;

            MASK_PASSWORD_START(mask_string, psrc->query_string);
            ereport(LOG,
                (errmsg("duration: %s ms  queryid %ld unique id %ld batch bind-execute %s%s%s: %s",
                     msec_str,
                     u_sess->debug_query_id,
                     u_sess->slow_query_cxt.slow_query.unique_sql_id,
                     *stmt_name ? stmt_name : "<unnamed>",
                     *portal_name ? "/" : "",
                     *portal_name ? portal_name : "",
                     mask_string),
                    errhidestmt(true)));
            MASK_PASSWORD_END(mask_string, psrc->query_string);
            break;
        }
        default:
            break;
    }


    if (MEMORY_TRACKING_QUERY_PEAK)
        ereport(LOG, (errmsg("execute batch execute %s, peak memory %ld(kb)", psrc->query_string,
                             (int64)(t_thrd.utils_cxt.peakedBytesInQueryLifeCycle/1024))));

    if (save_log_statement_stats) {
        ShowUsage("BATCH BIND MESSAGE STATISTICS");
    }

    t_thrd.postgres_cxt.debug_query_string = NULL;
    t_thrd.postgres_cxt.cur_command_tag = T_Invalid;
}

/* lock function for  g_instance.codegen_IRload_process_count Addition */
void lock_codegen_process_add()
{
    AutoMutexLock copyLock(&nodeDefCopyLock);
    copyLock.lock();
    g_instance.codegen_IRload_process_count++;
    copyLock.unLock();
}

/* lock function for  g_instance.codegen_IRload_process_count Subtraction */
void lock_codegen_process_sub(int count)
{
    if (count == 0) {
        return;
    }
    AutoMutexLock copyLock(&nodeDefCopyLock);
    copyLock.lock();
    g_instance.codegen_IRload_process_count = g_instance.codegen_IRload_process_count - count;
    copyLock.unLock();
}

/*
 * @Description: get the om online state, expansion/node replace or other.
 * @return : the state of current om online operator.
 */
OM_ONLINE_STATE get_om_online_state()
{
    char* gauss_home = NULL;
    char om_action_online_state_file[MAXPGPATH] = {0};
    const int max_file_path = 4096;
    char om_state_file_path[max_file_path] = {0};
    errno_t ret = EOK;

    /* Get the GAUSSHOME through security way. */
    gauss_home = getGaussHome();

    ret =
        snprintf_s(om_action_online_state_file, MAXPGPATH, MAXPGPATH - 1, "%s/bin/om_action_online.state", gauss_home);
    securec_check_ss(ret, "\0", "\0");

    /*
     * Currently if om_action_online_state_file is exists, it's online expansion.
     * If we support other online om operations later, here can be expanded.
     */

    realpath(om_action_online_state_file, om_state_file_path);

    FILE* fp = fopen(om_state_file_path, "r");
    if (fp != NULL) {
        fclose(fp);
        return OM_ONLINE_EXPANSION;
    } else {
        return OM_ONLINE_NODE_REPLACE;
    }

}

/* 
 * check whether sql_compatibility is valid
 */
bool checkCompArgs(const char *compFormat)
{
    /* make sure input is not null */
    if (compFormat == NULL) {
        return false;
    }
    if (pg_strncasecmp(compFormat, g_dbCompatArray[DB_CMPT_A].name, sizeof(g_dbCompatArray[DB_CMPT_A].name)) != 0 &&
        pg_strncasecmp(compFormat, g_dbCompatArray[DB_CMPT_B].name, sizeof(g_dbCompatArray[DB_CMPT_B].name)) != 0 &&
        pg_strncasecmp(compFormat, g_dbCompatArray[DB_CMPT_C].name, sizeof(g_dbCompatArray[DB_CMPT_C].name)) != 0 &&
        pg_strncasecmp(compFormat, g_dbCompatArray[DB_CMPT_PG].name, sizeof(g_dbCompatArray[DB_CMPT_PG].name)) != 0) {
        return false;
    }
    return true;
}

void ResetInterruptCxt()
{
    t_thrd.int_cxt.ignoreBackendSignal = false;

    t_thrd.int_cxt.InterruptHoldoffCount = 0;
    t_thrd.int_cxt.QueryCancelHoldoffCount = 0;

    t_thrd.int_cxt.InterruptCountResetFlag = true;

    t_thrd.int_cxt.CritSectionCount = 0;

}
