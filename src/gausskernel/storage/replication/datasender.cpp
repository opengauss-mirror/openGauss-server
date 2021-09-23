/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2011, PostgreSQL Global Development Group
 *
 *
 * datasender.cpp
 *
 * The DATA sender process (datasender) is new as of Postgres 9.0. It takes
 * care of sending XLOG from the primary server to a single recipient.
 * (Note that there can be more than one datasender process concurrently.)
 * It is started by the postmaster when the walreceiver of a standby server
 * connects to the primary server and requests XLOG streaming replication.
 * It attempts to keep reading XLOG records from the disk and sending them
 * to the standby server, as long as the connection is alive (i.e., like
 * any backend, there is a one-to-one relationship between a connection
 * and a datasender process).
 *
 * Normal termination is by SIGTERM, which instructs the datasender to
 * close the connection and exit(0) at next convenient moment. Emergency
 * termination is by SIGQUIT; like any backend, the datasender will simply
 * abort and exit on SIGQUIT. A close of the connection and a FATAL error
 * are treated as not a crash but approximately normal termination;
 * the datasender will exit quickly without sending any more XLOG records.
 *
 * If the server is shut down, postmaster sends us SIGUSR2 after all
 * regular backends have exited and the shutdown checkpoint has been written.
 * This instruct datasender to send any outstanding WAL, including the
 * shutdown checkpoint record, and then exit.
 *
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/datasender.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#ifndef WIN32
#include <syscall.h>
#endif

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/replnodes.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "replication/dataprotocol.h"
#include "replication/dataqueue.h"
#include "replication/datareceiver.h"
#include "replication/datasender.h"
#include "replication/datasender_private.h"
#include "replication/datasyncrep.h"
#include "replication/catchup.h"
#include "replication/walsender.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "gssignal/gs_signal.h"
#include "gs_bbox.h"

/* Flag indicates dummy are searching bcm files now */
static bool dummySearching;

GlobalIncrementalBcmDefinition g_incrementalBcmInfo;

/* Flags set by signal handlers for later service in main loop */
volatile uint32 send_dummy_count = 0;

#define NAPTIME_PER_CYCLE 100 /* max sleep time between cycles (100ms) */

/* Signal handlers */
static void DataSndSigHupHandler(SIGNAL_ARGS);
static void DataSndShutdownHandler(SIGNAL_ARGS);
static void DataSndQuickDieHandler(SIGNAL_ARGS);
static void DataSndXLogSendHandler(SIGNAL_ARGS);
static void DataSndLastCycleHandler(SIGNAL_ARGS);

/* Prototypes for private functions */
static bool HandleDataReplicationCommand(const char *cmd_string);
static int DataSndLoop(void);
static void InitDataSnd(void);
static void DataSndHandshake(void);
static void DataSndKill(int code, Datum arg);
static void DataSndShutdown(void) __attribute__((noreturn));
static void DataSend(bool *caughtup);

static void IdentifySystem(void);
static void StartDataReplication(StartDataReplicationCmd *cmd);
static void DataSndNotifyCatchup(void);

static void ProcessStandbyMessage(void);
static void ProcessStandbyReplyMessage(void);
static void ProcessRepliesIfAny(void);
static void ProcessStandbySearchMessage(void);

static void DataSndKeepalive(bool requestReply);
static void DataSndKeepaliveIfNecessary(TimestampTz now);
static void DataSndRmData(bool requestReply);

static long DataSndComputeSleeptime(TimestampTz now);
static void DataSndCheckTimeOut(TimestampTz now);
static uint32 DataSendReadData(char *buf, uint32 bufsize);
static bool DataSndCaughtup(void);
static const char *DataSndGetStateString(DataSndState state);

/* Main entry point for datasender process */
int DataSenderMain(void)
{
    MemoryContext datasnd_context;

    t_thrd.proc_cxt.MyProgName = "DataSender";

    /* Create a per-datasender data structure in shared memory */
    InitDataSnd();
    catchupDone = false;

    ereport(LOG, (errmsg("datasender thread started")));
    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     *
     * XXX: we don't actually attempt error recovery in datasender, we just
     * close the connection and exit.
     */
    datasnd_context = AllocSetContextCreate(t_thrd.top_mem_cxt, "Data Sender", ALLOCSET_DEFAULT_MINSIZE,
                                            ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContextSwitchTo(datasnd_context);

    /* Set up resource owner */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL,
        "datasender top-level resource owner", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /*
     * Let postmaster know that we're streaming. Once we've declared us as a
     * Data sender process, postmaster will let us outlive the openGauss and
     * kill us last in the shutdown sequence, so we get a chance to stream all
     * remaining data at shutdown.
     */
    MarkPostmasterChildDataSender();
    SendPostmasterSignal(PMSIGNAL_ADVANCE_STATE_MACHINE);

    /* Unblock signals (they were blocked when the postmaster forked us) */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    if (dummyStandbyMode) {
        ShutdownDataRcv();
        t_thrd.xlog_cxt.ThisTimeLineID = GetRecoveryTargetTLI();
        ereport(LOG, (errmsg("ThisTimeLineID: %u", t_thrd.xlog_cxt.ThisTimeLineID)));
    }

    /* Tell the standby that datasender is ready for receiving commands */
    ReadyForQuery_noblock(DestRemote, u_sess->attr.attr_storage.wal_sender_timeout);

    /* Handle handshake messages before streaming */
    DataSndHandshake();

    /* Initialize shared memory status */
    {
        /* use volatile pointer to prevent code rearrangement */
        volatile DataSnd *datasnd = t_thrd.datasender_cxt.MyDataSnd;

        SpinLockAcquire(&datasnd->mutex);
        datasnd->pid = t_thrd.proc_cxt.MyProcPid;
#ifndef WIN32
        datasnd->lwpId = syscall(SYS_gettid);
#else
        datasnd->lwpId = (int)t_thrd.proc_cxt.MyProcPid;
#endif
        SpinLockRelease(&datasnd->mutex);

        if (datasnd->sendRole == SNDROLE_PRIMARY_DUMMYSTANDBY) {
            pgstat_report_appname("DataSender to Secondary");
        } else if (datasnd->sendRole == SNDROLE_PRIMARY_BUILDSTANDBY) {
            pgstat_report_appname("DataSender to Build");
        } else if (datasnd->sendRole == SNDROLE_PRIMARY_STANDBY) {
            pgstat_report_appname("DataSender to Standby");
        }
    }

    /* init the dummy standby data num to write */
    if (dummyStandbyMode) {
        /* Init */
        dummySearching = false;
        InitDummyDataNum();
    }

    /* Main loop of datasender */
    return DataSndLoop();
}

/*
 * Execute commands from datareceiver, until we enter streaming mode.
 */
static void DataSndHandshake(void)
{
    StringInfoData input_message;
    bool replication_started = false;
    int sleep_time = 0;

    initStringInfo(&input_message);

    while (!replication_started) {
        int first_char;

        DataSndSetState(DATASNDSTATE_STARTUP);
        set_ps_display("idle", false);

        /* Wait for some data to arrive */
        if (!pq_select(NAPTIME_PER_CYCLE)) {
            sleep_time += NAPTIME_PER_CYCLE;

            /*
             * not yet data available without blocking,
             * check if it is under maximum timeout
             * period
             */
            if (u_sess->attr.attr_storage.wal_sender_timeout > 0 &&
                sleep_time >= u_sess->attr.attr_storage.wal_sender_timeout) {
                ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                    errmsg("no message received from standby for maximum time")));
                proc_exit(0);
            }
            continue;
        }

        sleep_time = 0;

        /*
         * Since select has indicated that data is available to read,
         * then we can call blocking function itself, as there must be
         * some data to get.
         */
        first_char = pq_getbyte();

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if (!PostmasterIsAlive()) {
            gs_thread_exit(1);
        }

        /*
         * Check for any other interesting events that happened while we
         * slept.
         */
        if (t_thrd.datasender_cxt.got_SIGHUP) {
            t_thrd.datasender_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (first_char != EOF) {
            /*
             * Read the message contents. This is expected to be done without
             * blocking because we've been able to get message type code.
             */
            if (pq_getmessage(&input_message, 0)) {
                first_char = EOF; /* suitable message already logged */
            }
        }

        /* Handle the very limited subset of commands expected in this phase */
        switch (first_char) {
            case 'Q': /* Query message */
            {
                const char *query_string = NULL;

                query_string = pq_getmsgstring(&input_message);
                pq_getmsgend(&input_message);

                if (HandleDataReplicationCommand(query_string)) {
                    replication_started = true;
                }
            } break;

            case 'X':
                /* standby is closing the connection */
                proc_exit(0);
                break;
            case EOF:
                /* standby disconnected unexpectedly */
                ereport(COMMERROR,
                        (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("unexpected EOF on standby connection")));
                proc_exit(0);
                break;
            default:
                ereport(FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                errmsg("invalid standby handshake message type %d", first_char)));
        }
    }
}

/*
 * IDENTIFY_SYSTEM
 */
static void IdentifySystem(void)
{
    StringInfoData buf;
    char sysid[32];
    char tli[11];
    int rc = EOK;

    /*
     * Reply with a result set with one row, three columns. First col is
     * system ID, second is timeline ID.
     */
    rc = snprintf_s(sysid, sizeof(sysid), sizeof(sysid) - 1, UINT64_FORMAT, GetSystemIdentifier());
    securec_check_ss(rc, "", "");
    rc = snprintf_s(tli, sizeof(tli), sizeof(tli) - 1, "%u", t_thrd.xlog_cxt.ThisTimeLineID);
    securec_check_ss(rc, "", "");

    /* Send a RowDescription message */
    pq_beginmessage(&buf, 'T');
    pq_sendint16(&buf, 2); /* 2 fields */

    /* first field */
    pq_sendstring(&buf, "systemid"); /* col name */
    pq_sendint32(&buf, 0);           /* table oid */
    pq_sendint16(&buf, 0);           /* attnum */
    pq_sendint32(&buf, TEXTOID);     /* type oid */
    pq_sendint16(&buf, UINT16_MAX);  /* typlen */
    pq_sendint32(&buf, 0);           /* typmod */
    pq_sendint16(&buf, 0);           /* format code */

    /* second field */
    pq_sendstring(&buf, "timeline"); /* col name */
    pq_sendint32(&buf, 0);           /* table oid */
    pq_sendint16(&buf, 0);           /* attnum */
    pq_sendint32(&buf, INT4OID);     /* type oid */
    pq_sendint16(&buf, 4);           /* typlen */
    pq_sendint32(&buf, 0);           /* typmod */
    pq_sendint16(&buf, 0);           /* format code */
    pq_endmessage_noblock(&buf);

    /* Send a DataRow message */
    pq_beginmessage(&buf, 'D');
    pq_sendint16(&buf, 2);             /* # of columns */
    pq_sendint32(&buf, strlen(sysid)); /* col1 len */
    pq_sendbytes(&buf, (char *)sysid, strlen(sysid));
    pq_sendint32(&buf, strlen(tli)); /* col2 len */
    pq_sendbytes(&buf, (char *)tli, strlen(tli));
    pq_endmessage_noblock(&buf);

    /* Send CommandComplete and ReadyForQuery messages */
    EndCommand_noblock("SELECT", DestRemote);
    ReadyForQuery_noblock(DestRemote, u_sess->attr.attr_storage.wal_sender_timeout);
    /* ReadyForQuery did pq_flush_if_available for us */
}

/*
 * START_REPLICATION(DATA)
 */
static void StartDataReplication(StartDataReplicationCmd *cmd)
{
    StringInfoData buf;

    /*
     * When we first start replication the standby will be behind the primary.
     * For some applications, for example, synchronous replication, it is
     * important to have a clear state for this initial catchup mode, so we
     * can trigger actions when we change streaming state later. We may stay
     * in this state for a long time, which is exactly why we want to be able
     * to monitor whether or not we are still here.
     */
    DataSndSetState(DATASNDSTATE_CATCHUP);

    /* Tell postmaster to start the catchup process. */
    if (!AmDataSenderToDummyStandby() && !dummyStandbyMode) {
        /* update the newest status before read catchup_online */
        pg_memory_barrier();
        if (catchup_online)
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("catchup thread is online, wait it shutdown")));

        SendPostmasterSignal(PMSIGNAL_START_CATCHUP);
    }

    /* Send a CopyBothResponse message, and start streaming */
    pq_beginmessage(&buf, 'W');
    pq_sendbyte(&buf, 0);
    pq_sendint16(&buf, 0);
    pq_endmessage_noblock(&buf);
    pq_flush_timedwait(u_sess->attr.attr_storage.wal_sender_timeout);
}

/*
 * This function is used to send notify message to dummystandby to search incremental files which used for catchup.
 */
static void DataSndNotifyCatchup()
{
    NotifyDummyCatchupMessage dummyCatchupMessage;
    errno_t errorno = EOK;

    dummyCatchupMessage.sendTime = GetCurrentTimestamp();
    ereport(LOG, (errmsg("primary catchup process notify dummy to start searching incremental files.")));

    /* Prepend with the message type and send it. */
    t_thrd.datasender_cxt.output_message[0] = 'b';
    errorno = memcpy_s(t_thrd.datasender_cxt.output_message + 1,
                       sizeof(DataPageMessageHeader) + g_instance.attr.attr_storage.MaxSendSize * 1024,
                       &dummyCatchupMessage, sizeof(NotifyDummyCatchupMessage));
    securec_check(errorno, "", "");
    (void)pq_putmessage_noblock('d', t_thrd.datasender_cxt.output_message, sizeof(NotifyDummyCatchupMessage) + 1);
}

/*
 * Execute an incoming replication command.
 */
static bool HandleDataReplicationCommand(const char *cmd_string)
{
    bool replication_started = false;
    int parse_rc;
    Node *cmd_node = NULL;
    MemoryContext cmd_context;
    MemoryContext old_context;
    replication_scanner_yyscan_t yyscanner = NULL;

    ereport(LOG, (errmsg("received data replication command: %s", cmd_string)));

    cmd_context = AllocSetContextCreate(CurrentMemoryContext, "Replication command context", ALLOCSET_DEFAULT_MINSIZE,
                                        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    yyscanner = replication_scanner_init(cmd_string);
    parse_rc = replication_yyparse(yyscanner);
    replication_scanner_finish(yyscanner);

    if (parse_rc != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR), (errmsg_internal("replication command parser returned %d", parse_rc))));
    }

    old_context = MemoryContextSwitchTo(cmd_context);

    cmd_node = t_thrd.replgram_cxt.replication_parse_result;

    switch (cmd_node->type) {
        case T_IdentifySystemCmd:
            IdentifySystem();
            break;

        case T_IdentifyModeCmd:
            IdentifyMode();
            break;

        case T_StartDataReplicationCmd:
            StartDataReplication((StartDataReplicationCmd *)cmd_node);
            /* break out of the loop */
            replication_started = true;
            break;

        default:
            ereport(FATAL,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid standby query string: %s", cmd_string)));
    }

    /* done */
    MemoryContextSwitchTo(old_context);
    MemoryContextDelete(cmd_context);

    return replication_started;
}

/*
 * Check if the remote end has closed the connection.
 */
static void ProcessRepliesIfAny(void)
{
    unsigned char firstchar;
    int r;
    bool received = false;

    for (;;) {
        r = pq_getbyte_if_available(&firstchar);
        if (r < 0) {
            /* unexpected error or EOF */
            ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("unexpected EOF on standby connection")));
            proc_exit(0);
        }
        if (r == 0) {
            /* no data available without blocking */
            break;
        }

        /* Handle the very limited subset of commands expected in this phase */
        switch (firstchar) {
                /*
                 * 'd' means a standby reply wrapped in a CopyData packet.
                 */
            case 'd':
                ProcessStandbyMessage();
                received = true;
                break;

                /*
                 * 'X' means that the standby is closing down the socket.
                 */
            case 'X':
                proc_exit(0);
                break;
            default:
                ereport(FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                errmsg("invalid standby message type \"%c\"", firstchar)));
        }
    }

    /*
     * Save the last reply timestamp if we've received at least one reply.
     */
    if (received) {
        t_thrd.datasender_cxt.last_reply_timestamp = GetCurrentTimestamp();
        t_thrd.datasender_cxt.ping_sent = false;
    }
}

void InitGlobalBcm(void)
{
    g_incrementalBcmInfo.receivedFileList = NULL;
    g_incrementalBcmInfo.msgLength = 0;
    SpinLockInit(&g_incrementalBcmInfo.mutex);
}

bool DataSndInSearching(void)
{
    return dummySearching;
}

void ReplaceOrFreeBcmFileListBuffer(char *fileList, int msgLength)
{
    char *oldFileList = NULL;
    SpinLockAcquire(&g_incrementalBcmInfo.mutex);
    oldFileList = g_incrementalBcmInfo.receivedFileList;
    g_incrementalBcmInfo.msgLength = (fileList != NULL) ? msgLength : 0;
    g_incrementalBcmInfo.receivedFileList = fileList;
    SpinLockRelease(&g_incrementalBcmInfo.mutex);
    pfree_ext(oldFileList);
}

static void ProcessStandbySearchMessage(void)
{
    char *fileList = NULL;
    int msgLength = 0;

    if (catchupState != CATCHUP_SEARCHING) {
        dummySearching = false;
        return;
    }
    /* No data need to be sent, only 'x' symbol */
    if (t_thrd.datasender_cxt.reply_message->len == 1) {
        catchupState = RECEIVED_NONE;
        pg_memory_barrier();
        dummySearching = false;
        return;
    }

    msgLength = t_thrd.datasender_cxt.reply_message->len - 1;
    fileList = (char *)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), msgLength);
    pq_copymsgbytes(t_thrd.datasender_cxt.reply_message, fileList, t_thrd.datasender_cxt.reply_message->len - 1);
    ReplaceOrFreeBcmFileListBuffer(fileList, msgLength);
    catchupState = RECEIVED_OK;
    pg_memory_barrier();
    dummySearching = false;
}

/*
 * Process a status update message received from standby.
 */
static void ProcessStandbyMessage(void)
{
    char msgtype;

    resetStringInfo(t_thrd.datasender_cxt.reply_message);

    /*
     * Read the message contents.
     */
    if (pq_getmessage(t_thrd.datasender_cxt.reply_message, 0)) {
        ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("unexpected EOF on standby connection")));
        proc_exit(0);
    }

    /*
     * Check message type from the first byte.
     */
    msgtype = pq_getmsgbyte(t_thrd.datasender_cxt.reply_message);

    switch (msgtype) {
        case 'r':
            ProcessStandbyReplyMessage();
            break;
        case 'x':
            ereport(LOG, (errmsg("receive file list message from dummy_standby")));
            ProcessStandbySearchMessage();
            break;
        default:
            ereport(COMMERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("unexpected message type \"%c\"", msgtype)));
            proc_exit(0);
    }
}

/*
 * Regular reply from standby advising of received data info on standby server.
 */
static void ProcessStandbyReplyMessage(void)
{
    volatile DataSnd *datasnd = t_thrd.datasender_cxt.MyDataSnd;
    StandbyDataReplyMessage reply;
    bool log_receivePosition = false;

    pq_copymsgbytes(t_thrd.datasender_cxt.reply_message, (char *)&reply, sizeof(StandbyDataReplyMessage));

    ereport(DEBUG2,
            (errmsg("receive queue position %u/%u", reply.receivePosition.queueid, reply.receivePosition.queueoff)));

    /* send a reply if the standby requested one */
    if (reply.replyRequested)
        DataSndKeepalive(false);

    if (DataQueuePtrIsInvalid(reply.receivePosition))
        return;

    /*
     * For secondery sender, the receiver position maybe more than
     * sendposition. Explame for the scene:
     * The secondery sender send 100, and receiver receive 100, the head2
     * is 50, standby reconnect to primary, the secondery sender set
     * sendPosition to 0, standby disconnect to primary, the secondery sender
     * send from 50, maybe it will receive receivePosition 100.
     */
    SpinLockAcquire(&datasnd->mutex);
    if (DQByteLE(reply.receivePosition, datasnd->sendPosition)) {
        datasnd->receivePosition.queueid = reply.receivePosition.queueid;
        datasnd->receivePosition.queueoff = reply.receivePosition.queueoff;
    } else {
        log_receivePosition = true;
    }
    SpinLockRelease(&datasnd->mutex);

    if (log_receivePosition)
        ereport(LOG,
                (errmsg("receive position more than send position: receivePosition[%u/%u],"
                        "sendPosition[%u/%u], use_head2[%u/%u]",
                        reply.receivePosition.queueid, reply.receivePosition.queueoff, datasnd->sendPosition.queueid,
                        datasnd->sendPosition.queueoff, t_thrd.dataqueue_cxt.DataSenderQueue->use_head2.queueid,
                        t_thrd.dataqueue_cxt.DataSenderQueue->use_head2.queueoff)));

    if (datasnd->sending)
        DataSyncRepReleaseWaiters();
}

/*
 * This function is used to send end data message to  standby.
 * If requestReply is set, sets a flag in the message requesting the standby
 * to send a message back to us, for heartbeat purposes.
 */
static void DataSndSyncStandbyDone(bool requestReply)
{
    EndDataMessage endDataMessage;
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    errno_t errorno = EOK;

    /* Construct a new message */
    SpinLockAcquire(&hashmdata->mutex);
    endDataMessage.peer_role = hashmdata->current_mode;
    SpinLockRelease(&hashmdata->mutex);
    endDataMessage.peer_state = get_local_dbstate();
    endDataMessage.sendTime = GetCurrentTimestamp();
    endDataMessage.percent = SYNC_DUMMY_STANDBY_END;

    ereport(dummyStandbyMode ? LOG : DEBUG2, (errmsg("sending standby end data message")));

    /* Prepend with the message type and send it. */
    t_thrd.datasender_cxt.output_message[0] = 'e';
    errorno = memcpy_s(t_thrd.datasender_cxt.output_message + 1,
                       sizeof(DataPageMessageHeader) + g_instance.attr.attr_storage.MaxSendSize * 1024, &endDataMessage,
                       sizeof(EndDataMessage));
    securec_check(errorno, "", "");
    (void)pq_putmessage_noblock('d', t_thrd.datasender_cxt.output_message, sizeof(EndDataMessage) + 1);
}

/* Main loop of datasender process */
static int DataSndLoop(void)
{
    bool caughtup = false;
    bool first_startup = true;
    bool rm_dummy_data_log = true;
    bool marked_stream_replication = true;
    TimestampTz last_send_catchup_timestamp;

    /*
     * Allocate buffer that will be used for each output message.  We do this
     * just once to reduce palloc overhead.  The buffer must be made large
     * enough for maximum-sized messages.
     */
    t_thrd.datasender_cxt.output_message =
        (char *)palloc(1 + sizeof(DataPageMessageHeader) + g_instance.attr.attr_storage.MaxSendSize * 1024);

    if (BBOX_BLACKLIST_DATA_MESSAGE_SEND) {
        bbox_blacklist_add(DATA_MESSAGE_SEND, t_thrd.datasender_cxt.output_message,
                           (uint64)(1 + sizeof(DataPageMessageHeader) +
                                    g_instance.attr.attr_storage.MaxSendSize * 1024));
    }

    /*
     * Allocate buffer that will be used for processing reply messages.  As
     * above, do this just once to reduce palloc overhead.
     */
    initStringInfo(t_thrd.datasender_cxt.reply_message);
    last_send_catchup_timestamp = GetCurrentTimestamp();

    /* Initialize the last reply timestamp */
    t_thrd.datasender_cxt.last_reply_timestamp = GetCurrentTimestamp();
    t_thrd.datasender_cxt.ping_sent = false;

    /* Loop forever, unless we get an error */
    for (;;) {
        TimestampTz now;

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.datasender_cxt.MyDataSnd->latch);

        pgstat_report_activity(STATE_RUNNING, NULL);

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if (!PostmasterIsAlive())
            gs_thread_exit(1);

        /* Process any requests or signals received recently */
        if (t_thrd.datasender_cxt.got_SIGHUP) {
            t_thrd.datasender_cxt.got_SIGHUP = false;
            marked_stream_replication = u_sess->attr.attr_storage.enable_stream_replication;
            ProcessConfigFile(PGC_SIGHUP);
        }

        /* Normal exit from the datasender is here */
        if (t_thrd.datasender_cxt.datasender_shutdown_requested) {
            /* Inform the standby that DATA streaming is done */
            pq_puttextmessage('C', "COPY 0");
            (void)pq_flush();

            ereport(LOG, (errmsg("data replication caughtup, ready to stop.")));

            proc_exit(0);
        }

        /* If changed to stream replication, request for catchup. */
        if (u_sess->attr.attr_storage.enable_stream_replication && !marked_stream_replication) {
            marked_stream_replication = u_sess->attr.attr_storage.enable_stream_replication;
            DataSndSetState(DATASNDSTATE_CATCHUP);
            if (!AmDataSenderToDummyStandby() && !dummyStandbyMode) {
                pg_memory_barrier();

                if (catchup_online)
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_STATUS), errmsg("catchup thread is online, wait it shutdown")));

                SendPostmasterSignal(PMSIGNAL_START_CATCHUP);
            }
        }

        /* Check for input from the client */
        ProcessRepliesIfAny();

        if (AmDataSenderToDummyStandby()) {
            /*
             * During the loop, if we got signal from sender which corresponding standby receiver,
             * We notify Dummy_standby to scan incremental files which used for catchup immediately.
             */
            if (catchupState == CATCHUP_STARTING) {
                dummySearching = true;
                pg_memory_barrier();
                catchupState = CATCHUP_SEARCHING;
                ereport(LOG, (errmsg("Send message to dummy to start searching incremental bcm file list.")));
                DataSndNotifyCatchup();
            }

            /*
             * If I am sender to Dummy standby and standby caught up primary,
             * do not need to send data to xlog Dummy standby;
             */
            if (DataSndInProgress(SNDROLE_PRIMARY_STANDBY)) {
                caughtup = true;

                SpinLockAcquire(&t_thrd.datasender_cxt.MyDataSnd->mutex);
                t_thrd.datasender_cxt.MyDataSnd->sending = false;
                t_thrd.datasender_cxt.MyDataSnd->sendPosition.queueid = 0;
                t_thrd.datasender_cxt.MyDataSnd->sendPosition.queueoff = 0;
                SpinLockRelease(&t_thrd.datasender_cxt.MyDataSnd->mutex);

                if (DataSndCaughtup()) {
                    /* Send rm_data command to Dummy Standby, false means not need response. */
                    DataSndRmData(false);

                    /*
                     * The primary send dummystandby rm data message at the first time, we will
                     * print a log.
                     */
                    if (rm_dummy_data_log) {
                        ereport(LOG, (errmsg("sending dummystandby rm data message.")));
                        rm_dummy_data_log = false;
                    }
                }
            } else {
                SpinLockAcquire(&t_thrd.datasender_cxt.MyDataSnd->mutex);
                t_thrd.datasender_cxt.MyDataSnd->sending = true;
                SpinLockRelease(&t_thrd.datasender_cxt.MyDataSnd->mutex);

                if (!pq_is_send_pending()) {
                    DataSend(&caughtup);
                    send_dummy_count++;
                    pg_memory_barrier();
                    rm_dummy_data_log = true;
                } else
                    caughtup = false;
            }
        } else {
            /*
             * If we don't have any pending data in the output buffer, try to send
             * some more.  If there is some, we don't bother to call XLogSend
             * again until we've flushed it ... but we'd better assume we are not
             * caught up.
             */
            if (!pq_is_send_pending())
                DataSend(&caughtup);
            else
                caughtup = false;

            if (caughtup && dummyStandbyMode) {
                ereport(LOG, (errmsg("standby \"%s\" has now caught up with dummystandby",
                                     u_sess->attr.attr_common.application_name)));

                if (!pq_is_send_pending()) {
                    DataSndSyncStandbyDone(false);
                    (void)pq_flush();
                    ereport(LOG, (errmsg("dummystandby data replication caughtup, ready to stop")));
                } else
                    ereport(DEBUG5, (errmsg("standby \"%s\" has now caught up with dummystandby, but pend on sending",
                                            u_sess->attr.attr_common.application_name)));
            }

            if (!dummyStandbyMode && !catchupDone && !catchup_online) {
                TimestampTz current;
                current = GetCurrentTimestamp();
                if (TimestampDifferenceExceeds(last_send_catchup_timestamp, current, 1000)) {
                    ereport(LOG, (errmsg("catchup thread create failed, try again")));
                    SendPostmasterSignal(PMSIGNAL_START_CATCHUP);
                    last_send_catchup_timestamp = current;
                }
            }
        }

        /* Try to flush pending output to the client */
        if (pq_flush_if_writable() != 0)
            break;

        /* If nothing remains to be sent right now ... */
        if (caughtup && !pq_is_send_pending() && !catchup_online) {
            /*
             * If we're in catchup state, move to streaming.  This is an
             * important state change for users to know about, since before
             * this point data loss might occur if the primary dies and we
             * need to failover to the standby. The state change is also
             * important for synchronous replication, since commits that
             * started to wait at that point might wait for some time.
             */
            if (t_thrd.datasender_cxt.MyDataSnd->state == DATASNDSTATE_CATCHUP) {
                ereport(DEBUG1, (errmsg("standby \"%s\" has now caught up with primary",
                                        u_sess->attr.attr_common.application_name)));

                /* sender to standby on primary */
                if (t_thrd.datasender_cxt.MyDataSnd->sendRole == SNDROLE_PRIMARY_STANDBY) {
                    /*
                     * When standby connect to primary, primary will create
                     * catchup thread to sync data, after catchup thread exit,
                     * and caughtup is true , datasnd will change its state.
                     * But when caughtup is true, the catchup thread is creating,
                     * we can't change it.
                     */
                    if (catchupDone)
                        DataSndSetState(DATASNDSTATE_STREAMING);
                } else
                    DataSndSetState(DATASNDSTATE_STREAMING);
                /* Refresh new state to peer */
                DataSndKeepalive(true);
            }

            /*
             * When SIGUSR2 arrives, we send any outstanding logs up to the
             * shutdown checkpoint record (i.e., the latest record) and exit.
             * This may be a normal termination at shutdown, or a promotion,
             * the walsender is not sure which.
             */
            if (t_thrd.datasender_cxt.datasender_ready_to_stop) {
                /*
                 * Let's just be real sure we're caught up. For dummy sender,
                 * during shutting down, if the sender to standby is in progress,
                 * skip to send outstanding logs.
                 */
                if (AmDataSenderToDummyStandby() && DataSndInProgress(SNDROLE_PRIMARY_STANDBY))
                    ; /* nothing to do */
                else
                    DataSend(&caughtup);
                if (caughtup && !pq_is_send_pending()) {
                    t_thrd.datasender_cxt.datasender_shutdown_requested = true;
                }
            }
        }

        now = GetCurrentTimestamp();

        /* Check for replication timeout. */
        DataSndCheckTimeOut(now);

        /* Send keepalive if the time has come. */
        DataSndKeepaliveIfNecessary(now);

        /*
         * We don't block if not caught up, unless there is unsent data
         * pending in which case we'd better block until the socket is
         * write-ready.  This test is only needed for the case where XLogSend
         * loaded a subset of the available data but then pq_flush_if_writable
         * flushed it all --- we should immediately try to send more.
         */
        if (caughtup || pq_is_send_pending()) {
            long sleeptime = 10000; /* 10 s */
            int wakeEvents;

            wakeEvents = WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_READABLE | WL_TIMEOUT;

            sleeptime = DataSndComputeSleeptime(now);

            if (pq_is_send_pending())
                wakeEvents |= WL_SOCKET_WRITEABLE;
            else if (first_startup) {
                /* Datasender first startup, send a keepalive to standby, no need reply. */
                DataSndKeepalive(false);
                first_startup = false;
            }

            /* Sleep until something happens or we time out */
            pgstat_report_activity(STATE_IDLE, NULL);
            t_thrd.int_cxt.ImmediateInterruptOK = true;
            CHECK_FOR_INTERRUPTS();
            WaitLatchOrSocket(&t_thrd.datasender_cxt.MyDataSnd->latch, wakeEvents, u_sess->proc_cxt.MyProcPort->sock,
                              sleeptime);
            t_thrd.int_cxt.ImmediateInterruptOK = false;
        }
    }

    DataSndShutdown();
    return 1; /* keep the compiler quiet */
}

/*
 * Compute how long send/receive loops should sleep.
 *
 * If wal_sender_timeout is enabled we want to wake up in time to send
 * keepalives and to abort the connection if wal_sender_timeout has been
 * reached.
 */
static long DataSndComputeSleeptime(TimestampTz now)
{
    long sleeptime = 10000; /* 10 s */

    if (u_sess->attr.attr_storage.wal_sender_timeout > 0 && t_thrd.datasender_cxt.last_reply_timestamp > 0) {
        TimestampTz wakeup_time;
        long sec_to_timeout;
        int microsec_to_timeout;

        /*
         * At the latest stop sleeping once wal_sender_timeout has been
         * reached.
         */
        wakeup_time = TimestampTzPlusMilliseconds(t_thrd.datasender_cxt.last_reply_timestamp,
                                                  u_sess->attr.attr_storage.wal_sender_timeout);

        /*
         * If no ping has been sent yet, wakeup when it's time to do so.
         * DataSndKeepaliveIfNecessary() wants to send a keepalive once half of
         * the timeout passed without a response.
         */
        if (!t_thrd.datasender_cxt.ping_sent)
            wakeup_time = TimestampTzPlusMilliseconds(t_thrd.datasender_cxt.last_reply_timestamp,
                                                      u_sess->attr.attr_storage.wal_sender_timeout / 2);

        /* Compute relative time until wakeup. */
        TimestampDifference(now, wakeup_time, &sec_to_timeout, &microsec_to_timeout);

        sleeptime = sec_to_timeout * 1000 + microsec_to_timeout / 1000;
    }

    return sleeptime;
}

/*
 * Check if time since last receive from standby has reached the
 * configured limit.
 */
static void DataSndCheckTimeOut(TimestampTz now)
{
    TimestampTz timeout;

    /* don't bail out if we're doing something that doesn't require timeouts */
    if (t_thrd.datasender_cxt.last_reply_timestamp <= 0)
        return;

    timeout = TimestampTzPlusMilliseconds(t_thrd.datasender_cxt.last_reply_timestamp,
                                          u_sess->attr.attr_storage.wal_sender_timeout);
    if (u_sess->attr.attr_storage.wal_sender_timeout > 0 && now >= timeout) {
        /*
         * Since typically expiration of replication timeout means
         * communication problem, we don't send the error message to the
         * standby.
         */
        ereport(COMMERROR, (errmsg("terminating Datasender process due to replication timeout")));
        DataSndShutdown();
    }
}

/* Initialize a per-walsender data structure for this walsender process */
static void InitDataSnd(void)
{
    int i;

    /*
     * WalSndCtl should be set up already (we inherit this by fork() or
     * EXEC_BACKEND mechanism from the postmaster).
     */
    if (t_thrd.datasender_cxt.DataSndCtl == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("DataSndCtl should not be null")));
    }
    if (t_thrd.datasender_cxt.MyDataSnd != NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("MyDataSnd should be null")));
    }

    /*
     * Find a free walsender slot and reserve it. If this fails, we must be
     * out of WalSnd structures.
     */
    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile DataSnd *datasnd = &t_thrd.datasender_cxt.DataSndCtl->datasnds[i];

        SpinLockAcquire(&datasnd->mutex);

        if (datasnd->pid != 0) {
            SpinLockRelease(&datasnd->mutex);
            continue;
        } else {
            /*
             * Found a free slot. Reserve it for us.
             */
            datasnd->pid = t_thrd.proc_cxt.MyProcPid;
            datasnd->state = DATASNDSTATE_STARTUP;
            datasnd->sendKeepalive = true;
            datasnd->sending = true;

            if (dummyStandbyMode) {
                datasnd->sendRole = SNDROLE_DUMMYSTANDBY_STANDBY;
            } else if (t_thrd.postmaster_cxt.senderToDummyStandby) {
                datasnd->sendRole = SNDROLE_PRIMARY_DUMMYSTANDBY;
            } else if (t_thrd.postmaster_cxt.senderToBuildStandby) {
                datasnd->sendRole = SNDROLE_PRIMARY_BUILDSTANDBY;
            } else {
                datasnd->sendRole = SNDROLE_PRIMARY_STANDBY;
            }

            datasnd->sendPosition.queueid = 0;
            datasnd->sendPosition.queueoff = 0;
            datasnd->receivePosition.queueid = 0;
            datasnd->receivePosition.queueoff = 0;

            SpinLockRelease(&datasnd->mutex);
            /* don't need the lock anymore */
            OwnLatch((Latch *)&datasnd->latch);
            t_thrd.datasender_cxt.MyDataSnd = (DataSnd *)datasnd;
            break;
        }
    }
    if (t_thrd.datasender_cxt.MyDataSnd == NULL)
        ereport(FATAL, (errcode(ERRCODE_TOO_MANY_CONNECTIONS), errmsg("number of requested standby connections "
                                                                      "exceeds max_wal_senders (currently %d)",
                                                                      g_instance.attr.attr_storage.max_wal_senders)));

    /* Arrange to clean up at walsender exit */
    on_shmem_exit(DataSndKill, 0);
}

/* Destroy the per-datasender data structure for this datasender process */
static void DataSndKill(int code, Datum arg)
{
    DataSnd *datasnd = t_thrd.datasender_cxt.MyDataSnd;

    Assert(t_thrd.datasender_cxt.MyDataSnd != NULL);

    /* DataSnd struct isn't mine anymore */
    t_thrd.datasender_cxt.MyDataSnd = NULL;

    DisownLatch(&datasnd->latch);

    if (code > 0) {
        /* * Sleep at least 0.1 second to wait for reporting the error to the client */
        pg_usleep(100000L);
    }

    /* Mark DataSnd struct no longer in use. */
    SpinLockAcquire(&datasnd->mutex);
    datasnd->pid = 0;
    SpinLockRelease(&datasnd->mutex);
    dummySearching = false;

    if (BBOX_BLACKLIST_DATA_MESSAGE_SEND) {
        bbox_blacklist_remove(DATA_MESSAGE_SEND, t_thrd.datasender_cxt.output_message);
    }

    ereport(LOG, (errmsg("datasender thread shut down")));
}

/*
 * Handle a client's connection abort in an orderly manner.
 */
static void DataSndShutdown(void)
{
    /*
     * Reset whereToSendOutput to prevent ereport from attempting to send any
     * more messages to the standby.
     */
    if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote)
        t_thrd.postgres_cxt.whereToSendOutput = DestNone;

    proc_exit(0);
    abort(); /* keep the compiler quiet */
}

static void DataSend(bool *caughtup)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile DataSnd *datasnd = t_thrd.datasender_cxt.MyDataSnd;
    char *datasndbuf = NULL;
    DataPageMessageHeader msghdr;
    DataQueuePtr startptr;
    DataQueuePtr endptr;
    uint32 sendsize;
    errno_t rc = 0;

    /* Need interface to check if we need to send some data this time  */
    SpinLockAcquire(&datasnd->mutex);
    startptr.queueid = datasnd->sendPosition.queueid;
    startptr.queueoff = datasnd->sendPosition.queueoff;
    endptr.queueid = datasnd->sendPosition.queueid;
    endptr.queueoff = datasnd->sendPosition.queueoff;
    SpinLockRelease(&datasnd->mutex);

    t_thrd.datasender_cxt.output_message[0] = 'd';

    datasndbuf = t_thrd.datasender_cxt.output_message + 1 + sizeof(DataPageMessageHeader);

    if (AmDataSenderOnDummyStandby()) {
        sendsize = DataSendReadData(datasndbuf, g_instance.attr.attr_storage.MaxSendSize * 1024);
        ereport(DEBUG5, (errmsg("AmDataSenderOnDummyStandby is true: sendsize=%u", sendsize)));
    } else {
        sendsize = GetFromDataQueue(datasndbuf, g_instance.attr.attr_storage.MaxSendSize * 1024, startptr, endptr,
                                    false, t_thrd.dataqueue_cxt.DataSenderQueue);
        ereport(DEBUG5, (errmsg("AmDataSenderOnDummyStandby is false: sendsize=%u startpos=%u/%u endpos=%u/%u",
                                sendsize, startptr.queueid, startptr.queueoff, endptr.queueid, endptr.queueoff)));
    }

    if (!u_sess->attr.attr_storage.enable_stream_replication || (sendsize == 0)) {
        /* update current sendPosition to correction position. */
        SpinLockAcquire(&datasnd->mutex);
        datasnd->sendPosition.queueid = startptr.queueid;
        datasnd->sendPosition.queueoff = startptr.queueoff;
        SpinLockRelease(&datasnd->mutex);
        *caughtup = true;
        ereport(DEBUG5, (errmsg("caughtup is true")));
        return;
    }

    /*
     * Fill the message header last so that the send timestamp is taken as late as possible.
     */
    msghdr.dataStart = startptr;
    msghdr.dataEnd = endptr;
    msghdr.sendTime = GetCurrentTimestamp();
    msghdr.catchup = (t_thrd.datasender_cxt.MyDataSnd->state == DATASNDSTATE_CATCHUP);
    rc = memcpy_s(t_thrd.datasender_cxt.output_message + 1, sizeof(DataPageMessageHeader), &msghdr,
                  sizeof(DataPageMessageHeader));
    securec_check(rc, "", "");

    pq_putmessage_noblock('d', t_thrd.datasender_cxt.output_message, 1 + sizeof(DataPageMessageHeader) + sendsize);

    SpinLockAcquire(&datasnd->mutex);
    datasnd->sendPosition.queueid = endptr.queueid;
    datasnd->sendPosition.queueoff = endptr.queueoff;
    SpinLockRelease(&datasnd->mutex);

    Assert(sendsize > 0);
    *caughtup = false;

    if (u_sess->attr.attr_storage.HaModuleDebug) {
        ereport(LOG, (errmsg("HA-DataSend done: send data from %u/%u to %u/%u, size %u", startptr.queueid,
                             startptr.queueoff, endptr.queueid, endptr.queueoff, sendsize)));
    }

    return;
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void DataSndSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.datasender_cxt.got_SIGHUP = true;
    if (t_thrd.datasender_cxt.MyDataSnd)
        SetLatch(&t_thrd.datasender_cxt.MyDataSnd->latch);

    errno = save_errno;
}

/* SIGTERM: set flag to shut down */
static void DataSndShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.datasender_cxt.datasender_shutdown_requested = true;
    if (t_thrd.datasender_cxt.MyDataSnd)
        SetLatch(&t_thrd.datasender_cxt.MyDataSnd->latch);

    /*
     * Set the standard (non-datasender) state as well, so that we can abort
     * things like do_pg_stop_backup().
     */
    InterruptPending = true;
    t_thrd.int_cxt.ProcDiePending = true;

    errno = save_errno;
}

/*
 * DataSndQuickDieHandler() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */
static void DataSndQuickDieHandler(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

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

/* SIGUSR1: set flag to send WAL records */
static void DataSndXLogSendHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    latch_sigusr1_handler();

    errno = save_errno;
}

/* SIGUSR2: set flag to do a last cycle and shut down afterwards */
static void DataSndLastCycleHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.datasender_cxt.datasender_ready_to_stop = true;
    if (t_thrd.datasender_cxt.MyDataSnd)
        SetLatch(&t_thrd.datasender_cxt.MyDataSnd->latch);

    errno = save_errno;
}

/* Set up signal handlers */
void DataSndSignals(void)
{
    /* Set up signal handlers */
    (void)gspqsignal(SIGHUP, DataSndSigHupHandler);    /* set flag to read config
                                                     * file */
    (void)gspqsignal(SIGINT, SIG_IGN);                 /* not used */
    (void)gspqsignal(SIGTERM, DataSndShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, DataSndQuickDieHandler); /* hard crash time */
    (void)gspqsignal(SIGALRM, handle_sig_alarm);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, DataSndXLogSendHandler);  /* request DATA sending */
    (void)gspqsignal(SIGUSR2, DataSndLastCycleHandler); /* request a last cycle and shutdown */

    /* Reset some signals that are accepted by postmaster but not here */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);
}

/* Report shared-memory space needed by DataSndShmemInit */
Size DataSndShmemSize(void)
{
    Size size = 0;

    size = offsetof(DataSndCtlData, datasnds);
    size = add_size(size, mul_size(g_instance.attr.attr_storage.max_wal_senders, sizeof(DataSnd)));

    return size;
}

/* Allocate and initialize datasender-related shared memory */
void DataSndShmemInit(void)
{
    bool found = false;
    int i;
    errno_t rc = 0;

    t_thrd.datasender_cxt.DataSndCtl = (DataSndCtlData *)ShmemInitStruct("Data Sender Ctl", DataSndShmemSize(), &found);

    if (!found) {
        /* First time through, so initialize */
        rc = memset_s(t_thrd.datasender_cxt.DataSndCtl, DataSndShmemSize(), 0, DataSndShmemSize());
        securec_check(rc, "", "");
        SHMQueueInit(&(t_thrd.datasender_cxt.DataSndCtl->SyncRepQueue));

        for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
            DataSnd *datasnd = &t_thrd.datasender_cxt.DataSndCtl->datasnds[i];

            rc = memset_s(datasnd, sizeof(DataSnd), 0, sizeof(DataSnd));
            securec_check(rc, "", "");
            datasnd->sendKeepalive = true;
            SpinLockInit(&datasnd->mutex);
            InitSharedLatch(&datasnd->latch);
        }
        SpinLockInit(&t_thrd.datasender_cxt.DataSndCtl->mutex);
    }
}

/* Wake up all datasenders */
void DataSndWakeup(void)
{
    int i;

    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++)
        SetLatch(&t_thrd.datasender_cxt.DataSndCtl->datasnds[i].latch);
}

/* check if there is any data sender alive. */
bool DataSndInProgress(int type)
{
    int i;

    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile DataSnd *datasnd = &t_thrd.datasender_cxt.DataSndCtl->datasnds[i];

        SpinLockAcquire(&datasnd->mutex);

        if (datasnd->pid != 0 && ((datasnd->sendRole & type) == datasnd->sendRole)) {
            SpinLockRelease(&datasnd->mutex);
            return true;
        }

        SpinLockRelease(&datasnd->mutex);
    }

    return false;
}

/* return true if any standby(except dummy standby) caught up */
static bool DataSndCaughtup(void)
{
    int i;

    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile DataSnd *datasnd = &t_thrd.datasender_cxt.DataSndCtl->datasnds[i];

        SpinLockAcquire(&datasnd->mutex);

        if (datasnd->pid != 0 && datasnd->sendRole == SNDROLE_PRIMARY_STANDBY &&
            datasnd->state == DATASNDSTATE_STREAMING) {
            SpinLockRelease(&datasnd->mutex);

            return true;
        }

        SpinLockRelease(&datasnd->mutex);
    }

    return false;
}

/* Set state for current datasender (only called in datasender) */
void DataSndSetState(DataSndState state)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile DataSnd *datasnd = t_thrd.datasender_cxt.MyDataSnd;

    Assert(t_thrd.datasender_cxt.am_datasender);

    if (datasnd->state == state)
        return;

    SpinLockAcquire(&datasnd->mutex);
    datasnd->state = state;
    if (state == DATASNDSTATE_CATCHUP)
        datasnd->catchupTime[0] = GetCurrentTimestamp();
    else if (state == DATASNDSTATE_STREAMING)
        datasnd->catchupTime[1] = GetCurrentTimestamp();
    SpinLockRelease(&datasnd->mutex);
}

/*
 * This function is used to send keepalive message to standby.
 * If requestReply is set, sets a flag in the message requesting the standby
 * to send a message back to us, for heartbeat purposes.
 */
static void DataSndKeepalive(bool requestReply)
{
    volatile DataSnd *datasnd = t_thrd.datasender_cxt.MyDataSnd;
    DataSndKeepaliveMessage keepalive_message;
    errno_t errorno = EOK;

    SpinLockAcquire(&datasnd->mutex);
    keepalive_message.sendPosition.queueid = datasnd->sendPosition.queueid;
    keepalive_message.sendPosition.queueoff = datasnd->sendPosition.queueoff;
    SpinLockRelease(&datasnd->mutex);
    keepalive_message.sendTime = GetCurrentTimestamp();
    keepalive_message.replyRequested = requestReply;
    keepalive_message.catchup = (t_thrd.datasender_cxt.MyDataSnd->state == DATASNDSTATE_CATCHUP);

    ereport(DEBUG2, (errmsg("sending data replication keepalive")));

    /* Prepend with the message type and send it. */
    t_thrd.datasender_cxt.output_message[0] = 'k';
    errorno = memcpy_s(t_thrd.datasender_cxt.output_message + 1,
                       sizeof(DataPageMessageHeader) + g_instance.attr.attr_storage.MaxSendSize * 1024,
                       &keepalive_message, sizeof(DataSndKeepaliveMessage));
    securec_check(errorno, "", "");
    (void)pq_putmessage_noblock('d', t_thrd.datasender_cxt.output_message, sizeof(DataSndKeepaliveMessage) + 1);

    /* Flush the keepalive message to standby immediately. */
    if (pq_flush_if_writable() != 0)
        DataSndShutdown();
}

/*
 * This function is used to send rm_data message to  dummystandby.
 * If requestReply is set, sets a flag in the message requesting the standby
 * to send a message back to us, for heartbeat purposes.
 */
static void DataSndRmData(bool requestReply)
{
    RmDataMessage rmDataMessage;
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    errno_t errorno = EOK;

    /* Construct a new message */
    SpinLockAcquire(&hashmdata->mutex);
    rmDataMessage.peer_role = hashmdata->current_mode;
    SpinLockRelease(&hashmdata->mutex);
    rmDataMessage.peer_state = get_local_dbstate();
    rmDataMessage.sendTime = GetCurrentTimestamp();
    rmDataMessage.replyRequested = requestReply;

    /* Prepend with the message type and send it. */
    t_thrd.datasender_cxt.output_message[0] = 'x';
    errorno = memcpy_s(t_thrd.datasender_cxt.output_message + 1,
                       sizeof(DataPageMessageHeader) + g_instance.attr.attr_storage.MaxSendSize * 1024, &rmDataMessage,
                       sizeof(RmDataMessage));
    securec_check(errorno, "", "");
    (void)pq_putmessage_noblock('d', t_thrd.datasender_cxt.output_message, sizeof(RmDataMessage) + 1);
}

static void DataSndKeepaliveIfNecessary(TimestampTz now)
{
    TimestampTz ping_time;

    /*
     * Don't send keepalive messages if timeouts are globally disabled or
     * we're doing something not partaking in timeouts.
     */
    if (u_sess->attr.attr_storage.wal_sender_timeout <= 0 || t_thrd.datasender_cxt.last_reply_timestamp <= 0)
        return;

    if (t_thrd.datasender_cxt.ping_sent)
        return;

    /*
     * If half of wal_sender_timeout has lapsed without receiving any reply
     * from the standby, send a keep-alive message to the standby requesting
     * an immediate reply.
     */
    ping_time = TimestampTzPlusMilliseconds(t_thrd.datasender_cxt.last_reply_timestamp,
                                            u_sess->attr.attr_storage.wal_sender_timeout / 2);
    if (now >= ping_time) {
        DataSndKeepalive(true);
        t_thrd.datasender_cxt.ping_sent = true;
    }
}

static uint32 DataSendReadData(char *buf, uint32 bufsize)
{
    uint32 bytesread = 0;
    char path[MAXPGPATH] = {0};
    uint32 nbytes = 0;
    uint32 total_len = 0;
    errno_t rc = EOK;
    int nRet = 0;

    rc = memset_s(buf, bufsize, 0, bufsize);
    securec_check(rc, "", "");

retry:
    /* open the file */
    while (t_thrd.datasender_cxt.dummy_data_read_file_fd == NULL) {
        /* if dummy standby have no data to send , return 0 */
        if (t_thrd.datasender_cxt.dummy_data_read_file_num > t_thrd.datarcvwriter_cxt.dummy_data_writer_file_num) {
            ereport(DEBUG5, (errmsg("no data to send.dummy_data_read_file_num=%u",
                                    t_thrd.datasender_cxt.dummy_data_read_file_num)));

            if (total_len > bufsize)
                ereport(PANIC, (errmsg("Secondery standby finish read data error, total len %u, bufsize %u", total_len,
                                       bufsize)));
            return total_len;
        }
        /* get the file path */
        nRet = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "base/dummy_standby/%u",
                          t_thrd.datasender_cxt.dummy_data_read_file_num);
        securec_check_ss(nRet, "\0", "\0");

        ereport(DEBUG5, (errmsg("DataSendReadData path=%s", path)));

        /* open the file */
        t_thrd.datasender_cxt.dummy_data_read_file_fd = fopen(path, "rb");
        if (t_thrd.datasender_cxt.dummy_data_read_file_fd == NULL) {
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not open data file \"%s\": %m", path)));
            t_thrd.datasender_cxt.dummy_data_read_file_num++;
        }
    }

    /* get many pages to bufsize */
    for (;;) {
        /*
         * OK to read the data:
         * 1. first to read the nbytes num;
         */
        bytesread = fread(&nbytes, 1, sizeof(nbytes), t_thrd.datasender_cxt.dummy_data_read_file_fd);
        if (bytesread != sizeof(nbytes)) {
            if (ferror(t_thrd.datasender_cxt.dummy_data_read_file_fd)) {
                ereport(PANIC, (errcode_for_file_access(),
                                errmsg("could not read to data file %s length %u: %m", path, nbytes)));
            }
            if (feof(t_thrd.datasender_cxt.dummy_data_read_file_fd)) {
                ereport(LOG, (errmsg("step1: data file num %u, read file fd %d",
                                     t_thrd.datasender_cxt.dummy_data_read_file_num,
                                     t_thrd.datasender_cxt.dummy_data_read_file_fd->_fileno)));
                t_thrd.datasender_cxt.dummy_data_read_file_num++;
                fclose(t_thrd.datasender_cxt.dummy_data_read_file_fd);
                t_thrd.datasender_cxt.dummy_data_read_file_fd = NULL;

                /*
                 * if we receive the eof when read the nbytes num, the file maybe
                 * interruption when writting. So we goto retry to read the next file.
                 */
                goto retry;
            }
        }

        /* if the bufsize is full, then we send it. */
        if (total_len + nbytes > bufsize) {
            if (fseek(t_thrd.datasender_cxt.dummy_data_read_file_fd, -(long)sizeof(nbytes), SEEK_CUR))
                ereport(PANIC,
                        (errmsg("fseek data file num %u error", t_thrd.datasender_cxt.dummy_data_read_file_num)));
            break;
        }

        /*
         * 2. then to read the data with the nbytes;
         */
        bytesread = fread(buf + total_len, 1, nbytes, t_thrd.datasender_cxt.dummy_data_read_file_fd);
        if (bytesread != nbytes) {
            if (ferror(t_thrd.datasender_cxt.dummy_data_read_file_fd)) {
                ereport(PANIC, (errcode_for_file_access(),
                                errmsg("could not read to data file %s length %u: %m", path, nbytes)));
            }
            if (feof(t_thrd.datasender_cxt.dummy_data_read_file_fd)) {
                ereport(LOG, (errmsg("step2: data file num %u, read file fd %d",
                                     t_thrd.datasender_cxt.dummy_data_read_file_num,
                                     t_thrd.datasender_cxt.dummy_data_read_file_fd->_fileno)));
                t_thrd.datasender_cxt.dummy_data_read_file_num++;
                fclose(t_thrd.datasender_cxt.dummy_data_read_file_fd);
                t_thrd.datasender_cxt.dummy_data_read_file_fd = NULL;

                /*
                 * if we receive the eof when read the data, the file maybe
                 * interruption when writting. So we goto retry to read the next file.
                 */
                goto retry;
            }
        }
        total_len += nbytes;
    }

    if (total_len > bufsize)
        ereport(PANIC, (errmsg("Secondery standby read data error, total len %u, bufsize %u", total_len, bufsize)));

    return total_len;
}

/*
 * return datasender state string.
 */
static const char *DataSndGetStateString(DataSndState state)
{
    switch (state) {
        case DATASNDSTATE_STARTUP:
            return "Startup";
        case DATASNDSTATE_CATCHUP:
            return "Catchup";
        case DATASNDSTATE_STREAMING:
            return "Streaming";
    }
    return "Unknown";
}

/*
 * Returns activity of datasenders, including pids and queue position sent to
 * standby servers.
 */
Datum pg_stat_get_data_senders(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_DATA_SENDER_COLS 13
    TupleDesc tupdesc;
    Tuplestorestate *tupstore = NULL;

    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    ServerMode local_role = UNKNOWN_MODE;
    int i = 0;
    errno_t rc = EOK;
    int ret = 0;

    tupstore = BuildTupleResult(fcinfo, &tupdesc);

    SpinLockAcquire(&hashmdata->mutex);
    local_role = hashmdata->current_mode;
    SpinLockRelease(&hashmdata->mutex);

    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile DataSnd *datasnd = &t_thrd.datasender_cxt.DataSndCtl->datasnds[i];
        Datum values[PG_STAT_GET_DATA_SENDER_COLS];
        bool nulls[PG_STAT_GET_DATA_SENDER_COLS];
        char location[64] = {0};
        int j = 0;

        ThreadId pid;
        int lwpid;
        SndRole snd_role;
        DataSndState state;
        TimestampTz catchup_time[2];

        uint32 queue_size;
        DataQueuePtr queue_header;
        DataQueuePtr queue_lower_tail;
        DataQueuePtr queue_upper_tail;
        DataQueuePtr send_position;
        DataQueuePtr receive_position;

        SpinLockAcquire(&datasnd->mutex);
        if (datasnd->pid == 0) {
            SpinLockRelease(&datasnd->mutex);
            continue;
        } else {
            pid = datasnd->pid;
            lwpid = datasnd->lwpId;
            snd_role = datasnd->sendRole;
            state = datasnd->state;
            catchup_time[0] = datasnd->catchupTime[0]; /* catchup start */
            catchup_time[1] = datasnd->catchupTime[1]; /* catchup end */
            send_position.queueid = datasnd->sendPosition.queueid;
            send_position.queueoff = datasnd->sendPosition.queueoff;
            receive_position.queueid = datasnd->receivePosition.queueid;
            receive_position.queueoff = datasnd->receivePosition.queueoff;
            SpinLockRelease(&datasnd->mutex);
        }

        SpinLockAcquire(&t_thrd.dataqueue_cxt.DataSenderQueue->use_mutex);
        queue_size = t_thrd.dataqueue_cxt.DataSenderQueue->size;
        queue_header = t_thrd.dataqueue_cxt.DataSenderQueue->use_head2;
        queue_lower_tail = t_thrd.dataqueue_cxt.DataSenderQueue->use_tail1;
        queue_upper_tail = t_thrd.dataqueue_cxt.DataSenderQueue->use_tail2;
        SpinLockRelease(&t_thrd.dataqueue_cxt.DataSenderQueue->use_mutex);

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "", "");
        /* pid */
        values[j++] = Int64GetDatum(pid);
        /* lwpid */
        values[j++] = Int32GetDatum(lwpid);

        if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
            /*
             * Only superusers can see details. Other users only get the pid
             * value to know it's a walsender, but no details.
             */
            rc = memset_s(&nulls[j], PG_STAT_GET_DATA_SENDER_COLS - j, true, PG_STAT_GET_DATA_SENDER_COLS - j);
            securec_check(rc, "", "");
        } else {
            /* local_role */
            values[j++] = CStringGetTextDatum(wal_get_role_string(local_role));

            /* peer_role */
            if (snd_role == SNDROLE_PRIMARY_DUMMYSTANDBY)
                values[j++] = CStringGetTextDatum("Secondary");
            else
                values[j++] = CStringGetTextDatum("Standby");

            /* state */
            values[j++] = CStringGetTextDatum(DataSndGetStateString(state));

            /* catchup time */
            if (catchup_time[0] != 0)
                values[j++] = TimestampTzGetDatum(catchup_time[0]);
            else
                nulls[j++] = true;
            if (catchup_time[1] != 0 && (state != DATASNDSTATE_CATCHUP))
                values[j++] = TimestampTzGetDatum(catchup_time[1]);
            else
                nulls[j++] = true;

            /* queue */
            values[j++] = UInt32GetDatum(queue_size);
            ret = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", queue_lower_tail.queueid,
                             queue_lower_tail.queueoff);
            securec_check_ss(ret, "", "");
            values[j++] = CStringGetTextDatum(location);
            ret = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", queue_header.queueid,
                             queue_header.queueoff);
            securec_check_ss(ret, "", "");
            values[j++] = CStringGetTextDatum(location);
            ret = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", queue_upper_tail.queueid,
                             queue_upper_tail.queueoff);
            securec_check_ss(ret, "", "");
            values[j++] = CStringGetTextDatum(location);
            /* send */
            ret = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", send_position.queueid,
                             send_position.queueoff);
            securec_check_ss(ret, "", "");
            values[j++] = CStringGetTextDatum(location);
            /* receive */
            ret = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", receive_position.queueid,
                             receive_position.queueoff);
            securec_check_ss(ret, "", "");
            values[j++] = CStringGetTextDatum(location);
        }
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
    tuplestore_donestoring(tupstore);

    return (Datum)0;
}
