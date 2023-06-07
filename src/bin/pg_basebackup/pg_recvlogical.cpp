/* -------------------------------------------------------------------------
 *
 * pg_recvlogical.c - receive data from a logical decoding slot in a streaming fashion
 *                   and write it to to a local file.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *       src/bin/pg_basebackup/pg_recvlogical.c
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/time.h>

/* local includes */
#include "streamutil.h"

#include "access/xlog_internal.h"
#include "common/fe_memutils.h"
#include "getopt_long.h"
#include "libpq/libpq-fe.h"
#include "libpq/pqsignal.h"
#include "libpq/pqexpbuffer.h"
#include "replication/walprotocol.h"
#include "securec.h"
#include "bin/elog.h"
/* Time to sleep between reconnection attempts */
#define RECONNECT_SLEEP_TIME 5

static const uint64 upperPart = 32;

/* Global Options */
static char* outfile = NULL;
static int verbose = 0;
static int noloop = 0;
static int fsync_interval = 10 * 1000;          /* 10 sec = default */
static XLogRecPtr startpos = InvalidXLogRecPtr;
static bool do_create_slot = false;
static bool do_start_slot = false;
static bool do_drop_slot = false;
static bool g_parallel_decode = false;
static char g_decode_style = 'b';
static bool g_batch_sending = false;
static bool g_raw = false;

/* filled pairwise with option, value. value may be NULL */
static char** options;
static size_t noptions = 0;
static bool g_change_plugin = false;
static const char* plugin = NULL;

/* Global State */
static int outfd = -1;
static volatile sig_atomic_t time_to_abort = false;
static volatile sig_atomic_t output_reopen = false;
static int64 output_last_fsync = -1;
static bool output_unsynced = false;
static XLogRecPtr output_written_lsn = InvalidXLogRecPtr;
static XLogRecPtr output_fsync_lsn = InvalidXLogRecPtr;

static void usage(void);
static void StreamLogicalLog();

static void usage(void)
{
    printf(_("%s receives logical change stream.\n\n"), progname);
    printf(_("Usage:\n"));
    printf(_("  %s [OPTION]...\n"), progname);
    printf(_("\nOptions:\n"));
    printf(_("  -f, --file=FILE        receive log into this file. - for stdout\n"));
    printf(_("  -n, --no-loop          do not loop on connection lost\n"));
    printf(_("  -v, --verbose          output verbose messages\n"));
    printf(_("  -V, --version          output version information, then exit\n"));
    printf(_("  -?, --help             show this help, then exit\n"));
    printf(_("\nConnection options:\n"));
    printf(_("  -d, --dbname=DBNAME    database to connect to\n"));
    printf(_("  -h, --host=HOSTNAME    database server host or socket directory\n"));
    printf(_("  -p, --port=PORT        database server port number\n"));
    printf(_("  -U, --username=NAME    connect as specified database user\n"));
    printf(_("  -w, --no-password      never prompt for password\n"));
    printf(_("  -W, --password         force password prompt (should happen automatically)\n"));
    printf(_("\nReplication options:\n"));
    printf(_("  -F  --fsync-interval=INTERVAL\n"
             "                         frequency of syncs to the output file (in seconds, defaults to 10)\n"));
    printf(_("  -o, --option=NAME[=VALUE]\n"
             "                         Specify option NAME with optional value VAL, to be passed\n"
             "                         to the output plugin\n"));
    printf(_("  -P, --plugin=PLUGIN    use output plugin PLUGIN (defaults to mppdb_decoding)\n"));
    printf(_("  -s, --status-interval=INTERVAL\n"
             "                         time between status packets sent to server (in seconds, defaults to 10)\n"));
    printf(_("  -S, --slot=SLOT        use existing replication slot SLOT instead of starting a new one\n"));
    printf(_("  -I, --startpos=PTR     Where in an existing slot should the streaming start\n"));
    printf(_("  -r, --raw              parallel decoding output raw results without converting to text format\n"));
    printf(_("\nAction to be performed:\n"));
    printf(_("      --create           create a new replication slot (for the slotname see --slot)\n"));
    printf(_("      --start            start streaming in a replication slot (for the slotname see --slot)\n"));
    printf(_("      --drop             drop the replication slot (for the slotname see --slot)\n"));
    printf(_("\n"));
}

/* showVersion
 *
 * This output format is intended to match GNU standards.
 */
static void showVersion(void)
{
    puts("pg_recvlogical" DEF_GS_VERSION);
}

/*
 * Converts an int64 from network byte order to native format.
 */
XLogRecPtr fe_recvint64(const char* buf)
{
    XLogRecPtr result;
    uint32 h32;
    uint32 l32;
    errno_t ret = 0;
    ret = memcpy_s(&h32, sizeof(uint32), buf, sizeof(uint32));
    securec_check(ret, "\0", "\0");
    ret = memcpy_s(&l32, sizeof(uint32), buf + sizeof(uint32), sizeof(uint32));
    securec_check(ret, "\0", "\0");
    h32 = ntohl(h32);
    l32 = ntohl(l32);

    result = h32;
    result <<= 32;
    result |= l32;

    return result;
}

/*
 * Send a Standby Status Update message to server.
 */
static bool sendFeedback(PGconn* conn, int64 now, bool force, bool replyRequested)
{
    static XLogRecPtr last_written_lsn = InvalidXLogRecPtr;
    static XLogRecPtr last_fsync_lsn = InvalidXLogRecPtr;

    int len = 0;
    char replybuf[sizeof(StandbyReplyMessage) + 1] = {0};

    StandbyReplyMessage* replymsg = NULL;

    /*
     * we normally don't want to send superflous feedbacks, but if it's
     * because of a timeout we need to, otherwise wal_sender_timeout will
     * kill us.
     */
    if (!force && XLByteEQ(last_written_lsn, output_written_lsn) && !XLByteEQ(last_fsync_lsn, output_fsync_lsn))
        return true;

    if (verbose)
        fprintf(stderr,
            _("%s: confirming write up to %X/%X, flush to %X/%X (slot %s) %ld\n"),
            progname,
            (uint32)(output_written_lsn >> 32),
            (uint32)output_written_lsn,
            (uint32)(output_fsync_lsn >> 32),
            (uint32)output_fsync_lsn,
            replication_slot,
            now);

    replybuf[len] = 'r';
    len += 1;

    {
        replymsg = (StandbyReplyMessage*)(replybuf + 1);
        replymsg->write = output_written_lsn;                     /* write */
        replymsg->flush = output_fsync_lsn;                       /* flush */
        replymsg->applyRead = output_fsync_lsn;                   /* applyRead: walsender need it to update lsn */
        replymsg->sendTime = now;                                 /* sendTime */
        replymsg->replyRequested = replyRequested ? true : false; /* replyRequested */
        len += sizeof(StandbyReplyMessage);
    }

    startpos = output_written_lsn;
    last_written_lsn = output_written_lsn;
    last_fsync_lsn = output_fsync_lsn;

    if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn)) {
        fprintf(stderr, _("%s: could not send feedback packet: %s"), progname, PQerrorMessage(conn));
        return false;
    }

    return true;
}

static bool OutputFsync(int64 now)
{
    output_last_fsync = now;

    output_fsync_lsn = output_written_lsn;

    if (fsync_interval <= 0) {
        return true;
    }

    if (!output_unsynced) {
        return true;
    }

    output_unsynced = false;

    /* Accept EINVAL, in case output is writing to a pipe or similar. */
    if (fsync(outfd) != 0 && errno != EINVAL) {
        fprintf(stderr, _("%s: could not fsync log file \"%s\": %s\n"), progname, outfile, strerror(errno));
        return false;
    }

    return true;
}

/*
 * Send START_REPLICATION SLOT LOGICAL cmd.
 */
static int sendStartReplicationCmd()
{
    int ret = 0;
    PGresult* res = NULL;
    size_t i = 0;
    PQExpBuffer query = NULL;

    query = createPQExpBuffer();
    if (query == NULL) {
        fprintf(stderr, _("%s: create exp buffer failed, out of memory.\n"), progname);
        return -1;
    }
    if (verbose)
        fprintf(stderr,
            _("%s: starting log streaming at %X/%X (slot %s)\n"),
            progname,
            (uint32)(startpos >> 32),
            (uint32)startpos,
            replication_slot);

    /* Initiate the replication stream at specified location */
    appendPQExpBuffer(query,
        "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X",
        replication_slot,
        (uint32)(startpos >> 32),
        (uint32)startpos);

    /* print options if there are any */
    if (noptions)
        appendPQExpBufferStr(query, " (");

    for (i = 0; i < noptions; i++) {
        /* separator */
        if (i > 0)
            appendPQExpBufferStr(query, ", ");

        /* write option name */
        appendPQExpBuffer(query, "\"%s\"", options[(i * 2)]);

        /* write option value if specified */
        if (options[(i * 2) + 1] != NULL)
            appendPQExpBuffer(query, " '%s'", options[(i * 2) + 1]);
    }

    if (noptions)
        appendPQExpBufferChar(query, ')');

    res = PQexec(conn, query->data);
    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        fprintf(stderr,
            _("%s: could not send replication command \"%s\": %s\n"),
            progname,
            query->data,
            PQresultErrorMessage(res));
        ret = -1;
    }
    PQclear(res);
    destroyPQExpBuffer(query);
    return ret;
}

/*
 * append comma as seperator
 */
static inline void AppendSeperator(PQExpBuffer res, uint16 attr, uint16 maxAttr)
{
    if (attr < maxAttr - 1) {
        appendPQExpBufferStr(res, ", ");
    }
}

/*
 * decode binary style tuple to text
 */
static void ResolveTuple(const char* stream, uint32* curpos, PQExpBuffer res, bool newTup)
{
    uint16 attrnum = ntohs(*(uint16 *)(stream + *curpos));
    *curpos += sizeof(attrnum);
    if (newTup) {
        appendPQExpBufferStr(res, "new_tuple: {");
    } else {
        appendPQExpBufferStr(res, "old_value: {");
    }
    for (uint16 i = 0; i < attrnum; i++) {
        uint16 colLen = ntohs(*(uint16 *)(stream + *curpos));
        *curpos += sizeof(colLen);
        assert(colLen != 0);
        appendBinaryPQExpBuffer(res, stream + *curpos, colLen);
        *curpos += colLen;
        Oid typid = ntohl(*(Oid *)(stream + *curpos));
        *curpos += sizeof(Oid);
        appendPQExpBuffer(res, "[typid = %u]: ", typid);

        uint32 dataLen = ntohl(*(uint32 *)(stream + *curpos));
        *curpos += sizeof(dataLen);
        const uint32 nullTag = 0XFFFFFFFF;
        if (dataLen == nullTag) {
            appendPQExpBufferStr(res, "\"NULL\"");
        } else if (dataLen == 0) {
            appendPQExpBufferStr(res, "\"\"");
        } else {
            appendPQExpBufferChar(res, '\"');
            appendBinaryPQExpBuffer(res, stream + *curpos, dataLen);
            appendPQExpBufferChar(res, '\"');
            *curpos += dataLen;
        }
        AppendSeperator(res, i, attrnum);
    }
    appendPQExpBufferChar(res, '}');
}

/*
 * decode binary style DML to text
 */
static void DMLToText(const char* stream, uint32 *curPos, PQExpBuffer res)
{
    char dtype = stream[*curPos];
    if (dtype == 'I') {
        appendPQExpBufferStr(res, "INSERT INTO ");
    } else if (dtype == 'U') {
        appendPQExpBufferStr(res, "UPDATE ");
    } else {
        appendPQExpBufferStr(res, "DELETE FROM ");
    }
    *curPos += 1;
    uint16 schemaLen = ntohs(*(uint16 *)(stream + *curPos));
    *curPos += sizeof(schemaLen);
    appendBinaryPQExpBuffer(res, stream + *curPos, schemaLen);
    *curPos += schemaLen;
    appendPQExpBufferChar(res, '.');
    uint16 tableLen = ntohs(*(uint16 *)(stream + *curPos));
    *curPos += sizeof(tableLen);
    appendBinaryPQExpBuffer(res, stream + *curPos, tableLen);
    *curPos += tableLen;

    if (stream[*curPos] == 'N') {
        *curPos += 1;
        appendPQExpBufferChar(res, ' ');
        ResolveTuple(stream, curPos, res, true);
    }
    if (stream[*curPos] == 'O') {
        *curPos += 1;
        appendPQExpBufferChar(res, ' ');
        ResolveTuple(stream, curPos, res, false);
    }
}

/*
 * decode binary style begin message to text
 */
static void BeginToText(const char* stream, uint32 *curPos, PQExpBuffer res)
{
    appendPQExpBufferStr(res, "BEGIN ");
    *curPos += 1;
    uint32 CSNupper = ntohl(*(uint32 *)(&stream[*curPos]));
    *curPos += sizeof(CSNupper);
    uint32 CSNlower = ntohl(*(uint32 *)(&stream[*curPos]));
    uint64 CSN = ((uint64)(CSNupper) << upperPart) + CSNlower;
    *curPos += sizeof(CSNlower);
    appendPQExpBuffer(res, "CSN: %lu ", CSN);

    uint32 LSNupper = ntohl(*(uint32 *)(&stream[*curPos]));
    *curPos += sizeof(LSNupper);
    uint32 LSNlower = ntohl(*(uint32 *)(&stream[*curPos]));
    *curPos += sizeof(LSNlower);
    appendPQExpBuffer(res, "first_lsn: %X/%X", LSNupper, LSNlower);

    if (stream[*curPos] == 'T') {
        *curPos += 1;
        uint32 timeLen = ntohl(*(uint32 *)(&stream[*curPos]));
        *curPos += sizeof(uint32);
        appendPQExpBufferStr(res, " commit_time: ");
        appendBinaryPQExpBuffer(res, &stream[*curPos], timeLen);
        *curPos += timeLen;
    }
    if (stream[*curPos] == 'O') {
        *curPos += 1;
        uint32 originid = ntohl(*(uint32 *)(&stream[*curPos]));
        *curPos += sizeof(uint32);
        appendPQExpBuffer(res, " origin_id: %d", originid);
    }
}

/*
 * decode binary style commit message to text
 */
static void CommitToText(const char* stream, uint32 *curPos, PQExpBuffer res)
{
    appendPQExpBufferStr(res, "COMMIT ");
    *curPos += 1;
    if (stream[*curPos] == 'X') {
        *curPos += 1;
        uint32 xidupper = ntohl(*(uint32 *)(&stream[*curPos]));
        *curPos += sizeof(xidupper);
        uint32 xidlower = ntohl(*(uint32 *)(&stream[*curPos]));
        *curPos += sizeof(xidlower);
        uint64 xid = ((uint64)(xidupper) << upperPart) + xidlower;
        appendPQExpBuffer(res, "xid: %lu", xid);
    }
    if (stream[*curPos] == 'T') {
        *curPos += 1;
        uint32 timeLen = ntohl(*(uint32 *)(&stream[*curPos]));
        *curPos += sizeof(uint32);
        appendPQExpBufferStr(res, " commit_time: ");
        appendBinaryPQExpBuffer(res, &stream[*curPos], timeLen);
        *curPos += timeLen;
    }
}

/*
 * decode binary style log stream to text
 */
static void StreamToText(const char* stream, PQExpBuffer res)
{
    uint32 pos = 0;
    uint32 dmlLen = ntohl(*(uint32 *)(&stream[pos]));
    /* if this is the end of stream, return */
    if (dmlLen == 0) {
        return;
    }

    pos += sizeof(dmlLen);
    uint32 LSNupper = ntohl(*(uint32 *)(&stream[pos]));
    pos += sizeof(LSNupper);
    uint32 LSNlower = ntohl(*(uint32 *)(&stream[pos]));
    pos += sizeof(LSNlower);
    appendPQExpBuffer(res, "current_lsn: %X/%X ", LSNupper, LSNlower);
    if (stream[pos] == 'B') {
        BeginToText(stream, &pos, res);
    } else if (stream[pos] == 'C') {
        CommitToText(stream, &pos, res);
    } else if (stream[pos] != 'P' && stream[pos] != 'F') {
        DMLToText(stream, &pos, res);
    }
    if (stream[pos] == 'P') {
        pos++;
        appendPQExpBufferChar(res, '\n');
        StreamToText(stream + pos, res);
    } else if (stream[pos] == 'F') {
        appendPQExpBufferChar(res, '\n');
    }
}

/*
 * decode batch sending result stream to text.
 */
static void BatchStreamToText(const char* stream, PQExpBuffer res)
{
    uint32 pos = 0;
    uint32 changeLen = ntohl(*(uint32 *)(&stream[pos]));
    /* if this is the end of stream, return */
    if (changeLen == 0) {
        return;
    }
    pos += sizeof(changeLen);
    pos += sizeof(XLogRecPtr);
    appendBinaryPQExpBuffer(res, stream + pos, changeLen - sizeof(XLogRecPtr));
    appendPQExpBufferChar(res, '\n');

    BatchStreamToText(stream + sizeof(changeLen) + changeLen, res);
}

/*
 * Check stream connection.
 */
static bool StreamCheckConn()
{
    PGresult* res = NULL;
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, _("%s: unexpected termination of replication stream: %s"), progname, PQresultErrorMessage(res));
        if (res != NULL) {
            PQclear(res);
        }
        return false;
    }
    PQclear(res);
    res = NULL;
    return true;
}

/*
 * Start the log streaming
 */
static void StreamLogicalLog(void)
{
    char* copybuf = NULL;
    int64 last_status = -1;

    output_written_lsn = InvalidXLogRecPtr;
    output_fsync_lsn = InvalidXLogRecPtr;

    /*
     * Connect in replication mode to the server
     */
    if (conn == NULL)
        conn = GetConnection();
    if (conn == NULL)
        /* Error message already written in GetConnection() */
        return;

    ClearAndFreePasswd();
    /*
     * Start the replication
     */
    if (sendStartReplicationCmd() == -1)
        goto error;

    if (verbose)
        fprintf(stderr, _("%s: initiated streaming\n"), progname);

    while (!time_to_abort) {
        int r;
        int bytes_left;
        int bytes_written;
        int64 now;
        int hdr_len;

        if (copybuf != NULL) {
            PQfreemem(copybuf);
            copybuf = NULL;
        }

        /*
         * Potentially send a status message to the master
         */
        now = feGetCurrentTimestamp();
        if (outfd != -1 && feTimestampDifferenceExceeds(output_last_fsync, now, fsync_interval)) {
            if (!OutputFsync(now))
                goto error;
        }

        if (standby_message_timeout > 0 && feTimestampDifferenceExceeds(last_status, now, standby_message_timeout)) {
            /* Time to send feedback! */
            if (!sendFeedback(conn, now, true, false))
                goto error;

            last_status = now;
        }

        r = PQgetCopyData(conn, &copybuf, 1);
        if (r == 0) {
            /*
             * In async mode, and no data available. We block on reading but
             * not more than the specified timeout, so that we can send a
             * response back to the client.
             */
            fd_set input_mask;
            int64 message_target = 0;
            int64 fsync_target = 0;
            struct timeval timeout;
            struct timeval* timeoutptr = NULL;

            FD_ZERO(&input_mask);
            FD_SET(PQsocket(conn), &input_mask);

            /* Compute when we need to wakeup to send a keepalive message. */
            if (standby_message_timeout)
                message_target = last_status + (standby_message_timeout - 1) * ((int64)1000);

            /* Compute when we need to wakeup to fsync the output file. */
            if (fsync_interval > 0 && output_unsynced == true)
                fsync_target = output_last_fsync + (fsync_interval - 1) * ((int64)1000);

            /* Now compute when to wakeup. */
            if (message_target > 0 || fsync_target > 0) {
                int64 targettime;
                long secs;
                long usecs;

                targettime = message_target;

                if (fsync_target > 0 && fsync_target < targettime)
                    targettime = fsync_target;

                feTimestampDifference(now, targettime, &secs, &usecs);
                if (secs <= 0)
                    timeout.tv_sec = 1; /* Always sleep at least 1 sec */
                else
                    timeout.tv_sec = secs;
                timeout.tv_usec = usecs;
                timeoutptr = &timeout;
            }

            r = select(PQsocket(conn) + 1, &input_mask, NULL, NULL, timeoutptr);
            if (r < 0) {
                fprintf(stderr, _("%s: select() failed: %s\n"), progname, strerror(errno));
                goto error;
            } else if (r == 0 || (r < 0 && errno == EINTR)) {
                /*
                 * Got a timeout or signal. Continue the loop and either
                 * deliver a status packet to the server or just go back into
                 * blocking.
                 */
                continue;
            }

            /* Else there is actually data on the socket */
            if (PQconsumeInput(conn) == 0) {
                fprintf(stderr, _("%s: could not receive data from WAL stream: %s"), progname, PQerrorMessage(conn));
                goto error;
            }
            continue;
        }

        /* Failure while reading the copy stream */
        if (r == -2) {
            fprintf(stderr, _("%s: could not read COPY data: %s"), progname, PQerrorMessage(conn));
            goto error;
        }

        /* End of copy stream */
        if (r == -1)
            break;

        /* Check the message type. */
        if (copybuf[0] == 'k') {
            int pos = 0;
            bool replyRequested = false;
            XLogRecPtr walEnd = InvalidXLogRecPtr;
            errno_t errorno = 0;
            PrimaryKeepaliveMessage keepalive_message;

            /*
             * Parse the keepalive message, enclosed in the CopyData message.
             * We just check if the server requested a reply, and ignore the
             * rest.
             */
            pos = 1; /* skip msgtype 'k' */

            errorno = memcpy_s(
                &keepalive_message, sizeof(PrimaryKeepaliveMessage), &copybuf[pos], sizeof(PrimaryKeepaliveMessage));
            securec_check(errorno, "\0", "\0");
            pos += sizeof(PrimaryKeepaliveMessage);
            walEnd = keepalive_message.walEnd;
            output_written_lsn = Max(walEnd, output_written_lsn);
            replyRequested = keepalive_message.replyRequested;
            if (!g_parallel_decode) {
                fprintf(stderr, _("%s: written_lsn = %lu, current time = %ld \n"), progname, output_written_lsn, now);
            }

            /* If the server requested an immediate reply, send one. */
            if (replyRequested) {
                /* fsync data, so we send a recent flush pointer */
                if (!OutputFsync(now))
                    goto error;

                now = feGetCurrentTimestamp();
                if (!sendFeedback(conn, now, true, false))
                    goto error;
                last_status = now;
            }
            continue;
        } else if (copybuf[0] != 'w') {
            fprintf(stderr, _("%s: unrecognized streaming header: \"%c\"\n"), progname, copybuf[0]);
            goto error;
        }

        /*
         * Read the header of the XLogData message, enclosed in the CopyData
         * message. We only need the WAL location field (dataStart), the rest
         * of the header is ignored.
         */
        hdr_len = 1;  /* msgtype 'w' */
        hdr_len += 8; /* dataStart */
        hdr_len += 8; /* walEnd */
        hdr_len += 8; /* sendTime */
        if (r < hdr_len + 1) {
            fprintf(stderr, _("%s: streaming header too small: %d\n"), progname, r);
            goto error;
        }

        /* Extract WAL location for this block */
        {
            XLogRecPtr temp = fe_recvint64(&copybuf[1]);

            output_written_lsn = Max(temp, output_written_lsn);
        }

        /* redirect output to stdout */
        if (outfd == -1 && strcmp(outfile, "-") == 0) {
            outfd = fileno(stdout);
        }

        /* got SIGHUP, close output file */
        if (outfd != -1 && output_reopen) {
            now = feGetCurrentTimestamp();
            if (!OutputFsync(now))
                goto error;
            close(outfd);
            outfd = -1;
            output_reopen = false;
        }

        if (outfd == -1) {
            outfd = open(outfile, O_CREAT | O_APPEND | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
            if (outfd == -1) {
                fprintf(stderr, _("%s: could not open log file \"%s\": %s\n"), progname, outfile, strerror(errno));
                goto error;
            }
        }

        bytes_left = r - hdr_len;
        bytes_written = 0;

        /* signal that a fsync is needed */
        output_unsynced = true;

        PQExpBuffer res = createPQExpBuffer();
        char *resultStream = copybuf + hdr_len;
        if (g_parallel_decode && !g_raw && g_decode_style == 'b') {
            StreamToText(copybuf +  hdr_len, res);
            bytes_left = res->len;
            resultStream = res->data;
        } else if (g_parallel_decode && g_batch_sending && !g_raw) {
            BatchStreamToText(copybuf +  hdr_len, res);
            bytes_left = res->len;
            resultStream = res->data;
        }
        while (bytes_left) {
            int ret = write(outfd, resultStream + bytes_written, bytes_left);
            if (ret < 0) {
                fprintf(stderr,
                    _("%s: could not write %d bytes to log file \"%s\": %s\n"),
                    progname, bytes_left, outfile, strerror(errno));
                goto error;
            }

            /* Write was successful, advance our position */
            bytes_written += ret;
            bytes_left -= ret;
        }

        if (g_parallel_decode && (g_decode_style == 'b' || g_batch_sending)) {
            continue;
        }
        if (write(outfd, "\n", 1) != 1) {
            fprintf(stderr,
                _("%s: could not write %d bytes to log file \"%s\": %s\n"),
                progname, 1, outfile, strerror(errno));
            goto error;
        }
    }

    if (!StreamCheckConn()) {
        goto error;
    }

    if (outfd != -1 && strcmp(outfile, "-") != 0) {
        int64 t = feGetCurrentTimestamp();

        /* no need to jump to error on failure here, we're finishing anyway */
        (void)OutputFsync(t);

        if (close(outfd) != 0) {
            fprintf(stderr, _("%s: could not close file \"%s\": %s\n"), progname, outfile, strerror(errno));
        } else {
            outfd = -1;
        }
    } else {
        outfd = -1;
    }
error:
    if (outfd != -1) {
        (void)close(outfd);
        outfd = -1;
    }
    if (copybuf != NULL) {
        PQfreemem(copybuf);
        copybuf = NULL;
    }
    PQfinish(conn);
    conn = NULL;
}

/*
 * Unfortunately we can't do sensible signal handling on windows...
 */
#ifndef WIN32

/*
 * When sigint is called, just tell the system to exit at the next possible
 * moment.
 */
static void sigint_handler(int signum)
{
    time_to_abort = true;
}

/*
 * Trigger the output file to be reopened.
 */
static void sighup_handler(int signum)
{
    output_reopen = true;
}
#endif

static bool checkIsDigit(const char* arg)
{
    int i = 0;
    while (arg[i] != '\0') {
        if (isdigit(arg[i]) == 0)
            return 0;
        i++;
    }
    return 1;
}

static void CheckParallelDecoding(const char *data, const char *val)
{
    if (strncmp(data, "parallel-decode-num", sizeof("parallel-decode-num")) == 0) {
        int parallelDecodeNum = atoi(val);
        g_parallel_decode = parallelDecodeNum == 1 ? false : true;
    } else if (strncmp(data, "decode-style", sizeof("decode-style")) == 0) {
        g_decode_style = *val;
    }
}

static void CheckBatchSending(const char *data, const char *val)
{
    if (strncmp(data, "sending-batch", sizeof("sending-batch")) == 0) {
        int batchSending = atoi(val);
        g_batch_sending = batchSending == 0 ? false : true;
    }
}

/**
 * Get options.
 */
static int getOptions(const int argc, char* const* argv)
{
    static struct option long_options[] = {/* general options */
        {"file", required_argument, NULL, 'f'},
        {"no-loop", no_argument, NULL, 'n'},
        {"verbose", no_argument, NULL, 'v'},
        {"version", no_argument, NULL, 'V'},
        {"help", no_argument, NULL, '?'},
        /* connection options */
        {"dbname", required_argument, NULL, 'd'},
        {"host", required_argument, NULL, 'h'},
        {"port", required_argument, NULL, 'p'},
        {"username", required_argument, NULL, 'U'},
        {"no-password", no_argument, NULL, 'w'},
        {"password", no_argument, NULL, 'W'},
        /* replication options */
        {"option", required_argument, NULL, 'o'},
        {"plugin", required_argument, NULL, 'P'},
        {"status-interval", required_argument, NULL, 's'},
        {"fsync-interval", required_argument, NULL, 'F'},
        {"slot", required_argument, NULL, 'S'},
        {"startpos", required_argument, NULL, 'I'},
        {"raw", no_argument, NULL, 'r'},
        /* action */
        {"create", no_argument, NULL, 1},
        {"start", no_argument, NULL, 2},
        {"drop", no_argument, NULL, 3},
        {NULL, 0, NULL, 0}};

    int c;
    int option_index;
    uint32 hi, lo;
    while ((c = getopt_long(argc, argv, "f:F:nvd:h:o:p:U:wWP:rs:S:I:", long_options, &option_index)) != -1) {
        switch (c) {
            /* general options */
            case 'r':
                g_raw = true;
                break;
            case 'f':
                check_env_value_c(optarg);
                if (outfile) {
                    pfree_ext(outfile);
                }
                outfile = pg_strdup(optarg);
                break;
            case 'n':
                noloop = 1;
                break;
            case 'v':
                verbose++;
                break;
                /* connnection options */
            case 'd':
                check_env_value_c(optarg);
                if (dbname) {
                    pfree_ext(dbname);
                }
                dbname = pg_strdup(optarg);
                break;
            case 'h':
                check_env_value_c(optarg);
                if (dbhost) {
                    pfree_ext(dbhost);
                }
                dbhost = pg_strdup(optarg);
                break;
            case 'p':
                check_env_value_c(optarg);
                if (checkIsDigit(optarg) == 0) {
                    fprintf(stderr, _("%s: invalid port number \"%s\"\n"), progname, optarg);
                    exit(1);
                }
                dbport = pg_strdup(optarg);
                break;
            case 'U':
                check_env_value_c(optarg);
                if (dbuser) {
                    pfree_ext(dbuser);
                }
                dbuser = pg_strdup(optarg);
                break;
            case 'w':
                dbgetpassword = -1;
                break;
            case 'W':
                dbgetpassword = 1;
                break;
                /* replication options */
            case 'o': {
                check_env_value_c(optarg);
                char* data = pg_strdup(optarg);
                char* val = strchr(data, '=');

                if (val != NULL) {
                    /* remove =; separate data from val */
                    *val = '\0';
                    val++;
                }

                noptions += 1;
                options = (char**)pg_realloc(options, sizeof(char*) * noptions * 2);

                options[(noptions - 1) * 2] = data;
                options[(noptions - 1) * 2 + 1] = val;
                CheckParallelDecoding(data, val);
                CheckBatchSending(data, val);
            }

            break;
            case 'P':
                check_env_value_c(optarg);
                if (plugin) {
                    pfree_ext(plugin);
                }
                plugin = pg_strdup(optarg);
                g_change_plugin = true;
                break;
            case 's':
                check_env_value_c(optarg);
                if (checkIsDigit(optarg) == 0) {
                    fprintf(stderr, _("%s: status interval reset to 0\n"), progname);
                }

                standby_message_timeout = atoi(optarg) * 1000;

                if (standby_message_timeout < 0 || standby_message_timeout > PG_INT32_MAX) {
                    standby_message_timeout = 0;
                }
                break;
            case 'F':
                check_env_value_c(optarg);
                if (checkIsDigit(optarg) == 0) {
                    fprintf(stderr, _("%s: fsync interval reset to 0\n"), progname);
                }

                fsync_interval = atoi(optarg) * 1000;
                if (fsync_interval < 0) {
                    fsync_interval = 0;
                }
                break;
            case 'S':
                check_env_value_c(optarg);
                if (replication_slot) {
                    pfree_ext(replication_slot);
                }
                replication_slot = pg_strdup(optarg);
                break;
            case 'I':
                check_env_value_c(optarg);
                if (sscanf_s(optarg, "%X/%X", &hi, &lo) != 2) {
                    fprintf(stderr, _("%s: could not parse start position \"%s\"\n"), progname, optarg);
                    return -1;
                }
                startpos = ((uint64)hi) << 32 | lo;
                break;
                /* action */
            case 1:
                do_create_slot = true;
                break;
            case 2:
                do_start_slot = true;
                break;
            case 3:
                do_drop_slot = true;
                break;

            default:

                /*
                 * getopt_long already emitted a complaint
                 */
                fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                return -1;
        }
    }

    /*
     * Any non-option arguments?
     */
    if (argc > optind) {
        fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"), progname, argv[optind]);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
        return -1;
    }

    return 0;
}

static void process_free_option(void)
{
    if (outfile != NULL) {
        pfree_ext(outfile);
    }
    if (dbname != NULL) {
        pfree_ext(dbname);
    }
    if (dbhost != NULL) {
        pfree_ext(dbhost);
    }
    if (dbport != NULL) {
        pfree_ext(dbport);
    }
    if (dbuser != NULL) {
        pfree_ext(dbuser);
    }
    if (g_change_plugin || plugin != NULL) {
        pfree_ext(plugin);
        plugin = NULL;
    }
    if (replication_slot != NULL) {
        pfree_ext(replication_slot);
    }
    if (noptions > 0) {
        for (size_t i = 0; i < noptions; i++) {
            pfree_ext(options[i * 2]);
            options[i * 2 + 1] = NULL;
        }
        noptions = 0;
        pfree_ext(options);
    }
}

int main(int argc, char** argv)
{
    PGresult* res = NULL;
    progname = get_progname("pg_recvlogical");
    plugin = pg_strdup("mppdb_decoding");
    int rc = 0;
    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_recvlogical"));

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage();
            exit(0);
        } else if (strcmp(argv[1], "-V") == 0 || strcmp(argv[1], "--version") == 0) {
            showVersion();
            exit(0);
        }
    }

    if (getOptions(argc, argv) == -1) {
        process_free_option();
        exit(1);
    }

    /*
     * Required arguments
     */
    if (replication_slot == NULL) {
        fprintf(stderr, _("%s: no slot specified\n"), progname);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
        exit(1);
    }

    if (do_start_slot && outfile == NULL) {
        fprintf(stderr, _("%s: no target file specified\n"), progname);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
        exit(1);
    }

    if (!do_drop_slot && dbname == NULL) {
        fprintf(stderr, _("%s: no database specified\n"), progname);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
        exit(1);
    }

    if (!do_drop_slot && !do_create_slot && !do_start_slot) {
        fprintf(stderr, _("%s: at least one action needs to be specified\n"), progname);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
        exit(1);
    }

    if (do_drop_slot && (do_create_slot || do_start_slot)) {
        fprintf(stderr, _("%s: --stop cannot be combined with --init or --start\n"), progname);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
        exit(1);
    }

    if (!XLogRecPtrIsInvalid(startpos) && (do_create_slot || do_drop_slot)) {
        fprintf(stderr, _("%s: --startpos cannot be combined with --init or --stop\n"), progname);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
        exit(1);
    }

#ifndef WIN32
    pqsignal(SIGINT, sigint_handler);
    pqsignal(SIGHUP, sighup_handler);
#endif

    /*
     * don't really need this but it actually helps to get more precise error
     * messages about authentication, required GUCs and such without starting
     * to loop around connection attempts lateron.
     */
    {
        conn = GetConnection();
        if (conn == NULL)
            /* Error message already written in GetConnection() */
            exit(1);

        ClearAndFreePasswd();
        /*
         * Run IDENTIFY_SYSTEM so we can get the timeline and current xlog
         * position.
         */
        res = PQexec(conn, "IDENTIFY_SYSTEM");
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr,
                _("%s: could not send replication command \"%s\": %s"),
                progname,
                "IDENTIFY_SYSTEM",
                PQerrorMessage(conn));
            disconnect_and_exit(1);
        }

        if (PQntuples(res) != 1 || PQnfields(res) < 4) {
            fprintf(stderr,
                _("%s: could not identify system: got %d rows and %d fields, expected %d rows and %d or more fields\n"),
                progname,
                PQntuples(res),
                PQnfields(res),
                1,
                4);
            disconnect_and_exit(1);
        }
        PQclear(res);
    }

    /*
     * stop a replication slot
     */
    if (do_drop_slot) {
        char query[256];

        if (verbose)
            fprintf(stderr, _("%s: freeing replication slot \"%s\"\n"), progname, replication_slot);

        rc = snprintf_s(query, sizeof(query), sizeof(query) - 1, "DROP_REPLICATION_SLOT \"%s\"", replication_slot);
        securec_check_ss_c(rc, "\0", "\0");
        res = PQexec(conn, query);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(
                stderr, _("%s: could not send replication command \"%s\": %s"), progname, query, PQerrorMessage(conn));
            disconnect_and_exit(1);
        }

        if (PQntuples(res) != 0 || PQnfields(res) != 0) {
            fprintf(stderr,
                _("%s: could not stop logical rep: got %d rows and %d fields, expected %d rows and %d fields\n"),
                progname,
                PQntuples(res),
                PQnfields(res),
                0,
                0);
            disconnect_and_exit(1);
        }

        PQclear(res);
        disconnect_and_exit(0);
    }

    /*
     * init a replication slot
     */
    if (do_create_slot) {
        char query[256];
        uint32 hi, lo;

        if (verbose)
            fprintf(stderr, _("%s: initializing replication slot \"%s\"\n"), progname, replication_slot);

        rc = snprintf_s(query,
            sizeof(query),
            sizeof(query) - 1,
            "CREATE_REPLICATION_SLOT \"%s\" LOGICAL %s",
            replication_slot,
            plugin);
        securec_check_ss_c(rc, "\0", "\0");

        res = PQexec(conn, query);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(
                stderr, _("%s: could not send replication command \"%s\": %s"), progname, query, PQerrorMessage(conn));
            disconnect_and_exit(1);
        }

        if (PQntuples(res) != 1 || PQnfields(res) != 4) {
            fprintf(stderr,
                _("%s: could not init logical rep: got %d rows and %d fields, expected %d rows and %d fields\n"),
                progname,
                PQntuples(res),
                PQnfields(res),
                1,
                4);
            disconnect_and_exit(1);
        }

        if (sscanf_s(PQgetvalue(res, 0, 1), "%X/%X", &hi, &lo) != 2) {
            fprintf(stderr, _("%s: could not parse log location \"%s\"\n"), progname, PQgetvalue(res, 0, 1));
            disconnect_and_exit(1);
        }
        startpos = ((uint64)hi) << 32 | lo;

        if (replication_slot != NULL) {
            pfree_ext(replication_slot);
        }
        replication_slot = strdup(PQgetvalue(res, 0, 0));
        if (replication_slot == NULL) {
            fprintf(stderr, "out of memory\n");
            disconnect_and_exit(1);
        }
        PQclear(res);
    }

    if (!do_start_slot) {
        disconnect_and_exit(0);
    }

    while (true) {
        StreamLogicalLog();
        if (time_to_abort) {
            /*
             * We've been Ctrl-C'ed. That's not an error, so exit without an
             * errorcode.
             */
            disconnect_and_exit(0);
        } else if (noloop) {
            fprintf(stderr, _("%s: disconnected.\n"), progname);
            exit(1);
        } else {
            fprintf(stderr,
                /* translator: check source for value for %d */
                _("%s: disconnected. Waiting %d seconds to try again.\n"),
                progname,
                RECONNECT_SLEEP_TIME);
            pg_usleep(RECONNECT_SLEEP_TIME * 1000000);
        }
    }
}
