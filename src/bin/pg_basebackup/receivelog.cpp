/* -------------------------------------------------------------------------
 *
 * receivelog.c - receive transaction log files using the streaming
 *                  replication protocol.
 *
 * Author: Magnus Hagander <magnus@hagander.net>
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *          src/bin/pg_basebackup/receivelog.c
 * -------------------------------------------------------------------------
 */

/*
 * We have to use postgres.h not postgres_fe.h here, because there's so much
 * backend-only stuff in the XLOG include files we need.  But we need a
 * frontend-ish environment otherwise.    Hence this ugly hack.
 */
#define FRONTEND 1
#include "postgres.h"
#include "knl/knl_variable.h"
#include "libpq/libpq-fe.h"
#include "access/xlog_internal.h"
#include "replication/walprotocol.h"

#include "receivelog.h"
#include "streamutil.h"

#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "access/xlog_internal.h"
#include "bin/elog.h"
#include "catalog/pg_tablespace.h"
#include "replication/dataqueue.h"
#include "replication/walprotocol.h"
#include "utils/datetime.h"
#include <time.h>
#include <signal.h>
#include <unistd.h>

/* Size of the streaming replication protocol headers */
#define STREAMING_HEADER_SIZE (1 + sizeof(WalDataMessageHeader))
#define STREAMING_KEEPALIVE_SIZE (1 + sizeof(PrimaryKeepaliveMessage))

static TimestampTz last_recv_timestamp = 0;
static bool ping_sent = false;
static bool reportFlushPosition = false;
static XLogRecPtr lastFlushPosition = InvalidXLogRecPtr;
extern char* basedir;
extern int standby_message_timeout;

#define HEART_BEAT_INIT 0
#define HEART_BEAT_RUN 1
#define HEART_BEAT_STOP 2

/*
 * The max size for single data file. copy from custorage.cpp.
 */
const uint64 MAX_FILE_SIZE = (uint64)RELSEG_SIZE * BLCKSZ;

extern int standby_recv_timeout;
extern bool cascade_standby;
const int HEART_BEAT = 5;
PGconn* xlogconn = NULL;
pthread_t hearbeatTimerId;
volatile uint32 timerFlag = 0;
volatile uint32 heartbeatRunning = HEART_BEAT_INIT;
pthread_mutex_t heartbeatMutex;
pthread_mutex_t heartbeatQuitMutex;
pthread_cond_t heartbeatQuitCV;

typedef enum { DO_WAL_DATA_WRITE_DONE, DO_WAL_DATA_WRITE_STOP, DO_WAL_DATA_WRITE_ERROR } DoWalDataWriteResult;
static TimestampTz localGetCurrentTimestamp(void);
static bool sendReplyToSender(PGconn* conn, TimestampTz nowtime, bool replyRequested);
static bool checkForReceiveTimeout(PGconn* conn);
static long CalculateCopyStreamSleeptime(TimestampTz now, int standby_message_timeout_local,
                                         TimestampTz last_status);
/*
 * Calculate how long send/receive loops should sleep
 */
static long
CalculateCopyStreamSleeptime(TimestampTz now, int standby_message_timeout_local,
                             TimestampTz last_status)
{
    TimestampTz status_targettime = 0;

    if (standby_message_timeout_local)
        status_targettime = last_status +
            (standby_message_timeout_local - 1) * (USECS_PER_SEC);

    if (status_targettime > 0)
    {
        long        secs;
        long        usecs;

        feTimestampDifference(now,
                              status_targettime,
                              &secs,
                              &usecs);
        /* Always sleep at least 1 sec */
        if (secs <= 0)
        {
            secs = 1;
            usecs = 0;
        }
        return secs;
    } else {
        return (-1);
    }
}

/*
 * Open a new WAL file in the specified directory. Store the name
 * (not including the full directory) in namebuf. Assumes there is
 * enough room in this buffer...
 *
 * The file will be padded to 16Mb with zeroes.
 */
static int open_walfile(XLogRecPtr startpoint, uint32 timeline, const char* basedir, char* namebuf)
{
/* the max real path length in linux is 4096, adapt this for realpath func */
#define MAX_REALPATH_LEN 4096
    char fn[MAXPGPATH];
    struct stat statbuf;
    char* zerobuf = NULL;
    int bytes = 0;
    char* retVal = NULL;
    char Lrealpath[MAX_REALPATH_LEN + 1] = {0};

    int nRet = snprintf_s(namebuf,
        MAXFNAMELEN,
        MAXFNAMELEN - 1,
        "%08X%08X%08X",
        timeline,
        (uint32)((startpoint / XLogSegSize) / XLogSegmentsPerXLogId),
        (uint32)((startpoint / XLogSegSize) % XLogSegmentsPerXLogId));
    securec_check_ss_c(nRet, "", "");

    nRet = snprintf_s(fn, sizeof(fn), sizeof(fn) - 1, "%s/%s.partial", basedir, namebuf);
    securec_check_ss_c(nRet, "", "");

    retVal = realpath(fn, Lrealpath);
    if (retVal == NULL && '\0' == Lrealpath[0]) {
        pg_log(PG_PRINT, _("%s: realpath WAL segment path %s failed : %s\n"), progname, Lrealpath, strerror(errno));
    }

    int f = open(Lrealpath, O_WRONLY | O_CREAT | PG_BINARY, S_IRUSR | S_IWUSR);
    if (f == -1) {
        pg_log(PG_PRINT, _("%s: Could not open WAL segment %s: %s\n"), progname, Lrealpath, strerror(errno));
        return -1;
    }

    /*
     * Verify that the file is either empty (just created), or a complete
     * XLogSegSize segment. Anything in between indicates a corrupt file.
     */
    if (fstat(f, &statbuf) != 0) {
        pg_log(PG_PRINT, _("%s: could not stat WAL segment %s: %s\n"), progname, Lrealpath, strerror(errno));
        if (close(f) != 0) {
            pg_log(PG_PRINT, _("%s: close file failed %s: %s\n"), progname, Lrealpath, strerror(errno));
        }
        f = -1;
        return -1;
    }
    if (statbuf.st_size == (off_t)XLogSegSize)
        return f; /* File is open and ready to use */
    if (statbuf.st_size != 0) {
        pg_log(PG_PRINT,
            _("%s: WAL segment %s is %d bytes, should be 0 or %lu\n"),
            progname,
            Lrealpath,
            (int)statbuf.st_size,
            XLogSegSize);
        if (close(f) != 0) {
            pg_log(PG_PRINT, _("%s: close file failed %s: %s\n"), progname, Lrealpath, strerror(errno));
        }
        f = -1;
        return -1;
    }

    /* New, empty, file. So pad it to 16Mb with zeroes */
    zerobuf = (char*)xmalloc0(XLOG_BLCKSZ);
    for (bytes = 0; bytes < (int)XLogSegSize; bytes += XLOG_BLCKSZ) {
        if (write(f, zerobuf, XLOG_BLCKSZ) != XLOG_BLCKSZ) {
            pg_log(PG_PRINT, _("%s: could not pad WAL segment %s: %s\n"), progname, Lrealpath, strerror(errno));
            if (close(f) != 0) {
                pg_log(PG_PRINT, _("%s: close file failed %s: %s\n"), progname, Lrealpath, strerror(errno));
            }
            f = -1;
            if (unlink(Lrealpath) != 0) {
                pg_log(PG_PRINT, _("%s: unlink file failed %s: %s\n"), progname, Lrealpath, strerror(errno));
            }
            free(zerobuf);
            zerobuf = NULL;
            return -1;
        }
    }
    free(zerobuf);
    zerobuf = NULL;

    if (lseek(f, SEEK_SET, 0) != 0) {
        pg_log(PG_PRINT,
            _("%s: could not seek back to beginning of WAL segment %s: %s\n"),
            progname,
            Lrealpath,
            strerror(errno));
        if (close(f) != 0) {
            pg_log(PG_PRINT, _("%s: close file failed %s: %s\n"), progname, Lrealpath, strerror(errno));
        }
        f = -1;
        return -1;
    }
    return f;
}

static void heartbeatSleep(void)
{
    struct timespec time_to_wait;
    int res = 0;

    pthread_mutex_lock(&heartbeatQuitMutex);
    clock_gettime(CLOCK_REALTIME, &time_to_wait);
    time_to_wait.tv_sec += HEART_BEAT;
    res = pthread_cond_timedwait(&heartbeatQuitCV, &heartbeatQuitMutex, &time_to_wait);
    pthread_mutex_unlock(&heartbeatQuitMutex);
}

/*
 * This is timer handler.
 * Sending keepalive packet during the walreceiver running.
 * Avoid unnecessary timeouts.
 */
void* heartbeatTimerHandler(void* data)
{
    if (xlogconn == NULL) {
        return NULL;
    }
    uint32 expected = HEART_BEAT_INIT;
    (void)pg_atomic_compare_exchange_u32(&heartbeatRunning, &expected, HEART_BEAT_RUN);
    while (pg_atomic_read_u32(&heartbeatRunning) == HEART_BEAT_RUN) {
        pthread_mutex_lock(&heartbeatMutex);
        (void)checkForReceiveTimeout(xlogconn);
        ping_sent = false;
        pthread_mutex_unlock(&heartbeatMutex);
        heartbeatSleep();
    }
    return NULL;
}

/*
 * Create a POSIX thread for sending keepalive packet.
 * Avoid unnecessary timeouts.
 */
void createHeartbeatTimer(void)
{
    timerFlag = 0;
    pthread_mutex_init(&heartbeatMutex, NULL);
    pthread_mutex_lock(&heartbeatMutex);
    if (pthread_create(&hearbeatTimerId, NULL, heartbeatTimerHandler, NULL) != 0) {
        pg_fatal("createHeartbeatTimer failed pid %lu.\n", hearbeatTimerId);
        return;
    }
    return;
}

/* Resume heartbeat timer, use atomic function for all CPU system */
void resumeHeartBeatTimer(void)
{
    pthread_mutex_unlock(&heartbeatMutex);
}

/*
 * Suspend heartbeat timer, use mutex to control the timer thread.
 * It will return after send finished.
 */
void suspendHeartBeatTimer(void)
{
    /* wait for send finished */
    pthread_mutex_lock(&heartbeatMutex);
}

void closeHearBeatTimer(void)
{
    pg_atomic_write_u32(&heartbeatRunning, HEART_BEAT_STOP);
    pthread_mutex_unlock(&heartbeatMutex);
    pthread_mutex_lock(&heartbeatQuitMutex);
    pthread_cond_signal(&heartbeatQuitCV);
    pthread_mutex_unlock(&heartbeatQuitMutex);
    (void)pthread_join(hearbeatTimerId, NULL);
    return;
}

/*
 * Close the current WAL file, and rename it to the correct filename if it's complete.
 *
 * If segment_complete is true, rename the current WAL file even if we've not
 * completed writing the whole segment.
 */
static bool close_walfile(int walfile, const char* basedir, char* walname, bool segment_complete, XLogRecPtr pos)
{
    off_t currpos = lseek(walfile, 0, SEEK_CUR);

    if (currpos == -1) {
        pg_log(PG_PRINT, _("%s: could not get current position in file \"%s/%s\": %s\n"), progname, basedir, walname,
            strerror(errno));
        return false;
    }

    if (fsync(walfile) != 0) {
        pg_log(PG_PRINT, _("%s: could not fsync file \"%s/%s\": %s\n"), progname, basedir, walname, strerror(errno));
        return false;
    }

    if (close(walfile) != 0) {
        pg_log(PG_PRINT, _("%s: could not close file \"%s/%s\": %s\n"), progname, basedir, walname, strerror(errno));
        return false;
    }

    /*
     * Rename the .partial file only if we've completed writing the
     * whole segment or segment_complete is true.
     */
    if (currpos == (off_t)XLogSegSize || segment_complete) {
        char oldfn[MAXPGPATH];
        char newfn[MAXPGPATH];
        errno_t nRet;

        nRet = snprintf_s(oldfn, sizeof(oldfn), sizeof(oldfn) - 1, "%s/%s.partial", basedir, walname);
        securec_check_ss_c(nRet, "", "");
        nRet = snprintf_s(newfn, sizeof(newfn), sizeof(newfn) - 1, "%s/%s", basedir, walname);
        securec_check_ss_c(nRet, "", "");
        if (rename(oldfn, newfn) != 0) {
            pg_log(PG_PRINT, _("%s: could not rename file \"%s/%s\": %s\n"), progname, basedir, walname,
                strerror(errno));
            return false;
        }
    } else {
        pg_log(PG_PRINT, _("%s: not renaming \"%s/%s\", segment is not complete.\n"), progname, basedir, walname);
    }

    lastFlushPosition = pos;
    return true;
}

/*
 * Local version of GetCurrentTimestamp(), since we are not linked with
 * backend code.
 */
static TimestampTz localGetCurrentTimestamp(void)
{
    TimestampTz result;
    struct timeval tp;

    (void)gettimeofday(&tp, NULL);

    result = (TimestampTz)tp.tv_sec - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

#ifdef HAVE_INT64_TIMESTAMP
    result = (result * USECS_PER_SEC) + tp.tv_usec;
#else
    result = result + (tp.tv_usec / 1000000.0);
#endif

    return result;
}

/*
 * Local version of TimestampDifferenceExceeds(), since we are not
 * linked with backend code.
 */
static bool localTimestampDifferenceExceeds(TimestampTz start_time, TimestampTz stop_time, int msec)
{
    TimestampTz diff = stop_time - start_time;

#ifdef HAVE_INT64_TIMESTAMP
    return (diff >= msec * INT64CONST(1000));
#else
    return (diff * 1000.0 >= msec);
#endif
}

/*
 * @@GaussDB@@
 * Brief        : Send reply to Sender task.
 * Description    :
 *         replyRequested is used to decide whether any immediate reply is expected
 *         from sender
 * Notes        :
 */
static bool sendReplyToSender(PGconn* conn, TimestampTz nowtime, bool replyRequested)
{
    /* Time to send feedback! */
    char replybuf[sizeof(StandbyReplyMessage) + 1];
    StandbyReplyMessage* replymsg = (StandbyReplyMessage*)(replybuf + 1);

    replymsg->receive = InvalidXLogRecPtr;
    replymsg->write = InvalidXLogRecPtr;
    if (reportFlushPosition)
        replymsg->flush = lastFlushPosition;
    else
        replymsg->flush = InvalidXLogRecPtr;
    replymsg->apply = InvalidXLogRecPtr;
    replymsg->sendTime = nowtime;
    replymsg->replyRequested = replyRequested;
    replymsg->peer_role = STANDBY_MODE;
    replymsg->peer_state = BUILDING_STATE;
    replybuf[0] = 'r';

    if (PQputCopyData(conn, replybuf, sizeof(replybuf)) <= 0 || PQflush(conn)) {
        pg_log(PG_PRINT, _("%s: could not send feedback packet: %s"), progname, PQerrorMessage(conn));
        return false;
    }

    return true;
}

/*
 * @@GaussDB@@
 * Brief        : check for receive timeout
 * Description    :
 *         Check if configured timeout has reached without receiving anything from server. If yes then assume that
 * connection broken. If timeout has not reached but half of timeout has reached without receiving anything, then send a
 * message to server along with request for an immediate reply.
 */
static bool checkForReceiveTimeout(PGconn* conn)
{
    /*
     * Check if time since last receive from master has reached the
     * configured limit.
     */
    if (standby_message_timeout > 0) {
        TimestampTz nowtime = localGetCurrentTimestamp();

        /*
         * We didn't receive anything new, for half of receiver
         * replication timeout. Ping the server.
         */
        if (localTimestampDifferenceExceeds(last_recv_timestamp, nowtime, (standby_message_timeout / 2))) {
            if (ping_sent == false) {
                if (sendReplyToSender(conn, nowtime, true) == false) {
                    return false;
                }
                ping_sent = true;
                last_recv_timestamp = nowtime;
            } else {
                pg_log(PG_PRINT, _("\nterminating XLogStream receiver due to timeout\n"));
                return false;
            }
        }
    }

    return true;
}

/* do xlog write */
static int DoWALWrite(const char* wal_buf, int len, XLogRecPtr& block_pos, const char* basedir, char* cur_wal_file,
    uint32 wal_file_timeline, int& walfile, stream_stop_callback stream_stop, PGconn* conn)
{
    int xlogoff = block_pos % XLogSegSize;
    int bytes_left = len;
    int bytes_to_write = 0;

    /*
     * Verify that the initial location in the stream matches where we
     * think we are.
     */
    if (walfile == -1) {
        /* No file open yet */
        if (xlogoff != 0) {
            pg_log(PG_PRINT, _("%s: received xlog record for offset %u with no file open\n"), progname, xlogoff);
            return DO_WAL_DATA_WRITE_ERROR;
        }
    } else {
        /* More data in existing segment */
        /* XXX: store seek value don't reseek all the time */
        if (lseek(walfile, 0, SEEK_CUR) != xlogoff) {
            pg_log(PG_PRINT,
                _("%s: got WAL data offset %08x, expected %08x\n"),
                progname,
                xlogoff,
                (int)lseek(walfile, 0, SEEK_CUR));
            return DO_WAL_DATA_WRITE_ERROR;
        }
    }

    int bytes_written = 0;
    resumeHeartBeatTimer();
    while (bytes_left) {

        /* If crossing a WAL boundary, only write up until we reach XLOG_SEG_SIZE. */
        if (xlogoff + bytes_left > (int)XLogSegSize)
            bytes_to_write = (int)XLogSegSize - xlogoff;
        else
            bytes_to_write = bytes_left;

        if (walfile == -1) {
            walfile = open_walfile(block_pos, wal_file_timeline, basedir, cur_wal_file);
            if (walfile == -1) {
                suspendHeartBeatTimer();
                /* Error logged by open_walfile */
                return DO_WAL_DATA_WRITE_ERROR;
            }
        }

        if (write(walfile, wal_buf + bytes_written, bytes_to_write) != bytes_to_write) {
            pg_log(PG_PRINT,
                _("%s: could not write %u bytes to WAL file %s: %s\n"),
                progname,
                bytes_to_write,
                cur_wal_file,
                strerror(errno));
            suspendHeartBeatTimer();
            return DO_WAL_DATA_WRITE_ERROR;
        }
        /* Write was successful, advance our position */
        bytes_written += bytes_to_write;
        bytes_left -= bytes_to_write;
        XLByteAdvance(block_pos, bytes_to_write);
        xlogoff += bytes_to_write;
        lastFlushPosition = block_pos;

        /* Did we reach the end of a WAL segment? */
        if (block_pos % XLogSegSize == 0) {
            if (!close_walfile(walfile, basedir, cur_wal_file, false, block_pos)) {
                suspendHeartBeatTimer();
                /* Error message written in close_walfile() */
                return DO_WAL_DATA_WRITE_ERROR;
            }

            walfile = -1;
            xlogoff = 0;

            if (stream_stop != NULL) {
                /*
                 * Callback when the segment finished, and return if it
                 * told us to.
                 */
                if (stream_stop(block_pos, wal_file_timeline, true)) {
                    suspendHeartBeatTimer();
                    return DO_WAL_DATA_WRITE_STOP;
                }
            }
        }
    }

    /* No more data left to write, start receiving next copy packet */
    suspendHeartBeatTimer();
    return DO_WAL_DATA_WRITE_DONE;
}

/*
 * Receive a log stream starting at the specified position.
 *
 * If sysidentifier is specified, validate that both the system
 * identifier and the timeline matches the specified ones
 * (by sending an extra IDENTIFY_SYSTEM command)
 *
 * All received segments will be written to the directory
 * specified by basedir.
 *
 * The stream_stop callback will be called every time data
 * is received, and whenever a segment is completed. If it returns
 * true, the streaming will stop and the function
 * return. As long as it returns false, streaming will continue
 * indefinitely.
 *
 * standby_message_timeout controls how often we send a message
 * back to the master letting it know our progress, in seconds.
 * This message will only contain the write location, and never
 * flush or replay.
 *
 * Note: The log position *must* be at a log segment start!
 */
bool ReceiveXlogStream(PGconn* conn, XLogRecPtr startpos, uint32 timeline, const char* sysidentifier,
    const char* basedir, stream_stop_callback stream_stop, int standby_message_timeout_local, bool rename_partial)
{
    char query[MAXPGPATH];
    char slotcmd[MAXPGPATH];
    char current_walfile_name[MAXPGPATH];
    PGresult* res = NULL;
    char* copybuf = NULL;
    char* wal_data_buf = NULL;
    int walfile = -1;
    int64 last_status = -1;
    XLogRecPtr blockpos = InvalidXLogRecPtr;
    PrimaryKeepaliveMessage keepalive;
    errno_t ss_c = EOK;
#ifndef WIN32
    pid_t my_p_pid;
    my_p_pid = getppid();
#endif
    xlogconn = conn;
    if (replication_slot != NULL) {
        /*
         * Report the flush position, so the primary can know what WAL we'll
         * possibly re-request, and remove older WAL safely.
         *
         * We only report it when a slot has explicitly been used, because
         * reporting the flush position makes one elegible as a synchronous
         * replica. People shouldn't include generic names in
         * synchronous_standby_names, but we've protected them against it so
         * far, so let's continue to do so in the situations when possible.
         * If they've got a slot, though, we need to report the flush position,
         * so that the master can remove WAL.
         */

        ss_c = snprintf_s(slotcmd, MAXPGPATH, MAXPGPATH - 1, "SLOT \"%s\" ", replication_slot);
        securec_check_ss_c(ss_c, "", "");
    } else {
        slotcmd[0] = 0;
    }
    reportFlushPosition = true;
    if (sysidentifier != NULL) {
        /* Validate system identifier and timeline hasn't changed */
        res = PQexec(conn, "IDENTIFY_SYSTEM");
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            pg_log(PG_WARNING, _(" could not identify system: %s\n"), PQerrorMessage(conn));
            PQclear(res);
            return false;
        }
        if (PQnfields(res) != 4 || PQntuples(res) != 1) {
            pg_log(PG_WARNING,
                _(" could not identify system, got %d rows and %d fields\n"),
                PQntuples(res),
                PQnfields(res));
            PQclear(res);
            return false;
        }
        if (strcmp(sysidentifier, PQgetvalue(res, 0, 0)) != 0) {
            pg_log(PG_WARNING, _(" system identifier does not match between base backup and streaming connection\n"));
            PQclear(res);
            return false;
        }
        if (timeline != (unsigned int)atoi(PQgetvalue(res, 0, 1))) {
            pg_log(PG_WARNING, _(" timeline does not match between base backup and streaming connection\n"));
            PQclear(res);
            return false;
        }
        PQclear(res);
    }

    fprintf(stderr, "%100s", "");
    fprintf(stderr, "\r");
    pg_log(PG_PROGRESS, _(" check identify system success\n"));

    /* Initiate the replication stream at specified location */
    ss_c = snprintf_s(query,
        MAXPGPATH,
        MAXPGPATH - 1,
        "START_REPLICATION %s%X/%X",
        slotcmd,
        (uint32)(startpos >> 32),
        (uint32)startpos);
    securec_check_ss_c(ss_c, "", "");
    res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        pg_log(PG_WARNING, _(" could not start replication: %s\n"), PQresultErrorMessage(res));
        PQclear(res);
        return false;
    }
    PQclear(res);

    fprintf(stderr, "%100s", "");
    fprintf(stderr, "\r");
    pg_log(PG_PROGRESS, _(" send %s success\n"), query);
    /* Set the last reply timestamp */
    last_recv_timestamp = localGetCurrentTimestamp();
    ping_sent = false;

    /*
     * initialize flush position to starting point, it's the caller's
     * responsibility that that's sane.
     */
    lastFlushPosition = startpos;
    createHeartbeatTimer();
    /*
     * Receive the actual xlog data
     */
    while (1) {
        int r;
        int bytes_left;
        int64 local_now = localGetCurrentTimestamp();
        errno_t ret = EOK;

#ifndef WIN32
        if (getppid() != my_p_pid) {
            goto error;
        }
#endif
        if (copybuf != NULL) {
            PQfreemem(copybuf);
            copybuf = NULL;
        }

        /*
         * Check if we should continue streaming, or abort at this point.
         */
        if ((stream_stop != NULL) && stream_stop(blockpos, timeline, false)) {
            closeHearBeatTimer();
            xlogconn = NULL;
            if (walfile != -1)
                /* Potential error message is written by close_walfile */
                return close_walfile(walfile, basedir, current_walfile_name, rename_partial, blockpos);
            return true;
        }

        /*
         * Potentially send a status message to the master
         */
        if (checkForReceiveTimeout(conn) == false) {
            goto error;
        }

        r = PQgetCopyData(conn, &copybuf, 1);
        if (r == 0) {
            /*
             * In async mode, and no data available. We block on reading but
             * not more than the specified timeout, so that we can send a
             * response back to the client.
             */
            fd_set input_mask;
            struct timeval timeout;
            struct timeval* timeoutptr = NULL;

            FD_ZERO(&input_mask);
            FD_SET(PQsocket(conn), &input_mask);
            if (standby_message_timeout_local) {
#ifdef HAVE_INT64_TIMESTAMP
            timeout.tv_sec = CalculateCopyStreamSleeptime(local_now,
                                                          standby_message_timeout_local / 2,
                                                          last_status);
#else
            timeout.tv_sec = last_status + (standby_message_timeout_local / 2) - local_now - 1;
#endif
                if (timeout.tv_sec <= 0)
                    timeout.tv_sec = 1; /* Always sleep at least 1 sec */
                timeout.tv_usec = 0;
                timeoutptr = &timeout;
            } else {
                timeoutptr = NULL;
            }

            r = select(PQsocket(conn) + 1, &input_mask, NULL, NULL, timeoutptr);
            if (r == 0 || (r < 0 && errno == EINTR)) {
                /*
                 * Got a timeout or signal. Before Continuing the loop, check for timeout.
                 * and then either deliver a status packet to the server or just go back into
                 * blocking.
                 */
                if (checkForReceiveTimeout(conn) == false) {
                    goto error;
                }
                /* Set the last reply timestamp */
                ping_sent = false;
                continue;
            } else if (r < 0) {
                pg_log(PG_WARNING, _(" select() failed: %m\n"));
                goto error;
            }
            /* Else there is actually data on the socket */
            if (PQconsumeInput(conn) == 0) {
                pg_log(PG_WARNING,
                    _(" Could not receive data from WAL stream, please check network state and configuration, e.g "
                      "timeout setting: %s\n"),
                    PQerrorMessage(conn));
                goto error;
            }

            /* Set the last reply timestamp */
            ping_sent = false;

            continue;
        }
        if (r == -1) {
            /* End of copy stream */
            break;
        }
        if (r == -2) {
            pg_log(PG_WARNING,
                _(" Could not read copy data, please check network state and configuration: %s\n"),
                PQerrorMessage(conn));
            goto error;
        }

        ping_sent = false;

        if (copybuf[0] == 'k') {
            /*
             * keepalive message, sent in 9.2 and newer. We just ignore
             * this message completely, but need to skip past it in the
             * stream.
             */
            if (r != STREAMING_KEEPALIVE_SIZE) {
                pg_log(PG_WARNING, _(" keepalive message is incorrect size: %d\n"), r);
                goto error;
            }
            fprintf(stderr, "%100s", "");
            fprintf(stderr, "\r");
            pg_log(PG_PRINT, _(" keepalive message is received\n"));
            /* copy the received buffer to keepalive */
            ret = memcpy_s(&keepalive, sizeof(PrimaryKeepaliveMessage), copybuf + 1, sizeof(PrimaryKeepaliveMessage));
            securec_check(ret, "\0", "\0");

            /* If as part of keepalive message from sender, an immediate reply is requested
             * then send the same to sender.
             */
            if (keepalive.replyRequested) {
                local_now = localGetCurrentTimestamp();
                if (sendReplyToSender(conn, local_now, false) == false) {
                    goto error;
                }

                last_status = local_now;
            }

            continue;
        } else if (copybuf[0] == 'm') {
            /* just ignore configure file sent from primary */
            pg_log(PG_WARNING, _("skip gaussdb config file while receiving xlog.\n"));
            continue;
        } else if (copybuf[0] != 'w') {
            pg_log(PG_WARNING, _(" unrecognized streaming header: \"%c\".\n"), copybuf[0]);
            goto error;
        }

        if (r < (int)STREAMING_HEADER_SIZE) {
            pg_log(PG_WARNING, _(" wal streaming header is too small: %d.\n"), r);
            goto error;
        } else if (r == (int)STREAMING_HEADER_SIZE) {
            pg_log(PG_WARNING, _(" wal streaming body is empty.\n"));
            continue;
        }

        /* Extract WAL location for this block */
        ret = memcpy_s(&blockpos, sizeof(XLogRecPtr), copybuf + 1, sizeof(XLogRecPtr));
        securec_check_c(ret, "", "");

        Assert(!XLogRecPtrIsInvalid(blockpos));

        wal_data_buf = copybuf + STREAMING_HEADER_SIZE;
        bytes_left = r - STREAMING_HEADER_SIZE;

        ret = DoWALWrite((const char*)wal_data_buf,
            bytes_left,
            blockpos,
            (const char*)basedir,
            current_walfile_name,
            timeline,
            walfile,
            stream_stop,
            conn);

        if (DO_WAL_DATA_WRITE_ERROR == ret)
            goto error;

        else if (DO_WAL_DATA_WRITE_STOP == ret) {
            PQfreemem(copybuf);
            copybuf = NULL;
            closeHearBeatTimer();
            xlogconn = NULL;
            if (walfile != -1)
                /* Potential error message is written by close_walfile */
                return close_walfile(walfile, basedir, current_walfile_name, rename_partial, blockpos);

            return true;
        }

        /* for return DO_XLOG_WRITE_DONE we keep writing the wal files. */
    }

    /*
     * The only way to get out of the loop is if the server shut down the
     * replication stream. If it's a controlled shutdown, the server will send
     * a shutdown message, and we'll return the latest xlog location that has
     * been streamed.
     */

    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        pg_log(PG_WARNING, _(" unexpected termination of replication stream: %s.\n"), PQresultErrorMessage(res));
        PQclear(res);
        goto error;
    }
    if (copybuf != NULL) {
        PQfreemem(copybuf);
        copybuf = NULL;
    }
    PQclear(res);

    if (walfile != -1 && close(walfile) != 0)
        pg_log(PG_WARNING, _(" could not close file \"%s/%s\": %s.\n"), basedir, current_walfile_name, strerror(errno));
    walfile = -1;

    closeHearBeatTimer();
    xlogconn = NULL;
    return true;

error:
    closeHearBeatTimer();
    xlogconn = NULL;
    if (copybuf != NULL) {
        PQfreemem(copybuf);
        copybuf = NULL;
    }
    if (walfile != -1 && close(walfile) != 0)
        pg_log(PG_WARNING, _(" could not close file \"%s/%s\": %s.\n"), basedir, current_walfile_name, strerror(errno));
    walfile = -1;
    return false;
}
