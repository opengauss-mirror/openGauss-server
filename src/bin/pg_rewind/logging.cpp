/* -------------------------------------------------------------------------
 *
 * logging.c
 *	 logging functions
 *
 *	Copyright (c) 2010-2015, PostgreSQL Global Development Group
 *
 * -------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "pg_rewind.h"
#include "logging.h"
#include "filemap.h"
#include "replication/replicainternal.h"
#include "pgtime.h"
#include "common/build_query/build_query.h"
#include "bin/elog.h"
#include "pg_build.h"

#include <sys/time.h>

/* Progress counters */
uint64 fetch_size;
uint64 fetch_done;

static pg_time_t last_progress_report = 0;
static pg_time_t last_caculate_time = 0;
static int old_percent = 0;
static uint64 checkpoint_size = 0;
static uint64 sync_speed = 0;

/* for time-stamp print */
#define FORMATTED_TS_LEN 128
static char formatted_log_time[FORMATTED_TS_LEN];

#define QUERY_ALLOC 8192

#define MESSAGE_WIDTH 60

static void pg_log_v(eLogType type, const char* fmt, va_list ap) __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 0)));

static void setup_formatted_log_time(void)
{
#define MAX_TIME_BUF_LEN 8
    struct timeval tv;
    time_t stamp_time;
    char msbuf[MAX_TIME_BUF_LEN];
    struct tm timeinfo;
    errno_t rc = EOK;

    (void)gettimeofday(&tv, NULL);
    stamp_time = (time_t)tv.tv_sec;
    (void)localtime_r(&stamp_time, &timeinfo);

    (void)strftime(formatted_log_time,
        FORMATTED_TS_LEN,
        /* leave room for milliseconds... */
        "%Y-%m-%d %H:%M:%S     %Z",
        &timeinfo);

    /* 'paste' milliseconds into place... */
    rc = snprintf_s(msbuf, MAX_TIME_BUF_LEN, MAX_TIME_BUF_LEN - 1, ".%03d", (int)(tv.tv_usec / 1000));
    securec_check_ss_c(rc, "", "");

    rc = strncpy_s(formatted_log_time + 19, MAX_TIME_BUF_LEN, msbuf, 4);
    securec_check_c(rc, "", "");
}

static void pg_log_v(eLogType type, const char* fmt, va_list ap)
{
    char message[QUERY_ALLOC];

    int ss_c = vsnprintf_s(message, QUERY_ALLOC, QUERY_ALLOC - 1, fmt, ap);
    securec_check_ss_c(ss_c, "\0", "\0");

    setup_formatted_log_time();

    switch (type) {
        case PG_DEBUG:
            if (debug)
                write_stderr(
                    _("[%s][%d][%s][%s]: %s"), formatted_log_time, process_id, pgxcnodename, progname, message);
            break;

        case PG_PROGRESS:
            if (showprogress)
                write_stderr(
                    _("[%s][%d][%s][%s]: %s"), formatted_log_time, process_id, pgxcnodename, progname, message);
            break;

        case PG_WARNING:
            write_stderr(_("[%s][%d][%s][%s]: %s"), formatted_log_time, process_id, pgxcnodename, progname, message);
            break;

        case PG_ERROR:
            write_stderr(_("[%s][%d][%s][%s]: %s"), formatted_log_time, process_id, pgxcnodename, progname, message);
            write_stderr(_(" \n%s receive ERROR, it will exit\n"), progname);
            if (strcmp(progname, "gs_rewind") == 0) {
                increment_return_code = BUILD_ERROR;
            } else {
                exit(1);
            }
            break;

        /* PG_FATAL only could use in incremental build, DON't USE in any other place */
        case PG_FATAL:
            write_stderr(_("[%s][%d][%s][%s]: %s"), formatted_log_time, process_id, pgxcnodename, progname, message);
            write_stderr(_(" \n%s receive FATAL, it will exit\n"), progname);
            if (strcmp(progname, "gs_rewind") == 0) {
                increment_return_code = BUILD_FATAL;
            } else {
                exit(2);
            }
            break;

        case PG_PRINT:
            write_stderr(_("%s"), message);
            break;

        default:
            break;
    }
    (void)fflush(stdout);
}

void pg_log(eLogType type, const char* fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    pg_log_v(type, fmt, args);
    va_end(args);
}

/*
 * Print an error message, and exit.
 */
void pg_fatal(const char* fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    pg_log_v(PG_FATAL, fmt, args);
    va_end(args);
}

/*
 * Print a progress report based on the global variables.
 *
 * Progress report is written at maximum once per second, unless the
 * force parameter is set to true.
 */
void progress_report(bool force)
{
    int percent;
    pg_time_t now;
    int ss_c = 0;
    GaussState g_state;
    int elapsed_secs = 0;
    int caculate_secs = 0;
    static bool print = true;

    now = (pg_time_t)time(NULL);
    percent = fetch_size ? (int)((fetch_done)*100 / fetch_size) : 0;

    /*
     * report and cacluate speed for every report_timeout or the sync percent changed.
     */

    elapsed_secs = abs(now - last_progress_report);
    if (elapsed_secs < REPORT_TIMEOUT && percent <= old_percent && !force)
        return;

    last_progress_report = now;
    old_percent = percent;

    caculate_secs = abs(now - last_caculate_time);
    if (caculate_secs >= CACULATE_MIN_TIME) {
        sync_speed = (fetch_done - checkpoint_size) / caculate_secs;
        checkpoint_size = fetch_done;
        last_caculate_time = now;
    }

    ss_c = memset_s(&g_state, sizeof(GaussState), 0, sizeof(GaussState));
    securec_check_c(ss_c, "\0", "\0");

    /*
     * Avoid overflowing past 100% or the full size. This may make the total
     * size number change as we approach the end of the backup (the estimate
     * will always be wrong if WAL is included), but that's better than having
     * the done column be bigger than the total.
     */
    if (percent > 100) {
        percent = 100;
    }
    if (fetch_done > fetch_size) {
        fetch_size = fetch_done;
    }

    g_state.mode = STANDBY_MODE;
    g_state.conn_num = 2;
    g_state.state = BUILDING_STATE;
    g_state.sync_stat = false;

    g_state.build_info.build_mode = INC_BUILD;
    g_state.build_info.total_done = fetch_done / 1024;
    g_state.build_info.total_size = fetch_size / 1024;
    g_state.build_info.process_schedule = percent;
    if (sync_speed > 0)
        g_state.build_info.estimated_time = (fetch_size - fetch_done) / sync_speed;
    else
        g_state.build_info.estimated_time = -1;
    UpdateDBStateFile(gaussdb_state_file, &g_state);

    if (print) {
        print = false;
        pg_log(PG_PROGRESS, "receiving and unpacking files...\n");
    }
}

