/* -------------------------------------------------------------------------
 *
 * pg_basebackup.c - receive a base backup using streaming replication protocol
 *
 * Author: Magnus Hagander <magnus@hagander.net>
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *          src/bin/pg_basebackup/pg_basebackup.c
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

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/time.h>

#ifdef HAVE_LIBZ
#include "zlib.h"
#endif

#include "libpq/pqsignal.h"
#include "pgtime.h"
#include "getopt_long.h"
#include "receivelog.h"
#include "streamutil.h"

#include "pg_build.h"
#include "backup.h"
#include "logging.h"

#include "bin/elog.h"
#include "file_ops.h"
#include "catalog/catalog.h"

#ifdef ENABLE_MOT
#include "fetchmot.h"
#endif

/* Maximum number of digit in integer. Used to allocate memory to copy int to string */
#define MAX_INT_SIZE 20
/* set build receive timeout during master getting in backup mode */
#define BUILD_RW_TIMEOUT 600

#define BUILD_PATH_LEN 2560 /* (MAXPGPATH*2 + 512) */
#define FORMATTED_TS_LEN 128

/* Global options */
char* basedir = NULL;
char format = 'p'; /* p(lain)/t(ar) */
char* label = "gs_ctl full build";
bool showprogress = true;
int verbose = 1;
int compresslevel = 0;
bool includewal = true;
bool streamwal = true;
/* modified checkpoint mode during build */
bool fastcheckpoint = true;

int standby_message_timeout = 10;  /* 10 sec = default */
int standby_recv_timeout = 120;    /* 120 sec = default */
int standby_connect_timeout = 120; /* 120 sec = default */

#define REPORT_TIMEOUT 30   /* report and calculate sync speed every 30s */
#define CACULATE_MIN_TIME 2 /* calculate sync speed at least every 2s */
#define BACKUP_LABEL_FILE "backup_label"
#define BACKUP_LABEL_FILE_ROACH "backup_label.roach"
#define FULL_BACKUP_LABEL_FILE "full_backup_label"
#define MAXFNAMELEN 64

static pg_time_t last_progress_report = 0;
static pg_time_t last_caculate_time = 0;
static int old_percent = 0;
static uint64 checkpoint_size = 0;
static uint64 sync_speed = 0;
static char remotelsn[MAXPGPATH] = {0};      /* LSN of remote node */
static char remotenodename[MAXPGPATH] = {0}; /* name of remote node */
static char conf_file[MAXPGPATH] = {0};
static char buildstart_file[MAXPGPATH] = {0};
static char builddone_file[MAXPGPATH] = {0};
static char build_label_file[MAXPGPATH] = {0};

/* Progress counters */
static uint64 totalsize = 0;
static uint64 totaldone = 0;

/* Pipe to communicate with background wal receiver process */
#ifndef WIN32
int bgpipe[2] = {-1, -1};
#endif

/* Handle to child process */
pid_t bgchild = -1;

volatile sig_atomic_t build_interrupted = false;

/* End position for xlog streaming, empty string if unknown yet */
static XLogRecPtr xlogendptr;
#ifndef WIN32
static int has_xlogendptr = 0;
#else
static volatile LONG has_xlogendptr = 0;
#endif

static void BuildReaper(SIGNAL_ARGS);
extern void UpdateDBStateFile(char* path, GaussState* state);
static void disconnect_and_exit(int code);
static void verify_dir_is_empty_or_create(char* dirname);
static void removeCreatedTblspace(void);
static void progress_report(int tablespacenum, const char* filename, bool force);
static void ReceiveAndUnpackTarFile(PGconn* conn, PGresult* res, int rownum);
static void BaseBackup(const char* dirname, uint32 term = 0);
static bool reached_end_position(XLogRecPtr segendpos, uint32 timeline, bool segment_finished);
void backup_incremental_xlog(char* dir);
static void create_backup_label(const char* dirname, const char* startsysid, TimeLineID starttli);
static void xlog_streamer_backup(const char* dirname);
static XLogRecPtr read_full_backup_label(
    const char* dirname, char* sysid, uint32 sysid_len, char* tline, uint32 tline_len);
static int replace_node_name(char* sSrc, const char* sMatchStr, const char* sReplaceStr);
static void show_full_build_process(const char* errmg);
static void backup_dw_file(const char* target_dir);
void get_xlog_location(char (&xlog_location)[MAXPGPATH]);

/*
 * tblspaceDirectory is used for saving the table space directory created by
 * full-build. tblspaceNum is the count of table space. The table space directory
 * should be empty before executing full-build, so when full-build failed, all
 * the directories should be removed. If not so ,the next full-build maybe fail
 * because some directory is not empty.
 */
static char** tblspaceDirectory = NULL;
static char** tblspaceParentDirectory = NULL;
static int tblspaceCount = 0;
static int tblspaceIndex = 0;

#define TBLESPACE_LIST_CREATE()                                                                                   \
    if (tblspaceCount > 0) {                                                                                      \
        errno_t rcm = 0;                                                                                          \
        tblspaceDirectory = (char**)malloc(tblspaceCount * sizeof(char*));                                        \
        tblspaceParentDirectory = (char**)malloc(tblspaceCount * sizeof(char*));                                  \
        if (NULL == tblspaceDirectory || NULL == tblspaceParentDirectory) {                                       \
            if (NULL != tblspaceDirectory)                                                                        \
                free(tblspaceDirectory);                                                                          \
            if (NULL != tblspaceParentDirectory)                                                                  \
                free(tblspaceParentDirectory);                                                                    \
            tblspaceDirectory = NULL;                                                                             \
            tblspaceParentDirectory = NULL;                                                                       \
            pg_log(PG_WARNING, _(" Out of memory occured during creating tablespace list"));                      \
            exit(1);                                                                                              \
        }                                                                                                         \
        rcm = memset_s(tblspaceDirectory, tblspaceCount * sizeof(char*), 0, tblspaceCount * sizeof(char*));       \
        securec_check_c(rcm, "", "");                                                                             \
        rcm = memset_s(tblspaceParentDirectory, tblspaceCount * sizeof(char*), 0, tblspaceCount * sizeof(char*)); \
        securec_check_c(rcm, "", "");                                                                             \
    }

#define TABLESPACE_LIST_RELEASE()                     \
    if (tblspaceDirectory != NULL) {                  \
        int k;                                        \
        for (k = 0; k < tblspaceCount; k++) {         \
            if (tblspaceDirectory[k] != NULL) {       \
                free(tblspaceDirectory[k]);           \
                tblspaceDirectory[k] = NULL;          \
            }                                         \
            if (tblspaceParentDirectory[k] != NULL) { \
                free(tblspaceParentDirectory[k]);     \
                tblspaceParentDirectory[k] = NULL;    \
            }                                         \
        }                                             \
        free(tblspaceDirectory);                      \
        tblspaceDirectory = NULL;                     \
        free(tblspaceParentDirectory);                \
        tblspaceParentDirectory = NULL;               \
    }

#define SAVE_TABLESPACE_DIRECTORY(dir, parentdir)                       \
    if (tblspaceDirectory != NULL && tblspaceParentDirectory != NULL) { \
        tblspaceDirectory[tblspaceIndex] = pg_strdup(dir);              \
        tblspaceParentDirectory[tblspaceIndex] = pg_strdup(parentdir);  \
        tblspaceIndex++;                                                \
    }

#define REMOVE_ALL_TABLESPACE_CREATED()             \
    if (tblspaceDirectory != NULL) {                \
        int k;                                      \
        for (k = 0; k < tblspaceCount; k++) {       \
            if (tblspaceDirectory[k] != NULL) {     \
                rmtree(tblspaceDirectory[k], true); \
            }                                       \
        }                                           \
    }

/* rename all tablespace name to local name */
#define RENAME_TABLESPACE_LIST()                                                                                     \
    if (NULL != tblspaceParentDirectory && NULL != tblspaceDirectory) {                                              \
        errno_t rcm = 0;                                                                                             \
        int k;                                                                                                       \
        char nodetblspcparentpath[MAXPGPATH] = {0};                                                                  \
        for (k = 0; k < tblspaceCount; k++) {                                                                        \
            if (tblspaceDirectory[k] != NULL && tblspaceParentDirectory[k] != NULL) {                                \
                rcm = snprintf_s(nodetblspcparentpath,                                                               \
                    MAXPGPATH,                                                                                       \
                    sizeof(nodetblspcparentpath) - 1,                                                                \
                    "%s/%s_%s",                                                                                      \
                    tblspaceParentDirectory[k],                                                                      \
                    TABLESPACE_VERSION_DIRECTORY,                                                                    \
                    remotenodename);                                                                                 \
                securec_check_ss_c(rcm, "", "");                                                                     \
                if (rename(nodetblspcparentpath, tblspaceDirectory[k]) < 0) {                                        \
                    pg_log(                                                                                          \
                        PG_WARNING, _(" failed to rename %s to %s.\n"), nodetblspcparentpath, tblspaceDirectory[k]); \
                    disconnect_and_exit(1);                                                                          \
                }                                                                                                    \
                rcm = memset_s(nodetblspcparentpath, sizeof(nodetblspcparentpath), 0, sizeof(nodetblspcparentpath)); \
                securec_check_ss_c(rcm, "", "");                                                                     \
            }                                                                                                        \
        }                                                                                                            \
    }

/* get LSN and node name from remote node */
#define GET_FLUSHED_LSN()                                                                        \
    if (NULL != streamConn) {                                                                    \
        PGresult* res;                                                                           \
        errno_t err = 0;                                                                         \
        char* return_value = NULL;                                                               \
        res = PQexec(streamConn, "IDENTIFY_MAXLSN");                                             \
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {                                            \
            pg_log(PG_WARNING, _(" could not identify maxlsn: %s"), PQerrorMessage(streamConn)); \
            disconnect_and_exit(1);                                                              \
        }                                                                                        \
        if (PQntuples(res) != 1 || PQnfields(res) != 1) {                                        \
            pg_log(PG_WARNING,                                                                   \
                _(" could not identify maxlsn, got %d rows and %d fields\n"),                    \
                PQntuples(res),                                                                  \
                PQnfields(res));                                                                 \
            disconnect_and_exit(1);                                                              \
        }                                                                                        \
        return_value = PQgetvalue(res, 0, 0);                                                    \
        if (NULL == return_value) {                                                              \
            pg_log(PG_WARNING, _(" get remote lsn failed\n"));                                   \
            disconnect_and_exit(1);                                                              \
        }                                                                                        \
        if (NULL == strchr(return_value, '|')) {                                                 \
            pg_log(PG_WARNING, _(" get remote lsn style failed\n"));                             \
            disconnect_and_exit(1);                                                              \
        }                                                                                        \
        err = strncpy_s(remotelsn, MAXPGPATH, return_value, MAXPGPATH - 1);                      \
        securec_check_c(err, "", "");                                                            \
        PQclear(res);                                                                            \
    }

/* get name of remote node */
#define GET_REMOTE_NODENAME()                                                        \
    if (0 == remotelsn[0]) {                                                         \
        pg_log(PG_WARNING, _(" get remote node name failed.\n"));                    \
        disconnect_and_exit(1);                                                      \
    }                                                                                \
    char* save = NULL;                                                               \
    char* rname = NULL;                                                              \
    errno_t rcm = 0;                                                                 \
    rname = strtok_r(remotelsn, "|", &save);                                         \
    if (NULL == rname) {                                                             \
        pg_log(PG_WARNING, _(" get remote node name failed from %s.\n"), remotelsn); \
        disconnect_and_exit(1);                                                      \
    }                                                                                \
    rcm = strncpy_s(remotenodename, MAXPGPATH, rname, MAXPGPATH - 1);                \
    remotenodename[MAXPGPATH - 1] = '\0';                                          \
    securec_check_c(rcm, "", "");

/* get LSN from remote node */
#define GET_REMOTE_LSN(xlogend)                                                    \
    if (0 != remotelsn[0]) {                                                       \
        char* save = NULL;                                                         \
        errno_t rcm = 0;                                                           \
        strtok_r(remotelsn, "|", &save);                                           \
        if (NULL == save) {                                                        \
            pg_log(PG_WARNING, _(" get remote lsn failed from %s.\n"), remotelsn); \
            disconnect_and_exit(1);                                                \
        }                                                                          \
        rcm = strncpy_s(xlogend, MAXFNAMELEN, save, MAXFNAMELEN - 1);              \
        (xlogend)[MAXFNAMELEN - 1] = '\0';                                         \
        securec_check_c(rcm, "", "");                                              \
    }

/* concat full file path */
#define CONCAT_BUILD_CONF_FILE(dirname)                                                                         \
    do {                                                                                                        \
        errno_t rcm = 0;                                                                                        \
        rcm = snprintf_s(conf_file, MAXPGPATH, sizeof(conf_file) - 1, "%s/%s", dirname, CONFIGRURE_FILE);       \
        securec_check_ss_c(rcm, "", "");                                                                        \
        rcm = snprintf_s(buildstart_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", dirname, BUILD_TAG_START);         \
        securec_check_ss_c(rcm, "", "");                                                                        \
        rcm = snprintf_s(builddone_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", dirname, BUILD_TAG_DONE);           \
        securec_check_ss_c(rcm, "", "");                                                                        \
        rcm = snprintf_s(build_label_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", dirname, FULL_BACKUP_LABEL_FILE); \
        securec_check_ss_c(rcm, "", "");                                                                        \
    } while (0)

/* crete build start tag file */
#define CREATE_BUILD_FILE(buildstart_file)                                          \
    if (0 != (buildstart_file)[0]) {                                                \
        errno_t rcm = 0;                                                            \
        rcm = CreateBuildtagFile(buildstart_file);                                  \
        if (rcm == FALSE) {                                                         \
            pg_log(PG_WARNING, _(" could not create file %s.\n"), buildstart_file); \
            disconnect_and_exit(1);                                                 \
        }                                                                           \
    }

/* rename build tag file to done */
#define RENAME_BUILD_FILE(buildstart_file, builddone_file)                                       \
    if (rename(buildstart_file, builddone_file) < 0) {                                           \
        pg_log(PG_WARNING, _(" failed to rename %s to %s.\n"), BUILD_TAG_START, BUILD_TAG_DONE); \
        exit(1);                                                                                 \
    }

static void disconnect_and_exit(int code)
{
    if (streamConn != NULL) {
        PQfinish(streamConn);
        streamConn = NULL;
    }

#ifndef WIN32
    /*
     * On windows, our background thread dies along with the process.
     * But on Unix, if we have started a subprocess, we want to kill
     * it off so it doesn't remain running trying to stream data.
     */
    if (bgchild > 0)
        (void)kill(bgchild, SIGTERM);
#endif

    removeCreatedTblspace();

    exit(code);
}

/*
 * Remove all tablespace directories created by full-build when full-build failed.
 * The function is only provided for disconnect_and_exit because disconnect_and_exit was
 * defined in build_util.h.
 */
static void removeCreatedTblspace(void)
{
    REMOVE_ALL_TABLESPACE_CREATED();
}

/*
 * Called in the background process every time data is received.
 * On Unix, we check to see if there is any data on our pipe
 * (which would mean we have a stop position), and if it is, check if
 * it is time to stop.
 * On Windows, we are in a single process, so we can just check if it's
 * time to stop.
 */
static bool reached_end_position(XLogRecPtr segendpos, uint32 timeline, bool segment_finished)
{
    if (!has_xlogendptr) {
#ifndef WIN32
        fd_set fds;
        struct timeval tv;
        int r;
        errno_t rcm = 0;
        uint32 hi = 0;
        uint32 lo = 0;

        /*
         * Don't have the end pointer yet - check our pipe to see if it has
         * been sent yet.
         */
        FD_ZERO(&fds);
        FD_SET(bgpipe[0], &fds);

        rcm = memset_s(&tv, sizeof(tv), 0, sizeof(tv));
        securec_check_c(rcm, "", "");

        r = select(bgpipe[0] + 1, &fds, NULL, NULL, &tv);
        if (r == 1) {
            char xlogend[MAXFNAMELEN];

            rcm = memset_s(xlogend, sizeof(xlogend), 0, sizeof(xlogend));
            securec_check_c(rcm, "\0", "\0");
            r = read(bgpipe[0], xlogend, sizeof(xlogend) - 1);
            if (r < 0) {
                pg_log(PG_WARNING, _(" could not read from ready pipe: %s\n"), strerror(errno));
                exit(1);
            }

            if (sscanf_s(xlogend, "%X/%X", &hi, &lo) != 2) {
                pg_log(PG_WARNING, _(" could not parse xlog end position \"%s\"\n"), xlogend);
                exit(1);
            }
            xlogendptr = (((uint64)hi) << 32) | lo;
            has_xlogendptr = 1;

            /*
             * Fall through to check if we've reached the point further
             * already.
             */
        } else {
            /*
             * No data received on the pipe means we don't know the end
             * position yet - so just say it's not time to stop yet.
             */
            return false;
        }
#else

        /*
         * On win32, has_xlogendptr is set by the main thread, so if it's not
         * set here, we just go back and wait until it shows up.
         */
        return false;
#endif
    }

    Assert(!XLogRecPtrIsInvalid(xlogendptr));

    /*
     * At this point we have an end pointer, so compare it to the current
     * position to figure out if it's time to stop.
     */
    if (segendpos >= xlogendptr)
        return true;

    /*
     * Have end pointer, but haven't reached it yet - so tell the caller to
     * keep streaming.
     */
    return false;
}

typedef struct {
    PGconn* bgconn;
    XLogRecPtr startptr;
    char xlogdir[MAXPGPATH];
    char* sysidentifier;
    int timeline;
    uint32 term;
} logstreamer_param;

static int LogStreamerMain(logstreamer_param* param)
{
    int ret = 0;

    /* get second connection info in child process for the sake of memmory leak */
    param->bgconn = check_and_conn(standby_connect_timeout, standby_recv_timeout, param->term);
    if (param->bgconn == NULL) {
        return 1;
    }

    if (!ReceiveXlogStream(param->bgconn,
        param->startptr,
        param->timeline,
        (const char*)param->sysidentifier,
        (const char*)param->xlogdir,
        reached_end_position,
        standby_message_timeout,
        true)) {
        /*
         * Any errors will already have been reported in the function process,
         * but we need to tell the parent that we didn't shutdown in a nice
         * way.
         */
        ret = 1;
    }

    PQfinish(param->bgconn);
    param->bgconn = NULL;

    return ret;
}

/*
 * Initiate background process for receiving xlog during the backup.
 * The background stream will use its own database connection so we can
 * stream the logfile in parallel with the backups.
 */
void StartLogStreamer(
    char* startpos, uint32 timeline, char* sysidentifier, const char* xloglocation, uint primaryTerm)
{
    logstreamer_param* param = NULL;

    uint32 hi = 0;
    uint32 lo = 0;

    param = (logstreamer_param*)xmalloc0(sizeof(logstreamer_param));
    param->timeline = timeline;
    param->sysidentifier = sysidentifier;
    param->term = primaryTerm;

    /* Convert the starting position */
    if (sscanf_s(startpos, "%X/%X", &hi, &lo) != 2) {
        pg_log(PG_WARNING, _(" invalid format of xlog location: %s.\n"), startpos);
        free(param);
        param = NULL;
        disconnect_and_exit(1);
    }
    param->startptr = (((uint64)hi) << 32) | lo;

    Assert(!XLogRecPtrIsInvalid(param->startptr));
    /* Round off to even segment position */
    param->startptr -= param->startptr % XLOG_SEG_SIZE;

#ifndef WIN32
    /* Create our background pipe */
    if (pipe(bgpipe) < 0) {
        pg_log(PG_WARNING, _(" invalid format of xlog location: %s.\n"), startpos);
        PQfreemem(param);
        param = NULL;
        disconnect_and_exit(1);
    }
#endif

    /*
     * Always in plain format, so we can write to basedir/pg_xlog. But the
     * directory entry in the tar file may arrive later, so make sure it's
     * created before we start.
     */
    errno_t rc = strncpy_s(param->xlogdir, MAXPGPATH, xloglocation, MAXPGPATH - 1);
    securec_check_c(rc, "", "");

    param->xlogdir[MAXPGPATH - 1] = '\0';
    verify_dir_is_empty_or_create(param->xlogdir);

    /*
     * Start a child process and tell it to start streaming. On Unix, this is
     * a fork(). On Windows, we create a thread.
     */
#ifndef WIN32
    /* before fork child process,flush father's buffer */
    fflush(stdout);
    fflush(stderr);
    bgchild = fork();
    if (bgchild == 0) {
        /* in child process */
        exit(LogStreamerMain(param));
    } else if (bgchild < 0) {
        pg_log(PG_WARNING, _(" could not create background process: %s.\n"), strerror(errno));
        PQfreemem(param);
        param = NULL;
        disconnect_and_exit(1);
    }

    /*
     * Else we are in the parent process and all is well.
     */
#else /* WIN32 */
    bgchild = _beginthreadex(NULL, 0, (void*)LogStreamerMain, param, 0, NULL);
    if (bgchild == 0) {
        pg_log(PG_WARNING, _(" could not create background thread: %s.\n"), strerror(errno));
        PQfreemem(param);
        param = NULL;
        disconnect_and_exit(1);
    }
#endif

    if (param->bgconn != NULL) {
        PQfinish(param->bgconn);
        param->bgconn = NULL;
    }
    free(param);
    param = NULL;
}

/*
 * Verify that the given directory exists and is empty. If it does not
 * exist, it is created. If it exists but is not empty, an error will
 * be give and the process ended.
 */
static void verify_dir_is_empty_or_create(char* dirname)
{
    switch (pg_check_dir(dirname)) {
        case 0:

            /*
             * Does not exist, so create
             */
            if (pg_mkdir_p(dirname, S_IRWXU) == -1) {
                pg_log(PG_WARNING, _("could not create directory \"%s\": %s\n"), dirname, strerror(errno));
                disconnect_and_exit(1);
            }
            return;
        case 1:

            /*
             * Exists, empty
             */
            return;
        case 2:

            /*
             * Exists, not empty
             */
            if (strcmp(progname, "gs_rewind") == 0) {
                pg_log(PG_WARNING, _("in gs_rewind proecess,so no need remove.\n"));
                return;
            }
            pg_log(PG_WARNING, _("directory \"%s\" exists but is not empty,so remove and recreate it\n"), dirname);

            if (!rmtree(dirname, true)) {
                pg_log(PG_WARNING, _("failed to remove dir %s.\n"), dirname);
                disconnect_and_exit(1);
            }
            /* recreate it */
            if (pg_mkdir_p(dirname, S_IRWXU) == -1) {
                pg_log(PG_WARNING, _("could not create directory \"%s\": %s\n"), dirname, strerror(errno));
                disconnect_and_exit(1);
            }
            return;
        case -1:

            /*
             * Access problem
             */
            pg_log(PG_WARNING, _("could not access directory \"%s\": %s\n"), dirname, strerror(errno));
            disconnect_and_exit(1);
        default:
            break;
    }
}

/*
 * Print a progress report based on the global variables. If verbose output
 * is enabled, also print the current file name.
 */
static void progress_report(int tablespacenum, const char* filename, bool force)
{
    int percent = (int)((totaldone / 1024) * 100 / totalsize);
    GaussState g_state;
    errno_t rc = 0;
    pg_time_t now = 0;
    int elapsed_secs = 0;
    int caculate_secs = 0;
    static bool print = true;

    /*
     * report and cacluate speed for every report_timeout or the sync percent changed.
     */
    now = (pg_time_t)time(NULL);
    elapsed_secs = abs(now - last_progress_report);
    if (elapsed_secs < REPORT_TIMEOUT && percent <= old_percent && !force) {
        return;
    }

    last_progress_report = now;
    old_percent = percent;

    caculate_secs = abs(now - last_caculate_time);
    if (caculate_secs >= CACULATE_MIN_TIME) {
        sync_speed = (totaldone / 1024 - checkpoint_size) / caculate_secs;
        checkpoint_size = totaldone / 1024;
        last_caculate_time = now;
    }

    rc = memset_s(&g_state, sizeof(GaussState), 0, sizeof(GaussState));
    securec_check_c(rc, "", "");

    /*
     * Avoid overflowing past 100% or the full size. This may make the total
     * size number change as we approach the end of the backup (the estimate
     * will always be wrong if WAL is included), but that's better than having
     * the done column be bigger than the total.
     */
    if (percent > 100) {
        percent = 100;
    }
    if (totaldone / 1024 > totalsize)
        totalsize = totaldone / 1024;

    g_state.mode = STANDBY_MODE;
    g_state.conn_num = replconn_num;
    g_state.state = BUILDING_STATE;
    g_state.sync_stat = false;

    g_state.build_info.build_mode = FULL_BUILD;
    g_state.build_info.total_done = totaldone / 1024;
    g_state.build_info.total_size = totalsize;
    g_state.build_info.process_schedule = percent;
    if (sync_speed > 0)
        g_state.build_info.estimated_time = (totalsize - totaldone / 1024) / sync_speed;
    else
        g_state.build_info.estimated_time = -1;
    UpdateDBStateFile(gaussdb_state_file, &g_state);

    if (print) {
        print = false;
        pg_log(PG_WARNING, _("receiving and unpacking files...\n"));
    }
}

/*
 * Receive a tar format stream from the connection to the server, and unpack
 * the contents of it into a directory. Only files, directories and
 * symlinks are supported, no other kinds of special files.
 *
 * If the data is for the main data directory, it will be restored in the
 * specified directory. If it's for another tablespace, it will be restored
 * in the original directory, since relocation of tablespaces is not
 * supported.
 */
static void ReceiveAndUnpackTarFile(PGconn* conn, PGresult* res, int rownum)
{
    char current_path[MAXPGPATH] = {0};
    char filename[MAXPGPATH] = {0};
    char absolut_path[MAXPGPATH] = {0};
    uint64 current_len_left = 0;
    uint64 current_padding = 0;
    char* copybuf = NULL;
    FILE* file = NULL;
    char* get_value = NULL;
    struct stat st;
    errno_t rc = EOK;
    int nRet = 0;

    if (PQgetisnull(res, rownum, 0)) {
        rc = strncpy_s(current_path, MAXPGPATH, basedir, strlen(basedir));
        securec_check_c(rc, "", "");

        current_path[MAXPGPATH - 1] = '\0';
    } else {
        get_value = PQgetvalue(res, rownum, 1);
        if (get_value == NULL) {
            pg_log(PG_WARNING, _("PQgetvalue get value failed\n"));
            disconnect_and_exit(1);
        }
        char* relative = PQgetvalue(res, rownum, 3);
        if (*relative == '1') {
            nRet = snprintf_s(current_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", basedir, get_value);
            securec_check_ss_c(nRet, "\0", "\0");
        } else {
            rc = strncpy_s(current_path, MAXPGPATH, get_value, strlen(get_value));
            securec_check_c(rc, "\0", "\0");
        }
        current_path[MAXPGPATH - 1] = '\0';
    }

    /*
     * Get the COPY data
     */
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COPY_OUT) {
        pg_log(PG_WARNING, _("could not get COPY data stream: %s"), PQerrorMessage(conn));
        disconnect_and_exit(1);
    }
    PQclear(res);

    while (1) {
        int r;

        if (build_interrupted) {
            pg_log(PG_WARNING, _("build walreceiver process terminated abnormally\n"));
            disconnect_and_exit(1);
        }

        if (copybuf != NULL) {
            PQfreemem(copybuf);
            copybuf = NULL;
        }

        r = PQgetCopyData(conn, &copybuf, 0);
        if (r == -1) {
            /*
             * End of chunk
             */
            if (file != NULL) {
                fclose(file);
                file = NULL;
            }
            break;
        } else if (r == -2) {
            pg_log(PG_WARNING, _("could not read COPY data: %s"), PQerrorMessage(conn));

            disconnect_and_exit(1);
        }

        if (file == NULL) {
            mode_t filemode;

            /*
             * No current file, so this must be the header for a new file
             */
            if (r != BUILD_PATH_LEN) {
                pg_log(PG_WARNING, _("invalid tar block header size: %d\n"), r);

                disconnect_and_exit(1);
            }
            totaldone += BUILD_PATH_LEN;

            if (sscanf_s(copybuf + 1048, "%20lo", &current_len_left) != 1) {
                pg_log(PG_WARNING, _("could not parse file size\n"));
                disconnect_and_exit(1);
            }

            /* Set permissions on the file */
            if (sscanf_s(&copybuf[1024], "%07o ", &filemode) != 1) {
                pg_log(PG_WARNING, _("could not parse file mode\n"));
                disconnect_and_exit(1);
            }

            /*
             * All files are padded up to 512 bytes
             */
            if (current_len_left < 0 || current_len_left > INT_MAX - 511) {
                pg_log(PG_WARNING, _("current_len_left is invalid\n"));
                disconnect_and_exit(1);
            }
            current_padding = ((current_len_left + 511) & ~511) - current_len_left;

            /*
             * First part of header is zero terminated filename
             */
            if (NULL != conn_str)
                (void)replace_node_name(copybuf, (const char*)remotenodename, (const char*)pgxcnodename);
            nRet = snprintf_s(filename, MAXPGPATH, sizeof(filename) - 1, "%s/%s", current_path, copybuf);
            securec_check_ss_c(nRet, "\0", "\0");

            if (filename[strlen(filename) - 1] == '/') {
                int len = strlen("/pg_xlog");
                const int bufOffset = 1080;
                /*
                 * Ends in a slash means directory or symlink to directory
                 */
                if (copybuf[bufOffset] == '5') {
                    /*
                     * Directory
                     */
                    filename[strlen(filename) - 1] = '\0'; /* Remove trailing slash */
                    if (stat(filename, &st) == 0 && S_ISDIR(st.st_mode)) {
                        continue;
                    } else {
                        if (mkdir(filename, S_IRWXU) != 0) {
                            /*
                             * When streaming WAL, pg_xlog will have been created
                             * by the wal receiver process, so just ignore failure
                             * on that.
                             */
                            if (!streamwal || strcmp(filename + strlen(filename) - len, "/pg_xlog") != 0) {
                                pg_log(PG_WARNING,
                                    _("could not create directory \"%s\": %s\n"),
                                    filename,
                                    strerror(errno));

                                disconnect_and_exit(1);
                            }
                        }
#ifndef WIN32
                        if (chmod(filename, filemode))
                            pg_log(PG_WARNING,
                                _("could not set permissions on directory \"%s\": %s\n"),
                                filename,
                                strerror(errno));

#endif
                    }
                } else if (copybuf[bufOffset] == '2') {
                    /*
                     * Symbolic link for absolute tablespace. please refer to function _tarWriteHeader
                     * description: we need refactor the communication protocol for well maintaining code
                     */
                    filename[strlen(filename) - 1] = '\0'; /* Remove trailing slash */
                    if (symlink(&copybuf[bufOffset + 1], filename) != 0) {
                        if (!streamwal || strcmp(filename + strlen(filename) - len, "/pg_xlog") != 0) {
                            pg_log(PG_WARNING,
                                _("could not create symbolic link from \"%s\" to \"%s\": %s\n"),
                                filename,
                                &copybuf[1081],
                                strerror(errno));
                            disconnect_and_exit(1);
                        }
                    }
                } else if (copybuf[bufOffset] == '3') {
                    /*
                     * Symbolic link for relative tablespace. please refer to function _tarWriteHeader
                     */
                    filename[strlen(filename) - 1] = '\0'; /* Remove trailing slash */

                    nRet = snprintf_s(absolut_path,
                        sizeof(absolut_path),
                        sizeof(absolut_path) - 1,
                        "%s/%s",
                        basedir,
                        &copybuf[bufOffset + 1]);
                    securec_check_ss_c(nRet, "\0", "\0");

                    if (symlink(absolut_path, filename) != 0) {
                        if (!streamwal || strcmp(filename + strlen(filename) - len, "/pg_xlog") != 0) {
                            pg_log(PG_WARNING,
                                _("could not create symbolic link from \"%s\" to \"%s\": %s\n"),
                                filename,
                                &copybuf[1081],
                                strerror(errno));
                            disconnect_and_exit(1);
                        }
                    }
                } else {
                    pg_log(PG_WARNING, _("unrecognized link indicator \"%c\"\n"), copybuf[1080]);
                    disconnect_and_exit(1);
                }
                continue; /* directory or link handled */
            }

            canonicalize_path(filename);
            /*
             * regular file
             */
            file = fopen(filename, "wb");
            if (NULL == file) {
                pg_log(PG_WARNING, _("could not create file \"%s\": %s\n"), filename, strerror(errno));
                disconnect_and_exit(1);
            }

#ifndef WIN32
            if (chmod(filename, filemode))
                pg_log(PG_WARNING, _("could not set permissions on file \"%s\": %s\n"), filename, strerror(errno));
#endif

            if (current_len_left == 0) {
                /*
                 * Done with this file, next one will be a new tar header
                 */
                if (file != NULL) {
                    fclose(file);
                    file = NULL;
                    continue;
                }
            }
        } else {
            /*
             * Continuing blocks in existing file
             */
            if (current_len_left == 0 && r == (int)current_padding) {
                /*
                 * Received the padding block for this file, ignore it and
                 * close the file, then move on to the next tar header.
                 */
                fclose(file);
                file = NULL;
                totaldone += r;
                continue;
            }

            if (fwrite(copybuf, r, 1, file) != 1) {
                pg_log(PG_WARNING, _("could not write to file \"%s\": %s\n"), filename, strerror(errno));
                disconnect_and_exit(1);
            }
            totaldone += r;
            if (showprogress)
                progress_report(rownum, filename, false);

            current_len_left -= r;
            if (current_len_left == 0 && current_padding == 0) {
                /*
                 * Received the last block, and there is no padding to be
                 * expected. Close the file and move on to the next tar
                 * header.
                 */
                fclose(file);
                file = NULL;
                continue;
            }
        } /* continuing data in existing file */
    }     /* loop over all data blocks */

    if (showprogress)
        progress_report(rownum, filename, true);

    if (file != NULL) {
        fclose(file);
        file = NULL;
        pg_log(PG_WARNING, _("COPY stream ended before last file was finished\n"));
        disconnect_and_exit(1);
    }

    if (copybuf != NULL) {
        PQfreemem(copybuf);
        copybuf = NULL;
    }
}

/*
 * Brief            : @@GaussDB@@
 * Description    :  create .done
 * Notes            :
 */
bool CreateBuildtagFile(const char* fulltagname)
{
/* the max real path length in linux is 4096, adapt this for realpath func */
#define MAX_REALPATH_LEN 4096
    char* retVal = NULL;
    int fd = -1;
    char Lrealpath[MAX_REALPATH_LEN + 1] = {0};
    retVal = realpath(fulltagname, Lrealpath);
    if (retVal == NULL && Lrealpath[0] == '\0') {
        pg_log(PG_WARNING, _(" realpath %s failed : %s\n"), fulltagname, strerror(errno));
        return false;
    }

    if ((fd = open(Lrealpath, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR)) == -1) {
        pg_log(PG_WARNING, _(" could not create file %s : %s\n"), BUILD_TAG_START, strerror(errno));
        return false;
    }
    close(fd);
    return true;
}

static void BaseBackup(const char* dirname, uint32 term)
{
    PGresult* res = NULL;
    char* sysidentifier = NULL;
    uint32 timeline;

    char current_path[MAXPGPATH] = {0};
    char nodetablespacepath[MAXPGPATH] = {0};
    char nodetablespaceparentpath[MAXPGPATH] = {0};
    char escaped_label[MAXPGPATH] = {0};
    int i;
    char xlogstart[MAXFNAMELEN] = {0};
    char xlogend[MAXFNAMELEN] = {0};
    bool ret = FALSE;
    char* get_value = NULL;
    char xlog_location[MAXPGPATH] = {0};
    errno_t rc = EOK;
    int nRet = 0;
    struct stat st;

    pqsignal(SIGCHLD, BuildReaper); /* handle child termination */
    /* concat file and path */
    CONCAT_BUILD_CONF_FILE(dirname);
#ifndef WIN32
    if (stat(dirname, &st) != 0) {
        pg_log(PG_WARNING, _("could not stat directory or file: %s\n"), strerror(errno));
    }

    /* if it is a symnol, chmod will change the auth of the true file */
    if (S_ISLNK(st.st_mode)) {
        pg_log(PG_WARNING, _("the file being chmod is a symbol link\n"));
    }

    chmod(dirname, (mode_t)S_IRWXU);
#endif
    /* save connection info from command line or postgresql file */
    get_conninfo(conf_file);

    /* find a available conn */
    streamConn = check_and_conn(standby_connect_timeout, standby_recv_timeout, term);
    if (streamConn == NULL) {
        show_full_build_process("could not connect to server.");
        disconnect_and_exit(1);
    }

    show_full_build_process("connect to server success, build started.");
    /* create  build tag file */
    ret = CreateBuildtagFile(buildstart_file);
    if (ret == FALSE) {
        pg_log(PG_WARNING, _("could not create file %s.\n"), buildstart_file);

        disconnect_and_exit(1);
    }

    show_full_build_process("create build tag file success");

    /* delete data/ and  pg_tblspc/, but keep .config */
    delete_datadir(dirname);

    show_full_build_process("clear old target dir success");

    /* create  build tag file again*/
    ret = CreateBuildtagFile(buildstart_file);
    if (ret == FALSE) {
        pg_log(PG_WARNING, _("could not create file again %s.\n"), buildstart_file);

        disconnect_and_exit(1);
    }

    show_full_build_process("create build tag file again success");

    /*
     * Run IDENTIFY_SYSTEM so we can get the timeline
     */
    res = PQexec(streamConn, "IDENTIFY_SYSTEM");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(PG_WARNING, _("could not identify system: %s"), PQerrorMessage(streamConn));

        disconnect_and_exit(1);
    }
    if (PQntuples(res) != 1 || PQnfields(res) != 4) {
        pg_log(PG_WARNING, _("could not identify system, got %d rows and %d fields\n"), PQntuples(res), PQnfields(res));
        disconnect_and_exit(1);
    }
    sysidentifier = pg_strdup(PQgetvalue(res, 0, 0));
    timeline = atoi(PQgetvalue(res, 0, 1));
    PQclear(res);

    show_full_build_process("get system identifier success");

    create_backup_label(dirname, sysidentifier, timeline);

    show_full_build_process("create backup label success");

    /*
     * Run IDENTIFY_MAXLSN then get the remote node name
     * The name use for rename tablespace name
     */
    GET_FLUSHED_LSN();
    GET_REMOTE_NODENAME();

    /*
     * Start the actual backup
     */
    (void)PQsetRwTimeout(streamConn, Max(BUILD_RW_TIMEOUT, standby_recv_timeout));
    (void)PQescapeStringConn(streamConn, escaped_label, label, sizeof(escaped_label), &i);
    nRet = snprintf_s(current_path,
        MAXPGPATH,
        sizeof(current_path) - 1,
        "BASE_BACKUP LABEL '%s' %s %s %s %s",
        escaped_label,
        showprogress ? "PROGRESS" : "",
        includewal && !streamwal ? "WAL" : "",
        fastcheckpoint ? "FAST" : "",
        includewal ? "NOWAIT" : "");
    securec_check_ss_c(nRet, "", "");

    if (PQsendQuery(streamConn, current_path) == 0) {
        pg_log(PG_WARNING, _("could not send base backup command: %s"), PQerrorMessage(streamConn));
        disconnect_and_exit(1);
    }

    /*
     *  get the xlog location
     */
    res = PQgetResult(streamConn);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(PG_WARNING, _("could not get xlog location: %s"), PQerrorMessage(streamConn));
        disconnect_and_exit(1);
    }
    if (PQntuples(res) != 1) {
        pg_log(PG_WARNING, _("no xlog location returned from server\n"));
        disconnect_and_exit(1);
    }
    get_value = PQgetvalue(res, 0, 0);
    /* if linkpath is NULL ? */
    if (get_value == NULL) {
        pg_log(PG_WARNING, _("get xlog location failed\n"));
        disconnect_and_exit(1);
    }

    /*
     * get xlog locatioin,
     * enable user define xlog directory, standby's xlog_location is decided by
     * itself, but not by primary
     */
    get_xlog_location(xlog_location);

    PQclear(res);

    /*
     * Get the starting xlog position
     */
    res = PQgetResult(streamConn);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(PG_WARNING, _("could not initiate base backup: %s"), PQerrorMessage(streamConn));
        disconnect_and_exit(1);
    }
    if (PQntuples(res) != 1) {
        pg_log(PG_WARNING, _("no start point returned from server\n"));
        disconnect_and_exit(1);
    }
    get_value = PQgetvalue(res, 0, 0);
    if (get_value == NULL) {
        pg_log(PG_WARNING, _("get xlog start point failed\n"));
        disconnect_and_exit(1);
    }
    rc = strncpy_s(xlogstart, sizeof(xlogstart), get_value, strlen(get_value));
    securec_check_c(rc, "", "");

    xlogstart[63] = '\0';
    if (verbose && includewal) {
        pg_log(PG_WARNING, "xlog start point: %s\n", xlogstart);
    }
    PQclear(res);
    rc = memset_s(xlogend, sizeof(xlogend), 0, sizeof(xlogend));
    securec_check_c(rc, "", "");

    (void)PQsetRwTimeout(streamConn, standby_recv_timeout);

    /*
     * Get the header
     */
    res = PQgetResult(streamConn);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(PG_WARNING, _("could not get backup header: %s"), PQerrorMessage(streamConn));
        disconnect_and_exit(1);
    }
    if (PQntuples(res) < 1) {
        pg_log(PG_WARNING, _("no data returned from server\n"));
        disconnect_and_exit(1);
    }

    /*
     * Sum up the total size, for progress reporting
     */
    totalsize = totaldone = 0;
    tblspaceCount = PQntuples(res);

    show_full_build_process("begin build tablespace list");

    /* make the tablespace directory list */
    TBLESPACE_LIST_CREATE();

    for (i = 0; i < PQntuples(res); i++) {
        if (showprogress)
            totalsize += atol(PQgetvalue(res, i, 2));

        /*
         * Verify tablespace directories are empty. Don't bother with the
         * first once since it can be relocated, and it will be checked before
         * we do anything anyway.
         */
        if (format == 'p' && !PQgetisnull(res, i, 1)) {
            char* tablespacepath = PQgetvalue(res, i, 1);
            char* relative = PQgetvalue(res, i, 3);
            char prefix[MAXPGPATH] = {'\0'};
            if (*relative == '1') {
                nRet = snprintf_s(prefix, MAXPGPATH, strlen(basedir) + 1, "%s/", basedir);
                securec_check_ss_c(nRet, "\0", "\0");
            }
            nRet = snprintf_s(nodetablespaceparentpath,
                MAXPGPATH,
                sizeof(nodetablespaceparentpath) - 1,
                "%s%s",
                prefix,
                tablespacepath);
            securec_check_ss_c(nRet, "\0", "\0");
            nRet = snprintf_s(nodetablespacepath,
                MAXPGPATH,
                sizeof(nodetablespacepath) - 1,
                "%s/%s_%s",
                nodetablespaceparentpath,
                TABLESPACE_VERSION_DIRECTORY,
                pgxcnodename);
            securec_check_ss_c(nRet, "\0", "\0");

            verify_dir_is_empty_or_create(nodetablespacepath);

            /* Save the tablespace directory here so we can remove it when errors happen. */
            SAVE_TABLESPACE_DIRECTORY(nodetablespacepath, nodetablespaceparentpath);
        }
    }

    show_full_build_process("finish build tablespace list");

    /*
     * When writing to stdout, require a single tablespace
     */
    if (format == 't' && strcmp(basedir, "-") == 0 && PQntuples(res) > 1) {
        pg_log(PG_WARNING, _("can only write single tablespace to stdout, database has %d\n"), PQntuples(res));
        disconnect_and_exit(1);
    }

    show_full_build_process("begin get xlog by xlogstream");

    /*
     * If we're streaming WAL, start the streaming session before we start
     * receiving the actual data chunks.
     */
    if (streamwal) {
        if (verbose) {
            pg_log(PG_WARNING, _("starting background WAL receiver\n"));
        }
        show_full_build_process("starting walreceiver");
        StartLogStreamer(xlogstart, timeline, sysidentifier, (const char*)xlog_location, term);
    }

    /* free sysidentifier after use */
    pg_free(sysidentifier);
    show_full_build_process("begin receive tar files");

    /*
     * Start receiving chunks, Loop over all tablespaces
     */
    for (i = 0; i < PQntuples(res); i++) {
        ReceiveAndUnpackTarFile(streamConn, res, i);
    }

    if (showprogress)
        progress_report(PQntuples(res), NULL, true);
    PQclear(res);

    show_full_build_process("finish receive tar files");

    /*
     * Get the stop position
     */
    res = PQgetResult(streamConn);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(PG_WARNING, _("could not get WAL end position from server: %s"), PQerrorMessage(streamConn));
        disconnect_and_exit(1);
    }
    if (PQntuples(res) != 1) {
        pg_log(PG_WARNING, _("no WAL end position returned from server\n"));
        disconnect_and_exit(1);
    }
    get_value = PQgetvalue(res, 0, 0);
    if (get_value == NULL) {
        pg_log(PG_WARNING, _("get xlog end point failed\n"));
        disconnect_and_exit(1);
    }
    rc = strncpy_s(xlogend, sizeof(xlogend), get_value, strlen(get_value));
    securec_check_c(rc, "", "");

    xlogend[63] = '\0';
    if (verbose && includewal) {
        pg_log(PG_WARNING, "xlog end point: %s\n", xlogend);
    }
    PQclear(res);

    res = PQgetResult(streamConn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        pg_log(PG_WARNING, _("final receive failed: %s"), PQerrorMessage(streamConn));
        disconnect_and_exit(1);
    }

    /*
     * End of copy data. Final result is already checked inside the loop.
     */
    PQclear(res);

#ifdef ENABLE_MOT
    res = PQgetResult(streamConn);
    if (res != NULL) {
        /*
         * We expect the result to be NULL, otherwise we received some unexpected result.
         * We just expect a 'Z' message and PQgetResult should set conn->asyncStatus to PGASYNC_IDLE,
         * otherwise we have problem! Report error and disconnect.
         */
        pg_log(PG_WARNING, _("unexpected result received after final result, status: %u\n"), PQresultStatus(res));
        disconnect_and_exit(1);
    }

    show_full_build_process("fetching MOT checkpoint");

    char* motChkptDir = GetMotCheckpointDir(dirname);
    FetchMotCheckpoint(motChkptDir ? (const char*)motChkptDir : dirname, streamConn, progname, (bool)verbose);
    if (motChkptDir) {
        free(motChkptDir);
    }
#endif

    if (bgchild > 0) {
#ifndef WIN32
        int status;
        int r;
#else
        DWORD status;
        uint32 hi = 0;
        uint32 lo = 0;
#endif

        if (verbose) {
            pg_log(PG_WARNING, _("waiting for background process to finish streaming...\n"));
        }

#ifndef WIN32
        if ((unsigned int)write(bgpipe[1], xlogend, strlen(xlogend)) != strlen(xlogend)) {
            pg_log(PG_WARNING, _("could not send command to background pipe: %s\n"), strerror(errno));
            disconnect_and_exit(1);
        }

        /* Just wait for the background process to exit */
        r = waitpid(bgchild, &status, 0);
        if (r == -1) {
            pg_log(PG_WARNING, _("could not wait for child process: %s\n"), strerror(errno));
            disconnect_and_exit(1);
        }
        if (r != bgchild) {
            pg_log(PG_WARNING, _("child %d died, expected %d\n"), r, (int)bgchild);
            disconnect_and_exit(1);
        }
        if (!WIFEXITED(status)) {
            pg_log(PG_WARNING, _("child process did not exit normally\n"));
            disconnect_and_exit(1);
        }
        if (WEXITSTATUS(status) != 0) {
            pg_log(PG_WARNING, _("child process exited with error %d\n"), WEXITSTATUS(status));
            disconnect_and_exit(1);
        }
        /* Exited normally, we're happy! */
#else /* WIN32 */

        /*
         * On Windows, since we are in the same process, we can just store the
         * value directly in the variable, and then set the flag that says
         * it's there.
         */
        if (sscanf_s(xlogend, "%X/%X", &hi, &lo) != 2) {
            pg_log(PG_WARNING, _("could not parse xlog end position \"%s\"\n"), xlogend);
            disconnect_and_exit(1);
        }
        xlogendptr = ((uint64)hi) << 32 | lo(void) InterlockedIncrement(&has_xlogendptr);

        /* First wait for the thread to exit */
        if (WaitForSingleObjectEx((HANDLE)bgchild, INFINITE, FALSE) != WAIT_OBJECT_0) {
            _dosmaperr(GetLastError());
            pg_log(PG_WARNING, _("could not wait for child thread: %s\n"), strerror(errno));
            disconnect_and_exit(1);
        }
        if (GetExitCodeThread((HANDLE)bgchild, &status) == 0) {
            _dosmaperr(GetLastError());
            pg_log(PG_WARNING, _("could not get child thread exit status: %s\n"), strerror(errno));
            disconnect_and_exit(1);
        }
        if (status != 0) {
            pg_log(PG_WARNING, _("child thread exited with error %u\n"), (unsigned int)status);
            disconnect_and_exit(1);
        }
        /* Exited normally, we're happy */
#endif
    }

    TABLESPACE_LIST_RELEASE();

    PQfinish(streamConn);
    streamConn = NULL;

    /* delete dw file if exists, recreate it and write a page of zero */
    backup_dw_file(dirname);
    show_full_build_process("build dummy dw file success");

    /* rename tag file to done */
    RENAME_BUILD_FILE(buildstart_file, builddone_file);

    show_full_build_process("rename build status file success");
}

/*
 * @@GaussDB@@
 * Brief            : the entry of full build
 * Description        :
 * Notes            :
 */
void backup_main(char* dir, uint32 term)
{
    if (dir == NULL) {
        pg_log(PG_PRINT, "%s: parameters dir is NULL.\n", progname);
        exit(1);
    } else {
        basedir = dir;
    }
    /* program name */
    progname = "gs_ctl";

    /* start backup */
    BaseBackup(dir, term);
}

/*
 * scene for CN build DN,used after full backup
 * and cluster should be locked.
 */
void backup_incremental_xlog(char* dir)
{
    /* data dir cannot be NULL */
    if (dir == NULL) {
        pg_log(PG_PRINT, "%s: data dir is NULL.\n", progname);
        exit(1);
    }

    /* program name */
    progname = "gs_ctl";
    basedir = dir;

    /* xlog backup */
    xlog_streamer_backup(dir);
}

/* BuildReaper -- signal handler after wal receiver dies. */
static void BuildReaper(SIGNAL_ARGS)
{
    build_interrupted = true;
}

/*
 * Create a full backup_label file that check sysidentifier with
 * incremental backup.
 */
static void create_backup_label(const char* dirname, const char* startsysid, TimeLineID starttli)
{
    char buf[1000];
    int len;

    /*
     * Construct backup label file
     */
    len = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "START SYSIDENTIFER: %s\n"
        "START TIMELINE: %u\n",
        startsysid,
        starttli);
    securec_check_ss_c(len, "", "");
    if (len >= (int)sizeof(buf)) {
        pg_log(PG_WARNING, _(" full backup label buffer too small\n"));
        disconnect_and_exit(1);
    }
    /* description: move old file out of the way, if any. */
    open_target_file("full_backup_label", true); /* BACKUP_LABEL_FILE */
    write_target_range(buf, 0, len, len);
    close_target_file();
}

/*
 * read_full_backup_label: check to see if a backup_label file is present
 * incremental backup base on a full backup.this function to find full backup start LSN.
 * copy xlog from start LSN until current LSN to local node.
 * copy xlog should lock cluster.
 */
static XLogRecPtr read_full_backup_label(
    const char* dirname, char* sysid, uint32 sysid_len, char* tline, uint32 tline_len)
{
#define MAX_REALPATH_LEN 4096
    char startxlogfilename[MAXFNAMELEN];
    TimeLineID tli;
    char ch;
    FILE* lfp = NULL;
    char backup_file[MAXPGPATH];
    int nRet = 0;
    XLogRecPtr xlogpos;
    uint32 hi = 0;
    uint32 lo = 0;
    char Lrealpath[MAX_REALPATH_LEN + 1] = {0};
    char* retVal = NULL;
    /*
     * See if label file is present
     */
    nRet = snprintf_s(backup_file, MAXPGPATH, MAXPGPATH - 1, "%s/backup_label", dirname);
    securec_check_ss_c(nRet, "\0", "\0");

    retVal = realpath(backup_file, Lrealpath);
    if (retVal == NULL && Lrealpath[0] == '\0') {
        pg_log(PG_WARNING, _("realpath failed : %s!\n"), strerror(errno));
        disconnect_and_exit(1);
    }

    /*
     * Read and parse the START WAL LOCATION and CHECKPOINT lines (this code
     * is pretty crude, but we are not expecting any variability in the file
     * format).
     */
    lfp = fopen(Lrealpath, "r");
    if (lfp == NULL) {
        pg_log(PG_WARNING, _(" could not read file \"%s\"\n"), BACKUP_LABEL_FILE);
        disconnect_and_exit(1);
    }

    if (fscanf_s(lfp,
        "START WAL LOCATION: %X/%X (file %08X%16s)%c",
        &hi,
        &lo,
        &tli,
        startxlogfilename,
        MAXFNAMELEN,
        &ch,
        1) != 5 ||
        ch != '\n') {
        pg_log(PG_WARNING, _(" invalid wal data in file \"%s\"\n"), BACKUP_LABEL_FILE);
        fclose(lfp);
        lfp = NULL;
        disconnect_and_exit(1);
    }
    xlogpos = (((uint64)hi) << 32) | lo;

    if (ferror(lfp)) {
        pg_log(PG_WARNING, _(" close file hanler failed\"%s\"\n"), BACKUP_LABEL_FILE);
        fclose(lfp);
        lfp = NULL;
        disconnect_and_exit(1);
    }

    fclose(lfp);
    lfp = NULL;

    nRet = memset_s(backup_file, sizeof(backup_file), 0, sizeof(backup_file));
    securec_check_ss_c(nRet, "", "");
    nRet = snprintf_s(backup_file, MAXPGPATH, MAXPGPATH - 1, "%s/full_backup_label", dirname);
    securec_check_ss_c(nRet, "", "");

    /*
     * Read and parse the START WAL LOCATION and CHECKPOINT lines (this code
     * is pretty crude, but we are not expecting any variability in the file
     * format).
     */
    retVal = realpath(backup_file, Lrealpath);
    if (retVal == NULL && Lrealpath[0] == '\0') {
        pg_log(PG_WARNING, _("realpath failed : %s!\n"), strerror(errno));
        disconnect_and_exit(1);
    }
    lfp = fopen(Lrealpath, "r");
    if (lfp == NULL) {
        pg_log(PG_WARNING, _(" could not read file \"%s\"\n"), FULL_BACKUP_LABEL_FILE);
        disconnect_and_exit(1);
    }

    if (fscanf_s(lfp, "START SYSIDENTIFER: %20s\n", sysid, sysid_len) != 1) {
        pg_log(PG_WARNING, _(" invalid sysidentifier data in file \"%s\"\n"), FULL_BACKUP_LABEL_FILE);
        fclose(lfp);
        lfp = NULL;
        disconnect_and_exit(1);
    }
    sysid[sysid_len - 1] = '\0';

    if (fscanf_s(lfp, "START TIMELINE: %20s\n", tline, tline_len) != 1) {
        pg_log(PG_WARNING, _(" invalid timeline data in file \"%s\"\n"), FULL_BACKUP_LABEL_FILE);
        fclose(lfp);
        lfp = NULL;
        disconnect_and_exit(1);
    }
    tline[tline_len - 1] = '\0';

    if (ferror(lfp)) {
        pg_log(PG_WARNING, _(" close file hanler failed\"%s\"\n"), FULL_BACKUP_LABEL_FILE);
        fclose(lfp);
        lfp = NULL;
        disconnect_and_exit(1);
    }

    fclose(lfp);
    lfp = NULL;

    return xlogpos;
}

/*
 * backup only xlog from full backup start lsn and current lsn.
 * after copy all there xlog to local directory,then startup database for redo.
 * before copy xlog should lock cluster
 */
static void xlog_streamer_backup(const char* dirname)
{
    char xlogstart[MAXFNAMELEN] = {0};
    char xlogend[MAXFNAMELEN] = {0};
    bool ret = 0;
    char xlog_location[MAXPGPATH] = {0};
    XLogRecPtr xlogpos;
    char tline[MAXPGPATH] = {0};
    char sysid[MAXPGPATH] = {0};
    int nRet = 0;
    struct stat st;

    (void)pqsignal(SIGCHLD, BuildReaper); /* handle child termination */

    if (stat(dirname, &st) != 0) {
        pg_log(PG_WARNING, _("could not stat directory or file: %s\n"), strerror(errno));
    }

    /* if it is a symnol, chmod will change the auth of the true file */
    if (S_ISLNK(st.st_mode)) {
        pg_log(PG_WARNING, _("the file being chmod is a symbol link\n"));
    }

    if (chmod(dirname, (mode_t)S_IRWXU)) {
        pg_log(PG_WARNING, _("could not set permissions on data directory \"%s\": %s\n"), dirname, strerror(errno));
    }

    CONCAT_BUILD_CONF_FILE(dirname);

    get_conninfo(conf_file);
    pg_log(PG_WARNING, _("increment build started.\n"));
    /* create build tag file */
    ret = CreateBuildtagFile(buildstart_file);
    if (ret == FALSE) {
        pg_log(PG_WARNING, _("could not create file %s.\n"), buildstart_file);
        disconnect_and_exit(1);
    }

    /*
     * get xlog locatioin,
     * enable user define xlog directory, standby's xlog_location is decided by
     * itself, but not by primary
     */
    get_xlog_location(xlog_location);

    /*
     * Get the starting xlog position
     */
    xlogpos = read_full_backup_label(dirname, sysid, MAXPGPATH, tline, MAXPGPATH);
    nRet = snprintf_s(
        xlogstart, sizeof(xlogstart), sizeof(xlogstart) - 1, "%X/%X", (uint32)(xlogpos >> 32), (uint32)xlogpos);
    securec_check_ss_c(nRet, "", "");
    xlogstart[63] = '\0';
    if (verbose && includewal) {
        pg_log(PG_PROGRESS, "xlog start point: %s\n", xlogstart);
    }

    /* find a available conn */
    streamConn = check_and_conn(standby_connect_timeout, standby_recv_timeout);
    if (streamConn == NULL) {
        pg_log(PG_WARNING, _("could not connect to server(%s).\n"), dirname);
        disconnect_and_exit(1);
    }
    pg_log(PG_PROGRESS, _("connect to server, incremental build started.\n"));

    /*
     * If we're streaming WAL, start the streaming session before we start
     * receiving the actual data chunks.
     */
    if (streamwal) {
        if (verbose) {
            pg_log(PG_WARNING, _("starting background WAL receiver\n"));
        }
        StartLogStreamer(xlogstart, (uint32)atoi(tline), sysid, (const char*)xlog_location);
    }

    /*
     * Run IDENTIFY_MAXLSN to get end lsn
     * This lsn is latest since last full backup
     */
    GET_FLUSHED_LSN();
    GET_REMOTE_LSN(xlogend);

    if (verbose && includewal) {
        pg_log(PG_WARNING, "xlog end point: %s\n", xlogend);
    }

    if (bgchild > 0) {
        int status;
        int r;

        if (verbose) {
            pg_log(PG_WARNING, _("waiting for background process to finish streaming...\n"));
        }

        if ((unsigned int)write(bgpipe[1], xlogend, strlen(xlogend)) != strlen(xlogend)) {
            pg_log(PG_WARNING, _("could not send command to background pipe: %s\n"), strerror(errno));
            disconnect_and_exit(1);
        }

        /* Just wait for the background process to exit */
        r = waitpid(bgchild, &status, 0);
        if (r == -1) {
            pg_log(PG_WARNING, _("could not wait for child process: %s\n"), strerror(errno));
            disconnect_and_exit(1);
        }
        if (r != bgchild) {
            pg_log(PG_WARNING, _("child %d died, expected %d\n"), r, (int)bgchild);
            disconnect_and_exit(1);
        }
        if (!WIFEXITED(status)) {
            pg_log(PG_WARNING, _("child process did not exit normally\n"));
            disconnect_and_exit(1);
        }
        if (WEXITSTATUS(status) != 0) {
            pg_log(PG_WARNING, _("child process exited with error %d\n"), WEXITSTATUS(status));
            disconnect_and_exit(1);
        }
        /* Exited normally, we're happy! */
    }

    /*
     * End of copy data. Final result is already checked inside the loop.
     */
    PQfinish(streamConn);
    streamConn = NULL;

    RENAME_BUILD_FILE(buildstart_file, builddone_file);
    /*
     * remove full backup label when incremental backup successful.
     */
    remove_target_file(FULL_BACKUP_LABEL_FILE, false);
}

/*
 * Replace str to another
 */
static int replace_node_name(char* sSrc, const char* sMatchStr, const char* sReplaceStr)
{
    size_t StringLen = 0;
    char caNewString[MAXPGPATH];
    errno_t rc = EOK;
    size_t src_len = 0;
    size_t match_len = 0;
    size_t rep_len = 0;
    char* FindPos = NULL;
    char* FindVerPos = NULL;

    src_len = strlen(sSrc);
    match_len = strlen(sReplaceStr);
    rep_len = strlen(sReplaceStr);
    if (src_len == 0 || match_len == 0 || rep_len == 0 || src_len >= MAXPGPATH || match_len >= MAXPGPATH ||
        rep_len >= MAXPGPATH) {
        pg_log(PG_WARNING, _(" exists illegal characters %s, %s ,%s\n"), sSrc, sMatchStr, sReplaceStr);
        return 0;
    }
    if (strncmp(sMatchStr, sReplaceStr, MAXPGPATH) == 0) {
        return 1;
    }
    FindPos = strstr(sSrc, sMatchStr);
    FindVerPos = strstr(sSrc, TABLESPACE_VERSION_DIRECTORY);
    /* if sSrc does not contain sMatchStr, do nothing*/
    if ((FindPos == NULL) || (FindVerPos == NULL)) {
        return 1;
    }
    rc = memset_s(caNewString, MAXPGPATH, 0, MAXPGPATH);
    securec_check_c(rc, "", "");
    StringLen = (size_t)(FindPos - sSrc);
    rc = strncpy_s(caNewString, MAXPGPATH, sSrc, StringLen);
    securec_check_c(rc, "", "");
    if ((StringLen + strlen(sReplaceStr)) >= MAXPGPATH) {
        pg_log(PG_WARNING, _(" exceed max characters %s, %s ,%s\n"), sSrc, sMatchStr, sReplaceStr);
        return 0;
    }
    rc = strcat_s(caNewString, MAXPGPATH, sReplaceStr);
    securec_check_c(rc, "", "");
    if ((StringLen + strlen(sReplaceStr) + strlen(FindPos + strlen(sMatchStr))) >= MAXPGPATH) {
        pg_log(PG_WARNING, _(" exceed max characters %s, %s ,%s\n"), sSrc, sMatchStr, sReplaceStr);
        return 0;
    }
    rc = strcat_s(caNewString, MAXPGPATH, FindPos + strlen(sMatchStr));
    securec_check_c(rc, "", "");
    rc = strcpy_s(sSrc, MAXPGPATH, caNewString);
    securec_check_c(rc, "", "");
    return 1;
}

static void show_full_build_process(const char* errmg)
{
    pg_log(PG_PROGRESS, _("%s\n"), errmg);
}

/**
 * delete existing double write file if existed, recreate it and write one page of zero
 * @param target_dir data base root dir
 */
static void backup_dw_file(const char* target_dir)
{
    int rc;
    int fd = -1;
    char dw_file_path[PATH_MAX];
    char real_file_path[PATH_MAX + 1] = {0};
    char* buf = NULL;
    char* unaligned_buf = NULL;

    /* Delete the dw file, if it exists. */
    rc = snprintf_s(dw_file_path, PATH_MAX, PATH_MAX - 1, "%s/%s", target_dir, DW_FILE_NAME);
    securec_check_ss_c(rc, "\0", "\0");
    if (realpath(dw_file_path, real_file_path) == NULL) {
        if (real_file_path[0] == '\0') {
            pg_log(PG_WARNING, _("could not get canonical path for file %s: %s\n"), dw_file_path, gs_strerror(errno));
            disconnect_and_exit(1);
        }
    }
    delete_target_file(real_file_path);

    rc = memset_s(real_file_path, (PATH_MAX + 1), 0, (PATH_MAX + 1));
    securec_check_c(rc, "\0", "\0");

    /* Delete the dw build file, if it exists. */
    rc = snprintf_s(dw_file_path, PATH_MAX, PATH_MAX - 1, "%s/%s", target_dir, DW_BUILD_FILE_NAME);
    securec_check_ss_c(rc, "\0", "\0");
    if (realpath(dw_file_path, real_file_path) == NULL) {
        if (real_file_path[0] == '\0') {
            pg_log(PG_WARNING, _("could not get canonical path for file %s: %s\n"), dw_file_path, gs_strerror(errno));
            disconnect_and_exit(1);
        }
    }

    delete_target_file(real_file_path);

    /* Create the dw build file. */
    if ((fd = open(real_file_path, (DW_FILE_FLAG | O_CREAT), DW_FILE_PERM)) < 0) {
        pg_log(PG_WARNING, _("could not create file %s: %s\n"), real_file_path, gs_strerror(errno));
        disconnect_and_exit(1);
    }

    unaligned_buf = (char*)malloc(BLCKSZ + BLCKSZ);
    if (unaligned_buf == NULL) {
        pg_log(PG_WARNING, _("out of memory"));
        close(fd);
        disconnect_and_exit(1);
    }

    buf = (char*)TYPEALIGN(BLCKSZ, unaligned_buf);
    rc = memset_s(buf, BLCKSZ, 0, BLCKSZ);
    securec_check_c(rc, "\0", "\0");

    if (write(fd, buf, BLCKSZ) != BLCKSZ) {
        pg_log(PG_WARNING, _("could not write data to file %s: %s\n"), real_file_path, gs_strerror(errno));
        close(fd);
        disconnect_and_exit(1);
    }

    free(unaligned_buf);
    close(fd);
}

void get_xlog_location(char (&xlog_location)[MAXPGPATH])
{
    /*
     * check if user define xlog dir using symbolic link,
     * yes, xlog_location set to linktarget directory,
     * no, xlog_loaction set to basedir/pg_xlog
     *
     * if basedir/pg_xlog not exist, set xlog_location to basedir/pg_xlog
     */
    char linkpath[MAXPGPATH] = {0};
    errno_t rc = EOK;
    struct stat stbuf;
    int nRet = snprintf_s(xlog_location, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog", basedir);
    securec_check_ss_c(nRet, "", "");

    if (lstat(xlog_location, &stbuf) == 0) {
#ifndef WIN32
        if (S_ISLNK(stbuf.st_mode)) {
#else
        if (pgwin32_is_junction(xlog_location)) {
#endif
#if defined(HAVE_READLINK) || defined(WIN32)
            int rllen;

            rllen = readlink(xlog_location, linkpath, sizeof(linkpath));
            if (rllen < 0) {
                pg_log(PG_WARNING, _("could not read symbolic link.\n"));
            }
            if (rllen >= (int)sizeof(linkpath)) {
                pg_log(PG_WARNING, _("symbolic link target is too long.\n"));
            }
            linkpath[MAXPGPATH - 1] = '\0';
#else
            pg_log(PG_WARNING, _("symbolic links are not supported on this platform.\n"));
            exit(1);
#endif
            rc = strncpy_s(xlog_location, MAXPGPATH, linkpath, MAXPGPATH - 1);
            securec_check_c(rc, "", "");
        }
    }
    xlog_location[MAXPGPATH - 1] = '\0';
}
