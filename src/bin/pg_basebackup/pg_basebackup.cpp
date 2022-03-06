/* ---------------------------------------------------------------------------------------
 *
 * pg_basebackup.cpp
 * receive a base backup using streaming replication protocol
 *
 * Author: Magnus Hagander <magnus@hagander.net>
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * src/bin/pg_basebackup/pg_basebackup.cpp
 *
 * ---------------------------------------------------------------------------------------
 */


#define FRONTEND 1

/*
 * We have to use postgres.h not postgres_fe.h here, because there's so much
 * backend-only stuff in the XLOG include files we need.  But we need a
 * frontend-ish environment otherwise. Hence this ugly hack.
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "libpq/libpq-fe.h"
#include <unistd.h>
#include <dirent.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "getopt_long.h"
#include "receivelog.h"
#include "streamutil.h"
#include "gs_tar_const.h"
#include "bin/elog.h"
#include "lib/string.h"

#ifdef ENABLE_MOT
#include "fetchmot.h"
#endif

typedef struct TablespaceListCell {
    struct TablespaceListCell* next;
    char old_dir[MAXPGPATH];
    char new_dir[MAXPGPATH];
} TablespaceListCell;

typedef struct TablespaceList {
    TablespaceListCell* head;
    TablespaceListCell* tail;
} TablespaceList;



/* Global options */
char* basedir = NULL;
static TablespaceList tablespacee_dirs = {NULL, NULL};
char format = 'p'; /* p(lain)/t(ar) */
char *label = "gs_basebackup base backup";
bool showprogress = false;
int verbose = 0;
int compresslevel = 0;
bool includewal = true;
bool streamwal = true;
bool fastcheckpoint = false;

extern char **tblspaceDirectory;
extern int tblspaceCount;
extern int tblspaceIndex;

extern int standby_message_timeout; /* 10 sec = default */

/* Progress counters */
static uint64 totalsize;
static uint64 totaldone;

static int standby_message_timeout_local = 10 ; /* 10 sec = default */

/* Pipe to communicate with background wal receiver process */
#ifndef WIN32
static int bgpipe[2] = {-1, -1};
#endif

/* Handle to child process */
static pid_t bgchild = -1;

/* End position for xlog streaming, empty string if unknown yet */
static XLogRecPtr xlogendptr;

#ifndef WIN32
static int has_xlogendptr = 0;
#else
static volatile LONG has_xlogendptr = 0;
#endif

/* Function headers */
static void usage(void);
static void GsTarUsage(void);
static void verify_dir_is_empty_or_create(char* dirname);
static void progress_report(int tablespacenum, const char* filename);

static void ReceiveTarFile(PGconn *conn, PGresult *res, int rownum);
static void ReceiveAndUnpackTarFile(PGconn *conn, PGresult *res, int rownum);
static void BaseBackup(void);
static void backup_dw_file(const char *target_dir);

static bool reached_end_position(XLogRecPtr segendpos, uint32 timeline, bool segment_finished);
static void free_basebackup();

static int GsTar(int argc, char** argv);
static int GsBaseBackup(int argc, char** argv);

static const char* get_tablespace_mapping(const char* dir);

static void TablespaceValueCheck(TablespaceListCell* cell, const char* arg)
{
    if (!*cell->old_dir || !*cell->new_dir) {
        fprintf(stderr, _("%s: invalid tablespace mapping format \"%s\", must be \"OLDDIR=NEWDIR\"\n"), progname, arg);
        exit(1);
    }

    /*
     * This check isn't absolutely necessary.  But all tablespaces are created
     * with absolute directories, so specifying a non-absolute path here would
     * just never match, possibly confusing users.  It's also good to be
     * consistent with the new_dir check.
     */
    if (!is_absolute_path(cell->old_dir)) {
        fprintf(stderr, _("%s: old directory is not an absolute path in tablespace mapping: %s\n"), progname,
                cell->old_dir);
        exit(1);
    }

    if (!is_absolute_path(cell->new_dir)) {
        fprintf(stderr, _("%s: new directory is not an absolute path in tablespace mapping: %s\n"), progname,
                cell->new_dir);
        exit(1);
    }
}

/*
 * Split argument into old_dir and new_dir and append to tablespace mapping
 * list.
 */
static void tablespace_list_append(const char* arg)
{
    TablespaceListCell* cell = (TablespaceListCell*)xmalloc0(sizeof(TablespaceListCell));
    char* dst = NULL;
    char* dst_ptr = NULL;
    const char* arg_ptr = NULL;

    dst_ptr = dst = cell->old_dir;
    for (arg_ptr = arg; *arg_ptr; arg_ptr++) {
        if (dst_ptr - dst >= MAXPGPATH) {
            fprintf(stderr, _("%s: directory name too long\n"), progname);
            exit(1);
        }

        if ((*arg_ptr == '\\') && (*(arg_ptr + 1) == '=')) {
            ; /* skip backslash escaping = */
        } else if (*arg_ptr == '=' && (arg_ptr == arg || *(arg_ptr - 1) != '\\')) {
            if (*cell->new_dir) {
                fprintf(stderr, _("%s: multiple \"=\" signs in tablespace mapping\n"), progname);
                exit(1);
            } else {
                dst = dst_ptr = cell->new_dir;
            }
        } else {
            *dst_ptr++ = *arg_ptr;
        }
    }
    TablespaceValueCheck(cell, arg);
    /*
     * Comparisons done with these values should involve similarly
     * canonicalized path values.  This is particularly sensitive on Windows
     * where path values may not necessarily use Unix slashes.
     */
    canonicalize_path(cell->old_dir);
    canonicalize_path(cell->new_dir);

    if (tablespacee_dirs.tail != NULL) {
        tablespacee_dirs.tail->next = cell;
    } else {
        tablespacee_dirs.head = cell;
    }
    tablespacee_dirs.tail = cell;
}

#define TABLESPACE_LIST_RELEASE()                     \
    if (tblspaceDirectory != NULL) {                  \
        int k;                                        \
        for (k = 0; k < tblspaceCount; k++) {         \
            if (tblspaceDirectory[k] != NULL) {       \
                free(tblspaceDirectory[k]);           \
                tblspaceDirectory[k] = NULL;          \
            }                                         \
        }                                             \
        free(tblspaceDirectory);                      \
        tblspaceDirectory = NULL;                     \
    }

static void show_full_build_process(const char *errmg)
{
    pg_log(PG_PROGRESS, _("%s\n"), errmg);
}

static void usage(void)
{
    printf(_("%s takes a base backup of a running openGauss server.\n\n"), progname);
    printf(_("Usage:\n"));
    printf(_("  %s [OPTION]...\n"), progname);
    printf(_("\nOptions controlling the output:\n"));
    printf(_("  -D, --pgdata=DIRECTORY receive base backup into directory\n"));
    printf(_("  -F, --format=p|t       output format (plain (default), tar)\n"));
    printf(_("  -T, --tablespace-mapping=OLDDIR=NEWDIR\n"
             "                         relocate tablespace in OLDDIR to NEWDIR\n"));
    printf(_("  -x, --xlog             include required WAL files in backup (fetch mode)\n"));
    printf(_("  -X, --xlog-method=fetch|stream\n"
             "                         include required WAL files with specified method\n"));
    printf(_("  -z, --gzip             compress tar output\n"));
    printf(_("  -Z, --compress=0-9     compress tar output with given compression level\n"));
    printf(_("\nGeneral options:\n"));
    printf(_("  -c, --checkpoint=fast|spread\n"
             "                         set fast or spread checkpointing\n"));
    printf(_("  -l, --label=LABEL      set backup label\n"));
    printf(_("  -P, --progress         show progress information\n"));
    printf(_("  -v, --verbose          output verbose messages\n"));
    printf(_("  -V, --version          output version information, then exit\n"));
    printf(_("  -?, --help             show this help, then exit\n"));
    printf(_("\nConnection options:\n"));
    printf(_("  -h, --host=HOSTNAME    database server host or socket directory\n"));
    printf(_("  -p, --port=PORT        database server port number\n"));
    printf(_("  -s, --status-interval=INTERVAL\n"
             "                         time between status packets sent to server (in seconds)\n"));
    printf(_("  -t, --rw-timeout=RW_TIMEOUT\n"
             "                         read-write timeout during idle connection.(in seconds)\n"));
    printf(_("  -U, --username=NAME    connect as specified database user\n"));
    printf(_("  -w, --no-password      never prompt for password\n"));
    printf(_("  -W, --password         force password prompt (should happen automatically)\n"));

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    printf(_("\nReport bugs to GaussDB support.\n"));
#else
    printf(_("\nReport bugs to community@opengauss.org> or join opengauss community <https://opengauss.org>.\n"));
#endif
}

static void GsTarUsage(void)
{
    printf(_("%s untars files created by gs_basebackup tar format.\n\n"), progname);
    printf(_("Usage:\n"));
    printf(_("  %s [OPTION]...\n"), progname);
    printf(_("\nOptions controlling the output:\n"));
    printf(_("  -D, --destination=DIRECTORY   untar files into directory\n"));
    printf(_("  -F, --filename=FILENAME       filename to be untar\n"));
}

static void tablespace_list_create()
{
    if (tblspaceCount <= 0) {
        return;
    }
    tblspaceDirectory = (char **)malloc(tblspaceCount * sizeof(char *));
    if (tblspaceDirectory == NULL) {
        pg_log(PG_WARNING, _(" Out of memory occured during creating tablespace list"));
        exit(1);
    }
    errno_t rcm = memset_s(tblspaceDirectory, tblspaceCount * sizeof(char *), 0, tblspaceCount * sizeof(char *));
    securec_check_c(rcm, "", "");
}

static void save_tablespace_dir(const char *dir)
{
    if (tblspaceDirectory != NULL) {
        tblspaceDirectory[tblspaceIndex] = xstrdup(dir);
        tblspaceIndex++;
    }
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
        errno_t rc = EOK;

        /*
         * Don't have the end pointer yet - check our pipe to see if it has
         * been sent yet.
         */
        FD_ZERO(&fds);
        FD_SET(bgpipe[0], &fds);

        rc = memset_s(&tv, sizeof(tv), 0, sizeof(tv));
        securec_check_c(rc, "", "");

        r = select(bgpipe[0] + 1, &fds, NULL, NULL, &tv);
        if (r == 1) {
            char xlogend[64];
            uint32 hi, lo;

            rc = memset_s(xlogend, sizeof(xlogend), 0, sizeof(xlogend));
            securec_check_c(rc, "", "");

            r = read(bgpipe[0], xlogend, sizeof(xlogend));
            if (r < 0) {
                pg_log(stderr, _("%s: could not read from ready pipe: %s\n"), progname, strerror(errno));
                exit(1);
            }

            xlogend[63] = '\0';

            if (sscanf_s(xlogend, "%X/%X", &hi, &lo) != 2) {
                pg_log(stderr, _("%s: could not parse transaction log location \"%s\"\n"), progname, xlogend);
                exit(1);
            }
            xlogendptr = ((uint64)hi) << 32 | lo;
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
    PGconn *bgconn;
    XLogRecPtr startptr;
    char xlogdir[MAXPGPATH];
    char *sysidentifier;
    int timeline;
} logstreamer_param;

static int LogStreamerMain(logstreamer_param *param)
{
    if (!ReceiveXlogStream(param->bgconn, param->startptr, param->timeline, (const char *)param->sysidentifier,
        (const char *)param->xlogdir, reached_end_position, standby_message_timeout_local, true))

        /*
         * Any errors will already have been reported in the function process,
         * but we need to tell the parent that we didn't shutdown in a nice
         * way.
         */
        return 1;

    PQfinish(param->bgconn);
    param->bgconn = NULL;
    return 0;
}

/*
 * Initiate background process for receiving xlog during the backup.
 * The background stream will use its own database connection so we can
 * stream the logfile in parallel with the backups.
 */
static void StartLogStreamer(const char *startpos, uint32 timeline, char *sysidentifier)
{
    logstreamer_param *param = NULL;
    uint32 hi, lo;
    uint32 pathlen = 0;
    errno_t errorno = EOK;

    param = (logstreamer_param *)xmalloc0(sizeof(logstreamer_param));
    param->timeline = timeline;
    param->sysidentifier = sysidentifier;

    /* Convert the starting position */
    if (sscanf_s(startpos, "%X/%X", &hi, &lo) != 2) {
        pg_log(stderr, _("%s: could not parse transaction log location \"%s\"\n"), progname, startpos);
        PQfreemem(param);
        param = NULL;
        disconnect_and_exit(1);
    }
    param->startptr = ((uint64)hi) << 32 | lo;
    /* Round off to even segment position */
    param->startptr -= param->startptr % XLOG_SEG_SIZE;

#ifndef WIN32
    /* Create our background pipe */
    if (pipe(bgpipe) < 0) {
        pg_log(stderr, _("%s: could not create pipe for background process: %s\n"), progname, strerror(errno));
        PQfreemem(param);
        param = NULL;
        disconnect_and_exit(1);
    }
#endif

    /* Get a second connection */
    param->bgconn = GetConnection();
    if (NULL == param->bgconn) {
        /* Error message already written in GetConnection() */
        PQfreemem(param);
        param = NULL;
        exit(1);
    }

    /*
     * Always in plain format, so we can write to basedir/pg_xlog. But the
     * directory entry in the tar file may arrive later, so make sure it's
     * created before we start.
     */
    pathlen = strlen("pg_xlog") + 1 + strlen(basedir) + 1;
    errorno = snprintf_s(param->xlogdir, sizeof(param->xlogdir), pathlen, "%s/pg_xlog", basedir);
    securec_check_ss_c(errorno, "", "");

    verify_dir_is_empty_or_create(param->xlogdir);

    /*
     * Start a child process and tell it to start streaming. On Unix, this is
     * a fork(). On Windows, we create a thread.
     */
#ifndef WIN32
    bgchild = fork();
    if (bgchild == 0) {
        /* in child process */
        exit(LogStreamerMain(param));
    } else if (bgchild < 0) {
        fprintf(stderr, _("%s: could not create background process: %s\n"), progname, strerror(errno));
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
        fprintf(stderr, _("%s: could not create background thread: %s\n"), progname, strerror(errno));
        PQfreemem(param);
        param = NULL;
        disconnect_and_exit(1);
    }
#endif
}

/*
 * Verify that the given directory exists and is empty. If it does not
 * exist, it is created. If it exists but is not empty, an error will
 * be give and the process ended.
 */
static void verify_dir_is_empty_or_create(char *dirname)
{
    switch (pg_check_dir(dirname)) {
        case 0:

            /*
             * Does not exist, so create
             */
            if (pg_mkdir_p(dirname, S_IRWXU) == -1) {
                fprintf(stderr, _("%s: could not create directory \"%s\": %s\n"), progname, dirname, strerror(errno));
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
            fprintf(stderr, _("%s: directory \"%s\" exists but is not empty\n"), progname, dirname);
            disconnect_and_exit(1);
        case -1:

            /*
             * Access problem
             */
            fprintf(stderr, _("%s: could not access directory \"%s\": %s\n"), progname, dirname, strerror(errno));
            disconnect_and_exit(1);
        default:
            break;
    }
}

/*
 * Print a progress report based on the global variables. If verbose output
 * is enabled, also print the current file name.
 */
static void progress_report(int tablespacenum, const char *filename)
{
    int percent = (int)((totaldone / 1024) * 100 / totalsize);
    char totaldone_str[32];
    char totalsize_str[32];
    errno_t errorno = EOK;

    /*
     * Avoid overflowing past 100% or the full size. This may make the total
     * size number change as we approach the end of the backup (the estimate
     * will always be wrong if WAL is included), but that's better than having
     * the done column be bigger than the total.
     */
    if (percent > 100) {
        percent = 100;
    }

    if (totaldone / 1024 > totalsize) {
        totalsize = totaldone / 1024;
    }

    /*
     * Separate step to keep platform-dependent format code out of
     * translatable strings.  And we only test for INT64_FORMAT availability
     * in snprintf, not fprintf.
     */
    errorno =
        snprintf_s(totaldone_str, sizeof(totalsize_str), sizeof(totaldone_str) - 1, INT64_FORMAT, totaldone / 1024);
    securec_check_ss_c(errorno, "", "");

    errorno = snprintf_s(totalsize_str, sizeof(totalsize_str), sizeof(totalsize_str) - 1, INT64_FORMAT, (int64)totalsize);
    securec_check_ss_c(errorno, "", "");

    if (verbose) {
        if (NULL == filename) {
            /*
             * No filename given, so clear the status line (used for last
             * call)
             */
            fprintf(stderr,
                ngettext("%s/%s kB (100%%), %d/%d tablespace %35s", "%s/%s kB (100%%), %d/%d tablespaces %35s",
                tblspaceCount),
                totaldone_str, totalsize_str, tablespacenum, tblspaceCount, "");
        } else {
            fprintf(stderr,
                ngettext("%s/%s kB (%d%%), %d/%d tablespace (%-30.30s)",
                "%s/%s kB (%d%%), %d/%d tablespaces (%-30.30s)", tblspaceCount),
                totaldone_str, totalsize_str, percent, tablespacenum, tblspaceCount, filename);
        }
    } else {
        fprintf(stderr,
            ngettext("%s/%s kB (%d%%), %d/%d tablespace", "%s/%s kB (%d%%), %d/%d tablespaces", tblspaceCount),
            totaldone_str, totalsize_str, percent, tablespacenum, tblspaceCount);
    }

    fprintf(stderr, "\r");
}

/*
 * Receive a tar format file from the connection to the server, and write
 * the data from this file directly into a tar file. If compression is
 * enabled, the data will be compressed while written to the file.
 *
 * The file will be named base.tar[.gz] if it's for the main data directory
 * or <tablespaceoid>.tar[.gz] if it's for another tablespace.
 *
 * No attempt to inspect or validate the contents of the file is done.
 */
static void ReceiveTarFile(PGconn *conn, PGresult *res, int rownum)
{
#define MAX_REALPATH_LEN 4096
    char filename[MAXPGPATH];
    char *copybuf = NULL;
    FILE *tarfile = NULL;
    errno_t errorno = EOK;
    bool basetablespace = (bool)PQgetisnull(res, rownum, 0);

#ifdef HAVE_LIBZ
    gzFile ztarfile = NULL;
    int duplicatedfd = -1;
#endif

    if (basetablespace) {
        /*
         * Base tablespaces
         */
        if (strcmp(basedir, "-") == 0) {
#ifdef HAVE_LIBZ
            if (compresslevel != 0) {
                duplicatedfd = dup(fileno(stdout));
                if (duplicatedfd == -1) {
                    pg_log(stderr, _("%s: could not allocate dup fd by fileno(stdout): %s\n"), progname,
                        strerror(errno));
                    disconnect_and_exit(1);
                }

                ztarfile = gzdopen(duplicatedfd, "wb");
                if (gzsetparams(ztarfile, compresslevel, Z_DEFAULT_STRATEGY) != Z_OK) {
                    pg_log(stderr, _("%s: could not set compression level %d: %s\n"), progname, compresslevel,
                        get_gz_error(ztarfile));
                    close(duplicatedfd);
                    duplicatedfd = -1;
                    disconnect_and_exit(1);
                }
                close(duplicatedfd);
                duplicatedfd = -1;
            } else
#endif
                tarfile = stdout;
            errorno = strcpy_s(filename, MAXPGPATH, "-");
            securec_check_c(errorno, "\0", "\0");
        } else {
#ifdef HAVE_LIBZ
            if (compresslevel != 0) {
                errorno = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/base.tar.gz", basedir);
                securec_check_ss_c(errorno, "", "");
                ztarfile = openGzFile(filename, compresslevel);
                if (ztarfile == NULL) {
                    pg_log(stderr,
                        _("%s: could not set compression level %d: %s\n"),
                        progname,
                        compresslevel,
                        get_gz_error(ztarfile));
                    disconnect_and_exit(1);
                }
            } else
#endif
            {
                errorno = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/base.tar", basedir);
                // basdir has been realpath before
                securec_check_ss_c(errorno, "", "");
                tarfile = fopen(filename, "wb");
            }
        }
    } else {
        /*
         * Specific tablespace
         */
#ifdef HAVE_LIBZ
        if (compresslevel != 0) {
            errorno = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/%s.tar.gz", basedir,
                PQgetvalue(res, rownum, 0));
            securec_check_ss_c(errorno, "", "");
            ztarfile = openGzFile(filename, compresslevel);
        } else
#endif
        {
            errorno = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/%s.tar", basedir,
                PQgetvalue(res, rownum, 0));
            securec_check_ss_c(errorno, "", "");
            tarfile = fopen(filename, "wb");
        }
    }

#ifdef HAVE_LIBZ
    if (compresslevel != 0) {
        if (ztarfile == NULL) {
            /* Compression is in use */
            pg_log(stderr, _("%s: could not create compressed file \"%s\": %s\n"), progname, filename,
                get_gz_error(ztarfile));
            disconnect_and_exit(1);
        }
    } else
#endif
    {
        /* Either no zlib support, or zlib support but compresslevel = 0 */
        if (NULL == tarfile) {
            pg_log(stderr, _("%s: could not create file \"%s\": %s\n"), progname, filename, strerror(errno));
            disconnect_and_exit(1);
        }
    }

    /*
     * Get the COPY data stream
     */
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COPY_OUT) {
        pg_log(stderr, _("%s: could not get COPY data stream: %s"), progname, PQerrorMessage(conn));
        if (tarfile != NULL) {
            fclose(tarfile);
            tarfile = NULL;
        }
        disconnect_and_exit(1);
    }
    PQclear(res);

    while (true) {
        if (copybuf != NULL) {
            PQfreemem(copybuf);
            copybuf = NULL;
        }

        int r = PQgetCopyData(conn, &copybuf, 0);
        if (r == -1) {
#ifdef HAVE_LIBZ
            if (ztarfile != NULL) {
                if (gzclose(ztarfile) != 0) {
                    pg_log(stderr,
                        _("%s: could not close compressed file \"%s\": %s\n"),
                        progname,
                        filename,
                        get_gz_error(ztarfile));
                    disconnect_and_exit(1);
                }
            } else
#endif
            {
                if (strcmp(basedir, "-") != 0) {
                    if (fclose(tarfile) != 0) {
                        pg_log(stderr, _("%s: could not close file \"%s\": %s\n"), progname, filename,
                            strerror(errno));
                        tarfile = NULL;
                        disconnect_and_exit(1);
                    }
                    tarfile = NULL;
                }
            }

            break;
        } else if (r == -2) {
            pg_log(stderr, _("%s: could not read COPY data: %s"), progname, PQerrorMessage(conn));
            disconnect_and_exit(1);
        }

#ifdef HAVE_LIBZ
        if (ztarfile != NULL) {
            if (!writeGzFile(ztarfile, copybuf, r)) {
                pg_log(stderr,
                    _("%s: could not write to compressed file \"%s\": %s\n"),
                    progname,
                    filename,
                    get_gz_error(ztarfile));
                disconnect_and_exit(1);
            }
        } else
#endif
        {
            if (fwrite(copybuf, r, 1, tarfile) != 1) {
                pg_log(stderr, _("%s: could not write to file \"%s\": %s\n"), progname, filename, strerror(errno));
                disconnect_and_exit(1);
            }
        }
        totaldone += r;
        if (showprogress)
            progress_report(rownum, filename);
    } /* while (1) */

    if (copybuf != NULL) {
        PQfreemem(copybuf);
        copybuf = NULL;
    }

    if (tarfile != NULL) {
        fclose(tarfile);
        tarfile = NULL;
    }
}

/*
 * Retrieve tablespace path, either relocated or original depending on whether
 * -T was passed or not.
 */
static const char* get_tablespace_mapping(const char* dir)
{
    TablespaceListCell* cell = NULL;
    char canon_dir[MAXPGPATH];
    errno_t errorno = EOK;

    /* Canonicalize path for comparison consistency */
    errorno = strcpy_s(canon_dir, MAXPGPATH, dir);
    securec_check_c(errorno, "\0", "\0");
    canonicalize_path(canon_dir);

    for (cell = tablespacee_dirs.head; cell; cell = cell->next) {
        if (strcmp(canon_dir, cell->old_dir) == 0) {
            return cell->new_dir;
        }
    }

    return dir;
}

static PGresult* backup_get_result(PGconn* conn)
{
    PGresult *res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COPY_OUT) {
        pg_log(stderr, _("%s: could not get COPY data stream: %s"), progname, PQerrorMessage(conn));
        disconnect_and_exit(1);
    }
    return res;
}

static bool check_input_path_relative_path(const char* input_path_value)
{
    if (strstr(input_path_value, "..") != NULL) {
        return true;
    }
    return false;
}

static bool IsXlogDir(const char* filename)
{
    size_t xlogDirLen = strlen("/pg_xlog");
    size_t filenameLen = strlen(filename);
    /* pg_xlog may be created in StartLogStreamer, which will be called when 'streamwal' is true */
    if (filenameLen >= xlogDirLen && streamwal &&
        strcmp(filename + filenameLen - xlogDirLen, "/pg_xlog") == 0) {
        return true;
    }
    return false;
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
static void ReceiveAndUnpackTarFile(PGconn *conn, PGresult *res, int rownum)
{
    char current_path[MAXPGPATH] = {0};
    char filename[MAXPGPATH] = {0};
    char absolut_path[MAXPGPATH] = {0};
    uint64 current_len_left = 0;
    uint64 current_padding = 0;
    const char* mapped_tblspc_path = NULL;
    bool basetablespace = (bool)PQgetisnull(res, rownum, 0);
    char* copybuf = NULL;
    FILE* file = NULL;
    const char* get_value = NULL;

    errno_t errorno = EOK;

    if (basetablespace) {
        errorno = strncpy_s(current_path, MAXPGPATH, basedir, strlen(basedir));
        securec_check_c(errorno, "", "");
        current_path[MAXPGPATH - 1] = '\0';
    } else {
        char* buffer = PQgetvalue(res, rownum, 1);
        if (buffer == NULL) {
            pg_log(PG_WARNING, _("PQgetvalue get value failed\n"));
            disconnect_and_exit(1);
        }
        get_value = get_tablespace_mapping(buffer);
        char* relative = PQgetvalue(res, rownum, 3);
        if (*relative == '1') {
            errorno = snprintf_s(current_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", basedir, get_value);
            securec_check_ss_c(errorno, "\0", "\0");
        } else {
            errorno = strncpy_s(current_path, MAXPGPATH, get_value, strlen(get_value));
            securec_check_c(errorno, "\0", "\0");
        }
        current_path[MAXPGPATH - 1] = '\0';
    }

    /*
     * Get the COPY data
     */
    res = backup_get_result(conn);
    PQclear(res);

    while (1) {
        int r;

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
        } else if (r == TAR_READ_ERROR) {
            pg_log(stderr, _("%s: could not read COPY data: %s"), progname, PQerrorMessage(conn));
            disconnect_and_exit(1);
        }

        if (file == NULL) {
            /* new file */
            int filemode;

            /*
             * No current file, so this must be the header for a new file
             */
            if (r != TAR_BLOCK_SIZE) {
                pg_log(stderr, _("%s: invalid tar block header size: %d\n"), progname, r);
                disconnect_and_exit(1);
            }
            totaldone += TAR_BLOCK_SIZE;

            if (sscanf_s(copybuf + TAR_LEN_LEFT, "%20lo", &current_len_left) != 1) {
                pg_log(stderr, _("%s: could not parse file size\n"), progname);
                disconnect_and_exit(1);
            }

            /* Set permissions on the file */
            if (sscanf_s(&copybuf[1024], "%07o ", (unsigned int*)&filemode) != 1) {
                pg_log(stderr, _("%s: could not parse file mode\n"), progname);
                disconnect_and_exit(1);
            }

            /*
             * All files are padded up to 512 bytes
             */
            if (current_len_left < 0 || current_len_left > INT_MAX - 511) {
                pg_log(stderr, _("%s: the file '%s' is too big or file size is invalid\n"), progname, copybuf);
                disconnect_and_exit(1);
            }
            current_padding = PADDING_LEFT(current_len_left);

            /*
             * First part of header is zero terminated filename
             */
            if (check_input_path_relative_path(copybuf) || check_input_path_relative_path(current_path)) {
                pg_log(stderr,
                       _("%s: the copybuf/current_path file path including .. is unallowed: %s\n"),
                       progname,
                       strerror(errno));
                disconnect_and_exit(1);
            }
            errorno = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/%s", current_path, copybuf);
            securec_check_ss_c(errorno, "", "");

            if (filename[strlen(filename) - 1] == '/') {
                /*
                 * Ends in a slash means directory or symlink to directory
                 */
                if (strstr(filename, "../") != NULL) {
                    pg_log(stderr,
                        _("%s: path is illegal \n"),
                        filename);
                    disconnect_and_exit(1);
                }
                if (copybuf[TAR_FILE_TYPE] == TAR_TYPE_DICTORY) {
                    /*
                     * Directory
                     */
                    filename[strlen(filename) - 1] = '\0'; /* Remove trailing slash */
                    if (mkdir(filename, S_IRWXU) != 0) {
                        /*
                         * When streaming WAL, pg_xlog will have been created
                         * by the wal receiver process. So just ignore creation
                         * failures on related directories.
                         */
                        if (!((pg_str_endswith(filename, "/pg_xlog") || pg_str_endswith(filename, "/archive_status")) &&
                                errno == EEXIST)) {
                            pg_log(stderr,
                                _("%s: could not create directory \"%s\": %s\n"),
                                progname,
                                filename,
                                strerror(errno));
                            disconnect_and_exit(1);
                        }
                    }
#ifndef WIN32
                    if (chmod(filename, (mode_t)filemode))
                        pg_log(stderr, _("%s: could not set permissions on directory \"%s\": %s\n"), progname,
                            filename, strerror(errno));
#endif
                } else if (copybuf[TAR_FILE_TYPE] == '2') {
                    /*
                     * Symbolic link
                     */
                    filename[strlen(filename) - 1] = '\0'; /* Remove trailing slash */
                    mapped_tblspc_path = get_tablespace_mapping(&copybuf[TAR_FILE_TYPE + 1]);
                    if (symlink(mapped_tblspc_path, filename) != 0) {
                        if (IsXlogDir(filename)) {
                            pg_log(stderr, _("WARNING: could not create symbolic link for pg_xlog,"
                                " will backup data to \"%s\" directly\n"), filename);
                        } else {
                            pg_log(stderr,
                                _("%s: could not create symbolic link from \"%s\" to \"%s\": %s\n"),
                                progname,
                                filename,
                                mapped_tblspc_path,
                                strerror(errno));
                            disconnect_and_exit(1);
                        }
                    }
                } else if (copybuf[1080] == '3') {
                    /*
                     * Symbolic link for relative tablespace. please refer to function _tarWriteHeader
                     */
                    filename[strlen(filename) - 1] = '\0'; /* Remove trailing slash */

                    errorno = snprintf_s(absolut_path,
                        sizeof(absolut_path),
                        sizeof(absolut_path) - 1,
                        "%s/%s",
                        basedir,
                        &copybuf[TAR_FILE_TYPE + 1]);
                    securec_check_ss_c(errorno, "\0", "\0");

                    if (symlink(absolut_path, filename) != 0) {
                        if (!IsXlogDir(filename)) {
                            pg_log(PG_WARNING, _("could not create symbolic link from \"%s\" to \"%s\": %s\n"),
                                filename, &copybuf[1081], strerror(errno));
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
                pg_log(stderr, _("%s: could not create file \"%s\": %s\n"), progname, filename, strerror(errno));
                disconnect_and_exit(1);
            }

#ifndef WIN32
            if (chmod(filename, (mode_t)filemode))
                pg_log(stderr, _("%s: could not set permissions on file \"%s\": %s\n"), progname, filename,
                    strerror(errno));
#endif

            if (current_len_left == 0) {
                /*
                 * Done with this file, next one will be a new tar header
                 */
                fclose(file);
                file = NULL;
                continue;
            }
        } else {
            /*
             * Continuing blocks in existing file
             */
            if (current_len_left == 0 && (uint64)r == current_padding) {
                /*
                 * Received the padding block for this file, ignore it and
                 * close the file, then move on to the next tar header.
                 */
                fclose(file);
                file = NULL;
                totaldone += (uint64)r;
                continue;
            }

            if (fwrite(copybuf, r, 1, file) != 1) {
                pg_log(stderr, _("%s: could not write to file \"%s\": %s\n"), progname, filename, strerror(errno));
                fclose(file);
                file = NULL;
                disconnect_and_exit(1);
            }
            totaldone += r;
            if (showprogress)
                progress_report(rownum, filename);

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

    if (file != NULL) {
        pg_log(stderr, _("%s: COPY stream ended before last file was finished\n"), progname);
        fclose(file);
        file = NULL;
        disconnect_and_exit(1);
    }

    if (copybuf != NULL) {
        PQfreemem(copybuf);
        copybuf = NULL;
    }
}

static void BaseBackup(void)
{
    PGresult *res = NULL;
    char *sysidentifier = NULL;
    uint32 timeline = 0;
    char current_path[MAXPGPATH] = {0};
    char nodetablespacepath[MAXPGPATH] = {0};
    char escaped_label[MAXPGPATH] = {0};
    int i = 0;
    char xlogstart[64];
    char xlogend[64];
    errno_t rc = EOK;
    char *get_value = NULL;

    /*
     * Connect in replication mode to the server, password is needed later, so don't clear it.
     */
    conn = GetConnection();
    if (NULL == conn)
        /* Error message already written in GetConnection() */
        exit(1);

    ClearAndFreePasswd();

    /*
     * Run IDENTIFY_SYSTEM so we can get the timeline
     */
    res = PQexec(conn, "IDENTIFY_SYSTEM");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(stderr, _("%s: could not send replication command \"%s\": %s"), progname, "IDENTIFY_SYSTEM",
            PQerrorMessage(conn));
        disconnect_and_exit(1);
    }
    if (PQntuples(res) != 1 || PQnfields(res) < 3) {
        pg_log(stderr,
            _("%s: could not identify system: got %d rows and %d fields, expected %d rows and %d or more fields\n"),
            progname, PQntuples(res), PQnfields(res), 1, 3);
        disconnect_and_exit(1);
    }
    sysidentifier = strdup(PQgetvalue(res, 0, 0));
    if (NULL == sysidentifier) {
        pg_log(stderr, _("%s: strdup for sysidentifier failed! \n"), progname);
        disconnect_and_exit(1);
    }
    timeline = atoi(PQgetvalue(res, 0, 1));
    PQclear(res);

    /*
     * Start the actual backup
     */
    PQescapeStringConn(conn, escaped_label, label, sizeof(escaped_label), &i);
    rc = snprintf_s(current_path, sizeof(current_path), sizeof(current_path) - 1,
        "BASE_BACKUP LABEL '%s' %s %s %s %s %s", escaped_label, showprogress ? "PROGRESS" : "",
        includewal && !streamwal ? "WAL" : "", fastcheckpoint ? "FAST" : "", includewal ? "NOWAIT" : "",
        format == 't' ? "TABLESPACE_MAP" : "");
    securec_check_ss_c(rc, "", "");

    if (PQsendQuery(conn, current_path) == 0) {
        pg_log(stderr, _("%s: could not send replication command \"%s\": %s"), progname, "BASE_BACKUP",
            PQerrorMessage(conn));
        PQfreemem(sysidentifier);
        sysidentifier = NULL;
        disconnect_and_exit(1);
    }
    /*
     *get the xlog location
     */
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(stderr,_("could not get xlog location: %s"), PQerrorMessage(conn));
        PQfreemem(sysidentifier);
        sysidentifier = NULL;
        disconnect_and_exit(1);
    }
    if (PQntuples(res) != 1) {
        pg_log(stderr,_("no xlog location returned from server\n"));
        PQfreemem(sysidentifier);
        sysidentifier = NULL;
        disconnect_and_exit(1);
    }
    get_value = PQgetvalue(res, 0, 0);
    /* if linkpath is NULL ? */
    if (NULL == get_value) {
        pg_log(stderr, _("get xlog location failed\n"));
        PQfreemem(sysidentifier);
        sysidentifier = NULL;
        disconnect_and_exit(1);
    }
    
    PQclear(res);
 

    /*
     * Get the starting xlog position
     */
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(stderr, _("%s: could not initiate base backup: %s"), progname, PQerrorMessage(conn));
        PQfreemem(sysidentifier);
        sysidentifier = NULL;
        disconnect_and_exit(1);
    }
    if (PQntuples(res) != 1) {
        pg_log(stderr, _("%s: no start point returned from server\n"), progname);
        PQfreemem(sysidentifier);
        sysidentifier = NULL;
        disconnect_and_exit(1);
    }
    rc = strncpy_s(xlogstart, sizeof(xlogstart), PQgetvalue(res, 0, 0), sizeof(xlogstart) - 1);
    securec_check_c(rc, "", "");
    if (verbose && includewal) {
        pg_log(stderr, "transaction log start point: %s\n", xlogstart);
    }
    PQclear(res);

    rc = memset_s(xlogend, sizeof(xlogend), 0, sizeof(xlogend));
    securec_check_c(rc, "", "");

    /*
     * Get the header
     */
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(stderr, _("%s: could not get backup header: %s"), progname, PQerrorMessage(conn));
        PQfreemem(sysidentifier);
        sysidentifier = NULL;
        disconnect_and_exit(1);
    }
    if (PQntuples(res) < 1) {
        pg_log(stderr, _("%s: no data returned from server\n"), progname);
        PQfreemem(sysidentifier);
        sysidentifier = NULL;
        disconnect_and_exit(1);
    }

    /*
     * Sum up the total size, for progress reporting
     */
    totalsize = totaldone = 0;
    long int pgValue = 0;
    tblspaceCount = PQntuples(res);

    show_full_build_process("begin build tablespace list");

    /* make the tablespace directory list */
    tablespace_list_create();

    for (i = 0; i < PQntuples(res); i++) {
        pgValue = atol(PQgetvalue(res, i, 2));
        if (showprogress && ((uint64)pgValue <= UINT_MAX - totalsize)) {
            totalsize += pgValue;
        }
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
                rc = snprintf_s(prefix, MAXPGPATH, strlen(basedir) + 1, "%s/", basedir);
                securec_check_ss_c(rc, "\0", "\0");
            }
            rc = snprintf_s(nodetablespacepath, MAXPGPATH, sizeof(nodetablespacepath) - 1, "%s%s", prefix,
                tablespacepath);
            securec_check_ss_c(rc, "\0", "\0");

            const char* mappingSpacePath = get_tablespace_mapping(nodetablespacepath);
            verify_dir_is_empty_or_create((char*)mappingSpacePath);
            /* Save the tablespace directory here so we can remove it when errors happen. */
            save_tablespace_dir(mappingSpacePath);
        }
    }

    show_full_build_process("finish build tablespace list");

    /*
     * When writing to stdout, require a single tablespace
     */
    if (format == 't' && strcmp(basedir, "-") == 0 && PQntuples(res) > 1) {
        pg_log(stderr, _("%s: can only write single tablespace to stdout, database has %d\n"), progname,
            PQntuples(res));
        PQfreemem(sysidentifier);
        sysidentifier = NULL;
        disconnect_and_exit(1);
    }
    if (streamwal) {
        show_full_build_process("begin get xlog by xlogstream");
    }

    /*
     * If we're streaming WAL, start the streaming session before we start
     * receiving the actual data chunks.
     */
    if (streamwal) {
        if (verbose) {
            pg_log(stderr, _("%s: starting background WAL receiver\n"), progname);
        }
        StartLogStreamer((const char *)xlogstart, timeline, sysidentifier);
    }

    /* free sysidentifier after use */
    PQfreemem(sysidentifier);
    sysidentifier = NULL;

    /*
     * Start receiving chunks
     */
    for (i = 0; i < PQntuples(res); i++) {
        if (format == 't')
            ReceiveTarFile(conn, res, i);
        else
            ReceiveAndUnpackTarFile(conn, res, i);
    } /* Loop over all tablespaces */

    if (showprogress) {
        progress_report(PQntuples(res), NULL);
        fprintf(stderr, "\n"); /* Need to move to next line */
    }
    PQclear(res);

    /*
     * Get the stop position
     */
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(stderr, _("%s: could not get transaction log end position from server: %s"), progname,
            PQerrorMessage(conn));
        disconnect_and_exit(1);
    }
    if (PQntuples(res) != 1) {
        pg_log(stderr, _("%s: no transaction log end position returned from server\n"), progname);
        disconnect_and_exit(1);
    }
    rc = strncpy_s(xlogend, sizeof(xlogend), PQgetvalue(res, 0, 0), sizeof(xlogend) - 1);
    securec_check_c(rc, "", "");
    if (verbose && includewal) {
        pg_log(stderr, "transaction log end point: %s\n", xlogend);
    }
    PQclear(res);

    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        pg_log(stderr, _("%s: final receive failed: %s"), progname, PQerrorMessage(conn));
        disconnect_and_exit(1);
    }

    /*
     * End of copy data. Final result is already checked inside the loop.
     */
    PQclear(res);

#ifdef ENABLE_MOT
    res = PQgetResult(conn);
    if (res != NULL) {
        /*
         * We expect the result to be NULL, otherwise we received some unexpected result.
         * We just expect a 'Z' message and PQgetResult should set conn->asyncStatus to PGASYNC_IDLE,
         * otherwise we have problem! Report error and disconnect.
         */
        pg_log(stderr,
            _("%s: unexpected result received after final result, status: %u\n"),
            progname,
            PQresultStatus(res));
        disconnect_and_exit(1);
    }

    if (verbose) {
        pg_log(stderr, "%s: fetching MOT checkpoint\n", progname);
    }

    FetchMotCheckpoint(basedir, conn, progname, (bool)verbose, format, compresslevel);
#endif

    if (bgchild > 0) {
#ifndef WIN32
        int status;
        int r;
#else
        DWORD status;
        uint32 hi, lo;
#endif

        if (verbose) {
            pg_log(stderr, _("%s: waiting for background process to finish streaming...\n"), progname);
        }

#ifndef WIN32
        if ((unsigned int)(write(bgpipe[1], xlogend, strlen(xlogend))) != strlen(xlogend)) {
            pg_log(stderr, _("%s: could not send command to background pipe: %s\n"), progname, strerror(errno));
            disconnect_and_exit(1);
        }

        /* Just wait for the background process to exit */
        r = waitpid(bgchild, &status, 0);
        if (r == -1) {
            pg_log(stderr, _("%s: could not wait for child process: %s\n"), progname, strerror(errno));
            disconnect_and_exit(1);
        }
        if (r != bgchild) {
            pg_log(stderr, _("%s: child %d died, expected %d\n"), progname, r, (int)bgchild);
            disconnect_and_exit(1);
        }
        if (!WIFEXITED(status)) {
            pg_log(stderr, _("%s: child process did not exit normally\n"), progname);
            disconnect_and_exit(1);
        }
        if (WEXITSTATUS(status) != 0) {
            pg_log(stderr, _("%s: child process exited with error %d\n"), progname, WEXITSTATUS(status));
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
            pg_log(stderr, _("%s: could not parse transaction log location \"%s\"\n"), progname, xlogend);
            disconnect_and_exit(1);
        }
        xlogendptr = ((uint64)hi) << 32 | lo;
        InterlockedIncrement(&has_xlogendptr);

        /* First wait for the thread to exit */
        if (WaitForSingleObjectEx((HANDLE)bgchild, INFINITE, FALSE) != WAIT_OBJECT_0) {
            _dosmaperr(GetLastError());
            pg_log(stderr, _("%s: could not wait for child thread: %s\n"), progname, strerror(errno));
            disconnect_and_exit(1);
        }
        if (GetExitCodeThread((HANDLE)bgchild, &status) == 0) {
            _dosmaperr(GetLastError());
            pg_log(stderr, _("%s: could not get child thread exit status: %s\n"), progname, strerror(errno));
            disconnect_and_exit(1);
        }
        if (status != 0) {
            pg_log(stderr, _("%s: child thread exited with error %u\n"), progname, (unsigned int)status);
            disconnect_and_exit(1);
        }
        /* Exited normally, we're happy */
#endif
    }

    TABLESPACE_LIST_RELEASE();

    PQfinish(conn);
    conn = NULL;

    if (format == 'p') {
        /* delete dw file if exists, recreate it and write a page of zero */
        backup_dw_file(basedir);
    }

    if (verbose) {
        pg_log(stderr, "%s: base backup completed\n", progname);
    }
}

static void remove_dw_file(const char *dw_file_name, const char *target_dir, char *real_file_path)
{
    char dw_file_path[MAXPGPATH];

    int rc = snprintf_s(dw_file_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", target_dir, dw_file_name);
    securec_check_ss_c(rc, "\0", "\0");

    if (realpath(dw_file_path, real_file_path) == NULL) {
        if (real_file_path[0] == '\0') {
            pg_log(stderr, _("could not get canonical path for file %s: %s\n"), dw_file_path, gs_strerror(errno));
            disconnect_and_exit(1);
        }
    }

    (void)unlink(real_file_path);
}

/* *
 * * delete existing double write file if existed, recreate it and write one page of zero
 * * @param target_dir data base root dir
 *     */
static void backup_dw_file(const char *target_dir)
{
    int rc;
    int fd = -1;
    char real_file_path[PATH_MAX + 1] = {0};
    char *buf = NULL;
    char *unaligned_buf = NULL;

    /* Delete the dw file, if it exists. */
    remove_dw_file(OLD_DW_FILE_NAME, target_dir, real_file_path);

    rc = memset_s(real_file_path, (PATH_MAX + 1), 0, (PATH_MAX + 1));
    securec_check_c(rc, "\0", "\0");

    /* Delete the dw build file, if it exists. */
    remove_dw_file(DW_BUILD_FILE_NAME, target_dir, real_file_path);

    /* Create the dw build file. */
    if ((fd = open(real_file_path, (DW_FILE_FLAG | O_CREAT), DW_FILE_PERM)) < 0) {
        pg_log(stderr, _("could not open file %s: %s\n"), real_file_path, gs_strerror(errno));
        disconnect_and_exit(1);
    }

    unaligned_buf = (char *)malloc(BLCKSZ + BLCKSZ);
    if (unaligned_buf == NULL) {
        pg_log(stderr, _("out of memory"));
        close(fd);
        disconnect_and_exit(1);
    }

    buf = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);
    rc = memset_s(buf, BLCKSZ, 0, BLCKSZ);
    securec_check_c(rc, "\0", "\0");

    if (write(fd, buf, BLCKSZ) != BLCKSZ) {
        pg_log(stderr, _("could not write data to file %s: %s\n"), real_file_path, gs_strerror(errno));
        free(unaligned_buf);
        close(fd);
        disconnect_and_exit(1);
    }

    free(unaligned_buf);
    close(fd);
}


int main(int argc, char **argv)
{
    progname = get_progname(argv[0]);
    if (!strcmp(progname, "gs_basebackup")) {
        return GsBaseBackup(argc, argv);
    } else if (!strcmp(progname, "gs_tar")) {
        return GsTar(argc, argv);
    } else {
        fprintf(stderr, _("unsupported progname: %s"), progname);
        return 0;
    }
}

static void GsTarHelp(int argc, char** argv) {
    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            GsTarUsage();
            exit(0);
        } else if (strcmp(argv[1], "-V") == 0 || strcmp(argv[1], "--version") == 0) {
            puts("gs_basebackup " DEF_GS_VERSION);
            exit(0);
        }
    }
}

static void GsTarDirCheck(char* tarfilename)
{
    if (basedir == NULL || tarfilename == NULL) {
        fprintf(stderr, _("-D option and -F option are required.\n"));
        exit(1);
    }

    DIR* dir = opendir(basedir);
    struct dirent* ent;
    while (1) {
        ent = readdir(dir);
        if (ent <= 0) {
            break;
        }
        if ((strcmp(".", ent->d_name) == 0) || (strcmp("..", ent->d_name) == 0)) {
            continue;
        }
        if ((ent->d_type == DT_DIR) || (ent->d_type == DT_REG)) {
            fprintf(stderr, _("%s: destination dir \"%s\" no empty.\n"), progname, basedir);
            (void)closedir(dir);
            dir = NULL;
            exit(1);
        }
    }
    (void)closedir(dir);
    dir = NULL;
}

static int GsTar(int argc, char** argv)
{
    static struct option long_options[] = {{"help", no_argument, NULL, '?'},
                                           {"version", no_argument, NULL, 'V'},
                                           {"destination", required_argument, NULL, 'D'},
                                           {"fileName", no_argument, NULL, 'F'},
                                           {NULL, 0, NULL, 0}};

    int c;
    int option_index;
    char* tarfilename = NULL;

    GsTarHelp(argc, argv);

    while ((c = getopt_long(argc, argv, "D:F:", long_options, &option_index)) != -1) {
        switch (c) {
            case 'D': {
                GS_FREE(basedir);
                check_env_value_c(optarg);
                char realDir[PATH_MAX] = {0};
                if (realpath(optarg, realDir) == nullptr) {
                    fprintf(stderr, _("%s: destination dir \"%s\" failed: %m\n"), progname, optarg);
                    exit(1);
                }
                basedir = xstrdup(realDir);
                break;
            }
            case 'F': {
                check_env_value_c(optarg);
                tarfilename = xstrdup(optarg);
                break;
            }
            default:
                /*
                 * getopt_long already emitted a complaint
                 */
                fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                exit(1);
                break;
        }
    }

    GsTarDirCheck(tarfilename);

    char current_path[MAXPGPATH] = {0};
    char filename[MAXPGPATH] = {0};
    char absolut_path[MAXPGPATH] = {0};
    FILE* tarfile = NULL;
    FILE* file = NULL;
    char* copybuf = NULL;
    uint64 current_len_left = 0;
    uint64 current_padding = 0;
    errno_t errorno = EOK;

    errorno = strncpy_s(current_path, MAXPGPATH, basedir, strlen(basedir));
    securec_check_c(errorno, "\0", "\0");
    tarfile = fopen(tarfilename, "rb");
    if (tarfile == NULL) {
        fprintf(stderr, _("WARNING: %s not found!\n"), filename);
        exit(1);
    }
    while (true) {
        size_t r;

        if (copybuf != NULL) {
            PQfreemem(copybuf);
            copybuf = NULL;
        }

        current_len_left = current_len_left == 0 && current_padding == 0 ? TAR_BLOCK_SIZE : current_len_left;
        uint64 mallocSize = (current_len_left == 0 && current_padding != 0) ? current_padding : current_len_left;
        copybuf = (char*)xmalloc0(mallocSize);
        r = fread(copybuf, 1, mallocSize, tarfile);
        if (r == 0) {
            /*
             * End of chunk
             */
            if (file != NULL) {
                CLOSE_AND_SET_NULL(file);
            }
            break;
        }
        if (file == NULL) {
            /* new file */
            int filemode;

            /* No current file, so this must be the header for a new file */
            if (r != TAR_BLOCK_SIZE) {
                fprintf(stderr, "%s: invalid tar block header size: %zu\n", progname, r);
                GS_FREE(copybuf);
                CLOSE_AND_RETURN(tarfile);
            }
            totaldone += TAR_BLOCK_SIZE;

            if (sscanf_s(copybuf + 1048, "%20lo", &current_len_left) != 1) {
                fprintf(stderr, "%s: could not parse file size\n", progname);
                GS_FREE(copybuf);
                CLOSE_AND_RETURN(tarfile);

            }

            /* Set permissions on the file */
            if (sscanf_s(&copybuf[1024], "%07o ", (unsigned int*)&filemode) != 1) {
                fprintf(stderr, "%s: could not parse file mode\n", progname);
                GS_FREE(copybuf);
                CLOSE_AND_RETURN(tarfile);
            }

            /*
             * All files are padded up to 512 bytes
             */
            current_padding = PADDING_LEFT(current_len_left);

            /*
             * First part of header is zero terminated filename
             */
            if (check_input_path_relative_path(copybuf) || check_input_path_relative_path(current_path)) {
                fprintf(stderr, "%s: the copybuf/current_path file path including .. is unallowed: %s\n", progname,
                    strerror(errno));
                GS_FREE(copybuf);
                CLOSE_AND_RETURN(tarfile);
            }
            errorno = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/%s", current_path, copybuf);
            securec_check_ss_c(errorno, "", "");

            if (filename[strlen(filename) - 1] == '/') {
                /*
                 * Ends in a slash means directory or symlink to directory
                 */
                if (copybuf[1080] == '5') {
                    /*
                     * Directory
                     */
                    filename[strlen(filename) - 1] = '\0'; /* Remove trailing slash */
                    if (mkdir(filename, S_IRWXU) != 0) {
                        /*
                         * When streaming WAL, pg_xlog will have been created
                         * by the wal receiver process, so just ignore failure
                         * on that.
                         */
                        if (!IsXlogDir(filename)) {
                            fprintf(stderr, "%s: could not create directory \"%s\": %s\n", progname, filename,
                                strerror(errno));
                            GS_FREE(copybuf);
                            CLOSE_AND_RETURN(tarfile);
                        }
                    }
#ifndef WIN32
                    if (chmod(filename, (mode_t)filemode))
                        fprintf(stderr,
                            "%s: could not set permissions on directory \"%s\": %s\n",
                            progname,
                            filename,
                            strerror(errno));
#endif
                } else if (copybuf[1080] == '2') {
                    /*
                     * Symbolic link
                     */
                    filename[strlen(filename) - 1] = '\0'; /* Remove trailing slash */
                    if (symlink(&copybuf[1081], filename) != 0) {
                        if (IsXlogDir(filename)) {
                            fprintf(stderr,
                                "WARNING: could not create symbolic link for pg_xlog,"
                                " will backup data to \"%s\" directly\n",
                                filename);
                        } else {
                            fprintf(stderr, "%s: could not create symbolic link from \"%s\" to \"%s\": %s\n", progname,
                                filename, &copybuf[1081], strerror(errno));
                            GS_FREE(copybuf);
                            CLOSE_AND_RETURN(tarfile);
                        }
                    }
                } else if (copybuf[1080] == '3') {
                    /*
                     * Symbolic link for relative tablespace. please refer to function _tarWriteHeader
                     */
                    filename[strlen(filename) - 1] = '\0'; /* Remove trailing slash */

                    errorno = snprintf_s(absolut_path, sizeof(absolut_path), sizeof(absolut_path) - 1, "%s/%s", basedir,
                        &copybuf[1080 + 1]);
                    securec_check_ss_c(errorno, "\0", "\0");

                    if (symlink(absolut_path, filename) != 0) {
                        if (!IsXlogDir(filename)) {
                            pg_log(PG_WARNING, "could not create symbolic link from \"%s\" to \"%s\": %s\n", filename,
                                &copybuf[1081], strerror(errno));
                            GS_FREE(copybuf);
                            CLOSE_AND_RETURN(tarfile);

                        }
                    }
                } else {
                    pg_log(PG_WARNING, "unrecognized link indicator \"%c\"\n", copybuf[TAR_FILE_TYPE]);
                    GS_FREE(copybuf);
                    CLOSE_AND_RETURN(tarfile);
                }
                continue; /* directory or link handled */
            }

            canonicalize_path(filename);
            /*
             * regular file
             */

            file = fopen(filename, "wb");
            if (NULL == file) {
                fprintf(stderr, "%s: could not create file \"%s\": %s\n", progname, filename, strerror(errno));
                GS_FREE(copybuf);
                CLOSE_AND_RETURN(tarfile);
            }

#ifndef WIN32
            if (chmod(filename, (mode_t) filemode)) {
                fprintf(stderr, "%s: could not set permissions on file \"%s\": %s\n", progname, filename,
                    strerror(errno));
            }
#endif

            if (current_len_left == 0) {
                /*
                 * Done with this file, next one will be a new tar header
                 */
                CLOSE_AND_SET_NULL(file);
                continue;
            }
        } else {
            /*
             * Continuing blocks in existing file
             */
            if (current_len_left == 0 && r == current_padding) {
                /*
                 * Received the padding block for this file, ignore it and
                 * close the file, then move on to the next tar header.
                 */
                CLOSE_AND_SET_NULL(file);
                totaldone += r;
                current_padding -= r;
                continue;
            }

            if (fwrite(copybuf, r, 1, file) != 1) {
                fprintf(stderr, "%s: could not write to file \"%s\": %s\n", progname, filename, strerror(errno));
                CLOSE_AND_SET_NULL(file);
                GS_FREE(copybuf);
                CLOSE_AND_RETURN(tarfile);
            }
            totaldone += r;

            current_len_left -= r;
            if (current_len_left == 0 && current_padding == 0) {
                /*
                 * Received the last block, and there is no padding to be
                 * expected. Close the file and move on to the next tar
                 * header.
                 */
                CLOSE_AND_SET_NULL(file);
                continue;
            }
        } /* continuing data in existing file */
    }     /* loop over all data blocks */
    /* delete dw file if exists, recreate it and write a page of zero */
    backup_dw_file(basedir);
    CLOSE_AND_SET_NULL(tarfile);
    if (file != NULL) {
        fprintf(stderr, "%s: COPY stream ended before last file was finished\n", progname);
        GS_FREE(copybuf);
        CLOSE_AND_RETURN(file);
    }
    GS_FREE(copybuf);
    return 0;
}

static int GsBaseBackup(int argc, char** argv)
{
    static struct option long_options[] = {{"help", no_argument, NULL, '?'},
                                           {"version", no_argument, NULL, 'V'},
                                           {"pgdata", required_argument, NULL, 'D'},
                                           {"format", required_argument, NULL, 'F'},
                                           {"checkpoint", required_argument, NULL, 'c'},
                                           {"tablespace-mapping", required_argument, NULL, 'T'},
                                           {"xlog", no_argument, NULL, 'x'},
                                           {"xlog-method", required_argument, NULL, 'X'},
                                           {"gzip", no_argument, NULL, 'z'},
                                           {"compress", required_argument, NULL, 'Z'},
                                           {"label", required_argument, NULL, 'l'},
                                           {"host", required_argument, NULL, 'h'},
                                           {"port", required_argument, NULL, 'p'},
                                           {"username", required_argument, NULL, 'U'},
                                           {"no-password", no_argument, NULL, 'w'},
                                           {"password", no_argument, NULL, 'W'},
                                           {"status-interval", required_argument, NULL, 's'},
                                           {"rw-timeout", required_argument, NULL, 't'},
                                           {"verbose", no_argument, NULL, 'v'},
                                           {"progress", no_argument, NULL, 'P'},
                                           {NULL, 0, NULL, 0}};
    int c = 0, option_index = 0;
    progname = "gs_basebackup";
    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("gs_basebackup"));

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage();
            exit(0);
        } else if (strcmp(argv[1], "-V") == 0 || strcmp(argv[1], "--version") == 0) {
            puts("gs_basebackup " DEF_GS_VERSION);
            exit(0);
        }
    }

    while ((c = getopt_long(argc, argv, "D:l:c:h:p:U:s:X:F:T:Z:t:wWvPxz", long_options, &option_index)) != -1) {
        switch (c) {
            case 'D': {
                GS_FREE(basedir);
                check_env_value_c(optarg);
                char realDir[PATH_MAX] = {0};
                if (realpath(optarg, realDir) == nullptr) {
                    pg_log(stderr, _("%s: realpath dir \"%s\" failed: %m\n"), progname, optarg);
                    exit(1);
                }
                basedir = xstrdup(realDir);
                break;
            }
            case 'F':
                if (strcmp(optarg, "p") == 0 || strcmp(optarg, "plain") == 0)
                    format = 'p';
                else if (strcmp(optarg, "t") == 0 || strcmp(optarg, "tar") == 0)
                    format = 't';
                else {
                    fprintf(stderr,
                            _("%s: invalid output format \"%s\", must be \"plain\" or \"tar\"\n"),
                            progname,
                            optarg);
                    exit(1);
                }
                break;
            case 'T':
                if (strlen(optarg) >= MAXPGPATH - 1) {
                    fprintf(stderr,
                        _("%s: invalid output length"), progname);
                    exit(1);
                }
                tablespace_list_append(optarg);
                break;
            case 'x':
                streamwal = false;
                break;
            case 'X':
                check_env_value_c(optarg);
                if (strcmp(optarg, "f") == 0 || strcmp(optarg, "fetch") == 0)
                    streamwal = false;
                else if (strcmp(optarg, "s") == 0 || strcmp(optarg, "stream") == 0)
                    streamwal = true;
                else {
                    fprintf(stderr,
                            _("%s: invalid xlog-method option \"%s\", must be \"fetch\" or \"stream\"\n"),
                            progname,
                            optarg);
                    exit(1);
                }
                break;
            case 'l':
                if (label != NULL && strcmp(label, "gs_basebackup base backup") != 0) {
                    GS_FREE(label);
                }
                check_env_value_c(optarg);
                label = xstrdup(optarg);
                break;
            case 'z':
#ifdef HAVE_LIBZ
                compresslevel = Z_DEFAULT_COMPRESSION;
#else
                compresslevel = 1; /* will be rejected below */
#endif
                break;
            case 'Z':
                check_env_value_c(optarg);
                compresslevel = atoi(optarg);
                if (compresslevel < 0 || compresslevel > 9) {
                    fprintf(stderr, _("%s: invalid compression level \"%s\"\n"), progname, optarg);
                    exit(1);
                }
                break;
            case 'c':
                if (pg_strcasecmp(optarg, "fast") == 0)
                    fastcheckpoint = true;
                else if (pg_strcasecmp(optarg, "spread") == 0)
                    fastcheckpoint = false;
                else {
                    fprintf(stderr,
                            _("%s: invalid checkpoint argument \"%s\", must be \"fast\" or \"spread\"\n"),
                            progname,
                            optarg);
                    exit(1);
                }
                break;
            case 'h':
                GS_FREE(dbhost);
                check_env_value_c(optarg);
                dbhost = xstrdup(optarg);
                break;
            case 'p':
                GS_FREE(dbport);
                check_env_value_c(optarg);
                dbport = inc_dbport(optarg);
                break;
            case 'U':
                GS_FREE(dbuser);
                check_env_name_c(optarg);
                dbuser = xstrdup(optarg);
                break;
            case 'w':
                dbgetpassword = -1;
                break;
            case 'W':
                dbgetpassword = 1;
                break;
            case 's':
                if ((atoi(optarg)) < 0 || (atoi(optarg) * 1000) > PG_INT32_MAX) {
                    fprintf(stderr, _("%s: invalid status interval \"%s\"\n"), progname, optarg);
                    exit(1);
                }
                standby_message_timeout = atoi(optarg) * 1000;
                break;
            case 't':
                if (atoi(optarg) < 0 || atoi(optarg)  > PG_INT32_MAX) {
                    fprintf(stderr, _("%s: invalid read-write timeout \"%s\"\n"), progname, optarg);
                    exit(1);
                }
                rwtimeout = atoi(optarg);
                break;
            case 'v':
                verbose++;
                break;
            case 'P':
                showprogress = true;
                break;
            default:

                /*
                 * getopt_long already emitted a complaint
                 */
                fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                exit(1);
                break;
        }
    }
    
    /* If port is not specified by using -p, obtain the port through environment variables */
    if (dbport == NULL) {
        char *value = NULL;
        value = gs_getenv_r("PGPORT");
        if (value != NULL) {
            check_env_value_c(value);
            dbport = inc_dbport(value);
        } else {
            fprintf(stderr, _("%s:The specified port is missing, it can be specified by -p parameter "
                   "or import environment variables PGPORT.\n"), progname);
            exit(1); 
        }
    }

    /*
     * Any non-option arguments?
     */
    if (optind < argc) {
        fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"), progname, argv[optind]);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
        exit(1);
    }

    /*
     * Required arguments
     */
    if (basedir == NULL) {
        fprintf(stderr, _("%s: no target directory specified\n"), progname);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
        exit(1);
    }

    /*
     * Mutually exclusive arguments
     */
    if (format == 'p' && compresslevel != 0) {
        fprintf(stderr, _("%s: only tar mode backups can be compressed\n"), progname);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
        exit(1);
    }

    if (format != 'p' && streamwal) {
        fprintf(stderr, _("%s: wal streaming can only be used in plain mode\n"), progname);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
        exit(1);
    }

#ifndef HAVE_LIBZ
    if (compresslevel != 0) {
        fprintf(stderr, _("%s: this build does not support compression\n"), progname);
        exit(1);
    }
#endif

    /*
     * Verify that the target directory exists, or create it. For plaintext
     * backups, always require the directory. For tar backups, require it
     * unless we are writing to stdout.
     */
    if (format == 'p' || strcmp(basedir, "-") != 0)
        verify_dir_is_empty_or_create(basedir);

    BaseBackup();

    free_basebackup();

    pg_log(stderr, _("%s: base backup successfully\n"), progname);

    return 0;
}

static void free_basebackup()
{
    GS_FREE(basedir);
    if (label != NULL && strcmp(label, "gs_basebackup base backup") != 0) {
        GS_FREE(label);
    }
    GS_FREE(dbhost);
    GS_FREE(dbport);
    GS_FREE(dbuser);
}
