/*
 *	util.c
 *
 *	utility functions
 *
 *	Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *	contrib/pg_upgrade/util.c
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "pg_upgrade.h"
#include "miscadmin.h"

#include <signal.h>

#define LOG_MAX_SIZE (16 * 1024 * 1024)
#define LOG_MAX_TIMELEN 80
#define curLogFileMark "-current.log"

LogOpts log_opts;

static int get_log_filename(
    char* current_logfile_name, char* prefix_name, const char* log_path, char* current_localtime);
/*
 * report_status()
 *
 *	Displays the result of an operation (ok, failed, error message,...)
 */
void report_status(eLogType type, const char* fmt, ...)
{
    va_list args;
    char message[MAX_STRING];

    va_start(args, fmt);
    /* we can not care the result*/
    (void)vsnprintf(message, sizeof(message), fmt, args);
    va_end(args);

    pg_log(type, "%s\n", message);
}

/*
 * prep_status
 *
 *	Displays a message that describes an operation we are about to begin.
 *	We pad the message out to MESSAGE_WIDTH characters so that all of the "ok" and
 *	"failed" indicators line up nicely.
 *
 *	A typical sequence would look like this:
 *		prep_status("about to flarb the next %d files", fileCount );
 *
 *		if(( message = flarbFiles(fileCount)) == NULL)
 *		  report_status(PG_REPORT, "ok" );
 *		else
 *		  pg_log(PG_FATAL, "failed - %s\n", message );
 */
void prep_status(const char* fmt, ...)
{
    va_list args;
    char message[MAX_STRING];

    va_start(args, fmt);
    (void)vsnprintf(message, sizeof(message), fmt, args);
    va_end(args);

    if (strlen(message) > 0 && message[strlen(message) - 1] == '\n')
        pg_log(PG_REPORT, "%s", message);
    else
        pg_log(PG_REPORT, "%-" MESSAGE_WIDTH "s", message);
}

static void set_log_filename(char* log_new_name, char* log_old_name)
{
    int len_log_old_name = 0;
    int len_suffix_name = 0;
    int len_log_new_name = 0;
    errno_t rc = 0;

    len_log_old_name = strlen(log_old_name);
    len_suffix_name = strlen(curLogFileMark);

    len_log_new_name = len_log_old_name - len_suffix_name;
    rc = strncpy_s(log_new_name, MAXPGPATH, log_old_name, len_log_new_name);
    securec_check_c(rc, "\0", "\0");
    rc = strncat_s(log_new_name, MAXPGPATH - len_log_new_name, ".log", strlen(".log"));
    securec_check_c(rc, "\0", "\0");
}

void pg_init_logfiles(char* log_path, char* instance_name)
{
    pg_time_t current_time;
    struct tm* systm;
    char current_localtime[LOG_MAX_TIMELEN] = {0};
    int32 i;
    struct passwd* pwd;
    char path[MAXPGPATH] = {0};

    char prefixname[MAXPGPATH] = {0};
    int nRet = 0;

    current_time = time(NULL);
    systm = localtime(&current_time);
    if (NULL != systm) {
        strftime(current_localtime, LOG_MAX_TIMELEN, "-%Y-%m-%d_%H%M%S", systm);
    }

    nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/gs_upgrade", log_path);
    securec_check_ss_c(nRet, "\0", "\0");
    pwd = getpwnam(new_cluster.user);
    if (NULL == pwd) {
        mkdir(path, 0777);
    } else {
        mkdir(path, 0700);
        chown(path, pwd->pw_uid, pwd->pw_gid);
    }

    nRet = snprintf_s(prefixname, MAXPGPATH, MAXPGPATH - 1, "gs_upgrade_server%s", instance_name);
    securec_check_ss_c(nRet, "\0", "\0");

    get_log_filename(SERVER_LOG_FILE, prefixname, path, current_localtime);

#ifndef WIN32
    nRet = snprintf_s(SERVER_START_LOG_FILE, MAXPGPATH, MAXPGPATH - 1, "%s", SERVER_LOG_FILE);
    securec_check_ss_c(nRet, "\0", "\0");
#else
    get_log_filename(SERVER_START_LOG_FILE, "gs_upgrade_server_start", path, current_localtime);
#endif

    nRet = snprintf_s(prefixname, MAXPGPATH, MAXPGPATH - 1, "gs_upgrade_restore%s", instance_name);
    securec_check_ss_c(nRet, "\0", "\0");
    get_log_filename(RESTORE_LOG_FILE, prefixname, path, current_localtime);

    nRet = snprintf_s(prefixname, MAXPGPATH, MAXPGPATH - 1, "gs_upgrade_utility%s", instance_name);
    securec_check_ss_c(nRet, "\0", "\0");
    get_log_filename(UTILITY_LOG_FILE, prefixname, path, current_localtime);

    nRet = snprintf_s(prefixname, MAXPGPATH, MAXPGPATH - 1, "gs_upgrade_internal%s", instance_name);
    securec_check_ss_c(nRet, "\0", "\0");
    get_log_filename(INTERNAL_LOG_FILE, prefixname, path, current_localtime);

    if (NULL != systm) {
        strftime(current_localtime, LOG_MAX_TIMELEN, "%Y-%m-%d %H:%M:%S", systm);
    }

    for (i = 0; i < PG_LOG_TYPE_BUT; i++) {
        FILE* fp = NULL;

#ifndef WIN32
        if (i == PG_LOG_TYPE_SERVER_START) {
            continue;
        }
#endif
        if ((fp = fopen_priv(output_files[i], "a")) == NULL)
            pg_log(PG_FATAL, "cannot write to log file %s\n", output_files[i]);

        /* Start with newline because we might be appending to a file. */
        fprintf(fp,
            "\n"
            "-----------------------------------------------------------------\n"
            "  gs_upgrade run on %s\n"
            "-----------------------------------------------------------------\n\n",
            current_localtime);
        fclose(fp);
        if (pwd == NULL) {
            chmod(output_files[i], 0777);
        } else {
            (void)chmod(output_files[i], 0600);
            (void)chown(output_files[i], pwd->pw_uid, pwd->pw_gid);
        }
    }
}

static int get_log_filename(char* current_logfile_name, char* prefix_name, const char* log_path, char* log_create_time)
{
    DIR* dir = NULL;
    struct dirent* de;
    char log_temp_name[MAXPGPATH] = {0};
    char log_new_name[MAXPGPATH] = {0};
    // check validity of current log file name
    char* name_ptr = NULL;
    int nRet = 0;

    struct stat statbuf;

    if (NULL == (dir = opendir(log_path))) {
        pg_log(PG_FATAL, "cannot open to log folder %s\n", log_path);

        return -1;
    }

    while (NULL != (de = readdir(dir))) {
        // exist current log file
        if (NULL != strstr(de->d_name, prefix_name)) {
            name_ptr = strstr(de->d_name, curLogFileMark);
            if (NULL != name_ptr) {
                name_ptr += strlen(curLogFileMark);
                if ('\0' == (*name_ptr)) {
                    nRet = memset_s(current_logfile_name, MAXPGPATH, 0, MAXPGPATH);
                    securec_check_c(nRet, "\0", "\0");
                    nRet = snprintf_s(current_logfile_name, MAXPGPATH, MAXPGPATH - 1, "%s/%s", log_path, de->d_name);
                    securec_check_ss_c(nRet, "\0", "\0");
                    (void)lstat(current_logfile_name, &statbuf);

                    if (statbuf.st_size > LOG_MAX_SIZE) {
                        set_log_filename(log_temp_name, de->d_name);
                        nRet = snprintf_s(log_new_name, MAXPGPATH, MAXPGPATH - 1, "%s/%s", log_path, log_temp_name);
                        securec_check_ss_c(nRet, "\0", "\0");
                        (void)rename(current_logfile_name, log_new_name);
                        break;
                    }

                    closedir(dir);
                    return 0;
                }
            }
        }
    }

    // current log file not exists or renamed to another file
    nRet = memset_s(current_logfile_name, MAXPGPATH, 0, MAXPGPATH);
    securec_check_c(nRet, "\0", "\0");
    nRet = snprintf_s(current_logfile_name,
        MAXPGPATH,
        MAXPGPATH - 1,
        "%s/%s%s%s",
        log_path,
        prefix_name,
        log_create_time,
        curLogFileMark);
    securec_check_ss_c(nRet, "\0", "\0");

    closedir(dir);
    return 0;
}

void pg_log(eLogType type, char* fmt, ...)
{
    va_list args;
    char message[MAX_STRING] = {0};

    va_start(args, fmt);
    (void)vsnprintf(message, sizeof(message), fmt, args);
    va_end(args);

    pg_time_t current_time;
    struct tm* systm;
    char current_localtime[LOG_MAX_TIMELEN] = {0};

    current_time = time(NULL);
    systm = localtime(&current_time);
    if (NULL != systm) {
        strftime(current_localtime, LOG_MAX_TIMELEN, "%Y-%m-%d %H:%M:%S", systm);
    }

    /* PG_VERBOSE is only output in verbose mode */
    /* fopen() on log_opts.internal might have failed, so check it */
    if ((type == PG_VERBOSE || log_opts.verbose) && log_opts.internal != NULL) {
        /*
         * There's nothing much we can do about it if fwrite fails, but some
         * platforms declare fwrite with warn_unused_result.  Do a little
         * dance with casting to void to shut up the compiler in such cases.
         */
        size_t rc;

        rc = fwrite(message, strlen(message), 1, log_opts.internal);
        /* if we are using OVERWRITE_MESSAGE, add newline to log file */
        if (strchr(message, '\r') != NULL)
            rc = fwrite("\n", 1, 1, log_opts.internal);
        (void)rc;
        fflush(log_opts.internal);
    }

    switch (type) {
        case PG_VERBOSE:
            if (log_opts.verbose)
                printf("%s %s", current_localtime, _(message));
            break;

        case PG_REPORT:
        case PG_WARNING:
            printf("%s %s", current_localtime, _(message));
            break;

        case PG_FATAL:
            printf("\n%s %s", current_localtime, _(message));
            printf("%s Failure, exiting\n", current_localtime);
            exit(1);
            break;

        default:
            break;
    }
    fflush(stdout);
}

void check_ok(void)
{
    /* all seems well */
    report_status(PG_REPORT, "ok");
    fflush(stdout);
}

/*
 * quote_identifier()
 *		Properly double-quote a SQL identifier.
 *
 * The result should be pg_free'd, but most callers don't bother because
 * memory leakage is not a big deal in this program.
 */
char* quote_identifier(const char* s)
{
    char* result = (char*)pg_malloc(strlen(s) * 2 + 3);
    char* r = result;

    *r++ = '"';
    while (*s) {
        if (*s == '"')
            *r++ = *s;
        *r++ = *s;
        s++;
    }
    *r++ = '"';
    *r++ = '\0';

    return result;
}

/*
 * get_user_info()
 * (copied from initdb.c) find the current user
 */
int get_user_info(char** user_name)
{
    int user_id;

#ifndef WIN32
    struct passwd* pw = getpwuid(geteuid());

    user_id = geteuid();
#else /* the windows code */
    struct passwd_win32 {
        int pw_uid;
        char pw_name[128];
    } pass_win32;
    struct passwd_win32* pw = &pass_win32;
    DWORD pwname_size = sizeof(pass_win32.pw_name) - 1;

    GetUserName(pw->pw_name, &pwname_size);

    user_id = 1;
#endif

    *user_name = pg_strdup(pw->pw_name);

    return user_id;
}

void* pg_malloc(size_t size)
{
    void* p = NULL;

    /* Avoid unportable behavior of malloc(0) */
    if (size == 0)
        size = 1;
    p = malloc(size);
    if (p == NULL)
        pg_log(PG_FATAL, "%s: out of memory\n", os_info.progname);
    return p;
}

void* pg_realloc(void* ptr, size_t size)
{
    void* p = NULL;

    /* Avoid unportable behavior of realloc(NULL, 0) */
    if (ptr == NULL && size == 0)
        size = 1;
    p = realloc(ptr, size);
    if (p == NULL)
        pg_log(PG_FATAL, "%s: out of memory\n", os_info.progname);
    return p;
}

void pg_free(void* ptr)
{
    if (ptr != NULL)
        free(ptr);
}

char* pg_strdup(const char* s)
{
    char* result = strdup(s);

    if (result == NULL)
        pg_log(PG_FATAL, "%s: out of memory\n", os_info.progname);

    return result;
}

#define MAX_P_READ_BUF 256
int g_max_commands_parallel = 0;
int g_cur_commands_parallel = 0;

typedef struct tag_pcommand {
    FILE* pfp;
    char readbuf[512];
    int cur_buf_loc;
    char* nodename;
    char* instance_name;
    char* commad_type;
    int retvalue;
} PARALLEL_COMMAND_S;

PARALLEL_COMMAND_S* g_parallel_command_cxt = NULL;

void execute_popen_commands_parallel(char* cmd, char* nodename, char* instance_name)
{
    PARALLEL_COMMAND_S* curr_cxt = NULL;
    errno_t nRet = 0;

    if (g_cur_commands_parallel >= g_max_commands_parallel) {
        g_max_commands_parallel += 128;
        if (NULL == g_parallel_command_cxt)
            g_parallel_command_cxt =
                (PARALLEL_COMMAND_S*)pg_malloc(g_max_commands_parallel * sizeof(PARALLEL_COMMAND_S));
        else
            g_parallel_command_cxt = (PARALLEL_COMMAND_S*)pg_realloc(
                g_parallel_command_cxt, g_max_commands_parallel * sizeof(PARALLEL_COMMAND_S));
    }

    curr_cxt = &g_parallel_command_cxt[g_cur_commands_parallel];
    g_cur_commands_parallel++;

    curr_cxt->cur_buf_loc = 0;
    nRet = memset_s(curr_cxt->readbuf, sizeof(curr_cxt->readbuf), '\0', sizeof(curr_cxt->readbuf));
    securec_check_c(nRet, "\0", "\0");
    curr_cxt->nodename = nodename ? pg_strdup(nodename) : NULL;
    curr_cxt->instance_name = instance_name ? pg_strdup(instance_name) : NULL;

    curr_cxt->pfp = popen(cmd, "r");

    if (NULL != curr_cxt->pfp) {
        int flags;
        int fd = fileno(curr_cxt->pfp);
        flags = fcntl(fd, F_GETFL, 0);
        flags |= O_NONBLOCK;
        fcntl(fd, F_SETFL, flags);
    }
}

void SleepInMilliSec(uint32_t sleepMs)
{
    struct timespec ts;
    ts.tv_sec = (sleepMs - (sleepMs % 1000)) / 1000;
    ts.tv_nsec = (sleepMs % 1000) * 1000;

    (void)nanosleep(&ts, NULL);
}

void read_popen_output_parallel()
{
    int idx;
    bool read_pending = false;
    char* fcmd = NULL;
    PARALLEL_COMMAND_S* curr_cxt = NULL;
    bool error_in_execution = false;

    fcmd = (char*)pg_malloc(MAX_P_READ_BUF + 1);
    memset_s(fcmd, MAX_P_READ_BUF + 1, '\0', MAX_P_READ_BUF + 1);

    read_pending = true;
    while (true == read_pending) {
        read_pending = false;
        for (idx = 0; idx < g_cur_commands_parallel; idx++) {
            curr_cxt = g_parallel_command_cxt + idx;

            if (NULL == curr_cxt->pfp) {
                continue;
            }
            errno = 0;
            if (fgets(fcmd, MAX_P_READ_BUF - 1, curr_cxt->pfp) != NULL) {
                int len = strlen(fcmd);
                int hasnewline = false;

                read_pending = true;
                if (len > 1 && fcmd[len - 1] == '\n') {
                    hasnewline = true;
                } else if ((curr_cxt->cur_buf_loc + len + 1) < (int)sizeof(curr_cxt->readbuf)) {
                    (void)strncpy_s(curr_cxt->readbuf + curr_cxt->cur_buf_loc,
                        sizeof(curr_cxt->readbuf) - curr_cxt->cur_buf_loc,
                        fcmd,
                        len + 1);

                    curr_cxt->cur_buf_loc += len;
                    continue;
                }

                if (curr_cxt->nodename && curr_cxt->instance_name) {
                    pg_log(PG_REPORT,
                        "[%s] [%s] %s%s%s",
                        curr_cxt->nodename,
                        curr_cxt->instance_name,
                        curr_cxt->readbuf,
                        fcmd,
                        hasnewline ? "" : "\n");
                } else if (curr_cxt->nodename) {
                    pg_log(
                        PG_REPORT, "[%s] %s%s%s", curr_cxt->nodename, curr_cxt->readbuf, fcmd, hasnewline ? "" : "\n");
                } else if (curr_cxt->instance_name) {
                    pg_log(PG_REPORT,
                        "[%s] %s%s%s",
                        curr_cxt->instance_name,
                        curr_cxt->readbuf,
                        fcmd,
                        hasnewline ? "" : "\n");
                } else {
                    pg_log(PG_REPORT, "%s%s%s", curr_cxt->readbuf, fcmd, hasnewline ? "" : "\n");
                }

                curr_cxt->readbuf[0] = '\0';
                curr_cxt->cur_buf_loc = 0;
            } else if (errno == EAGAIN) {
                read_pending = true;
                continue;
            } else {
                curr_cxt->retvalue = pclose(curr_cxt->pfp);
                curr_cxt->pfp = NULL;
                if (curr_cxt->retvalue != 0) {
                    error_in_execution = true;
                    pg_log(PG_WARNING,
                        "gs_upgrade failed for %s %s.\n",
                        curr_cxt->instance_name ? curr_cxt->instance_name : curr_cxt->nodename,
                        curr_cxt->instance_name ? "instance name" : "node name");
                }

                if (curr_cxt->nodename) {
                    free(curr_cxt->nodename);
                    curr_cxt->nodename = NULL;
                }

                if (curr_cxt->instance_name) {
                    free(curr_cxt->instance_name);
                    curr_cxt->instance_name = NULL;
                }
                if (curr_cxt->retvalue != 0) {
                    break;
                }
            }
        }
        if (true == error_in_execution) {
            break;
        }
        SleepInMilliSec(100);
    }

    if (error_in_execution) {
        for (idx = 0; idx < g_cur_commands_parallel; idx++) {
            curr_cxt = g_parallel_command_cxt + idx;

            if (NULL == curr_cxt->pfp) {
                continue;
            }

            /*
             * Use the OM tool to make an exception after the process and the removal of the remaining files
             */
            if (curr_cxt->nodename)
                free(curr_cxt->nodename);
            curr_cxt->nodename = NULL;
            if (curr_cxt->instance_name)
                free(curr_cxt->instance_name);
            curr_cxt->instance_name = NULL;
        }

        pg_log(PG_FATAL, "gs_upgrade failed, please check last warning message for more details.\n");
    }

    g_cur_commands_parallel = 0;

    free(fcmd);
}

int execute_popen_command(char* cmd, char* nodename, char* instance_name)
{
    FILE* pfp = NULL;
    char* fcmd = NULL;

    fcmd = (char*)pg_malloc(MAX_P_READ_BUF);
    fcmd[MAX_P_READ_BUF - 1] = 0;

    pfp = popen(cmd, "r");

    while (fgets(fcmd, MAX_P_READ_BUF - 1, pfp) != NULL) {
        if (nodename && instance_name)
            pg_log(PG_REPORT, "[%s] [%s] %s", nodename, instance_name, fcmd);
        else if (nodename)
            pg_log(PG_REPORT, "[%s] %s", nodename, fcmd);
        else
            pg_log(PG_REPORT, "[%s] %s", instance_name, fcmd);
    }

    return pclose(pfp);
}

/*
 * getErrorText()
 *
 *	Returns the text of the error message for the given error number
 *
 *	This feature is factored into a separate function because it is
 *	system-dependent.
 */
const char* getErrorText(int errNum)
{
#ifdef WIN32
    _dosmaperr(GetLastError());
#endif
    return pg_strdup(strerror(errNum));
}

/*
 *	str2uint()
 *
 *	convert string to oid
 */
unsigned int str2uint(const char* str)
{
    return strtoul(str, NULL, 10);
}

/*
 *	str2uint64()
 *
 *	convert string to 64-bit unsigned int
 */
uint64 str2uint64(const char* str)
{
#ifdef _MSC_VER /* MSVC only */
    return _strtoui64(str, NULL, 10);
#elif defined(HAVE_STRTOULL) && SIZEOF_LONG < 8
    return strtoull(str, NULL, 10);
#else
    return strtoul(str, NULL, 10);
#endif
}

/*
 *	pg_putenv()
 *
 *	This is like putenv(), but takes two arguments.
 *	It also does unsetenv() if val is NULL.
 */
void pg_putenv(const char* var, const char* val)
{
    if (val) {
#ifndef WIN32
        char* envstr = (char*)pg_malloc(strlen(var) + strlen(val) + 2);
        int nRet = 0;

        nRet = snprintf_s(envstr, strlen(var) + strlen(val) + 2, strlen(var) + strlen(val) + 1, "%s=%s", var, val);
        securec_check_ss_c(nRet, "\0", "\0");
        putenv(envstr);

        /*
         * Do not free envstr because it becomes part of the environment on
         * some operating systems.	See port/unsetenv.c::unsetenv.
         */
#else
        SetEnvironmentVariableA(var, val);
#endif
    } else {
#ifndef WIN32
        unsetenv(var);
#else
        SetEnvironmentVariableA(var, "");
#endif
    }
}
