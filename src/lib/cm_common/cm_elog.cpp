/**
 * @file cm_elog.cpp
 * @brief error logging and reporting
 * @author xxx
 * @version 1.0
 * @date 2020-08-06
 *
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 *
 */
#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <ctype.h>
#include <syslog.h>
#include <limits.h>
#include "cm/path.h"

#include "cm/cm_c.h"
#include "cm/stringinfo.h"
#include "cm/elog.h"
#include "alarm/alarm.h"
#include "cm/cm_misc.h"
#include "cm/be_module.h"

#include <sys/time.h>
#if !defined(WIN32)
#include <sys/syscall.h>
#define gettid() syscall(__NR_gettid)
#else
/* windows. */
#endif
#include <sys/stat.h>

#undef _
#define _(x) x

/*
 * We really want line-buffered mode for logfile output, but Windows does
 * not have it, and interprets _IOLBF as _IOFBF (bozos).  So use _IONBF
 * instead on Windows.
 */
#ifdef WIN32
#define LBF_MODE _IONBF
#else
#define LBF_MODE _IOLBF
#endif

int log_destion_choice = LOG_DESTION_FILE;

/* declare the global variable of alarm module. */
int g_alarmReportInterval;
char g_alarmComponentPath[MAXPGPATH];
int g_alarmReportMaxCount;

char sys_log_path[MAX_PATH_LEN] = {0};  /* defalut cmData/cm_server  or cmData/cm_agent. */
char cm_krb_server_keyfile[MAX_PATH_LEN] = {0};
int Log_RotationSize = 16 * 1024 * 1024L;
pthread_rwlock_t syslog_write_lock;
pthread_rwlock_t dotCount_lock;
static bool dotCountNotZero = false;

FILE* syslogFile = NULL;
const char* prefix_name = NULL;

char curLogFileName[MAXPGPATH] = {0};
volatile int log_min_messages = WARNING;
volatile bool incremental_build = true;
volatile bool security_mode = false;
volatile int maxLogFileSize = 16 * 1024 * 1024;
volatile bool logInitFlag = false;
/* undocumentedVersion:
 * It's for inplace upgrading. This variable means which version we are
 * upgrading from. Zero means we are not upgrading.
 */
volatile uint32 undocumentedVersion = 0;
bool log_file_set = false;
/* unify log style */
THR_LOCAL const char* thread_name = NULL;

FILE* logfile_open(const char* filename, const char* mode);
static void get_alarm_report_interval(const char* conf);
static void TrimPathDoubleEndQuotes(char* path);

#define BUF_LEN 1024
#define COUNTSTR_LEN 128
#define MSBUF_LENGTH 8
#define FORMATTED_TS_LEN 128

static THR_LOCAL char  errbuf_errdetail[EREPORT_BUF_LEN];
static THR_LOCAL char  errbuf_errcode[EREPORT_BUF_LEN];
static THR_LOCAL char  errbuf_errmodule[EREPORT_BUF_LEN];
static THR_LOCAL char  errbuf_errmsg[EREPORT_BUF_LEN];
static THR_LOCAL char  errbuf_errcause[EREPORT_BUF_LEN];
static THR_LOCAL char  errbuf_erraction[EREPORT_BUF_LEN];

static THR_LOCAL char formatted_log_time[FORMATTED_TS_LEN];

/**
 * @brief When a parent process opens a file, the child processes will inherit the
 * file handle of the parent process. If the file is deleted and the child processes
 * are still running, the file handle will not be freed and take up disk space.
 * We set the FD_CLOEXEC flag to the file, so that the child processes don't inherit
 * the file handle of the parent process, and do not cause handle leak.
 *
 * @param fp open file object
 * @return int 0 means successfully set the flag.
 */
int SetFdCloseExecFlag(FILE* fp)
{
    int fd = fileno(fp);
    int flags = fcntl(fd, F_GETFD);
    if (flags < 0) {
        (void)printf("fcntl get flags failed.\n");
        return flags;
    }
    flags |= FD_CLOEXEC;
    int ret = fcntl(fd, F_SETFD, flags);
    if (ret == -1) {
        (void)printf("fcntl set flags failed.\n");
    }

    return ret;
}

void AlarmLogImplementation(int level, const char* prefix, const char* logtext)
{
    switch (level) {
        case ALM_DEBUG:
            write_runlog(LOG, "%s%s\n", prefix, logtext);
            break;
        case ALM_LOG:
            write_runlog(LOG, "%s%s\n", prefix, logtext);
            break;
        default:
            break;
    }
}

/*
 * setup formatted_log_time, for consistent times between CSV and regular logs
 */
static void setup_formatted_log_time(void)
{
    struct timeval tv = {0};
    time_t stamp_time;
    char msbuf[MSBUF_LENGTH];
    struct tm timeinfo = {0};
    int rc;
    errno_t rcs;

    (void)gettimeofday(&tv, NULL);
    stamp_time = (time_t)tv.tv_sec;
    (void)localtime_r(&stamp_time, &timeinfo);

    (void)strftime(formatted_log_time,
        FORMATTED_TS_LEN,
        /* leave room for milliseconds... */
        "%Y-%m-%d %H:%M:%S     %Z",
        &timeinfo);

    /* 'paste' milliseconds into place... */
    rc = sprintf_s(msbuf, MSBUF_LENGTH, ".%03d", (int)(tv.tv_usec / 1000));
    securec_check_ss_c(rc, "\0", "\0");
    rcs = strncpy_s(formatted_log_time + 19, FORMATTED_TS_LEN - 19, msbuf, 4);
    securec_check_c(rcs, "\0", "\0");
}

void add_log_prefix(int elevel, char* str)
{
    char errbuf_tmp[BUF_LEN * 3] = {0};
    errno_t rc;
    int rcs;

    setup_formatted_log_time();

    /* unify log style */
    if (thread_name == NULL) {
        thread_name = "";
    }
    rcs = snprintf_s(errbuf_tmp,
        sizeof(errbuf_tmp),
        sizeof(errbuf_tmp) - 1,
        "%s tid=%ld %s %s: ",
        formatted_log_time,
        gettid(),
        thread_name,
        log_level_int_to_string(elevel));
    securec_check_intval(rcs, );
    /* max message length less than 2048. */
    rc = strncat_s(errbuf_tmp, BUF_LEN * 3, str, BUF_LEN * 3 - strlen(errbuf_tmp));

    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(str, BUF_LEN * 2, errbuf_tmp, BUF_LEN * 2 - 1);
    securec_check_c(rc, "\0", "\0");
    str[BUF_LEN * 2 - 1] = '\0';
}

/*
 * is_log_level_output -- is elevel logically >= log_min_level?
 *
 * We use this for tests that should consider LOG to sort out-of-order,
 * between ERROR and FATAL.  Generally this is the right thing for testing
 * whether a message should go to the postmaster log, whereas a simple >=
 * test is correct for testing whether the message should go to the client.
 */
static bool is_log_level_output(int elevel, int log_min_level)
{
    if (elevel == LOG) {
        if (log_min_level == LOG || log_min_level <= ERROR) {
            return true;
        }
    } else if (log_min_level == LOG) {
        /* elevel not equal to LOG */
        if (elevel >= FATAL)
            return true;
    } else if (elevel >= log_min_level) {
        /* Neither is LOG */
        return true;
    }

    return false;
}

/*
 * Write errors to stderr (or by equal means when stderr is
 * not available).
 */
void write_runlog(int elevel, const char* fmt, ...)
{
    va_list ap;
    va_list bp;
    char errbuf[2048] = {0};
    char fmtBuffer[2048] = {0};
    int count = 0;
    int ret = 0;
    bool output_to_server = false;

    /* Get whether the record will be logged into the file. */
    output_to_server = is_log_level_output(elevel, log_min_messages);
    if (!output_to_server) {
        return;
    }

    /* Obtaining international texts. */
    fmt = _(fmt);

    va_start(ap, fmt);

    if (prefix_name != NULL && strcmp(prefix_name, "cm_ctl") == 0) {
        /* Skip the wait dot log and the line break log. */
        if (strcmp(fmt, ".") == 0) {
            (void)pthread_rwlock_wrlock(&dotCount_lock);
            dotCountNotZero = true;
            (void)pthread_rwlock_unlock(&dotCount_lock);
            (void)vfprintf(stdout, fmt, ap);
            (void)fflush(stdout);
            va_end(ap);
            return;
        }

        /**
         * Log the record to std error.
         * 1. The log level is greater than the level "LOG", and the process name is "cm_ctl".
         * 2. The log file path was not initialized.
         */
        if (elevel >= LOG || sys_log_path[0] == '\0') {
            if (dotCountNotZero == true) {
                fprintf(stdout, "\n");
                (void)pthread_rwlock_wrlock(&dotCount_lock);
                dotCountNotZero = false;
                (void)pthread_rwlock_unlock(&dotCount_lock);
            }

            /* Get the print out format. */
            ret = snprintf_s(fmtBuffer, sizeof(fmtBuffer), sizeof(fmtBuffer) - 1, "%s: %s", prefix_name, fmt);
            securec_check_ss_c(ret, "\0", "\0");
            va_copy(bp, ap);
            (void)vfprintf(stdout, fmtBuffer, bp);
            (void)fflush(stdout);
            va_end(bp);
        }
    }

    /* Format the log record. */
    count = vsnprintf_s(errbuf, sizeof(errbuf), sizeof(errbuf) - 1, fmt, ap);
    va_end(ap);

    switch (log_destion_choice) {
        case LOG_DESTION_FILE:
            add_log_prefix(elevel, errbuf);
            write_log_file(errbuf, count);
            break;

        default:
            break;
    }
}

int add_message_string(char* errmsg_tmp, char* errdetail_tmp, char* errmodule_tmp, char* errcode_tmp, const char* fmt)
{
    int rcs = 0;
    char *p = NULL;
    char errbuf_tmp[BUF_LEN] = {0};

    rcs = snprintf_s(errbuf_tmp, sizeof(errbuf_tmp), sizeof(errbuf_tmp) - 1, "%s", fmt);
    securec_check_intval(rcs, );
    if ((p = strstr(errbuf_tmp, "[ERRMSG]:")) != NULL) {
        rcs = snprintf_s(errmsg_tmp, BUF_LEN, BUF_LEN - 1, "%s", fmt + strlen("[ERRMSG]:"));
    } else if ((p = strstr(errbuf_tmp, "[ERRDETAIL]:")) != NULL) {
        rcs = snprintf_s(errdetail_tmp, BUF_LEN, BUF_LEN - 1, "%s", fmt);
    } else if ((p = strstr(errbuf_tmp, "[ERRMODULE]:")) != NULL) {
        rcs = snprintf_s(errmodule_tmp, BUF_LEN, BUF_LEN - 1, "%s", fmt + strlen("[ERRMODULE]:"));
    } else if ((p = strstr(errbuf_tmp, "[ERRCODE]:")) != NULL) {
        rcs = snprintf_s(errcode_tmp, BUF_LEN, BUF_LEN - 1, "%s", fmt + strlen("[ERRCODE]:"));
    }
    securec_check_intval(rcs, );
    return 0;
}

int add_message_string(char* errmsg_tmp, char* errdetail_tmp, char* errmodule_tmp, char* errcode_tmp,
    char* errcause_tmp, char* erraction_tmp, const char* fmt)
{
    int rcs = 0;
    char *p = NULL;
    char errbuf_tmp[BUF_LEN] = {0};

    rcs = snprintf_s(errbuf_tmp, sizeof(errbuf_tmp), sizeof(errbuf_tmp) - 1, "%s", fmt);
    securec_check_intval(rcs, );
    if ((p = strstr(errbuf_tmp, "[ERRMSG]:")) != NULL) {
        rcs = snprintf_s(errmsg_tmp, BUF_LEN, BUF_LEN - 1, "%s", fmt + strlen("[ERRMSG]:"));
    } else if ((p = strstr(errbuf_tmp, "[ERRDETAIL]:")) != NULL) {
        rcs = snprintf_s(errdetail_tmp, BUF_LEN, BUF_LEN - 1, "%s", fmt);
    } else if ((p = strstr(errbuf_tmp, "[ERRMODULE]:")) != NULL) {
        rcs = snprintf_s(errmodule_tmp, BUF_LEN, BUF_LEN - 1, "%s", fmt + strlen("[ERRMODULE]:"));
    } else if ((p = strstr(errbuf_tmp, "[ERRCODE]:")) != NULL) {
        rcs = snprintf_s(errcode_tmp, BUF_LEN, BUF_LEN - 1, "%s", fmt + strlen("[ERRCODE]:"));
    } else if ((p = strstr(errbuf_tmp, "[ERRCAUSE]:")) != NULL) {
        rcs = snprintf_s(errcause_tmp, BUF_LEN, BUF_LEN - 1, "%s", fmt);
    } else if ((p = strstr(errbuf_tmp, "[ERRACTION]:")) != NULL) {
        rcs = snprintf_s(erraction_tmp, BUF_LEN, BUF_LEN - 1, "%s", fmt);
    }
    securec_check_intval(rcs, );
    return 0;
}


void add_log_prefix2(int elevel, const char* errmodule_tmp, const char* errcode_tmp, char* str)
{
    char errbuf_tmp[BUF_LEN * 3] = {0};
    errno_t rc;
    int rcs;

    setup_formatted_log_time();

    /* unify log style */
    if (thread_name == NULL) {
        thread_name = "";
    }
    if (errmodule_tmp[0] && errcode_tmp[0]) {
        rcs = snprintf_s(errbuf_tmp,
            sizeof(errbuf_tmp),
            sizeof(errbuf_tmp) - 1,
            "%s tid=%ld %s [%s] %s %s: ",
            formatted_log_time,
            gettid(),
            thread_name,
            errmodule_tmp,
            errcode_tmp,
            log_level_int_to_string(elevel));
    } else {
        rcs = snprintf_s(errbuf_tmp,
            sizeof(errbuf_tmp),
            sizeof(errbuf_tmp) - 1,
            "%s tid=%ld %s %s: ",
            formatted_log_time,
            gettid(),
            thread_name,
            log_level_int_to_string(elevel));
    }
    securec_check_intval(rcs, );
    /* max message length less than 2048. */
    rc = strncat_s(errbuf_tmp, BUF_LEN * 3, str, BUF_LEN * 3 - strlen(errbuf_tmp));

    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(str, BUF_LEN * 2, errbuf_tmp, BUF_LEN * 2 - 1);
    securec_check_c(rc, "\0", "\0");
    str[BUF_LEN * 2 - 1] = '\0';
}

/*
 * Write errors to stderr (or by equal means when stderr is
 * not available).
 */
void write_runlog3(int elevel, const char* errmodule_tmp, const char* errcode_tmp, const char* fmt, ...)
{
    va_list ap;
    va_list bp;
    char errbuf[2048] = {0};
    char fmtBuffer[2048] = {0};
    int count = 0;
    int ret = 0;
    bool output_to_server = false;

    /* Get whether the record will be logged into the file. */
    output_to_server = is_log_level_output(elevel, log_min_messages);
    if (!output_to_server) {
        return;
    }

    /* Obtaining international texts. */
    fmt = _(fmt);

    va_start(ap, fmt);

    if (prefix_name != NULL && strcmp(prefix_name, "cm_ctl") == 0) {
        /* Skip the wait dot log and the line break log. */
        if (strcmp(fmt, ".") == 0) {
            (void)pthread_rwlock_wrlock(&dotCount_lock);
            dotCountNotZero = true;
            (void)pthread_rwlock_unlock(&dotCount_lock);
            (void)vfprintf(stdout, fmt, ap);
            (void)fflush(stdout);
            va_end(ap);
            return;
        }

        /**
         * Log the record to std error.
         * 1. The log level is greater than the level "LOG", and the process name is "cm_ctl".
         * 2. The log file path was not initialized.
         */
        if (elevel >= LOG || sys_log_path[0] == '\0') {
            if (dotCountNotZero == true) {
                fprintf(stdout, "\n");
                (void)pthread_rwlock_wrlock(&dotCount_lock);
                dotCountNotZero = false;
                (void)pthread_rwlock_unlock(&dotCount_lock);
            }

            /* Get the print out format. */
            ret = snprintf_s(fmtBuffer, sizeof(fmtBuffer), sizeof(fmtBuffer) - 1, "%s: %s", prefix_name, fmt);
            securec_check_intval(ret, );
            va_copy(bp, ap);
            (void)vfprintf(stdout, fmtBuffer, bp);
            (void)fflush(stdout);
            va_end(bp);
        }
    }

    /* Format the log record. */
    count = vsnprintf_s(errbuf, sizeof(errbuf), sizeof(errbuf) - 1, fmt, ap);
    securec_check_intval(count, );
    va_end(ap);

    switch (log_destion_choice) {
        case LOG_DESTION_FILE:
            add_log_prefix2(elevel, errmodule_tmp, errcode_tmp, errbuf);
            write_log_file(errbuf, count);
            break;

        default:
            break;
    }
}

/*
 * Open a new logfile with proper permissions and buffering options.
 *
 * If allow_errors is true, we just log any open failure and return NULL
 * (with errno still correct for the fopen failure).
 * Otherwise, errors are treated as fatal.
 */
FILE* logfile_open(const char* log_path, const char* mode)
{
    FILE* fh = NULL;
    mode_t oumask;
    char log_file_name[MAXPGPATH] = {0};
    char log_temp_name[MAXPGPATH] = {0};
    char log_create_time[LOG_MAX_TIMELEN] = {0};
    DIR* dir = NULL;
    struct dirent* de = NULL;
    bool is_exist = false;
    pg_time_t current_time;
    struct tm* systm = NULL;
    /* check validity of current log file name */
    char* name_ptr = NULL;
    errno_t rc = 0;
    int ret = 0;

    if (log_path == NULL) {
        (void)printf("logfile_open,log file path is null.\n");
        return NULL;
    }

    /*
     * Note we do not let Log_file_mode disable IWUSR,
     * since we certainly want to be able to write the files ourselves.
     */
    oumask = umask((mode_t)((~(mode_t)(S_IRUSR | S_IWUSR | S_IXUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO)));

    /* find current log file. */
    if ((dir = opendir(log_path)) == NULL) {
        printf(_("%s: opendir %s failed! \n"), prefix_name, log_path);
        return NULL;
    }
    while ((de = readdir(dir)) != NULL) {
        /* exist current log file. */
        if (strstr(de->d_name, prefix_name) != NULL) {
            name_ptr = strstr(de->d_name, "-current.log");
            if (name_ptr != NULL) {
                name_ptr += strlen("-current.log");
                if ((*name_ptr) == '\0') {
                    is_exist = true;
                    break;
                }
            }
        }
    }

    rc = memset_s(log_file_name, MAXPGPATH, 0, MAXPGPATH);
    securec_check_errno(rc, );
    if (!is_exist) {
        /* create current log file name. */
        current_time = time(NULL);
        systm = localtime(&current_time);
        if (systm != NULL) {
            (void)strftime(log_create_time, LOG_MAX_TIMELEN, "-%Y-%m-%d_%H%M%S", systm);
        }
        ret =
            snprintf_s(log_temp_name, MAXPGPATH, MAXPGPATH - 1, "%s%s%s", prefix_name, log_create_time, curLogFileMark);
        securec_check_intval(ret, );
        ret = snprintf_s(log_file_name, MAXPGPATH, MAXPGPATH - 1, "%s/%s", log_path, log_temp_name);
        securec_check_intval(ret, );
    } else {
        /* if log file exist, get its file name. */
        ret = snprintf_s(log_file_name, MAXPGPATH, MAXPGPATH - 1, "%s/%s", log_path, de->d_name);
        securec_check_intval(ret, );
    }
    (void)closedir(dir);
    fh = fopen(log_file_name, mode);

    (void)umask(oumask);

    if (fh != NULL) {
        (void)setvbuf(fh, NULL, LBF_MODE, 0);

#ifdef WIN32
        /* use CRLF line endings on Windows */
        _setmode(_fileno(fh), _O_TEXT);
#endif
        /*
         * when parent process(cm_agent) open the cm_agent_xxx.log, the child processes(cn\dn\gtm\cm_server)
         * inherit the file handle of the parent process. If the file is deleted and the child processes
         * are still running, the file handle will not be freed, it will take up disk space, so we set
         * the FD_CLOEXEC flag to the file, so that the child processes don't inherit the file handle of the
         * parent process.
         */
        if (SetFdCloseExecFlag(fh) == -1) {
            (void)printf("set file flag failed, filename:%s, errmsg: %s.\n", log_file_name, strerror(errno));
        }
    } else {
        int save_errno = errno;

        (void)printf("logfile_open could not open log file:%s %s.\n", log_file_name, strerror(errno));
        errno = save_errno;
    }

    /* store current log file name */
    rc = memset_s(curLogFileName, MAXPGPATH, 0, MAXPGPATH);
    securec_check_errno(rc, );
    rc = strncpy_s(curLogFileName, MAXPGPATH, log_file_name, strlen(log_file_name));
    securec_check_errno(rc, );

    return fh;
}

int logfile_init()
{
    int rc;
    errno_t rcs;

    rc = pthread_rwlock_init(&syslog_write_lock, NULL);
    if (rc != 0) {
        fprintf(stderr, "logfile_init lock failed.exit\n");
        exit(1);
    }
    rc = pthread_rwlock_init(&dotCount_lock, NULL);
    if (rc != 0) {
        fprintf(stderr, "logfile_init dot_count_lock failed.exit\n");
        exit(1);
    }
    rcs = memset_s(sys_log_path, MAX_PATH_LEN, 0, MAX_PATH_LEN);
    securec_check_c(rcs, "\0", "\0");

    return 0;
}

int is_comment_line(const char* str)
{
    size_t ii = 0;

    if (str == NULL) {
        printf("bad config file line\n");
        exit(1);
    }

    /* skip blank */
    for (;;) {
        if (*(str + ii) == ' ') {
            ii++;  /* skip blank */
        } else {
            break;
        }
    }

    if (*(str + ii) == '#') {
        return 1;  /* comment line */
    }

    return 0;  /* not comment line */
}

int get_authentication_type(const char* config_file)
{
    char buf[BUF_LEN];
    FILE* fd = NULL;
    int type = CM_AUTH_TRUST;

    if (config_file == NULL) {
        return CM_AUTH_TRUST;  /* default level */
    }

    fd = fopen(config_file, "r");
    if (fd == NULL) {
        char errBuffer[ERROR_LIMIT_LEN];
        printf("can not open config file: %s %s\n", config_file, pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        exit(1);
    }

    while (!feof(fd)) {
        errno_t rc;
        rc = memset_s(buf, BUF_LEN, 0, BUF_LEN);
        securec_check_c(rc, "\0", "\0");
        (void)fgets(buf, BUF_LEN, fd);

        if (is_comment_line(buf) == 1) {
            continue;  /* skip  # comment */
        }

        if (strstr(buf, "cm_auth_method") != NULL) {
            /* check all lines */
            if (strstr(buf, "trust") != NULL) {
                type = CM_AUTH_TRUST;
            }

            if (strstr(buf, "gss") != NULL) {
                type = CM_AUTH_GSS;
            }
        }
    }

    fclose(fd);
    return type;
}

/* trim successive characters on both ends */
static char* TrimToken(char* src, const char& delim)
{
    char* s = 0;
    char* e = 0;
    char* c = 0;

    for (c = src; (c != NULL) && *c; ++c) {
        if (*c == delim) {
            if (e == NULL) {
                e = c;
            }
        } else {
            if (s == NULL) {
                s = c;
            }
            e = NULL;
        }
    }

    if (s == NULL) {
        s = src;
    }

    if (e != NULL) {
        *e = 0;
    }

    return s;
}

static void TrimPathDoubleEndQuotes(char* path)
{
    int pathLen = strlen(path);

    /* make sure buf[MAXPGPATH] can copy the whole path, last '\0' included */
    if (pathLen > MAXPGPATH - 1) {
        return;
    }

    char* pathTrimed = NULL;
    pathTrimed = TrimToken(path, '\'');
    pathTrimed = TrimToken(pathTrimed, '\"');

    char buf[MAXPGPATH] = {0};
    errno_t rc = 0;

    rc = strncpy_s(buf, MAXPGPATH, pathTrimed, strlen(pathTrimed));
    securec_check_errno(rc, );

    rc = strncpy_s(path, pathLen + 1, buf, strlen(buf));
    securec_check_errno(rc, );
}

void get_krb_server_keyfile(const char* config_file)
{
    char buf[MAXPGPATH];
    FILE* fd = NULL;

    int ii = 0;

    char* subStr = NULL;
    char* subStr1 = NULL;
    char* subStr2 = NULL;
    char* subStr3 = NULL;

    char* saveptr1 = NULL;
    char* saveptr2 = NULL;
    char* saveptr3 = NULL;
    errno_t rc = 0;

    if (config_file == NULL) {
        return;
    } else {
        logInitFlag = true;
    }

    fd = fopen(config_file, "r");
    if (fd == NULL) {
        printf("get_krb_server_keyfile confDir error\n");
        exit(1);
    }

    while (!feof(fd)) {
        rc = memset_s(buf, MAXPGPATH, 0, MAXPGPATH);
        securec_check_errno(rc, );

        (void)fgets(buf, MAXPGPATH, fd);
        buf[MAXPGPATH - 1] = 0;

        if (is_comment_line(buf) == 1) {
            continue;  /* skip  # comment */
        }

        subStr = strstr(buf, "cm_krb_server_keyfile");
        if (subStr == NULL) {
            continue;
        }

        subStr = strstr(subStr + 7, "=");
        if (subStr == NULL) {
            continue;
        }

        /* = is last char */
        if (subStr + 1 == 0) {
            continue;
        }

        /* skip blank */
        ii = 1;
        for (;;) {
            if (*(subStr + ii) == ' ') {
                ii++;  /* skip blank */
            } else {
                break;
            }
        }
        subStr = subStr + ii;

        /* beging check blank */
        subStr1 = strtok_r(subStr, " ", &saveptr1);
        if (subStr1 == NULL) {
            continue;
        }

        subStr2 = strtok_r(subStr1, "\n", &saveptr2);
        if (subStr2 == NULL) {
            continue;
        }

        subStr3 = strtok_r(subStr2, "\r", &saveptr3);
        if (subStr3 == NULL) {
            continue;
        }
        if (subStr3[0] == '\'') {
            subStr3 = subStr3 + 1;
        }
        if (subStr3[strlen(subStr3) - 1] == '\'') {
            subStr3[strlen(subStr3) - 1] = '\0';
        }
        if (strlen(subStr3) > 0) {
            rc = memcpy_s(cm_krb_server_keyfile, sizeof(sys_log_path), subStr3, strlen(subStr3) + 1);
            securec_check_errno(rc, );
        }
    }

    fclose(fd);

    TrimPathDoubleEndQuotes(cm_krb_server_keyfile);

    return;  /* default value warning */
}

void GetStringFromConf(const char* configFile, char* itemValue, size_t itemValueLenth, const char* itemName)
{
    char buf[MAXPGPATH];
    FILE* fd = NULL;

    int ii = 0;

    char* subStr = NULL;
    char* subStr1 = NULL;
    char* subStr2 = NULL;
    char* subStr3 = NULL;

    char* saveptr1 = NULL;
    char* saveptr2 = NULL;
    char* saveptr3 = NULL;
    errno_t rc = 0;

    if (configFile == NULL) {
        return;
    } else {
        logInitFlag = true;
    }

    fd = fopen(configFile, "r");
    if (fd == NULL) {
        printf("%s confDir error\n", itemName);
        exit(1);
    }

    while (!feof(fd)) {
        rc = memset_s(buf, MAXPGPATH, 0, MAXPGPATH);
        securec_check_errno(rc, );

        (void)fgets(buf, MAXPGPATH, fd);
        buf[MAXPGPATH - 1] = 0;

        if (is_comment_line(buf) == 1) {
            continue;  /* skip  # comment */
        }

        subStr = strstr(buf, itemName);
        if (subStr == NULL) {
            continue;
        }

        subStr = strstr(subStr + strlen(itemName), "=");
        if (subStr == NULL) {
            continue;
        }

        if (subStr + 1 == 0) {
            continue;  /* = is last char */
        }

        /* skip blank */
        ii = 1;
        for (;;) {
            if (*(subStr + ii) == ' ') {
                ii++;  /* skip blank */
            } else {
                break;
            }
        }
        subStr = subStr + ii;

        /* beging check blank */
        subStr1 = strtok_r(subStr, " ", &saveptr1);
        if (subStr1 == NULL) {
            continue;
        }

        subStr2 = strtok_r(subStr1, "\n", &saveptr2);
        if (subStr2 == NULL) {
            continue;
        }

        subStr3 = strtok_r(subStr2, "\r", &saveptr3);
        if (subStr3 == NULL) {
            continue;
        }
        if (subStr3[0] == '\'') {
            subStr3 = subStr3 + 1;
        }
        if (subStr3[strlen(subStr3) - 1] == '\'') {
            subStr3[strlen(subStr3) - 1] = '\0';
        }
        if (strlen(subStr3) > 0) {
            rc = memcpy_s(itemValue, itemValueLenth, subStr3, strlen(subStr3) + 1);
            securec_check_errno(rc, );
        } else {
            write_runlog(ERROR, "invalid value for parameter \" %s \" in %s.\n", itemName, configFile);
        }
    }

    fclose(fd);

    return;  /* default value warning */
}

/* used for cm_agent and cm_server */
/* g_currentNode->cmDataPath  -->  confDir */
void get_log_level(const char* config_file)
{
    char buf[BUF_LEN];
    FILE* fd = NULL;

    if (config_file == NULL) {
        return;
    } else {
        logInitFlag = true;
    }

    fd = fopen(config_file, "r");
    if (fd == NULL) {
        char errBuffer[ERROR_LIMIT_LEN];
        printf("can not open config file: %s %s\n", config_file, pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        exit(1);
    }

    while (!feof(fd)) {
        errno_t rc;
        rc = memset_s(buf, BUF_LEN, 0, BUF_LEN);
        securec_check_c(rc, "\0", "\0");
        (void)fgets(buf, BUF_LEN, fd);

        if (is_comment_line(buf) == 1) {
            continue;  /* skip  # comment */
        }

        if (strstr(buf, "log_min_messages") != NULL) {
            /* check all lines */
            if (strcasestr(buf, "DEBUG5") != NULL) {
                log_min_messages = DEBUG5;
                break;
            }

            if (strcasestr(buf, "DEBUG1") != NULL) {
                log_min_messages = DEBUG1;
                break;
            }

            if (strcasestr(buf, "WARNING") != NULL) {
                log_min_messages = WARNING;
                break;
            }

            if (strcasestr(buf, "ERROR") != NULL) {
                log_min_messages = ERROR;
                break;
            }

            if (strcasestr(buf, "FATAL") != NULL) {
                log_min_messages = FATAL;
                break;
            }

            if (strcasestr(buf, "LOG") != NULL) {
                log_min_messages = LOG;
                break;
            }
        }
    }

    fclose(fd);
    return;  /* default value warning */
}

/* used for cm_agent */
void get_build_mode(const char* config_file)
{
    char buf[BUF_LEN];
    FILE* fd = NULL;

    if (config_file == NULL) {
        return;
    }

    fd = fopen(config_file, "r");
    if (fd == NULL) {
        char errBuffer[ERROR_LIMIT_LEN];
        printf("can not open config file: %s %s\n", config_file, pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        exit(1);
    }

    while (!feof(fd)) {
        errno_t rc;
        rc = memset_s(buf, BUF_LEN, 0, BUF_LEN);
        securec_check_c(rc, "\0", "\0");
        (void)fgets(buf, BUF_LEN, fd);

        /* skip # comment */
        if (is_comment_line(buf) == 1) {
            continue;
        }

        /* check all lines */
        if (strstr(buf, "incremental_build") != NULL) {
            if (strstr(buf, "on") != NULL) {
                incremental_build = true;
            } else if (strstr(buf, "off") != NULL) {
                incremental_build = false;
            } else {
                incremental_build = true;
                write_runlog(FATAL, "invalid value for parameter \"incremental_build\" in %s.\n", config_file);
            }
        }
    }

    fclose(fd);
    return;
}

/* used for cm_agent and cm_server */
void get_log_file_size(const char* config_file)
{
    char buf[BUF_LEN];
    FILE* fd = NULL;

    if (config_file == NULL) {
        return;  /* default size */
    } else {
        logInitFlag = true;
    }

    fd = fopen(config_file, "r");
    if (fd == NULL) {
        printf("get_log_file_size error\n");
        exit(1);
    }

    while (!feof(fd)) {
        errno_t rc;
        rc = memset_s(buf, BUF_LEN, 0, BUF_LEN);
        securec_check_c(rc, "\0", "\0");
        (void)fgets(buf, BUF_LEN, fd);

        if (is_comment_line(buf) == 1) {
            continue;  /* skip  # comment */
        }

        if (strstr(buf, "log_file_size") != NULL) {
            /* only check the first line */
            char* subStr = NULL;
            char countStr[COUNTSTR_LEN] = {0};
            int ii = 0;
            int jj = 0;

            subStr = strchr(buf, '=');
            if (subStr != NULL) {
                /* find = */
                ii = 1;  /* 1 is = */

                /* skip blank */
                for (;;) {
                    if (*(subStr + ii) == ' ') {
                        ii++;  /* skip blank */
                    } else if (*(subStr + ii) >= '0' && *(subStr + ii) <= '9') {
                        break;  /* number find.break */
                    } else {
                        /* invalid character. */
                        goto out;
                    }
                }

                while (*(subStr + ii) >= '0' && *(subStr + ii) <= '9') {
                    /* end when no more number. */
                    if (jj > (int)sizeof(countStr) - 2) {
                        printf("too large log file size.\n");
                        exit(1);
                    } else {
                        countStr[jj] = *(subStr + ii);
                    }

                    ii++;
                    jj++;
                }
                countStr[jj] = 0;  /* jj maybe have added itself.terminate string. */

                if (countStr[0] != 0) {
                    maxLogFileSize = atoi(countStr) * 1024 * 1024;  /* byte */
                } else {
                    write_runlog(ERROR, "invalid value for parameter \"log_file_size\" in %s.\n", config_file);
                }
            }
        }
    }

out:
    fclose(fd);
    return;  /* default value is warning */
}

int get_cm_thread_count(const char* config_file)
{
#define DEFAULT_THREAD_NUM 5

    char buf[BUF_LEN];
    FILE* fd = NULL;
    int thread_count = DEFAULT_THREAD_NUM;
    errno_t rc = 0;

    if (config_file == NULL) {
        printf("no cmserver config file! exit.\n");
        exit(1);
    }

    fd = fopen(config_file, "r");
    if (fd == NULL) {
        printf("open cmserver config file :%s ,error:%m\n", config_file);
        exit(1);
    }

    while (!feof(fd)) {
        rc = memset_s(buf, sizeof(buf), 0, sizeof(buf));
        securec_check_errno(rc, );
        (void)fgets(buf, BUF_LEN, fd);

        if (is_comment_line(buf) == 1) {
            continue;  /* skip  # comment */
        }

        if (strstr(buf, "thread_count") != NULL) {
            /* only check the first line */
            char* subStr = NULL;
            char countStr[COUNTSTR_LEN] = {0};
            int ii = 0;
            int jj = 0;

            subStr = strchr(buf, '=');
            /* find = */
            if (subStr != NULL) {
                ii = 1;

                /* skip blank */
                for (;;) {
                    if (*(subStr + ii) == ' ') {
                        ii++;  /* skip blank */
                    } else if (*(subStr + ii) >= '0' && *(subStr + ii) <= '9') {
                        /* number find.break */
                        break;
                    } else {
                        /* invalid character. */
                        goto out;
                    }
                }

                /* end when no number */
                while (*(subStr + ii) >= '0' && *(subStr + ii) <= '9') {
                    if (jj > (int)sizeof(countStr) - 2) {
                        printf("too large thread count.\n");
                        exit(1);
                    } else {
                        countStr[jj] = *(subStr + ii);
                    }

                    ii++;
                    jj++;
                }
                countStr[jj] = 0;  /* jj maybe have added itself.terminate string. */

                if (countStr[0] != 0) {
                    thread_count = atoi(countStr);

                    if (thread_count < 2 || thread_count > 1000) {
                        printf("invalid thread count %d, range [2 - 1000].\n", thread_count);
                        exit(1);
                    }
                } else {
                    thread_count = DEFAULT_THREAD_NUM;
                }
            }
        }
    }

out:
    fclose(fd);
    return thread_count;
}

/*
 * @Description:  get value of paramater from configuration file
 *
 * @in config_file: configuration file path
 * @in key: name of paramater
 * @in defaultValue: default value of parameter
 *
 * @out: value of parameter
 */
int get_int_value_from_config(const char* config_file, const char* key, int defaultValue)
{
    int64 i64 = get_int64_value_from_config(config_file, key, defaultValue);
    if (i64 > INT_MAX) {
        return defaultValue;
    } else if (i64 < INT_MIN) {
        return defaultValue;
    }

    return (int)i64;
}

/*
 * @Description:  get value of paramater from configuration file
 *
 * @in config_file: configuration file path
 * @in key: name of paramater
 * @in defaultValue: default value of parameter
 *
 * @out: value of parameter
 */
uint32 get_uint32_value_from_config(const char* config_file, const char* key, uint32 defaultValue)
{
    int64 i64 = get_int64_value_from_config(config_file, key, defaultValue);
    if (i64 > UINT_MAX) {
        return defaultValue;
    } else if (i64 < 0) {
        return defaultValue;
    }

    return (uint32)i64;
}

/*
 * @Description:  get value of paramater from configuration file
 *
 * @in config_file: configuration file path
 * @in key: name of paramater
 * @in defaultValue: default value of parameter
 *
 * @out: value of parameter
 */
int64 get_int64_value_from_config(const char* config_file, const char* key, int64 defaultValue)
{
    char buf[BUF_LEN];
    FILE* fd = NULL;
    int64 int64Value = defaultValue;
    errno_t rc = 0;

    Assert(key);
    if (config_file == NULL) {
        printf("no config file! exit.\n");
        exit(1);
    }

    fd = fopen(config_file, "r");
    if (fd == NULL) {
        char errBuffer[ERROR_LIMIT_LEN];
        printf("open config file failed:%s ,error:%s\n", config_file, pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        exit(1);
    }

    while (!feof(fd)) {
        rc = memset_s(buf, sizeof(buf), 0, sizeof(buf));
        securec_check_errno(rc, );
        (void)fgets(buf, BUF_LEN, fd);

        if (is_comment_line(buf) == 1) {
            continue;  /* skip  # comment */
        }

        if (strstr(buf, key) != NULL) {
            /* only check the first line */
            char* subStr = NULL;
            char countStr[COUNTSTR_LEN] = {0};
            int ii = 0;
            int jj = 0;

            subStr = strchr(buf, '=');
            if (subStr != NULL) {
                /* find = */
                ii = 1;

                /* skip blank */
                while (1) {
                    if (*(subStr + ii) == ' ') {
                        ii++;  /* skip blank */
                    } else if (isdigit(*(subStr + ii))) {
                        /* number find.break */
                        break;
                    } else {
                        /* invalid character. */
                        goto out;
                    }
                }

                while (isdigit(*(subStr + ii))) {
                    /* end when no number */
                    if (jj >= COUNTSTR_LEN - 1) {
                        write_runlog(ERROR, "length is not enough for constr\n");
                        goto out;
                    }
                    countStr[jj] = *(subStr + ii);

                    ii++;
                    jj++;
                }
                countStr[jj] = 0; /* jj maybe have added itself.terminate string. */

                if (countStr[0] != 0) {
                    int64Value = strtoll(countStr, NULL, 10);
                }
                break;
            }
        }
    }

out:
    fclose(fd);
    return int64Value;
}

#define ALARM_REPORT_INTERVAL "alarm_report_interval"
#define ALARM_REPORT_INTERVAL_DEFAULT 10

#define ALARM_REPORT_MAX_COUNT "alarm_report_max_count"
#define ALARM_REPORT_MAX_COUNT_DEFAULT 5

/* trim blank characters on both ends */
char* trim(char* src)
{
    char* s = 0;
    char* e = 0;
    char* c = 0;

    for (c = src; (c != NULL) && *c; ++c) {
        if (isspace(*c)) {
            if (e == NULL) {
                e = c;
            }
        } else {
            if (s == NULL) {
                s = c;
            }
            e = 0;
        }
    }
    if (s == NULL) {
        s = src;
    }
    if (e != NULL) {
        *e = 0;
    }

    return s;
}

/* Check this line is comment line or not, which is in cm_server.conf file */
static bool is_comment_entity(char* str_line)
{
    char* src = NULL;
    if (str_line == NULL || strlen(str_line) < 1) {
        return false;
    }
    src = str_line;
    src = trim(src);
    if (src == NULL || strlen(src) < 1) {
        return true;
    }
    if (*src == '#') {
        return true;
    }

    return false;
}

int is_digit_string(char* str)
{
#define isDigital(_ch) (((_ch) >= '0') && ((_ch) <= '9'))

    int i = 0;
    int len = -1;
    char* p = NULL;
    if (str == nullptr) {
        return 0;
    }
    if ((len = strlen(str)) <= 0) {
        return 0;
    }
    p = str;
    for (i = 0; i < len; i++) {
        if (!isDigital(p[i])) {
            return 0;
        }
    }
    return 1;
}
static void get_alarm_parameters(const char* config_file)
{
    char buf[BUF_LEN] = {0};
    FILE* fd = NULL;
    char* index1 = NULL;
    char* index2 = NULL;
    char* src = NULL;
    char* key = NULL;
    char* value = NULL;
    errno_t rc = 0;

    if (config_file == NULL) {
        return;
    }

    fd = fopen(config_file, "r");
    if (fd == NULL) {
        return;
    }

    while (!feof(fd)) {
        rc = memset_s(buf, BUF_LEN, 0, BUF_LEN);
        securec_check_c(rc, "\0", "\0");
        (void)fgets(buf, BUF_LEN, fd);

        if (is_comment_entity(buf) == true) {
            continue;
        }
        index1 = strchr(buf, '#');
        if (index1 != NULL) {
            *index1 = '\0';
        }
        index2 = strchr(buf, '=');
        if (index2 == NULL) {
            continue;
        }
        src = buf;
        src = trim(src);
        index2 = strchr(src, '=');
        key = src;
        /* jump to the beginning of recorded values */
        value = index2 + 1;

        key = trim(key);
        value = trim(value);
        if (strncmp(key, ALARM_REPORT_INTERVAL, strlen(ALARM_REPORT_INTERVAL)) == 0) {
            if (is_digit_string(value)) {
                g_alarmReportInterval = atoi(value);
                if (g_alarmReportInterval == -1) {
                    g_alarmReportInterval = ALARM_REPORT_INTERVAL_DEFAULT;
                }
            }
            break;
        }
    }
    fclose(fd);
}

static void get_alarm_report_max_count(const char* config_file)
{
    char buf[BUF_LEN] = {0};
    FILE* fd = NULL;
    char* index1 = NULL;
    char* index2 = NULL;
    char* src = NULL;
    char* key = NULL;
    char* value = NULL;
    errno_t rc = 0;

    if (config_file == NULL) {
        return;
    }

    fd = fopen(config_file, "r");
    if (fd == NULL) {
        return;
    }

    while (!feof(fd)) {
        rc = memset_s(buf, BUF_LEN, 0, BUF_LEN);
        securec_check_c(rc, "\0", "\0");
        (void)fgets(buf, BUF_LEN, fd);

        if (is_comment_entity(buf)) {
            continue;
        }
        index1 = strchr(buf, '#');
        if (index1 != NULL) {
            *index1 = '\0';
        }
        index2 = strchr(buf, '=');
        if (index2 == NULL) {
            continue;
        }
        src = buf;
        src = trim(src);
        index2 = strchr(src, '=');
        key = src;
        /* jump to the beginning of recorded values */
        value = index2 + 1;

        key = trim(key);
        value = trim(value);
        if (strncmp(key, ALARM_REPORT_MAX_COUNT, strlen(ALARM_REPORT_MAX_COUNT)) == 0) {
            if (is_digit_string(value)) {
                g_alarmReportMaxCount = atoi(value);
                if (g_alarmReportMaxCount == -1) {
                    g_alarmReportMaxCount = ALARM_REPORT_MAX_COUNT_DEFAULT;
                }
            }
            break;
        }
    }
    fclose(fd);
}

/*
 * This function is for reading cm_server.conf parameters, which have been applied at server side.
 * In cm_server this function is ugly, it should be rewritten at new version.
 */
static void get_alarm_report_interval(const char* conf)
{
    get_alarm_parameters(conf);
}

void get_log_paramter(const char* confDir)
{
    get_log_level(confDir);
    get_log_file_size(confDir);
    GetStringFromConf(confDir, sys_log_path, sizeof(sys_log_path), "log_dir");
    GetStringFromConf(confDir, g_alarmComponentPath, sizeof(g_alarmComponentPath), "alarm_component");
    get_alarm_report_interval(confDir);
    get_alarm_report_max_count(confDir);
}

/*
 * @GaussDB@
 * Brief			:  close the current  file, and open the next   file
 * Description		:
 * Notes			:
 */
void switchLogFile(void)
{
    char log_new_name[MAXPGPATH] = {0};
    mode_t oumask;
    char current_localtime[LOG_MAX_TIMELEN] = {0};
    pg_time_t current_time;
    struct tm* systm;

    int len_log_cur_name = 0;
    int len_suffix_name = 0;
    int len_log_new_name = 0;
    int ret = 0;
    errno_t rc = 0;

    current_time = time(NULL);

    systm = localtime(&current_time);

    if (systm != nullptr) {
        (void)strftime(current_localtime, LOG_MAX_TIMELEN, "-%Y-%m-%d_%H%M%S", systm);
    }

    /* close the current  file */
    if (syslogFile != NULL) {
        fclose(syslogFile);
        syslogFile = NULL;
    }

    /* renamed the current file without  Mark */
    len_log_cur_name = strlen(curLogFileName);
    len_suffix_name = strlen(curLogFileMark);
    len_log_new_name = len_log_cur_name - len_suffix_name;

    rc = strncpy_s(log_new_name, MAXPGPATH, curLogFileName, len_log_new_name);
    securec_check_errno(rc, );
    rc = strncat_s(log_new_name, MAXPGPATH, ".log", strlen(".log"));
    securec_check_errno(rc, );
    ret = rename(curLogFileName, log_new_name);
    if (ret != 0) {
        printf(_("%s: rename log file %s failed! \n"), prefix_name, curLogFileName);
        return;
    }

    /* new current file name */
    rc = memset_s(curLogFileName, MAXPGPATH, 0, MAXPGPATH);
    securec_check_errno(rc, );
    ret = snprintf_s(curLogFileName,
        MAXPGPATH,
        MAXPGPATH - 1,
        "%s/%s%s%s",
        sys_log_path,
        prefix_name,
        current_localtime,
        curLogFileMark);
    securec_check_intval(ret, );

    oumask = umask((mode_t)((~(mode_t)(S_IRUSR | S_IWUSR | S_IXUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO)));

    syslogFile = fopen(curLogFileName, "a");

    (void)umask(oumask);

    if (syslogFile == NULL) {
        (void)printf("switchLogFile,switch new log file failed %s\n", strerror(errno));
    } else {
        if (SetFdCloseExecFlag(syslogFile) == -1) {
            (void)printf("set file flag failed, filename:%s, errmsg: %s.\n", curLogFileName, strerror(errno));
        }
    }
}

/*
 * @GaussDB@
 * Brief:
 * Description: write info to the files
 * Notes: if the current  file size is full, switch to the next
 */
void write_log_file(const char* buffer, int count)
{
    int rc = 0;

    (void)pthread_rwlock_wrlock(&syslog_write_lock);

    if (syslogFile == NULL) {
        /* maybe syslogFile no init. */
        syslogFile = logfile_open(sys_log_path, "a");
    }
    if (syslogFile != NULL) {
        count = strlen(buffer);

        /* switch to the next file when current file full */
        if ((ftell(syslogFile) + count) > (maxLogFileSize)) {
            switchLogFile();
        }

        if (syslogFile != NULL) {
            rc = fwrite(buffer, 1, count, syslogFile);
            if (rc != count) {
                printf("could not write to log file: %s %m\n", curLogFileName);
            }
        } else {
            printf("write_log_file could not open log file  %s : %m\n", curLogFileName);
        }
    } else {
        printf("write_log_file,log file is null now:%s\n", buffer);
    }

    (void)pthread_rwlock_unlock(&syslog_write_lock);
}

char *errmsg(const char* fmt, ...)
{
    va_list ap;
    int count = 0;
    int rcs;
    errno_t rc;
    char errbuf[BUF_LEN] = {0};
    fmt = _(fmt);
    va_start(ap, fmt);
    rc = memset_s(errbuf_errmsg, EREPORT_BUF_LEN, 0, EREPORT_BUF_LEN);
    securec_check_c(rc, "\0", "\0");
    count = vsnprintf_s(errbuf, sizeof(errbuf), sizeof(errbuf) - 1, fmt, ap);
    securec_check_intval(count, );
    va_end(ap);
    
    rcs = snprintf_s(errbuf_errmsg, EREPORT_BUF_LEN, EREPORT_BUF_LEN - 1, "%s", "[ERRMSG]:");
    securec_check_intval(rcs, );
    rc = memcpy_s(errbuf_errmsg + strlen(errbuf_errmsg), BUF_LEN - strlen(errbuf_errmsg),
        errbuf, BUF_LEN - strlen(errbuf_errmsg) - 1);
    securec_check_errno(rc, (void)rc);
    return errbuf_errmsg;
}

char* errdetail(const char* fmt, ...)
{
    va_list ap;
    int count = 0;
    int rcs;
    errno_t rc;
    char errbuf[BUF_LEN] = {0};
    fmt = _(fmt);
    va_start(ap, fmt);
    rc = memset_s(errbuf_errdetail, EREPORT_BUF_LEN, 0, EREPORT_BUF_LEN);
    securec_check_c(rc, "\0", "\0");
    count = vsnprintf_s(errbuf, sizeof(errbuf), sizeof(errbuf) - 1, fmt, ap);
    securec_check_intval(count, );
    va_end(ap);
    rcs = snprintf_s(errbuf_errdetail, EREPORT_BUF_LEN,
        EREPORT_BUF_LEN - 1, "%s", "[ERRDETAIL]:");
    securec_check_intval(rcs, );
    rc = memcpy_s(errbuf_errdetail + strlen(errbuf_errdetail), BUF_LEN - strlen(errbuf_errdetail),
        errbuf, BUF_LEN - strlen(errbuf_errdetail) - 1);
    securec_check_errno(rc, (void)rc);
    return errbuf_errdetail;
}

char* errcode(int sql_state)
{
    int i;
    int rcs;
    errno_t rc;
    char buf[6] = {0};
    rc = memset_s(errbuf_errcode, EREPORT_BUF_LEN, 0, EREPORT_BUF_LEN);
    securec_check_c(rc, "\0", "\0");
    /* the length of sql code is 5 */
    for (i = 0; i < 5; i++) {
        buf[i] = PGUNSIXBIT(sql_state);
        sql_state >>= 6;
    }
    buf[i] = '\0';
    rcs = snprintf_s(errbuf_errcode, EREPORT_BUF_LEN, EREPORT_BUF_LEN - 1, "%s%s", "[ERRCODE]:", buf);
    securec_check_intval(rcs, );
    return errbuf_errcode;
}

char* errcause(const char* fmt, ...)
{
    va_list ap;
    int count = 0;
    int rcs;
    errno_t rc;
    char errbuf[BUF_LEN] = {0};
    fmt = _(fmt);
    va_start(ap, fmt);
    rc = memset_s(errbuf_errcause, EREPORT_BUF_LEN, 0, EREPORT_BUF_LEN);
    securec_check_c(rc, "\0", "\0");
    count = vsnprintf_s(errbuf, sizeof(errbuf), sizeof(errbuf) - 1, fmt, ap);
    securec_check_intval(count, );
    va_end(ap);
    rcs = snprintf_s(errbuf_errcause, EREPORT_BUF_LEN,
        EREPORT_BUF_LEN - 1, "%s", "[ERRCAUSE]:");
    securec_check_intval(rcs, );
    rc = memcpy_s(errbuf_errcause + strlen(errbuf_errcause), BUF_LEN - strlen(errbuf_errcause),
        errbuf, BUF_LEN - strlen(errbuf_errcause) - 1);
    securec_check_errno(rc, (void)rc);
    return errbuf_errcause;
}

char* erraction(const char* fmt, ...)
{
    va_list ap;
    int count = 0;
    int rcs;
    errno_t rc;
    char errbuf[BUF_LEN] = {0};
    fmt = _(fmt);
    va_start(ap, fmt);
    rc = memset_s(errbuf_erraction, EREPORT_BUF_LEN, 0, EREPORT_BUF_LEN);
    securec_check_c(rc, "\0", "\0");
    count = vsnprintf_s(errbuf, sizeof(errbuf), sizeof(errbuf) - 1, fmt, ap);
    securec_check_intval(count, );
    va_end(ap);
    rcs = snprintf_s(errbuf_erraction, EREPORT_BUF_LEN,
        EREPORT_BUF_LEN - 1, "%s", "[ERRACTION]:");
    securec_check_intval(rcs, );
    rc = memcpy_s(errbuf_erraction + strlen(errbuf_erraction), BUF_LEN - strlen(errbuf_erraction),
        errbuf, BUF_LEN - strlen(errbuf_erraction) - 1);
    securec_check_errno(rc, (void)rc);
    return errbuf_erraction;
}


char* errmodule(ModuleId id)
{
    errno_t rc = memset_s(errbuf_errmodule, EREPORT_BUF_LEN, 0, EREPORT_BUF_LEN);
    securec_check_c(rc, "\0", "\0");
    int rcs = snprintf_s(errbuf_errmodule, EREPORT_BUF_LEN - 1,
        EREPORT_BUF_LEN - 1, "%s", "[ERRMODULE]:");
    securec_check_intval(rcs, (void)rcs);
    rcs = snprintf_s(errbuf_errmodule + strlen(errbuf_errmodule),
        EREPORT_BUF_LEN - strlen(errbuf_errmodule),
        EREPORT_BUF_LEN - strlen(errbuf_errmodule) - 1, "%s",
        get_valid_module_name(id));
    securec_check_intval(rcs, (void)rcs);
    return errbuf_errmodule;
}
