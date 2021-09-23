/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 *  elog.cpp
 *        create log for command
 *
 * IDENTIFICATION
 *        src/lib/elog/elog.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#ifdef WIN32
/*
 * Need this to get defines for restricted tokens and jobs. And it
 * has to be set before any header from the Win32 API is loaded.
 */
#define _WIN32_WINNT 0x0501
#endif

#include <locale.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#include "postgres_fe.h"

#ifdef WIN32_PG_DUMP
#undef PGDLLIMPORT
#define PGDLLIMPORT
#endif

#include "pgtime.h"
#include "bin/elog.h"

#if defined(__CYGWIN__)
#include <sys/cygwin.h>
#define _WINSOCKAPI_
#include <windows.h>
/* Cygwin defines WIN32 in windows.h, but we don't want it. */
#undef WIN32
#endif

#define curLogFileMark "-current.log"
// optimize,to suppose pirnt to file and screen
static bool allow_log_store = false;
void check_env_value_c(const char* input_env_value);
/*
 * @@GaussDB@@
 * Brief            :
 * Description      : Write system log on windows
 * Notes            :
 */
#if defined(WIN32) || defined(__CYGWIN__)
static void write_eventlog(int level, const char* line)
{
    static HANDLE evtHandle = INVALID_HANDLE_VALUE;

    if (level == EVENTLOG_INFORMATION_TYPE) {
        return;
    }

    evtHandle = RegisterEventSource(NULL, "PostgreSQL");
    if (evtHandle == NULL) {
        evtHandle = INVALID_HANDLE_VALUE;
        return;
    }

    (void)ReportEvent(evtHandle,
        level,
        0,
        0,  // All events are Id 0
        NULL,
        1,
        0,
        &line,
        NULL);
}

/*
 * On Win32, we print to stderr if running on a console, or write to
 * eventlog if running as a service
 */
void wirte_to_stderr_or_eventlog(const char* fmt, va_list* ap)
{
    if (!isatty(fileno(stderr))) {  // Running as a service
        char errbuf[2048];          // Arbitrary size?
        int nRet = 0;

        nRet = vsnprintf_s(errbuf, sizeof(errbuf), sizeof(errbuf) - 1, fmt, *ap);
        securec_check_ss_c(nRet, "\0", "\0");

        write_eventlog(EVENTLOG_ERROR_TYPE, errbuf);
    } else  // Not running as service, write to stderr
        (void)vfprintf(stderr, fmt, *ap);
}

#endif

/*
 * @@GaussDB@@
 * Brief            :
 * Description      : Write log file and print screen if allow_log_store is true
 * Notes            :
 */
void write_stderr(const char* fmt, ...)
{
    va_list ap;
    // optimize,to suppose pirnt to file and screen
    va_list bp;

    (void)va_start(ap, fmt);
    // optimize,to suppose pirnt to file and screen
    (void)va_start(bp, fmt);
#if !defined(WIN32) && !defined(__CYGWIN__)
    // On Unix, we just fprintf to stdout, print screen
    (void)vfprintf(stdout, fmt, ap);
    (void)fflush(stdout);
    if (allow_log_store) {
        (void)vfprintf(stderr, fmt, bp);  // optimize,to suppose pirnt to file and screen,print to file
        (void)fflush(stderr);
    }
#else

    //  On Win32, we print to stderr if running on a console, or write to
    //  eventlog if running as a service
    wirte_to_stderr_or_eventlog(fmt, &ap);

#endif
    va_end(ap);
    va_end(bp);
}

/*
 * @@GaussDB@@
 * Brief            :
 * Description      : Only write log file.
 * Notes            :
 */
void write_log(const char* fmt, ...)
{
    va_list ap;
    va_list bp;

    (void)va_start(ap, fmt);
    (void)va_start(bp, fmt);
#if !defined(WIN32) && !defined(__CYGWIN__)
    // On Unix, we just write to the log file.
    (void)vfprintf(stderr, fmt, bp);  // print to log file
    (void)fflush(stderr);
#else
    //  On Win32, we print to stderr if running on a console, or write to
    //  eventlog if running as a service
    wirte_to_stderr_or_eventlog(fmt, &ap);

#endif
    va_end(ap);
    va_end(bp);
}

static void set_log_filename(char* log_new_name, const char* log_old_name)
{
    int len_log_old_name = 0;
    int len_suffix_name = 0;
    int len_log_new_name = 0;
    errno_t rc = 0;

    len_log_old_name = strlen(log_old_name);
    len_suffix_name = strlen(curLogFileMark);

    len_log_new_name = len_log_old_name - len_suffix_name;
    rc = strncpy_s(log_new_name, len_log_new_name + 1, log_old_name, len_log_new_name);
    securec_check_c(rc, "\0", "\0");
    rc = strncat_s(log_new_name, len_log_new_name + 5, ".log", strlen(".log"));
    securec_check_c(rc, "\0", "\0");
}

static int create_log_file(const char* prefix_name, const char* log_path)
{
#define LOG_MAX_SIZE (16 * 1024 * 1024)
#define LOG_MAX_TIMELEN 80
    DIR* dir = NULL;
    struct dirent* de = NULL;
    bool is_exist = false;
    char log_file_name[MAXPGPATH] = {0};
    char log_temp_name[MAXPGPATH] = {0};
    char log_new_name[MAXPGPATH] = {0};
    char log_create_time[LOG_MAX_TIMELEN] = {0};
    char current_localtime[LOG_MAX_TIMELEN] = {0};
    // check validity of current log file name
    char* name_ptr = NULL;

    int fd = 0;
    int ret = 0;
    struct stat statbuf;

    pg_time_t current_time;
    struct tm tmp;
    struct tm* systm = &tmp;
    int nRet = 0;
    errno_t rc = 0;

    rc = memset_s(&statbuf, sizeof(statbuf), 0, sizeof(statbuf));
    securec_check_c(rc, "\0", "\0");

    current_time = time(NULL);
    localtime_r((const time_t*)&current_time, systm);
    if (NULL != systm) {
        (void)strftime(current_localtime, LOG_MAX_TIMELEN, "%Y-%m-%d %H:%M:%S", systm);
        (void)strftime(log_create_time, LOG_MAX_TIMELEN, "-%Y-%m-%d_%H%M%S", systm);
    }

    if (NULL == (dir = opendir(log_path))) {
        printf(_("%s: opendir %s failed! %s\n"), prefix_name, log_path, gs_strerror(errno));

        return -1;
    }

    while (NULL != (de = readdir(dir))) {
        // exist current log file
        if (NULL != strstr(de->d_name, prefix_name)) {
            name_ptr = strstr(de->d_name, "-current.log");
            if (NULL != name_ptr) {
                name_ptr += strlen("-current.log");
                if ('\0' == (*name_ptr)) {
                    rc = memset_s(log_file_name, MAXPGPATH, 0, MAXPGPATH);
                    securec_check_c(rc, "\0", "\0");
                    nRet = snprintf_s(log_file_name, MAXPGPATH, MAXPGPATH - 1, "%s/%s", log_path, de->d_name);
                    securec_check_ss_c(nRet, "\0", "\0");
                    is_exist = true;
                    fd = open(log_file_name, O_WRONLY | O_APPEND, 0);
                    if (-1 == fd) {
                        printf(_("%s: open log file %s failed!\n"), prefix_name, log_file_name);
                        (void)closedir(dir);
                        return -1;
                    }

                    (void)dup2(fd, fileno(stderr));
                    (void)fprintf(stderr, _("[%s]\n"), current_localtime);  // add current time to log
                    close(fd);
                    (void)lstat(log_file_name, &statbuf);
                    if (statbuf.st_size > LOG_MAX_SIZE) {
                        set_log_filename(log_temp_name, de->d_name);
                        nRet = snprintf_s(log_new_name, MAXPGPATH, MAXPGPATH - 1, "%s/%s", log_path, log_temp_name);
                        securec_check_ss_c(nRet, "\0", "\0");
                        ret = rename(log_file_name, log_new_name);
                        if (0 != ret) {
                            printf(_("%s: rename log file %s failed!\n"), prefix_name, log_file_name);
                            (void)closedir(dir);
                            return -1;
                        }
                    }
                    (void)closedir(dir);
                    return 0;
                }
            }
        }
    }
    // current log file not exist
    if (!is_exist) {
        rc = memset_s(log_file_name, MAXPGPATH, 0, MAXPGPATH);
        securec_check_c(rc, "\0", "\0");
        nRet = snprintf_s(log_file_name,
            MAXPGPATH,
            MAXPGPATH - 1,
            "%s/%s%s%s",
            log_path,
            prefix_name,
            log_create_time,
            curLogFileMark);
        securec_check_ss_c(nRet, "\0", "\0");

        fd = open(log_file_name, O_WRONLY | O_CREAT | O_APPEND, S_IRUSR | S_IWUSR);
        if (-1 == fd) {
            printf(_("%s: open log file %s failed!\n"), prefix_name, log_file_name);
            (void)closedir(dir);
            return -1;
        }

        (void)dup2(fd, fileno(stderr));
        (void)fprintf(stderr, _("[%s]\n"), current_localtime);  // add current time to log
        close(fd);
    }
    (void)closedir(dir);
    return 0;
}

static void redirect_output(const char* prefix_name, const char* log_dir)
{

    if (0 != create_log_file(prefix_name, log_dir)) {
        printf(_("Warning: create_log_file failed!\n"));
        return;
    }
}

void init_log(char* prefix_name)
{
    char* gausslog_dir = NULL;
    char log_dir[MAXPGPATH] = {0};
    bool is_redirect = false;
    int nRet = 0;
    gausslog_dir = gs_getenv_r("GAUSSLOG");
    check_env_value_c(gausslog_dir);
    if ((NULL == gausslog_dir) || ('\0' == gausslog_dir[0])) {
        is_redirect = false;
    } else {
        if (is_absolute_path(gausslog_dir)) {
            nRet = snprintf_s(log_dir, MAXPGPATH, MAXPGPATH - 1, "%s/bin/%s", gausslog_dir, prefix_name);
            securec_check_ss_c(nRet, "\0", "\0");
            // log_dir not exist, create log_dir path
            if (0 != pg_mkdir_p(log_dir, S_IRWXU)) {
                if (EEXIST != errno) {
                    printf(_("could not create directory %s: %m\n"), log_dir);
                    return;
                }
            }
            is_redirect = true;
        } else {
            printf(_("current path is not absolute path ,can't find the exec path.\n"));
            return;
        }
    }
    allow_log_store = is_redirect;  // if false print to screen, if true print to file
    if (true == is_redirect) {
        redirect_output(prefix_name, log_dir);
    }
}

/*
 * @Description: check if the input value have danger character
 */
void check_danger_character(const char* inputEnvValue)
{
    if (inputEnvValue == NULL)
        return;

    const char* dangerCharacterList[] = {";", "`", "\\", "'", "\"", ">", "<", "&", "|", "!", NULL};
    int i = 0;

    for (i = 0; dangerCharacterList[i] != NULL; i++) {
        if (strstr(inputEnvValue, dangerCharacterList[i]) != NULL) {
            fprintf(stderr, _("ERROR: Failed to check input value: invalid token \"%s\".\n"), dangerCharacterList[i]);
            exit(1);
        }
    }
}

/*
 * @Description: check the value to prevent command injection.
 * The username and database name support special symbols $.
 * @in input_env_value : the input value need be checked.
 */
void check_env_value_c(const char* input_env_value)
{
    check_danger_character(input_env_value);
}

/*
 * @Description: check the value to prevent command injection.
 * The username and database name support special symbols $.
 * @in input_env_value : the input value need be checked.
 */
void check_env_name_c(const char* input_env_value)
{
    check_danger_character(input_env_value);
}
