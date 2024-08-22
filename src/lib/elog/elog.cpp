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

#include <stdio.h>
#include <stdlib.h>
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
#include "flock.h"

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

typedef struct ToolLogInfo {
    time_t mTime;
    time_t cTime;
    char fileName[0];
} ToolLogInfo;

#ifndef palloc
#define palloc(sz) malloc(sz)
#endif
#ifndef pfree
#define pfree(ptr) free(ptr)
#endif

#define LOG_MAX_COUNT    50
#define GS_LOCKFILE_SIZE 1024
#define curLogFileMark "-current.log"
#define LOG_DIR_FMT "%s/bin/%s"
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

static int gs_srvtool_lock(const char *prefix_name, const char *log_dir, FILE **fd)
{
    int ret;
    struct stat statbuf;
    int fildes = 0;
    char lockfile[MAXPGPATH] = {'\0'};

    ret = snprintf_s(lockfile, sizeof(lockfile), sizeof(lockfile) - 1, "%s/%s.%s", log_dir, prefix_name, "lock");
    securec_check_ss_c(ret, "\0", "\0");

    canonicalize_path(lockfile);

    /* If lock file dose not exist, create it */
    if (stat(lockfile, &statbuf) != 0) {
        char content[GS_LOCKFILE_SIZE] = {0};
        *fd = fopen(lockfile, PG_BINARY_W);
        if (*fd == NULL) {
            printf(_("%s: can't create lock file \"%s\" : %s\n"), prefix_name, lockfile, gs_strerror(errno));
            exit(1);
        } 
        
        fildes = fileno(*fd);
        if (fchmod(fildes, S_IRUSR | S_IWUSR) == -1) {
            printf(_("%s: can't chmod lock file \"%s\" : %s\n"), prefix_name, lockfile, gs_strerror(errno));
            /* Close file and Nullify the pointer for retry */
            fclose(*fd);
            *fd = NULL;
            exit(1);
        }

        if (fwrite(content, GS_LOCKFILE_SIZE, 1, *fd) != 1) {
            fclose(*fd);
            *fd = NULL;
            printf(_("%s: can't write lock file \"%s\" : %s\n"), prefix_name, lockfile, gs_strerror(errno));
            exit(1);
        }
        fclose(*fd);
        *fd = NULL;
    }

    if ((*fd = fopen(lockfile, PG_BINARY_W)) == NULL) {
        printf(_("%s: can't open lock file \"%s\" : %s\n"), prefix_name, lockfile, gs_strerror(errno));
        exit(1);
    }

    ret = flock(fileno(*fd), LOCK_EX | LOCK_NB, 0, START_LOCATION, GS_LOCKFILE_SIZE);
    return ret;
}

static inline int gs_srvtool_unlock(FILE *fd)
{
    int ret = -1;

    if (fd != NULL) {
        ret = flock(fileno(fd), LOCK_UN, 0, START_LOCATION, GS_LOCKFILE_SIZE);
        fclose(fd);
        fd = NULL;
    }

    return ret;
}

static inline int file_time_cmp(const void *v1, const void *v2)
{
    const ToolLogInfo *l1 = *(ToolLogInfo **)v1;
    const ToolLogInfo *l2 = *(ToolLogInfo **)v2;

    int result = l1->mTime - l2->mTime;
    if (result == 0) {
        return l1->cTime - l2->cTime;
    }
    return result;
}

static inline void free_file_list(ToolLogInfo **file_list, int count)
{
    for (int i = 0; i < count; i++) {
        pfree(file_list[i]);
    }
    pfree(file_list);
}

static inline bool str_end_with(const char *str, const char *end)
{
    int slen = strlen(str);
    int elen = strlen(end);
    if (elen > slen) {
        return false;
    } else {
        return (strcmp(str + slen - elen, end) == 0);
    }
}

static void remove_oldest_log(const char *prefix_name, const char *log_path, int count)
{
    DIR *dir = NULL;
    struct dirent *de = NULL;
    errno_t rc = EOK;

    int file_len = strlen(prefix_name) + strlen("-yyyy-mm-dd_hhmmss.log");
    size_t info_size = sizeof(ToolLogInfo) + file_len + 1;
    ToolLogInfo **file_list = (ToolLogInfo **)palloc(sizeof(ToolLogInfo *) * count);
    if (file_list == NULL) {
        printf(_("%s: palloc memory failed! %s\n"), prefix_name, gs_strerror(errno));
        return;
    }

    for (int i = 0; i < count; i++) {
        file_list[i] = (ToolLogInfo *)palloc(info_size);
        if (file_list[i] == NULL) {
            printf(_("%s: palloc memory failed! %s\n"), prefix_name, gs_strerror(errno));
            free_file_list(file_list, i);
            return;
        }
        rc = memset_s(file_list[i], info_size, 0, info_size);
        securec_check_c(rc, "\0", "\0");
    }

    if ((dir = opendir(log_path)) == NULL) {
        free_file_list(file_list, count);
        printf(_("%s: opendir %s failed! %s\n"), prefix_name, log_path, gs_strerror(errno));
        return;
    }

    int slot = 0;
    struct stat fst;
    char pathname[MAXPGPATH] = {'\0'};
    while ((de = readdir(dir)) != NULL) {
        if (strncmp(de->d_name, prefix_name, strlen(prefix_name)) == 0 &&
            !str_end_with(de->d_name, curLogFileMark) &&
            !str_end_with(de->d_name, ".lock")) {
            rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s/%s", log_path, de->d_name);
            securec_check_ss_c(rc, "\0", "\0");
            if (stat(pathname, &fst) < 0) {
                printf(_("%s: could not stat file %s\n"), prefix_name, pathname, gs_strerror(errno));
                continue;
            }

            file_list[slot]->mTime = fst.st_mtime;
            file_list[slot]->cTime = fst.st_ctime;
            rc = strncpy_s(file_list[slot]->fileName, file_len + 1, de->d_name, strlen(de->d_name));
            securec_check_c(rc, "\0", "\0");
            slot++;
        }
    }

    qsort(file_list, slot, sizeof(ToolLogInfo *), file_time_cmp);
    printf(_("%s: log file count %d, exceeds %d, remove the oldest ones\n"), prefix_name, count, LOG_MAX_COUNT);

    int remove_cnt = 0;
    while (remove_cnt < count - LOG_MAX_COUNT) {
        rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s/%s", log_path, file_list[remove_cnt]->fileName);
        securec_check_ss_c(rc, "\0", "\0");
        if (remove(pathname) < 0) {
            printf(_("%s: remove log file %s failed!\n"), prefix_name, pathname, gs_strerror(errno));
            continue;
        }

        remove_cnt++;
        printf(_("%s: remove log file %s successfully, remain %d files\n"), prefix_name, pathname, count - remove_cnt);
    }

    free_file_list(file_list, count);
    (void)closedir(dir);
}

static int create_log_file(const char* prefix_name, const char* log_path, int *count)
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
        if (NULL != strstr(de->d_name, prefix_name) && !str_end_with(de->d_name, ".lock")) {
            *count += 1;
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
                        is_exist = false;
                    }
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
        *count += 1;
    }
    (void)closedir(dir);
    return 0;
}

static void redirect_output(const char* prefix_name, const char* log_dir, int *count)
{

    if (0 != create_log_file(prefix_name, log_dir, count)) {
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
        int file_count = 0;
        FILE *fd = NULL;
        if (gs_srvtool_lock(prefix_name, log_dir, &fd) == -1) {
            printf(_("another %s command is running, init_log failed!\n"), prefix_name);
            exit(1);
        }

        redirect_output(prefix_name, log_dir, &file_count);
        if (file_count > LOG_MAX_COUNT) {
            remove_oldest_log(prefix_name, log_dir, file_count);
        }

        gs_srvtool_unlock(fd);
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

void GenerateProgressBar(int percent, char* progressBar)
{
    if (percent > 100) {
        percent = 100;
    }

    int barWidth = 50;
    int filledWidth = (percent * barWidth) / 100;

    progressBar[0] = '[';

    for (int i = 1; i <= barWidth; i++) {
        progressBar[i] = (i <= filledWidth) ? '=' : ' ';
    }

    progressBar[barWidth + 1] = ']';
    progressBar[barWidth + 2] = '\0';
}

static FILE *create_audit_file(const char* prefix_name, const char* log_path) {
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

    FILE *fp;
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

        return NULL;
    }

    while (NULL != (de = readdir(dir))) {
        // exist current log file
        if (NULL != strstr(de->d_name, prefix_name)) {
            name_ptr = strstr(de->d_name, "-current.log");
            if (NULL != name_ptr) {
                name_ptr += strlen("-current.log");
                if ('\0' == (*name_ptr)) {
                    nRet = snprintf_s(log_file_name, MAXPGPATH, MAXPGPATH - 1, "%s/%s", log_path, de->d_name);
                    securec_check_ss_c(nRet, "\0", "\0");
                    is_exist = true;
                    fp = fopen(log_file_name, "a");
                    if (fp == NULL) {
                        printf(_("%s: open audit file %s failed!\n"), prefix_name, log_file_name);
                        (void)closedir(dir);
                        return NULL;
                    }

                    (void)lstat(log_file_name, &statbuf);
                    if (statbuf.st_size > LOG_MAX_SIZE) {
                        set_log_filename(log_temp_name, de->d_name);
                        nRet = snprintf_s(log_new_name, MAXPGPATH, MAXPGPATH - 1, "%s/%s", log_path, log_temp_name);
                        securec_check_ss_c(nRet, "\0", "\0");
                        ret = rename(log_file_name, log_new_name);
                        if (0 != ret) {
                            printf(_("%s: rename audit file %s failed!\n"), prefix_name, log_file_name);
                            (void)closedir(dir);
                            return NULL;
                        }
                    }
                    (void)closedir(dir);
                    return fp;
                }
            }
        }
    }
    // current log file not exist
    if (!is_exist) {
        nRet = snprintf_s(log_file_name,
            MAXPGPATH,
            MAXPGPATH - 1,
            "%s/%s%s%s",
            log_path,
            prefix_name,
            log_create_time,
            curLogFileMark);
        securec_check_ss_c(nRet, "\0", "\0");

        fp = fopen(log_file_name, "a");
        if (fp == NULL) {
            printf(_("%s: open audit file %s failed!\n"), prefix_name, log_file_name);
            (void)closedir(dir);
            return NULL;
        }
        rc = chmod(log_file_name, S_IRUSR | S_IWUSR);
        if (rc != 0) {
            printf(_("%s: chmod audit file %s failed!\n"), prefix_name, log_file_name);
            (void)fclose(fp);
            (void)closedir(dir);
            return NULL;
        }
    }
    (void)closedir(dir);
    return fp;
}

static void get_cur_time(char * current_localtime) {
    pg_time_t current_time = time(NULL);
    struct tm tmp;
    struct tm* systm = &tmp;
    localtime_r((const time_t*)&current_time, systm);
    if (NULL != systm) {
        (void)strftime(current_localtime, LOG_MAX_TIMELEN, "%Y-%m-%d %H:%M:%S", systm);
    }
}

static void report_command(FILE *fp, auditConfig *audit_cfg) {
    const char* process_name = audit_cfg->process_name;
    int argc = audit_cfg->argc;
    char** argv = audit_cfg->argv;
    bool is_success = audit_cfg->is_success;

    errno_t rc;
    char command[MAXPGPATH] = {0};

    rc = strcat_s(command, MAXPGPATH, process_name);
    securec_check_c(rc, "\0", "\0");
    rc = strcat_s(command, MAXPGPATH, " ");
    securec_check_c(rc, "\0", "\0");
    
    bool is_pass = false;
    for (int i = 1; i<argc; i++) {
        if (!strcmp(argv[i], "-W") || !strcmp(argv[i], "--password")) {
            is_pass = true;
            continue;
        }
        if (is_pass) {
            is_pass = false;
            continue;
        }
        if (strncmp(argv[i], "postgresql://", strlen("postgresql://")) == 0) {
            char *off_argv = argv[i] + strlen("postgresql://");
            rc = memset_s(off_argv, strlen(off_argv), '*', strlen(off_argv));
            securec_check_c(rc, "\0", "\0");
        } else if (strncmp(argv[i], "postgres://", strlen("postgres://")) == 0) {
            char *off_argv = argv[i] + strlen("postgres://");
            rc = memset_s(off_argv, strlen(off_argv), '*', strlen(off_argv));
            securec_check_c(rc, "\0", "\0");
        } else if (strncmp(argv[i], "--password", strlen("--password")) == 0) {
            continue;
        } else if (strncmp(argv[i], "-W", strlen("-W")) == 0) {
            continue;
        }

        rc = strcat_s(command, MAXPGPATH, argv[i]);
        securec_check_c(rc, "\0", "\0");
        if (i<argc - 1) {
            rc = strcat_s(command, MAXPGPATH, " ");
            securec_check_c(rc, "\0", "\0");
        }
    }

    char current_localtime[LOG_MAX_TIMELEN] = {0};
    get_cur_time(current_localtime);

    struct passwd *pwd = getpwuid(getuid());  // Check for NULL!
    if (pwd != NULL) {
        (void)fprintf(fp, _("[%s] [%s] [%s] %s\n"), current_localtime, (is_success ? "SUCCESS" : "FAILURE"), pwd->pw_name, command);
    } else {
        (void)fprintf(fp, _("[%s] [%s] %s\n"), current_localtime, (is_success ? "SUCCESS" : "FAILURE"), command);
    }
}

static void get_log_dir(const char *process_name, char *log_dir) {
    char* gausslog_dir = NULL;
    int nRet = 0;
    gausslog_dir = gs_getenv_r("GAUSSLOG");
    check_env_value_c(gausslog_dir);
    if ((NULL == gausslog_dir) || ('\0' == gausslog_dir[0])) {
        return;
    } 
    if (!is_absolute_path(gausslog_dir)) {
        printf(_("current path is not absolute path ,can't find the exec path.\n"));
        return;
    }
    nRet = snprintf_s(log_dir, MAXPGPATH, MAXPGPATH - 1, LOG_DIR_FMT, gausslog_dir, process_name);
    securec_check_ss_c(nRet, "\0", "\0");
}

auditConfig audit_cfg;

static void audit_report() {
    if (!audit_cfg.has_init) {
        return;
    }
    // init log
    char log_dir[MAXPGPATH] = {0};
    get_log_dir(audit_cfg.process_name, log_dir);
    if ('\0' == log_dir[0]) {
        return;
    }

    if (0 != pg_mkdir_p(log_dir, S_IRWXU)) {
        if (EEXIST != errno) {
            printf(_("could not create directory %s: %m\n"), log_dir);
            return;
        }
    }   
    // create audit file
    FILE *fp = create_audit_file("audit", log_dir);
    if (fp == NULL) {
        printf(_("Warning: create_audit_file failed!\n"));
        return;
    }

    // audit report
    report_command(fp, &audit_cfg);

    //close fd
    fclose(fp);
}

void init_audit(const char* process_name, int argc, char** argv) {
    audit_cfg.has_init = true;
    audit_cfg.is_success = false;
    audit_cfg.process_name = process_name;
    audit_cfg.argc = argc;
    audit_cfg.argv = argv;
    atexit(audit_report);
}

void audit_success() {
    audit_cfg.is_success = true;
}
