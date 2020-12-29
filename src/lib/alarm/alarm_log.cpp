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
 * alarm_log.c
 *    alarm logging and reporting
 *
 * IDENTIFICATION
 *    src/lib/alarm/alarm_log.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <signal.h>
#include <ctype.h>
#include <syslog.h>
#include <limits.h>
#include "cm/path.h"
#include "cm/cm_c.h"
#include "cm/stringinfo.h"
#include "alarm/alarm_log.h"
#include <sys/time.h>
#if !defined(WIN32)
#include <sys/syscall.h>
#define gettid() syscall(__NR_gettid)
#else
/* windows */
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

#define SYSTEM_ALARM_LOG "system_alarm"
#define MAX_SYSTEM_ALARM_LOG_SIZE (128 * 1024 * 1024) /* 128MB */
#define CURLOGFILEMARK "-current.log"

const int LOG_MAX_TIMELEN = 80;
const int COMMAND_SIZE = 4196;
const int REPORT_MSG_SIZE = 4096;

char g_alarm_scope[MAX_BUF_SIZE] = {0};
char system_alarm_log[MAXPGPATH] = {0};
static char system_alarm_log_name[MAXPGPATH];
pthread_rwlock_t alarm_log_write_lock;
FILE* alarmLogFile = NULL;
typedef int64 pg_time_t;
char sys_alarm_log_path[MAX_PATH_LEN] = {0};

/*
 * Open a new logfile with proper permissions and buffering options.
 *
 */
static FILE* logfile_open(const char* filename, const char* mode)
{
    FILE* fh = NULL;
    mode_t oumask;

    // Note we do not let Log_file_mode disable IWUSR, since we certainly want to be able to write the files ourselves.
    oumask = umask((mode_t)((~(mode_t)(S_IRUSR | S_IWUSR | S_IXUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO)));
    fh = fopen(filename, mode);
    (void)umask(oumask);
    if (fh != NULL) {
        setvbuf(fh, NULL, LBF_MODE, 0);

#ifdef WIN32
        /* use CRLF line endings on Windows */
        _setmode(_fileno(fh), _O_TEXT);
#endif
    } else {
        AlarmLog(ALM_LOG, "could not open log file \"%s\"\n", filename);
    }
    return fh;
}

static void create_new_alarm_log_file(const char* sys_log_path)
{
    pg_time_t current_time;
    struct tm systm;
    char log_create_time[LOG_MAX_TIMELEN] = {0};
    char log_temp_name[MAXPGPATH] = {0};
    errno_t rc;

    rc = memset_s(&systm, sizeof(systm), 0, sizeof(systm));
    securec_check_c(rc, "\0", "\0");
    /* create new log file */
    rc = memset_s(system_alarm_log, MAXPGPATH, 0, MAXPGPATH);
    securec_check_c(rc, "\0", "\0");

    current_time = time(NULL);
    if (localtime_r(&current_time, &systm) != NULL) {
        (void)strftime(log_create_time, LOG_MAX_TIMELEN, "-%Y-%m-%d_%H%M%S", &systm);
    } else {
        AlarmLog(ALM_LOG, "get localtime_r failed\n");
    }

    rc = snprintf_s(
        log_temp_name, MAXPGPATH, MAXPGPATH - 1, "%s%s%s", SYSTEM_ALARM_LOG, log_create_time, CURLOGFILEMARK);
    securec_check_ss_c(rc, "\0", "\0");
    rc = snprintf_s(system_alarm_log, MAXPGPATH, MAXPGPATH - 1, "%s/%s", sys_log_path, log_temp_name);
    securec_check_ss_c(rc, "\0", "\0");
    rc = memset_s(system_alarm_log_name, MAXPGPATH, 0, MAXPGPATH);
    securec_check_c(rc, "\0", "\0");
    rc = strncpy_s(system_alarm_log_name, MAXPGPATH, log_temp_name, strlen(log_temp_name));
    securec_check_c(rc, "\0", "\0");

    alarmLogFile = logfile_open(system_alarm_log, "a");
}

static bool rename_alarm_log_file(const char* sys_log_path)
{
    int len_log_old_name, len_suffix_name, len_log_new_name;
    char logFileBuff[MAXPGPATH] = {0};
    char log_new_name[MAXPGPATH] = {0};
    errno_t rc;
    int ret;

    /* renamed the current file without  Mark */
    len_log_old_name = strlen(system_alarm_log_name);
    len_suffix_name = strlen(CURLOGFILEMARK);
    len_log_new_name = len_log_old_name - len_suffix_name;

    rc = strncpy_s(logFileBuff, MAXPGPATH, system_alarm_log_name, len_log_new_name);
    securec_check_c(rc, "\0", "\0");
    rc = strncat_s(logFileBuff, MAXPGPATH, ".log", strlen(".log"));
    securec_check_c(rc, "\0", "\0");

    rc = snprintf_s(log_new_name, MAXPGPATH, MAXPGPATH - 1, "%s/%s", sys_log_path, logFileBuff);
    securec_check_ss_c(rc, "\0", "\0");

    /* close the current  file  */
    if (alarmLogFile != NULL) {
        fclose(alarmLogFile);
        alarmLogFile = NULL;
    }

    ret = rename(system_alarm_log, log_new_name);
    if (ret != 0) {
        AlarmLog(ALM_LOG, "ERROR: %s: rename log file %s failed! \n", system_alarm_log, system_alarm_log);
        return false;
    }
    return true;
}

/* write alarm info to alarm log file */
static void write_log_file(const char* buffer)
{
    int rc;
    (void)pthread_rwlock_wrlock(&alarm_log_write_lock);

    if (alarmLogFile == NULL) {
        if (strncmp(system_alarm_log, "/dev/null", strlen("/dev/null")) == 0) {
            create_system_alarm_log(sys_alarm_log_path);
        }
        alarmLogFile = logfile_open(system_alarm_log, "a");
    }

    if (alarmLogFile != NULL) {
        int count = strlen(buffer);

        rc = fwrite(buffer, 1, count, alarmLogFile);
        if (rc != count) {
            AlarmLog(ALM_LOG, "could not write to log file: %s\n", system_alarm_log);
        }
        fflush(alarmLogFile);
        fclose(alarmLogFile);
        alarmLogFile = NULL;
    } else {
        AlarmLog(ALM_LOG, "write_log_file, log file is null now: %s\n", buffer);
    }

    (void)pthread_rwlock_unlock(&alarm_log_write_lock);
}

/* unify log style */
void create_system_alarm_log(const char* sys_log_path)
{
    DIR* dir = NULL;
    struct dirent* de = NULL;
    bool is_exist = false;

    /* check validity of current log file name */
    char* name_ptr = NULL;
    errno_t rc;

    if (strlen(sys_alarm_log_path) == 0) {
        rc = strncpy_s(sys_alarm_log_path, MAX_PATH_LEN, sys_log_path, strlen(sys_log_path));
        securec_check_c(rc, "\0", "\0");
    }

    if ((dir = opendir(sys_log_path)) == NULL) {
        AlarmLog(ALM_LOG, "opendir %s failed! \n", sys_log_path);
        rc = strncpy_s(system_alarm_log, MAXPGPATH, "/dev/null", strlen("/dev/null"));
        securec_check_ss_c(rc, "\0", "\0");
        return;
    }

    while ((de = readdir(dir)) != NULL) {
        /* exist current log file */
        if (strstr(de->d_name, SYSTEM_ALARM_LOG) != NULL) {
            name_ptr = strstr(de->d_name, CURLOGFILEMARK);
            if (name_ptr != NULL) {
                name_ptr += strlen(CURLOGFILEMARK);
                if ((*name_ptr) == '\0') {
                    is_exist = true;
                    break;
                }
            }
        }
    }
    if (is_exist) {
        rc = memset_s(system_alarm_log_name, MAXPGPATH, 0, MAXPGPATH);
        securec_check_c(rc, "\0", "\0");
        rc = memset_s(system_alarm_log, MAXPGPATH, 0, MAXPGPATH);
        securec_check_c(rc, "\0", "\0");
        rc = snprintf_s(system_alarm_log, MAXPGPATH, MAXPGPATH - 1, "%s/%s", sys_log_path, de->d_name);
        securec_check_ss_c(rc, "\0", "\0");
        rc = strncpy_s(system_alarm_log_name, MAXPGPATH, de->d_name, strlen(de->d_name));
        securec_check_c(rc, "\0", "\0");
    } else {
        /* create current log file name */
        create_new_alarm_log_file(sys_log_path);
    }
    (void)closedir(dir);
}

void clean_system_alarm_log(const char* file_name, const char* sys_log_path)
{
    Assert(file_name != NULL);

    unsigned long filesize = 0;
    struct stat statbuff;
    int ret;

    errno_t rc = memset_s(&statbuff, sizeof(statbuff), 0, sizeof(statbuff));
    securec_check_c(rc, "\0", "\0");

    ret = stat(file_name, &statbuff);
    if (ret != 0 || (strncmp(file_name, "/dev/null", strlen("/dev/null")) == 0)) {
        AlarmLog(ALM_LOG, "ERROR: stat system alarm log %s error.ret=%d\n", file_name, ret);
        return;
    } else {
        filesize = statbuff.st_size;
    }
    if (filesize > MAX_SYSTEM_ALARM_LOG_SIZE) {
        (void)pthread_rwlock_wrlock(&alarm_log_write_lock);
        /* renamed the current file without  Mark */
        if (rename_alarm_log_file(sys_log_path)) {
            /* create new log file */
            create_new_alarm_log_file(sys_log_path);
        }
        (void)pthread_rwlock_unlock(&alarm_log_write_lock);
    }
    return;
}

void write_alarm(Alarm* alarmItem, const char* alarmName, const char* alarmLevel, AlarmType type,
    AlarmAdditionalParam* additionalParam)
{
    char command[COMMAND_SIZE];
    char reportInfo[REPORT_MSG_SIZE];
    errno_t rcs = 0;

    if (strlen(system_alarm_log) == 0)
        return;

    errno_t rc = memset_s(command, COMMAND_SIZE, 0, COMMAND_SIZE);
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(reportInfo, REPORT_MSG_SIZE, 0, REPORT_MSG_SIZE);
    securec_check_c(rc, "\0", "\0");
    if (type == ALM_AT_Fault || type == ALM_AT_Event) {
        rcs = snprintf_s(reportInfo,
            REPORT_MSG_SIZE,
            REPORT_MSG_SIZE - 1,
            "{" SYSQUOTE "id" SYSQUOTE SYSCOLON SYSQUOTE "%016ld" SYSQUOTE SYSCOMMA SYSQUOTE
            "name" SYSQUOTE SYSCOLON SYSQUOTE "%s" SYSQUOTE SYSCOMMA SYSQUOTE "level" SYSQUOTE SYSCOLON SYSQUOTE
            "%s" SYSQUOTE SYSCOMMA SYSQUOTE "scope" SYSQUOTE SYSCOLON "%s" SYSCOMMA SYSQUOTE
            "source_tag" SYSQUOTE SYSCOLON SYSQUOTE "%s-%s" SYSQUOTE SYSCOMMA SYSQUOTE
            "op_type" SYSQUOTE SYSCOLON SYSQUOTE "%s" SYSQUOTE SYSCOMMA SYSQUOTE "details" SYSQUOTE SYSCOLON SYSQUOTE
            "%s" SYSQUOTE SYSCOMMA SYSQUOTE "clear_type" SYSQUOTE SYSCOLON SYSQUOTE "%s" SYSQUOTE SYSCOMMA SYSQUOTE
            "start_timestamp" SYSQUOTE SYSCOLON "%ld" SYSCOMMA SYSQUOTE "end_timestamp" SYSQUOTE SYSCOLON "%d"
            "}\n",
            alarmItem->id,
            alarmName,
            alarmLevel,
            g_alarm_scope,
            additionalParam->hostName,
            (strlen(additionalParam->instanceName) != 0) ? additionalParam->instanceName : additionalParam->clusterName,
            "firing",
            additionalParam->additionInfo,
            "ADAC",
            alarmItem->startTimeStamp,
            0);
    } else if (type == ALM_AT_Resume) {
        rcs = snprintf_s(reportInfo,
            REPORT_MSG_SIZE,
            REPORT_MSG_SIZE - 1,
            "{" SYSQUOTE "id" SYSQUOTE SYSCOLON SYSQUOTE "%016ld" SYSQUOTE SYSCOMMA SYSQUOTE
            "name" SYSQUOTE SYSCOLON SYSQUOTE "%s" SYSQUOTE SYSCOMMA SYSQUOTE "level" SYSQUOTE SYSCOLON SYSQUOTE
            "%s" SYSQUOTE SYSCOMMA SYSQUOTE "scope" SYSQUOTE SYSCOLON "%s" SYSCOMMA SYSQUOTE
            "source_tag" SYSQUOTE SYSCOLON SYSQUOTE "%s-%s" SYSQUOTE SYSCOMMA SYSQUOTE
            "op_type" SYSQUOTE SYSCOLON SYSQUOTE "%s" SYSQUOTE SYSCOMMA SYSQUOTE "start_timestamp" SYSQUOTE SYSCOLON
            "%d" SYSCOMMA SYSQUOTE "end_timestamp" SYSQUOTE SYSCOLON "%ld"
            "}\n",
            alarmItem->id,
            alarmName,
            alarmLevel,
            g_alarm_scope,
            additionalParam->hostName,
            (strlen(additionalParam->instanceName) != 0) ? additionalParam->instanceName : additionalParam->clusterName,
            "resolved",
            0,
            alarmItem->endTimeStamp);
    }
    securec_check_ss_c(rcs, "\0", "\0");
    write_log_file(reportInfo);
}
