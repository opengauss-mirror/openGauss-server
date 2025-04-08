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
 * -------------------------------------------------------------------------
 *
 * alarmchecker.cpp
 *
 * 	openGauss Alarm checker thread Implementation
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/alarmchecker.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "alarm/alarm.h"
#include "alarm/alarm_log.h"
#include "utils/elog.h"
#include "pgxc/pgxc.h"
#include "postmaster/alarmchecker.h"
#include "gssignal/gs_signal.h"
#include "replication/walsender.h"

// declare the global variable of alarm module
int g_alarmReportInterval;
char g_alarmComponentPath[MAXPGPATH];
int g_alarmReportMaxCount;
/* seconds, interval of alarm check loop. */
static const int AlarmCheckInterval = 1;

bool enable_alarm = false;

static Alarm* DataInstAlarmList = NULL;

static int g_dataInstAlarmListSize = 7;

char sys_log_path[MAXPGPATH] = {0};

AlarmCheckResult DataOrRedoDirNotExistChecker(Alarm* alarm, AlarmAdditionalParam* additionalParam);
AlarmCheckResult XlogOverloadChecker(Alarm* alarm, AlarmAdditionalParam* additionalParam);

static void DataInstAlarmItemInitialize(void);

static void acSighupHandler(SIGNAL_ARGS);
static void acSigquitHandler(SIGNAL_ARGS);

extern AlarmCheckResult DataInstArchChecker(Alarm* alarm, AlarmAdditionalParam* additionalParam);
extern AlarmCheckResult ConnAuthMethodChecker(Alarm* alarm, AlarmAdditionalParam* additionalParam);
extern AlarmCheckResult DataInstConnToGTMChecker(Alarm* alarm, AlarmAdditionalParam* additionalParam);

void DataInstAlarmItemInitialize(void)
{
    DataInstAlarmList = (Alarm*)AlarmAlloc(sizeof(Alarm) * g_dataInstAlarmListSize);
    if (NULL == DataInstAlarmList) {
        AlarmLog(ALM_LOG, "Out of memory: DataInstAlarmItemInitialize failed.");
        exit(1);
    }
    int i = 0;
    // ALM_AI_MissingDataInstDataOrRedoDir
    AlarmItemInitialize(
        &(DataInstAlarmList[i++]), ALM_AI_MissingDataInstDataOrRedoDir, ALM_AS_Normal, DataOrRedoDirNotExistChecker);
    // ALM_AI_MissingDataInstWalSegmt
    AlarmItemInitialize(
        &(DataInstAlarmList[i++]), ALM_AI_MissingDataInstWalSegmt, ALM_AS_Normal, WalSegmentsRemovedChecker);
    // ALM_AI_TooManyDataInstConn
    AlarmItemInitialize(
        &(DataInstAlarmList[i++]), ALM_AI_TooManyDataInstConn, ALM_AS_Normal, ConnectionOverloadChecker);
    // ALM_AI_AbnormalDataInstArch
    AlarmItemInitialize(&(DataInstAlarmList[i++]), ALM_AI_AbnormalDataInstArch, ALM_AS_Normal, DataInstArchChecker);
    // ALM_AI_AbnormalDataInstConnAuthMethod
    AlarmItemInitialize(
        &(DataInstAlarmList[i++]), ALM_AI_AbnormalDataInstConnAuthMethod, ALM_AS_Normal, ConnAuthMethodChecker);
    // ALM_AI_AbnormalDataInstConnToGTM
    AlarmItemInitialize(
        &(DataInstAlarmList[i++]), ALM_AI_AbnormalDataInstConnToGTM, ALM_AS_Normal, DataInstConnToGTMChecker);
    // ALM_AI_XlogAccumulate
    AlarmItemInitialize(
        &(DataInstAlarmList[i++]), ALM_AI_XlogAccumulate, ALM_AS_Normal, XlogOverloadChecker);
}

void PrepareAlarmEnvironment()
{
    errno_t rc = 0;
    rc = memcpy_s(g_alarmComponentPath, MAXPGPATH - 1, Alarm_component, strlen(Alarm_component));
    securec_check(rc, "\0", "\0");
    g_alarmReportInterval = AlarmReportInterval;
    AlarmEnvInitialize(u_sess->attr.attr_common.log_hostname);
    /* assign gs_log to sys_log_path, use it as alarm directory */
    bool isAbsolute = is_absolute_path(u_sess->attr.attr_common.Log_directory);
    if (isAbsolute) {
        rc = strncpy_s(sys_log_path, MAXPGPATH, u_sess->attr.attr_common.Log_directory,
            strlen(u_sess->attr.attr_common.Log_directory));
        securec_check(rc, "\0", "\0");
    } else {
        rc = snprintf_s(sys_log_path, MAXPGPATH, MAXPGPATH - 1, "%s/gs_log",
                        g_instance.attr.attr_common.data_directory);
        securec_check_ss(rc, "\0", "\0");
    }
    create_system_alarm_log(sys_log_path);
}

ThreadId startAlarmChecker(void)
{
    if (!IsPostmasterEnvironment || !enable_alarm) {
        return 0;
    }

    return initialize_util_thread(ALARMCHECK);
}

NON_EXEC_STATIC void AlarmCheckerMain()
{

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* reord my name */
    t_thrd.proc_cxt.MyProgName = "AlarmChecker";

    /* Identify myself via ps */
    init_ps_display("AlarmChecker", "", "", "");

    AlarmLog(ALM_LOG, "alarm checker started.");

    InitializeLatchSupport(); /* needed for latch waits */

    /* Initialize private latch for use by signal handlers */
    InitLatch(&t_thrd.alarm_cxt.AlarmCheckerLatch);

    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * Note: we deliberately ignore SIGTERM, because during a standard Unix
     * system shutdown cycle, init will SIGTERM all processes at once.	We
     * want to wait for the backends to exit, whereupon the postmaster will
     * tell us it's okay to shut down (via SIGUSR2).
     */
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGHUP, acSighupHandler); /* set flag to read config file */
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, SIG_IGN);
    (void)gspqsignal(SIGQUIT, acSigquitHandler);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /* all is done info top memory context. */
    (void)MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));

    DataInstAlarmItemInitialize();

    for (;;) {
        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.alarm_cxt.AlarmCheckerLatch);

        /* the normal shutdown case */
        if (t_thrd.alarm_cxt.gotSigdie)
            break;

        /*
         * reload the postgresql.conf
         */
        if (t_thrd.alarm_cxt.gotSighup) {
            t_thrd.alarm_cxt.gotSighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        AlarmCheckerLoop(DataInstAlarmList, g_dataInstAlarmListSize);

        /*
         * Sleep until there's something to do
         */
        (void)WaitLatch(&t_thrd.alarm_cxt.AlarmCheckerLatch, WL_LATCH_SET | WL_TIMEOUT, AlarmCheckInterval * 1000);
    }

    AlarmLog(ALM_LOG, "alarm checker shutting down...");

    proc_exit(0);
}

/*
 * signal handle functions
 */
/*
 * @@GaussDB@@
 * Brief		: handle SIGHUP signal and set t_thrd.alarm_cxt.gotSighup flag
 * Description	:
 * Notes		:
 */
static void acSighupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.alarm_cxt.gotSighup = true;

    SetLatch(&t_thrd.alarm_cxt.AlarmCheckerLatch);

    errno = save_errno;
}

/*
 * @@GaussDB@@
 * Brief		: handle SIGTERM, SIGINT signal and set t_thrd.alarm_cxt.gotSigdie flag
 * Description	:
 * Notes		:
 */
static void acSigquitHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.alarm_cxt.gotSigdie = true;

    SetLatch(&t_thrd.alarm_cxt.AlarmCheckerLatch);

    errno = save_errno;
}

bool isDirExist(const char* dir)
{
    struct stat stat_buf;

    if (stat(dir, &stat_buf) != 0)
        return false;

    if (!S_ISDIR(stat_buf.st_mode))
        return false;

#if !defined(WIN32) && !defined(__CYGWIN__)

    if (stat_buf.st_uid != geteuid())
        return false;

    if ((stat_buf.st_mode & S_IRWXU) != S_IRWXU)
        return false;

#endif

    return true;
}

AlarmCheckResult XlogOverloadChecker(Alarm* alarm, AlarmAdditionalParam* additionalParam)
{
    DIR* dir;
    int ret;
    struct dirent* entry;
    struct stat statbuf;
    int total = 0;
    if ((dir = opendir(g_instance.datadir_cxt.xlogDir)) == NULL) {
        // not overload
        return ALM_ACR_Normal;
    }
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        char full_path[MAX_PATH_LEN] = {0};
        ret = snprintf_s(full_path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s",
                         g_instance.datadir_cxt.xlogDir, entry->d_name);
        securec_check_ss_c(ret, "\0", "\0");
        if (lstat(full_path, &statbuf) == -1) {
            continue;
        }
        if (S_ISREG(statbuf.st_mode)) {
            total++;
        }
    }
    closedir(dir);
    int xlogOverloadThreshold
        = 2 * (u_sess->attr.attr_storage.wal_keep_segments + 2 * u_sess->attr.attr_storage.CheckPointSegments + 1);
    if (total > xlogOverloadThreshold) {
        // fill the alarm message
        WriteAlarmAdditionalInfo(additionalParam,
                                 g_instance.attr.attr_common.PGXCNodeName,
                                 "",
                                 "",
                                 alarm,
                                 ALM_AT_Fault,
                                 g_instance.attr.attr_common.PGXCNodeName);
        return ALM_ACR_Abnormal;
    } else {
        // fill the alarm message
        WriteAlarmAdditionalInfo(additionalParam,
                                 g_instance.attr.attr_common.PGXCNodeName,
                                 "",
                                 "",
                                 alarm,
                                 ALM_AT_Resume,
                                 g_instance.attr.attr_common.PGXCNodeName);
        return ALM_ACR_Normal;
    }
}

AlarmCheckResult DataOrRedoDirNotExistChecker(Alarm* alarm, AlarmAdditionalParam* additionalParam)
{
    if (isDirExist(t_thrd.proc_cxt.DataDir) && isDirExist("pg_xlog")) {
        // fill the alarm message
        WriteAlarmAdditionalInfo(additionalParam,
            g_instance.attr.attr_common.PGXCNodeName,
            "",
            "",
            alarm,
            ALM_AT_Resume,
            g_instance.attr.attr_common.PGXCNodeName);
        return ALM_ACR_Normal;
    } else {
        // fill the alarm message
        WriteAlarmAdditionalInfo(additionalParam,
            g_instance.attr.attr_common.PGXCNodeName,
            "",
            "",
            alarm,
            ALM_AT_Fault,
            g_instance.attr.attr_common.PGXCNodeName);
        return ALM_ACR_Abnormal;
    }
}

/* implementation of alarm module. */
void AlarmFree(void* pointer)
{
    if (pointer != NULL)
        pfree(pointer);
}

void* AlarmAlloc(size_t size)
{
    return palloc(size);
}

void AlarmLogImplementation(int level, const char* prefix, const char* logtext)
{
    switch (level) {
        case ALM_DEBUG:
            ereport(DEBUG3, (errmsg("%s%s", prefix, logtext)));
            break;
        case ALM_LOG:
            ereport(LOG, (errmsg("%s%s", prefix, logtext)));
            break;
        default:
            break;
    }
}
