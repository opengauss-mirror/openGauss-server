/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * ss_init.cpp
 *  initialize for DMS shared storage.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_init.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "utils/builtins.h"
#include <string>
#include <sys/sysinfo.h>
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "utils/elog.h"
#include "knl/knl_instance.h"
#include "securec.h"
#include "nodes/pg_list.h"
#include "storage/buf/bufmgr.h"
#include "ddes/dms/ss_init.h"
#include "ddes/dms/ss_dms_callback.h"
#include "ddes/dms/ss_dms.h"
#include "ddes/dms/ss_reform_common.h"
#include "postmaster/postmaster.h"
#include "ddes/dms/ss_dms_auxiliary.h"

#define IS_NULL_STR(str) ((str) == NULL || (str)[0] == '\0')

#define FIXED_NUM_OF_INST_IP_PORT 3
#define BYTES_PER_KB 1024
#define NON_PROC_NUM 4

const int MAX_CPU_STR_LEN = 5;
const int DEFAULT_DIGIT_RADIX = 10;
static void scanURL(dms_profile_t* profile, char* ipportstr, int index)
{
    List* l = NULL;
    /* syntax: inst_id:ip:port */
    if (!SplitIdentifierString(ipportstr, ':', &l) || list_length(l) != FIXED_NUM_OF_INST_IP_PORT) {
        ereport(FATAL,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid ip:port syntax %s", ipportstr)));
    }

    errno_t ret;
    char* ipstr = (char*)lsecond(l);
    char* portstr = (char*)lthird(l);
    ret = strncpy_s(profile->inst_net_addr[index].ip, DMS_MAX_IP_LEN, ipstr, strlen(ipstr));
    if (ret != EOK) {
        ereport(FATAL,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid ip string: %s", ipstr)));
    }
    profile->inst_net_addr[index].port = (uint16)pg_strtoint32(portstr);
    profile->inst_net_addr[index].inst_id = index;
    profile->inst_net_addr[index].need_connect = true;

    ret = strcpy_s(g_instance.dms_cxt.dmsInstAddr[index], DMS_MAX_IP_LEN, ipstr);
    securec_check(ret, "", "");

    profile->inst_map |= ((uint64)1 << index);

    return;
}

static void scanURLList(dms_profile_t* profile, List* l)
{
    char* ipport = NULL;
    ListCell* cell = NULL;
    int i = 0;

    foreach(cell, l) {
        ipport = (char*)lfirst(cell);
        scanURL(profile, ipport, i++);
    }
    profile->inst_cnt = (unsigned int)i;
}

static void parseInternalURL(dms_profile_t *profile)
{
    List* l = NULL;
    char* rawstring = g_instance.attr.attr_storage.dms_attr.interconnect_url;
    char* copystring = pstrdup(rawstring);
    /* syntax: inst_id0:ip0:port0, inst_id1:ip1:port1, ... */
    if (!SplitIdentifierString(copystring, ',', &l)) {
        ereport(FATAL,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid list syntax for \"ss_interconnect_url\"")));
    }

    if (list_length(l) == 0 || list_length(l) > DMS_MAX_INSTANCE) {
        ereport(FATAL,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("too many ip:port list for \"ss_interconnect_url\"")));
    }

    scanURLList(profile, l);
}

static inline dms_conn_mode_t convertInterconnectType()
{
    if (!strcasecmp(g_instance.attr.attr_storage.dms_attr.interconnect_type, "TCP")) {
        return DMS_CONN_MODE_TCP;
    } else {
        return DMS_CONN_MODE_RDMA;
    }
}

static void SetDmsParam(const char *dmsParamName, const char *dmsParamValue)
{
    if (dms_set_ssl_param(dmsParamName, dmsParamValue) != 0)
        ereport(WARNING, (errmsg("Failed to set DMS %s: %s.", dmsParamName, dmsParamValue)));
}

static void InitDmsSSL()
{
    char *parentdir = NULL;
    KeyMode keymode = SERVER_MODE;
    if (is_absolute_path(g_instance.attr.attr_security.ssl_key_file)) {
        parentdir = pstrdup(g_instance.attr.attr_security.ssl_key_file);
        get_parent_directory(parentdir);
        decode_cipher_files(keymode, NULL, parentdir, u_sess->libpq_cxt.server_key);
    } else {
        decode_cipher_files(keymode, NULL, t_thrd.proc_cxt.DataDir, u_sess->libpq_cxt.server_key);
        parentdir = pstrdup(t_thrd.proc_cxt.DataDir);
    }
    pfree_ext(parentdir);

    /* never give a change to log it */
    dms_set_ssl_param("SSL_PWD_PLAINTEXT", reinterpret_cast<char *>(u_sess->libpq_cxt.server_key));
    /* clear the sensitive info in server_key */
    errno_t errorno = EOK;
    errorno = memset_s(u_sess->libpq_cxt.server_key, CIPHER_LEN + 1, 0, CIPHER_LEN + 1);
    securec_check(errorno, "\0", "\0");

    char ssl_file_path[PATH_MAX + 1] = {0};
    if (NULL != realpath(g_instance.attr.attr_security.ssl_ca_file, ssl_file_path)) {
        SetDmsParam("SSL_CA", ssl_file_path);
    }

    errorno = memset_s(ssl_file_path, PATH_MAX + 1, 0, PATH_MAX + 1);
    securec_check(errorno, "\0", "\0");

    if (NULL != realpath(g_instance.attr.attr_security.ssl_key_file, ssl_file_path)) {
        SetDmsParam("SSL_KEY", ssl_file_path);
    }

    errorno = memset_s(ssl_file_path, PATH_MAX + 1, 0, PATH_MAX + 1);
    securec_check(errorno, "\0", "\0");

    if (NULL != realpath(g_instance.attr.attr_security.ssl_crl_file, ssl_file_path)) {
        SetDmsParam("SSL_CRL", ssl_file_path);
    }

    errorno = memset_s(ssl_file_path, PATH_MAX + 1, 0, PATH_MAX + 1);
    securec_check(errorno, "\0", "\0");

    if (NULL != realpath(g_instance.attr.attr_security.ssl_cert_file, ssl_file_path)) {
        SetDmsParam("SSL_CERT", ssl_file_path);
    }

    /* to limit line width */
    int dms_guc_param = u_sess->attr.attr_security.ssl_cert_notify_time;
    SetDmsParam("SSL_CERT_NOTIFY_TIME", std::to_string(dms_guc_param).c_str());
}

static void splitDigitNumber(char *str, char *output, uint32 outputLen, uint32* len)
{
    if (str == NULL) {
        output[0] = '\0';
        return;
    }

    uint32_t start = 0;
    while (*str != '\0' && *str == ' ') {
        ++start;
        ++str;
    }

    if (*str == '\0') {
        output[0] = '\0';
        return;
    }

    char* startPtr = str;
    uint32 idx = 0;
    while (*str != '\0' && *str != ' ') {
        if (*str >= '0' && *str <= '9') {
            ++str;
            ++idx;
        } else {
            output[0] = '\0';
            return;
        }
    }

    // if the count of digit number is larger than outputLen, it is out of range
    if (idx >= outputLen) {
        output[0] = '\0';
        return;
    }
    int ret = strncpy_s(output, outputLen, startPtr, idx);
    securec_check_c(ret, "\0", "\0");
    *len = start + idx;
}

bool is_err(char *err)
{
    if (err == NULL) {
        return false;
    }

    while (*err != '\0') {
        if (*err != ' ') {
            return true;
        }
        err++;
    }

    return false;
}

static bool setBindCoreConfig(char *config, unsigned char *startCore, unsigned char *endCore) {
    char lowStr[MAX_CPU_STR_LEN] = {0};
    char highStr[MAX_CPU_STR_LEN] = {0};
    uint32_t offset = 0;

    if (config == NULL || config[0] == '\0') {
        return false;
    }

    // if number >= MAX_CPU_STR_LEN, it exceeded the number of CPUs.
    splitDigitNumber(config, lowStr, MAX_CPU_STR_LEN, &offset);
    splitDigitNumber(config + offset, highStr, MAX_CPU_STR_LEN, &offset);
    if (lowStr[0] != '\0' && highStr[0] != '\0') {
        // if number of decimal digits is less than DEFAULT_DIGIT_RADIX(5), The number range must be within Int64.
        char *err = NULL;
        int64 lowCpu = strtoll(lowStr, &err, DEFAULT_DIGIT_RADIX);
        int64 highCpu = strtoll(highStr, &err, DEFAULT_DIGIT_RADIX);
        if (lowCpu > highCpu) {
            return false;
        }

        // get cpu count
        int64 cpuCount = get_nprocs_conf();
        if (lowCpu >= cpuCount || highCpu >= cpuCount) {
            return false;
        }

        *startCore = (uint8)lowCpu;
        *endCore = (uint8)highCpu;
        return true;
    }

    return false;
}

static void setRdmaWorkConfig(dms_profile_t *profile)
{
    knl_instance_attr_dms *dms_attr = &g_instance.attr.attr_storage.dms_attr;
    profile->rdma_rpc_use_busypoll = false;
    profile->rdma_rpc_is_bind_core = false;
    if (dms_attr->rdma_work_config == NULL || dms_attr->rdma_work_config[0] == '\0') {
        return;
    }

    if (setBindCoreConfig(dms_attr->rdma_work_config, &profile->rdma_rpc_bind_core_start,
        &profile->rdma_rpc_bind_core_end)) {
        profile->rdma_rpc_use_busypoll = true;
        profile->rdma_rpc_is_bind_core = true;
    }
}

static void setScrlConfig(dms_profile_t *profile)
{
    knl_instance_attr_dms *dms_attr = &g_instance.attr.attr_storage.dms_attr;

    profile->enable_scrlock = dms_attr->enable_scrlock;
    if (profile->enable_scrlock == false) {
        return;
    }

    profile->primary_inst_id = g_instance.dms_cxt.SSReformerControl.primaryInstId;
    profile->enable_ssl = dms_attr->enable_ssl;
    profile->enable_scrlock_server_sleep_mode = dms_attr->enable_scrlock_sleep_mode;
    profile->scrlock_worker_cnt = dms_attr->scrlock_worker_count;
    profile->scrlock_server_port = dms_attr->scrlock_server_port;
    profile->scrlock_log_level = u_sess->attr.attr_common.log_min_messages;

    // server bind
    (void)setBindCoreConfig(dms_attr->scrlock_server_bind_core_config, &profile->scrlock_server_bind_core_start,
        &profile->scrlock_server_bind_core_end);

    // worker bind
    if (setBindCoreConfig(dms_attr->scrlock_worker_bind_core_config, &profile->scrlock_worker_bind_core_start,
        &profile->scrlock_worker_bind_core_end)) {
        profile->enable_scrlock_worker_bind_core = true;
    }
}

static void SetOckLogPath(knl_instance_attr_dms* dms_attr, char *ock_log_path)
{
    int ret = memset_s(ock_log_path, DMS_OCK_LOG_PATH_LEN, 0, DMS_OCK_LOG_PATH_LEN);
    securec_check_c(ret, "\0", "\0");
    int len = strlen(dms_attr->ock_log_path);
    char realPath[PATH_MAX + 1] = {0};
    if (len == 0) {
        char* loghome = gs_getenv_r("GAUSSLOG");
        if (loghome && '\0' != loghome[0]) {
            check_backend_env(loghome);
            if (realpath(loghome, realPath) == NULL) {
                ereport(FATAL, (errmsg("failed to realpath $GAUSSLOG/pg_log")));
                ock_log_path[0] = '.';
                return;
            }
            ret = snprintf_s(ock_log_path, DMS_OCK_LOG_PATH_LEN, DMS_OCK_LOG_PATH_LEN - 1, "%s/pg_log", realPath);
            securec_check_ss(ret, "", "");
            // ock_log_path not exist, create ock_log_path path
            if (0 != pg_mkdir_p(ock_log_path, S_IRWXU) && errno != EEXIST) {
                ereport(FATAL, (errmsg("failed to mkdir $GAUSSLOG/pg_log")));
                return;
            }
            return;
        } else {
            ock_log_path[0] = '.';
        }
    } else {
        check_backend_env(dms_attr->ock_log_path);
        if (realpath(dms_attr->ock_log_path, realPath) == NULL) {
            ereport(FATAL,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Failed to realpath config param ss_ock_log_path")));
            ock_log_path[0] = '.';
            return;
        }
        ret = strncpy_s(ock_log_path, DMS_OCK_LOG_PATH_LEN, realPath, strlen(realPath));
        securec_check_c(ret, "\0", "\0");

        // ock_log_path not exist, create ock_log_path path
        if (0 != pg_mkdir_p(ock_log_path, S_IRWXU) && errno != EEXIST) {
            ereport(FATAL, (errmsg("failed to mkdir ss_ock_log_path")));
            return;
        }
    }
}

static void SetWorkThreadpoolConfig(dms_profile_t *profile) 
{    
    char* attr = TrimStr(g_instance.attr.attr_storage.dms_attr.work_thread_pool_attr);
    if (IS_NULL_STR(attr)) {
        profile->enable_mes_task_threadpool = false;
        profile->mes_task_worker_max_cnt = 0;
        return;
    }

    char* replStr = NULL;
    replStr = pstrdup(attr);
    profile->mes_task_worker_max_cnt = (unsigned int)pg_strtoint32(replStr);
    profile->enable_mes_task_threadpool = true;
}

static void setDMSProfile(dms_profile_t* profile)
{
    knl_instance_attr_dms* dms_attr = &g_instance.attr.attr_storage.dms_attr;
    profile->resource_catalog_centralized = (unsigned int)dms_attr->enable_catalog_centralized;
    profile->inst_id = (uint32)dms_attr->instance_id;
    profile->page_size = BLCKSZ;
    profile->data_buffer_size = (unsigned long long)((int64)TOTAL_BUFFER_NUM * BLCKSZ);
    profile->recv_msg_buf_size = (unsigned long long)((int64)dms_attr->recv_msg_pool_size * BYTES_PER_KB);
    profile->channel_cnt = (uint32)dms_attr->channel_count;
    profile->work_thread_cnt = (uint32)dms_attr->work_thread_count;
    profile->max_session_cnt = DMS_MAX_SESSIONS;
    profile->time_stat_enabled = TRUE;
    profile->pipe_type = convertInterconnectType();
    profile->conn_created_during_init = TRUE;
    setRdmaWorkConfig(profile);
    setScrlConfig(profile);
    SetOckLogPath(dms_attr, profile->ock_log_path);
    profile->inst_map = 0;
    profile->enable_reform = (unsigned char)dms_attr->enable_reform;
    profile->parallel_thread_num = dms_attr->parallel_thread_num;
    profile->max_wait_time = DMS_MSG_MAX_WAIT_TIME;

    if (dms_attr->enable_ssl && g_instance.attr.attr_security.EnableSSL) {
        InitDmsSSL();
    }
    parseInternalURL(profile);
    SetWorkThreadpoolConfig(profile);

    /* some callback initialize */
    DmsInitCallback(&profile->callback);
}

static inline void DMSDfxStatReset(){
    g_instance.dms_cxt.SSDFxStats.txnstatus_varcache_gets = 0;
    g_instance.dms_cxt.SSDFxStats.txnstatus_hashcache_gets = 0;
    g_instance.dms_cxt.SSDFxStats.txnstatus_network_io_gets = 0;
    g_instance.dms_cxt.SSDFxStats.txnstatus_total_hcgets_time = 0;
    g_instance.dms_cxt.SSDFxStats.txnstatus_total_niogets_time = 0;
    g_instance.dms_cxt.SSDFxStats.txnstatus_total_evictions = 0;
    g_instance.dms_cxt.SSDFxStats.txnstatus_total_eviction_refcnt = 0;
}

void DMSInit()
{
    if (ss_dms_func_init() != DMS_SUCCESS) {
        ereport(FATAL, (errmsg("failed to init dms library")));
    }
    if (dms_register_thread_init(DmsCallbackThreadShmemInit)) {
        ereport(FATAL, (errmsg("failed to register dms memcxt callback!")));
    }
    if (dms_register_thread_deinit(DmsThreadDeinit)) {
        ereport(FATAL, (errmsg("failed to register DmsThreadDeinit!")));
    }

    uint32 TotalProcs = (uint32)(GLOBAL_ALL_PROCS);
    uint32 MesMaxRooms = dms_get_mes_max_watting_rooms();
    if (TotalProcs + NON_PROC_NUM >= MesMaxRooms) {
        ereport(FATAL, (errmsg("The thread ID range is too large when dms enable. Please set the related GUC "
                               "parameters to a smaller value.")));
    }

    dms_profile_t profile;
    errno_t rc = memset_s(&profile, sizeof(dms_profile_t), 0, sizeof(dms_profile_t));
    securec_check(rc, "\0", "\0");
    setDMSProfile(&profile);

    DMSInitLogger();
    DMSDfxStatReset();

    g_instance.dms_cxt.log_timezone = u_sess->attr.attr_common.log_timezone;

    if (dms_init(&profile) != DMS_SUCCESS) {
        int32 err;
        const char *msg = NULL;
        dms_get_error(&err, &msg);
        ereport(FATAL,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("failed to initialize dms, errno: %d, reason: %s", err, msg)));
    }
    g_instance.dms_cxt.dmsInited = true;

    for (int i = profile.inst_cnt; i < MAX_REPLNODE_NUM; i++) {
        rc = memset_s(g_instance.dms_cxt.dmsInstAddr[i], IP_LEN, '\0', IP_LEN);
        securec_check(rc, "", "");
    }
    rc = memset_s(g_instance.dms_cxt.conninfo, MAXCONNINFO, '\0', MAXCONNINFO);
    securec_check(rc, "", "");

#ifdef USE_ASSERT_CHECKING
    if (!ENABLE_REFORM && SS_NORMAL_STANDBY) {
        SSStandbySetLibpqswConninfo();
    }
#endif
}

void GetSSLogPath(char *sslog_path)
{
    int ret;
    char realPath[PATH_MAX + 1] = {0};
    char *log_home = gs_getenv_r("GAUSSLOG");
    if (log_home == NULL || log_home[0] == '\0') {
        log_home = t_thrd.proc_cxt.DataDir;
        if (log_home == NULL || log_home[0] == '\0') {
            ereport(FATAL, (errmsg("failed to get $GAUSSLOG or DataDir for log")));
        }
    }

    check_backend_env(log_home);
    if (realpath(log_home, realPath) == NULL) {
        ereport(FATAL, (errmsg("failed to realpaht $GAUSSLOG[DataDir]/pg_log")));
    }

    sslog_path[0] = '\0';
    ret = snprintf_s(sslog_path, DMS_LOG_PATH_LEN, DMS_LOG_PATH_LEN - 1, "%s/pg_log", realPath);
    securec_check_ss(ret, "", "");
    if (pg_mkdir_p(sslog_path, S_IRWXU) != 0 && errno !=EEXIST) {
        ereport(FATAL, (errmsg("failed to mkdir $GAUSSLOG[DataDir]/pg_log")));
    }
    return ;
}

void DMSInitLogger()
{
    if (ss_dms_func_init() != DMS_SUCCESS) {
        ereport(FATAL, (errmsg("failed to init dms library")));
    }

    knl_instance_attr_dms *dms_attr = &g_instance.attr.attr_storage.dms_attr;
    logger_param_t log_param;
    log_param.log_level = (unsigned int)(dms_attr->sslog_level);
    log_param.log_backup_file_count = (unsigned int)(dms_attr->sslog_backup_file_count);
    log_param.log_max_file_size = ((uint64)(dms_attr->sslog_max_file_size)) * 1024;
    GetSSLogPath(log_param.log_home);

    if (dms_init_logger(&log_param) != DMS_SUCCESS) {
        ereport(FATAL,(errmsg("failed to init dms logger")));
    }
}

void DMSRefreshLogger(char *log_field, unsigned long long *value)
{
    dms_refresh_logger(log_field, value);
}

void DMSUninit()
{
    if (!ENABLE_DMS || !g_instance.dms_cxt.dmsInited) {
        return;
    }

    if (g_instance.pid_cxt.DmsAuxiliaryPID != 0) {
        ereport(LOG, (errmsg("[SS] notify dms auxiliary thread exit")));
        signal_child(g_instance.pid_cxt.DmsAuxiliaryPID, SIGTERM, -1);
        SSWaitDmsAuxiliaryExit();
    }

    g_instance.dms_cxt.dmsInited = false;
    ereport(LOG, (errmsg("DMS uninit worker threads, DRC, errdesc and DL")));
    dms_uninit();
}

// order: DMS reform finish -> CBReformDoneNotify finish -> startup exit (if has)
int32 DMSWaitReform()
{
    uint32 has_offline; /* currently not used in openGauss */
    return dms_wait_reform(&has_offline);
}

static bool DMSReformCheckStartup()
{
    if (g_instance.dms_cxt.SSRecoveryInfo.ready_to_startup) {
        g_instance.pid_cxt.StartupPID = initialize_util_thread(STARTUP);
        Assert(g_instance.pid_cxt.StartupPID != 0);
        pmState = PM_STARTUP;
        g_instance.dms_cxt.SSRecoveryInfo.ready_to_startup = false;
        return true;
    }

    if (g_instance.dms_cxt.SSRecoveryInfo.restart_failover_flag) {
        SSRestartFailoverPromote();
        return true;
    }
    return false;
}

bool DMSWaitInitStartup()
{
    ereport(LOG, (errmsg("[SS reform] Node:%d first-round reform wait to initialize startup thread.", SS_MY_INST_ID)));
    g_instance.dms_cxt.dms_status = (dms_status_t)DMS_STATUS_JOIN;

    while (g_instance.pid_cxt.StartupPID == 0) {
        (void)DMSReformCheckStartup();
        if (dms_reform_last_failed()) {
            return false;
        }
        pg_usleep(REFORM_WAIT_TIME);
    }

    if (g_instance.pid_cxt.StartupPID != 0) {
        ereport(LOG, (errmsg("[SS reform] Node:%d initialize startup thread success.", SS_MY_INST_ID)));
    }

    return true;
}

void StartupWaitReform()
{
    while (g_instance.dms_cxt.SSReformInfo.in_reform) {
        if (dms_reform_failed() || dms_reform_last_failed()) {
            if (g_instance.dms_cxt.SSReformInfo.in_reform) {
                ereport(LOG, (errmsg("[SS reform] reform failed, startup no need wait.")));
                break;
            }
        }
        pg_usleep(5000L);
    }
}

