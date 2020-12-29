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
 * kt_log_manage.h
 *     Log manage module : 
 *     1. reading and writing log file;
 *     2. providing log management interface. 
 *
 * IDENTIFICATION
 *      src/bin/gs_ktool/kt_log_manage.h
 *
 * -------------------------------------------------------------------------
 */

#include "kt_log_manage.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include "kt_common.h"

bool g_has_init_log_module = false;
static const char log_manage_conf[][MAX_CONF_KEY_LEN] = {"is_logmodule_open", "log_file"};

/* the log format recorded in log file : date time operation result [object] [level] information */
static const KtLog g_log_type[] = {
    /* log type   operation    object   level */
    {L_CREATE_FILE, "create file", "system", "high"},
    {L_INIT_KMC, "initlize kmc", "system", "high"},
    {L_OPEN_FILE, "open file", "system", "high"},

    {L_GEN_CMK, "generate cmk", "cmk", "high"},
    {L_DEL_CMK, "delete cmk", "cmk", "high"},
    {L_SELECT_CMK, "select cmk", "cmk", "high"},
    {L_EXPORT_CMK, "export cmk", "cmk", "high"},
    {L_IMPORT_CMK, "import cmk", "cmk", "high"},
    {L_SELECT_CMK_LEN, "select cmk len", "cmk", "high"},
    {L_SELECT_CMK_PLAIN, "select cmk plain", "cmk", "high"},

    {L_SELECT_RK, "select rk", "rk", "high"},
    {L_UPDATE_RK, "update rk", "rk", "high"},
    {L_SET_RK_VALIDITY, "set rk validity", "rk", "high"},
    {L_CHECK_RK_VALIDITY, "check rk validity", "rk", "high"},

    {L_PARSE_USER_INPUT, "parse user input", "user input", "low"},
    {L_CHECK_USER_INPUT, "check user input", "user input", "low"},

    {L_READ_CONF, "read configuration", "configuration", "low"},
    {L_SET_CONF, "set configuration", "configuration", "low"},

    {L_REG_CALLBACK_FUNC, "register callback function", "system", "middle"},
    {L_MALLOC_MEMORY, "malloc memory", "system", "middle"},
    {L_GET_TIME, "get system time", "system", "middle"},
    {L_GET_ENV_VALUE, "get environment value", "system", "middle"},
    {L_CHECK_ENV_VALUE, "check environment value", "system", "middle"},
    {L_OPEN_SEM, "open posix semaphore", "system", "middle"},
    {L_INIT_SEM, "init posix semaphore", "system", "middle"},
    {L_POST_SEM, "post posix semaphore", "system", "middle"},
    {L_WAIT_SEM, "wait posix semaphore", "system", "middle"},
};

/*
 * initialize log module.
 * including read configure file, check or create log file.
 */
static bool set_global_env_value();
static bool load_conf_file(void);
static bool load_log_manage_conf();
static bool set_global_log_conf(LogManageConf log_conf, const char *conf_key, const char *conf_value);

static bool has_exist_logfile();
static bool create_logfile();

static void get_log_by_logtype(const KtLogType log_type, KtLog *log);
static char* log_class_enum_to_str(LogClass log_class);

static bool set_global_env_value()
{
    char *gs_kt_file_path = NULL;
    errno_t rc = 0;
    /*
     * this env path is not used alone, but used like : GS_KTOOL_FILE_PATH/filename
     * so it is no need to check the path here, as well as the GAUSSHOME
     */
    gs_kt_file_path = gs_getenv_r("GS_KTOOL_FILE_PATH");
    if (gs_kt_file_path == NULL || realpath(gs_kt_file_path, g_env_value) == NULL) {
        /* try to get GAUSSHOME */
        gs_kt_file_path = gs_getenv_r("GAUSSHOME");
        /* fail to get GAUSSHOME and GS_KTOOL_FILE_PATH */
        if (gs_kt_file_path == NULL || realpath(gs_kt_file_path, g_env_value) == NULL) {
            printf("ERROR: failed to get environment value : %s.\n", "GS_KTOOL_FILE_PATH");
            return false;
        }

        /* fail to get GS_KTOOL_FILE_PATH but get GAUSSHOME */
        rc = sprintf_s(g_env_value, MAX_REAL_PATH_LEN, "%s/%s", gs_kt_file_path, "/etc/gs_ktool_file");
        securec_check_ss_c(rc, "", "");
    }

    if (!check_kt_env_value(gs_kt_file_path)) {
        printf("ERROR: failed to check environment value, '$GS_KTOOL_FILE_PATH' contains dangerous character.\n");
        return false;
    }

    return true;
}

static bool load_conf_file(void)
{
    FILE *fp = NULL;
    errno_t rc = 0;

    if (g_env_value == NULL || g_conf_file == NULL) {
        return false;
    }

    rc = sprintf_s(g_conf_file, MAX_REAL_PATH_LEN, "%s/%s", g_env_value, CONF_FILE_NAME);
    securec_check_ss_c(rc, "", "");

    if (!check_kt_env_value(g_conf_file)) {
        printf("ERROR: environment value '%s' contains dangerous character.\n", g_conf_file);
        return false;
    }

    fp = fopen(g_conf_file, "rb");
    if (fp == NULL) {
        insert_format_log(L_OPEN_FILE, KT_ERROR, 
            "please make sure the configuration file '$GS_KTOOL_FILE_PATH/gs_ktool_conf.ini' exists");
        return false;
    }
    fclose(fp);

    return true;
}

static bool load_log_manage_conf()
{
    const char required_conf[][MAX_CONF_KEY_LEN] = {"is_logmodule_open", "log_file"};
    size_t required_conf_cnt = sizeof(required_conf) / sizeof(required_conf[0]);
    
    char key_list[MAX_CONF_ITEM_QUANTITY][MAX_CONF_KEY_LEN] = {0};
    char value_list[MAX_CONF_ITEM_QUANTITY][MAX_CONF_VALUE_LEN] = {0};
    size_t lost_conf_pos = 0;
    size_t conf_item_cnt = 0;
    bool is_set_log_conf = true;

    if (!read_conf_segment(g_conf_file, "LogManage", key_list, value_list, &conf_item_cnt)) {
        insert_format_log(L_READ_CONF, KT_ERROR, "cannot find configuration segment : %s", "LogManage");
        return false;
    }

    if (!compare_list_element(required_conf, required_conf_cnt, key_list, conf_item_cnt, &lost_conf_pos)) {
        insert_format_log(L_READ_CONF, KT_ERROR, "lost configuration item : %s", required_conf[lost_conf_pos]);
        return false;
    }

    /* check confs (because the amount of data is small, use the algorithm whose time complexity is O(n^2)) */ 
    for (size_t i = 0; i < conf_item_cnt; i++) {
        for (size_t j = 0; j < sizeof(log_manage_conf) / sizeof(log_manage_conf[0]); j++) {
            if (strcmp(key_list[i], log_manage_conf[j]) == 0) {
                switch (j) {
                    case IS_LOGMODULE_OPEN:
                        is_set_log_conf = set_global_log_conf(IS_LOGMODULE_OPEN, key_list[j], value_list[j]);
                        break;
                    case LOG_FILE:
                        is_set_log_conf = set_global_file(g_conf.log_file, G_LOG_FILE, value_list[j]);
                        break;
                    /* remain to add new log configuration */
                    default:
                        break;
                }

                if (!is_set_log_conf) {
                    return false;
                }
            }
        }
    }

    return true;
}

static bool set_global_log_conf(LogManageConf log_conf, const char *conf_key, const char *conf_value)
{
    switch (log_conf) {
        case IS_LOGMODULE_OPEN:
            if (strcmp(conf_value, "ON") == 0) {
                g_conf.is_logmodule_open = ON;
            } else if (strcmp(conf_value, "OFF") == 0) {
                g_conf.is_logmodule_open = OFF;
            } else {
                insert_format_log(L_SET_CONF, KT_ERROR, "the log configuration %s should be in one of {ON, OFF}", 
                    conf_value);
                return false;
            }
            break;
        /* remain to add new log configuration : such as LOG_VALIDITY, LOG_QUANTITY and etc. */
        default:
            break;
    }

    return true;
}

static bool has_exist_logfile()
{
    FILE *lfp = fopen(g_conf.log_file, "r");
    if (lfp == NULL) {
        return false;
    }

    fclose(lfp);
    return true;
}

static bool create_logfile()
{
    int logfile_handle = 0;
    char log_file_header[MAX_LOG_RESULT_LEN] = { 0 };
    errno_t rc = 0;

    logfile_handle = open(g_conf.log_file, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (logfile_handle == -1) {
        printf("ERROR: failed to create log file '%s'.\n", g_conf.log_file);
        return false;
    }

    /* create header */
    rc = sprintf_s(log_file_header, MAX_LOG_RESULT_LEN, "%-12s %-12s %-30s %-10s %-14s %-8s %s\n", "create_date", 
        "create_time", "operation", "result", "object", "level", "information");
    securec_check_ss_c(rc, "", "");

    write(logfile_handle, log_file_header, strlen(log_file_header));
    close(logfile_handle);

    g_has_init_log_module = true;

    printf("NOTICE: created new log file : %s.\n", g_conf.log_file);
    insert_format_log(L_CREATE_FILE, KT_NOTICE, "new log file : %s", g_conf.log_file);

    return true;
}

bool initialize_logmodule()
{ 
    if (!set_global_env_value()) {
        return false;
    };

    if (!load_conf_file()) {
        return false;
    }
    
    if (!load_log_manage_conf()) {
        return false;
    }

    if (g_conf.is_logmodule_open != ON) {
        return false;
    }

    if (!has_exist_logfile()) {
        if (!create_logfile()) {
            return false;
        }
    }

    g_has_init_log_module = true;
    return true;
}

static void get_log_by_logtype(const KtLogType log_type, KtLog *log) 
{
    for (size_t i = 0; i < sizeof(g_log_type) / sizeof(g_log_type[0]); i++) {
        if (g_log_type[i].log_type == log_type) {
            *log = g_log_type[i];
        }
    }
}

static char* log_class_enum_to_str(LogClass log_class) 
{
    char *log_class_str = NULL;
    const char all_log_class[][MAX_LOG_CLASS_STR_LEN] = {"error", "succeeded", "alarm", "warning", "notice", "hint"};
    error_t rc = 0;

    log_class_str = (char *) malloc(MAX_LOG_CLASS_STR_LEN);
    if (log_class_str == NULL) {
        printf("ERROR: failed to malloc memory.\n");
        exit(1);
    }

    rc = strcpy_s(log_class_str, MAX_LOG_CLASS_STR_LEN, all_log_class[log_class]);
    securec_check_c(rc, "", "");

    return log_class_str;
}

void insert_raw_log(const KtLogType log_type, const LogClass log_class, const char op_info[MAX_OP_RESULT_LEN])
{
    KtLog kt_log = {0};
    Time sys_time = {0};
    errno_t rc = 0;
    char create_date[strlen("0000-00-00/")] = {0};
    char create_time[strlen("00:00:00/")] = {0};
    char *result = NULL;
    FILE *lfp = NULL;
    char log_buf[MAX_LOG_RESULT_LEN] = {0};

    get_log_by_logtype(log_type, &kt_log);

    if (log_class == KT_ERROR && g_is_print_err) {
        if (strlen(op_info) > 0) {
            printf("ERROR: failed to %s, %s.\n", kt_log.operation, op_info);
        } else {
            printf("ERROR: failed to %s.\n", kt_log.operation);
        }
    }

    if (!g_has_init_log_module || g_conf.is_logmodule_open == OFF) {
        return;
    }

    if (!get_sys_time(&sys_time)) {
        printf("ERROR: failed to get system time.\n");
        return;
    }
    date_ttos(sys_time, create_date, sizeof(create_date));
    time_ttos(sys_time, create_time, sizeof(create_time));

    result = log_class_enum_to_str(log_class);
    rc = sprintf_s(log_buf, MAX_LOG_RESULT_LEN, "%-12s %-12s %-30s %-10s %-14s %-8s %s\n", create_date, 
        create_time, kt_log.operation, result, kt_log.object, kt_log.level, op_info);
    securec_check_ss_c(rc, "", "");
    free(result);

    lfp = fopen(g_conf.log_file, "a");
    if (lfp == NULL) {
        printf("ERROR: failed to open log file '%s'.\n", g_conf.log_file);
        return;
    }
    fwrite(log_buf, strlen(log_buf), sizeof(char), lfp);
    fclose(lfp);
}