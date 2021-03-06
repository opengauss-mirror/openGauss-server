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
 * kt_common.h
 *    Some common functions used by both key manage_module and log manage module, including:
 *      1. define global var read from configuration file
 *      2. report error
 *      3. read configuration file
 *      4. converse time and date format
 *
 * IDENTIFICATION
 *    src/bin/gs_ktool/kt_common.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_KT_COMMON_H
#define GS_KT_COMMON_H

#include "kt_kmc_callback.h"

extern const char *CONF_FILE_NAME;
extern const char *ZERO_BUF;

/* check length from conf file && from gs_ktool interfacs */
const int MAX_CONF_ITEM_QUANTITY = 100;
const int MAX_CONF_LINE_LEN = 2048;
const int MAX_CONF_KEY_LEN = 512;
const int MAX_CONF_VALUE_LEN = 512;
const int MAX_SETMENT_NAME_LEN = 1024;
const int MAX_RK_VALIDITY = 10000;
const int MAX_CMK_VALIDITY = 3660;
const int MAX_FILE_NAME_LEN = 512;
const int MAX_REAL_PATH_LEN = 4096;

const int MIN_CMK_LEN = 16;
const int MAX_CMK_LEN = 112; /* 112 bytes (the max length that KMC support) */
const int DEFAULT_DOMAIN = 2;

/* general limit */
const int MAX_LOG_RESULT_LEN = 2048;
const int MAX_USER_INPUT_LEN = 1024;

typedef enum KeyManageConf {
    RK_VALIDITY = 0,
    CMK_VALIDITY,
    CMK_LENGTH,
    PRI_KSF,
    SEC_KSF,
    EXPORT_KSF, 
} KeyManageConf;

enum LogManageConf {
    IS_LOGMODULE_OPEN = 0,
    LOG_FILE,
};

typedef enum GlobalKtoolFile {
    G_CONF_FILE = 0,
    G_PRI_KSF,
    G_SEC_KSF,
    G_EXPORT_KSF,
    G_LOG_FILE,
} GlobalKtoolFile;

typedef enum LogModuleStatus {
    ON = 0,
    OFF,
} LogModuleStatus;

typedef struct Time {
    unsigned int year;
    unsigned int month;
    unsigned int day;
    unsigned int hour;
    unsigned int minute;
    unsigned int second;
} Time;

typedef struct ConfIterm {
    unsigned int rk_validity;
    unsigned int cmk_validity;
    unsigned int cmk_length;
    char pri_ksf[MAX_REAL_PATH_LEN ];
    char sec_ksf[MAX_REAL_PATH_LEN ];
    char export_ksf[MAX_REAL_PATH_LEN ];

    unsigned char is_logmodule_open;
    char log_file[MAX_REAL_PATH_LEN ];
} ConfIterm;

#define return_false_if(condition) {\
        if ((condition)) {          \
            return false;           \
        }                           \
    }

#define kt_free(ptr) \
    {                \
        free(ptr);   \
        ptr = NULL;  \
    }

extern char g_env_value[MAX_REAL_PATH_LEN];
extern char g_conf_file[MAX_REAL_PATH_LEN ];
/* globa vars read from configuration file */
extern ConfIterm g_conf;
extern bool g_is_print_err;

extern char *gs_getenv_r(const char *name);
extern bool read_conf_segment(const char *conf_file, const char *conf_segment_name, char key_list[][MAX_CONF_KEY_LEN],
    char value_list[][MAX_CONF_VALUE_LEN], size_t *conf_item_num);
extern bool set_global_file(char *kt_file, const GlobalKtoolFile file_type, const char *file_str);
extern bool check_kt_env_value(const char *env_value);

extern bool compare_list_element(const char list_a[][MAX_CONF_KEY_LEN], size_t list_a_cnt,
    char list_b[][MAX_CONF_KEY_LEN], size_t list_b_cnt, size_t *loss_pos);
void make_table_line(char *line_buf, const size_t buf_len, const size_t line_len);
bool atoi_strictly(const char *str, unsigned int *num);

/* convert and calculate the time */
extern bool get_sys_time(Time *sys_time);
void date_ttos(const Time date_t, char *date_s, const size_t buf_size);
void time_ttos(const Time time_t, char *time_s, const size_t buf_size);
void trans_wsectime_to_time(WsecSysTime wsec_time_t, Time *time_t);
void timecpy(Time *dest_time, Time src_time);
/* to determine whether the key has expired */
int calculate_interval(Time start, Time end);
#endif
