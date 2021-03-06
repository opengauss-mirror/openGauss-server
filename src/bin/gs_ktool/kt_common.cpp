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
 * kt_common.cpp
 *      Some common functions used by both key manage_module and log manage module, including:
 *      1. define global var read from configuration file
 *      2. report error
 *      3. read configuration file
 *      4. converse time and date format
 *
 * IDENTIFICATION
 *    src/bin/gs_ktool/kt_common.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "kt_common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include "securec.h"
#include "securec_check.h"
#include "pthread.h"

const char *CONF_FILE_NAME = "gs_ktool_conf.ini";
const char *ZERO_BUF = " ";

char g_env_value[MAX_REAL_PATH_LEN] = {0};
char g_conf_file[MAX_REAL_PATH_LEN ] = {0};
/* globa vars read from configuration file */
ConfIterm g_conf = { 0 };
static pthread_mutex_t g_env_lock;
bool g_is_print_err = true;

static bool find_conf_segment(FILE *conf_file_handle, const char *conf_segment_name);
static void read_conf_lines(FILE *conf_file_handle, char key_list[][MAX_CONF_KEY_LEN],
    char value_list[][MAX_CONF_VALUE_LEN], size_t *conf_item_num);
static unsigned int trans_date_to_int(const Time date);

char *gs_getenv_r(const char *name)
{
    char *ret = NULL;
    (void)pthread_mutex_lock(&g_env_lock);
    ret = getenv(name);
    (void)pthread_mutex_unlock(&g_env_lock);
    return ret;
}

static bool find_conf_segment(FILE *conf_file_handle, const char *conf_segment_name)
{
    int conf_item_cnt = 0;
    char cur_line[MAX_CONF_LINE_LEN] = {0};
    char cur_segment[MAX_SETMENT_NAME_LEN] = {0};
    bool is_find_segment = false;

    while (is_find_segment == false && fgets(cur_line, MAX_CONF_LINE_LEN, conf_file_handle)) {
        if (conf_item_cnt > MAX_CONF_ITEM_QUANTITY) {
            printf("WARING: only %d configuration items can be read.\n", MAX_CONF_ITEM_QUANTITY);
            return false;
        }

        if (strlen(cur_line) < strlen("[]")) {
            continue;
        }

        /* find configuration segment location */
        size_t ps = 0; /* position of segment */
        for (size_t i = 0; i < strlen(cur_line); i++) {
            if (cur_line[i] == ' ') {
                continue;
            } else if (cur_line[i] == '[') { 
                /* the configuration segment is defined as [Segment], if we find '[', then we will try to find ']' */
                for (i = i + 1; i < strlen(cur_line); i++) {
                    if (cur_line[i] == ']') {
                        cur_segment[ps] = '\0';
                        break;
                    } else if (i == strlen(cur_line) - 1) {
                        printf("WARNING: incomplete configuration segment declaration: '%s'\n", cur_line);
                        break;
                    } else {
                        cur_segment[ps] = cur_line[i];
                        ps++;
                    }
                }
                if (strcmp(cur_segment, conf_segment_name) == 0) {
                    is_find_segment = true;
                }
            } else {
                break;
            }
        }
    }

    return is_find_segment;
}

static void read_conf_lines(FILE *conf_file_handle, char key_list[][MAX_CONF_KEY_LEN],
    char value_list[][MAX_CONF_VALUE_LEN], size_t *conf_item_num)
{
    bool is_in_segment = true;
    char cur_line[MAX_CONF_LINE_LEN] = {0};
    char cur_key[MAX_CONF_KEY_LEN] = {0};
    char cur_value[MAX_CONF_VALUE_LEN] = {0};
    size_t pl = 0; /* position of line */
    size_t pk = 0; /* position of key */
    size_t pv = 0; /* position of value */
    size_t conf_item_cnt = 0;
    errno_t rc = 0;

    while (is_in_segment == true && fgets(cur_line, MAX_CONF_LINE_LEN, conf_file_handle)) {
        if (strlen(cur_line) < strlen(" = ;")) {
            continue;
        }

        /* check segment end, then read key from configureation line */
        for (; pl < strlen(cur_line); pl++) {
            if (cur_line[pl] == '#') {
                break;
            } else if (cur_line[pl] == ' ') {
                continue;
            } else if (cur_line[pl] == '[') {
                is_in_segment = false;
                break;
            } else if (cur_line[pl] == '=') {
                cur_key[pk] = '\0';
                break;
            } else {
                cur_key[pk] = cur_line[pl];
                pk++;
            }
        }

        /* read value from configureation line */
        if (is_in_segment == true) {
            for (pl = pl + 1; pl < strlen(cur_line); pl++) {
                if (cur_line[pl] == ' ') {
                    continue;
                } else if (cur_line[pl] == ';') {
                    cur_value[pv] = '\0';
                    break;
                } else {
                    cur_value[pv] = cur_line[pl];
                    pv++;
                }
            }

            if (strlen(cur_key) > 0 && strlen(cur_value) > 0) {
                rc = strcpy_s(key_list[conf_item_cnt], MAX_CONF_KEY_LEN, cur_key);
                securec_check_c(rc, "", "");
                rc = strcpy_s(value_list[conf_item_cnt], MAX_CONF_VALUE_LEN, cur_value);
                securec_check_c(rc, "", "");
                conf_item_cnt++;
            }
        }

        rc = memset_s(cur_line, MAX_CONF_LINE_LEN, 0, MAX_CONF_LINE_LEN);
        securec_check_c(rc, "", "");
        rc = memset_s(cur_key, MAX_CONF_KEY_LEN, 0, MAX_CONF_KEY_LEN);
        securec_check_c(rc, "", "");
        rc = memset_s(cur_value, MAX_CONF_VALUE_LEN, 0, MAX_CONF_VALUE_LEN);
        securec_check_c(rc, "", "");
        pl = 0;
        pk = 0;
        pv = 0;
    }

    *conf_item_num = conf_item_cnt;
}

bool read_conf_segment(const char *conf_file, const char *conf_segment_name, char key_list[][MAX_CONF_KEY_LEN],
    char value_list[][MAX_CONF_VALUE_LEN], size_t *conf_item_num)
{
    FILE *fp = NULL;

    fp = fopen(conf_file, "rb");
    if (fp == NULL) {
        return false;
    }

    if (!find_conf_segment(fp, conf_segment_name)) {
        fclose(fp);
        return false;
    }

    read_conf_lines(fp, key_list, value_list, conf_item_num);
    fclose(fp);

    return true;
}

bool set_global_file(char *kt_file, const GlobalKtoolFile file_type, const char *file_str)
{
    char default_kt_file_name[MAX_FILE_NAME_LEN];
    errno_t rc = 0;
    
    if (kt_file == NULL || g_env_value == NULL) {
        return false;
    }

    if (strlen(file_str) >= MAX_REAL_PATH_LEN) {
        return false;
    }

    /*
     * file_str is read from configure file, it has 2 types:
     * (1) “real_path/file_name” : the real_path and file_name are set by user.
     * (2) “DEFAULT” ：in this case, we will use the default file_path and file_name
     */
    if (strcmp(file_str, "DEFAULT") != 0) {
        /* file_str is a real path read from configuration file */
        rc = sprintf_s(kt_file, MAX_REAL_PATH_LEN, "%s", file_str);
        securec_check_ss_c(rc, "", "");

        if (!check_kt_env_value(kt_file)) {
            printf("ERROR: failed to set configuration, the path '%s' contains angerous character.\n", kt_file);
            return false;
        }
        return true;
    } else {
        switch (file_type) {
            case G_PRI_KSF:
                rc = strcpy_s(default_kt_file_name, MAX_FILE_NAME_LEN, "primary_ksf.dat");
                securec_check_c(rc, "", "");
                break;
            case G_SEC_KSF:
                rc = strcpy_s(default_kt_file_name, MAX_FILE_NAME_LEN, "secondary_ksf.dat");
                securec_check_c(rc, "", "");
                break;
            case G_EXPORT_KSF:
                rc = strcpy_s(default_kt_file_name, MAX_FILE_NAME_LEN, "export_ksf.dat");
                securec_check_c(rc, "", "");
                break;
            case G_LOG_FILE:
                rc = strcpy_s(default_kt_file_name, MAX_FILE_NAME_LEN, "gs_ktool.log");
                securec_check_c(rc, "", "");
                break;
            default:
                break;
        }

        rc = sprintf_s(kt_file, MAX_REAL_PATH_LEN, "%s/%s", g_env_value, default_kt_file_name);
        securec_check_ss_c(rc, "", "");
    }

    return true;
}

bool check_kt_env_value(const char *env_value)
{
    const char* danger_char_list[] = {"|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"",
                                      "{", "}", "(", ")", "[", "]", "~", "*", "?", "!", "\n", NULL};

    for (int i = 0; danger_char_list[i] != NULL; i++) {
        if (strstr(env_value, danger_char_list[i]) != NULL) {
            return false;
        }
    }

    return true;
}

bool compare_list_element(const char list_a[][MAX_CONF_KEY_LEN], size_t list_a_cnt, char list_b[][MAX_CONF_KEY_LEN],
    size_t list_b_cnt, size_t *loss_pos)
{
    bool is_find_element = false;

    for (size_t i = 0; i < list_a_cnt; i++) {
        for (size_t j = 0; j < list_b_cnt; j++) {
            if (strcmp(list_a[i], list_b[j]) == 0) {
                is_find_element = true;
                break;
            }
        }

        if (!is_find_element) {
            *loss_pos = i;
            return false;
        } else {
            is_find_element = false;
        }
    }

    return true;
}

/* to print pretty table, create line like "---------" */
void make_table_line(char *line_buf, const size_t buf_len, const size_t line_len)
{
    size_t i = 0;

    if (line_len >= buf_len) {
        return;
    }

    for (i = 0; i < line_len; i++) {
        line_buf[i] = '-';
    }

    line_buf[i] = '\0';
}

bool atoi_strictly(const char *str, unsigned int *num)
{
    for (size_t i = 0; i < strlen(str); i++) {
        if (str[i] < '0' || str[i] > '9') {
            return false;
        } 
    }

    *num = atoi(str);
    return true;
}

bool get_sys_time(Time *sys_time)
{
    time_t t_time = { 0 };
    struct tm *tm_time = { 0 };
    unsigned int initial_year = 1900;
    unsigned int initial_moth = 1;

    time(&t_time);
    tm_time = localtime(&t_time);
    if (tm_time == NULL) {
        return false;
    }

    sys_time->year = tm_time->tm_year + initial_year;
    sys_time->month = tm_time->tm_mon + initial_moth;
    sys_time->day = tm_time->tm_mday;
    sys_time->hour = tm_time->tm_hour;
    sys_time->minute = tm_time->tm_min;
    sys_time->second = tm_time->tm_sec;
    return true;
}

void date_ttos(const Time date_t, char *date_s, const size_t buf_size)
{
    errno_t rc = 0;
    rc = sprintf_s(date_s, buf_size, "%04u-%02u-%02u", date_t.year, date_t.month, date_t.day);
    securec_check_ss_c(rc, "", "");
}

void time_ttos(const Time time_t, char *time_s, const size_t buf_size)
{
    errno_t rc = 0;
    rc = sprintf_s(time_s, buf_size, "%02u:%02u:%02u", time_t.hour, time_t.minute, time_t.second);
    securec_check_ss_c(rc, "", "");
}

void trans_wsectime_to_time(WsecSysTime wsec_time_t, Time *time_t)
{
    time_t->year = wsec_time_t.kmcYear;
    time_t->month = wsec_time_t.kmcMonth;
    time_t->day = wsec_time_t.kmcDate;
    time_t->hour = wsec_time_t.kmcHour;
    time_t->minute = wsec_time_t.kmcMinute;
    time_t->second = wsec_time_t.kmcSecond;
}

static unsigned int trans_date_to_int(const Time date)
{
    unsigned int days = 0;
    /* my epochs, not use 1970.1.1, but use 2019.1.1 */
    unsigned int origin_y = 2019;
    unsigned int origin_m = 1;
    /* origin_d = 1 */
    const unsigned int leap_year[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const unsigned int lunar_year[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    for (unsigned int y = (origin_y + 1); y <= date.year; y++) {
        if ((y - 1) % 4 == 0) {
            days += 366;
        } else {
            days += 365;
        }
    }

    for (unsigned int m = (origin_m + 1); m <= date.month; m++) {
        if (date.year % 4 == 0) {
            days += leap_year[m - 1];
        } else {
            days += lunar_year[m - 1];
        }
    }

    days += date.day;

    return days;
}

void timecpy(Time *dest_time, Time src_time)
{
    securec_check_c(memcpy_s(dest_time, sizeof(Time), &src_time, sizeof(Time)), "\0", "\0");
}

/* to determine whether the key has expired */
int calculate_interval(Time start, Time end)
{
    return (trans_date_to_int(end) - trans_date_to_int(start));
}