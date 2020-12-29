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
 * hotpatch.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/hotpatch/hotpatch.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef HOTPATCH_H
#define HOTPATCH_H

#define HP_OK (0)
#define HP_ERR (-1)
const int g_max_length_time = 64;
const int g_max_length_act = 16;
const int g_max_length_path = 512;
const int g_max_length_std_path = 4096;
const int g_first_err = 0x1000;
const int g_lock_file_size = 1024;
const int g_length_statstr = 50;
const int g_max_number_patch = 1000;
const int g_length_okstr = 15;
const int g_length_list = 4;
const int g_max_length_line = 128;
const int g_rev_bytes_info = 3;

#define MAX_LENGTH_RETURN_STRING (1024)

typedef enum HP_STATE {
    HP_STATE_UNKNOW,
    HP_STATE_UNLOAD,
    HP_STATE_DEACTIVE,
    HP_STATE_ACTIVED,
} HP_STATE_T;

typedef enum HP_ERROR_CODE {
    HP_ERROR_NAME_NUMBER = g_first_err,
    HP_ERROR_INSUFFICIENT_MEM,
    HP_ERROR_SUFFIX_INCORRECT,
    HP_ERROR_UNKNOWN_NAME,
    HP_ERROR_EMPTY_NAME,
    HP_ERROR_EMPTY_COMMAND,
    HP_ERROR_LOADED_BEFORE,
    HP_ERROR_CANNOT_UNLOAD,
    HP_ERROR_CANNOT_ACTIVE,
    HP_ERROR_CANNOT_DEACTIVE,
    HP_ERROR_RANGE,
    HP_ERROR_NO_PATCH_LOADED,
    HP_ERROR_EMPTY_PARAMETER,
    HP_ERROR_INITED_BEFORE,
    HP_ERROR_LIB_INIT_ERROR,
    HP_ERROR_NOT_INIT,
    HP_ERROR_UNKNOWN_ACTION,
    HP_ERROR_FETCH_ERROR,
    HP_ERROR_VERSION,
    HP_ERROR_GTM_UNCONNECTED,
    HP_ERROR_GTM_START_ERROR,
    HP_ERROR_GTM_SEND_ERROR,
    HP_ERROR_GTM_SEND_END_ERROR,
    HP_ERROR_GTM_FLUSH_ERROR,
    HP_ERROR_GTM_RECEIVE_ERROR,
    HP_ERROR_OPERATION_LOG_ERR,
    HP_ERROR_COMMUNICATION_ERR,
    HP_ERROR_UNDER_PATCHING,
    HP_ERROR_SYSTEM_ERROR,
    HP_ERROR_FILE_SEEK_ERROR,
    HP_ERROR_FILE_WRITE_ERROR,
    HP_ERROR_FILE_READ_ERROR,
    HP_ERROR_FILE_OPEN_ERROR,
    HP_ERROR_FILE_SYNC_ERROR,
    HP_ERROR_FILE_LOCK_ERROR,
    HP_ERROR_FILE_RENAME_ERROR,
    HP_ERROR_FILE_PATH_ERROR,
    HP_ERROR_PATCH_NUMBER_ERROR,
    HP_ERROR_PATCH_INFO_ERROR,
    HP_ERROR_CM_DATADIR_NULL,
    HP_ERROR_REG_LOG_CALLBACK_ERROR,
    HP_ERROR_REG_PAGESIZE_CALLBACK_ERROR,
    HP_ERROR_COMMON_ERROR_MAX,
} HP_ERROR_CODE_T;

typedef enum HP_INFO_STATE {
    HP_INFO_NOT_EXIST,
    HP_INFO_EXIST,
    HP_INFO_NEED_RFRESH,
} HP_INFO_STATE_T;

typedef struct PATCH_LOG {
    char time_stamp[g_max_length_time];
    char operation[g_max_length_act];
    char patch_name[g_max_length_path];
    char rev[16];
} PATCH_LOG_T;

typedef struct PATCH_INFO_HEADER {
    int max_patch_number;  // record how many patchs has been loaded
    int rev;
} PATCH_INFO_HEADER_T;

typedef struct PATCH_INFO {
    int patch_number;
    char patch_state;
    char rev[g_rev_bytes_info];
    char patch_name[g_max_length_path];
} PATCH_INFO_T;

typedef void (*HOTPATCH_LOG_FUNC)(unsigned int level, const char* fmt);

extern int hotpatch_init(const char* default_dir, HOTPATCH_LOG_FUNC log_func);
extern int exec_hotpatch_command(const char* filename, const char* action, char* output_string, size_t string_length);
void patch_err_info_handler(int error_code, char* out_string, size_t string_length);
char* strip_path_from_pathname(const char* name_withpath);
#endif
