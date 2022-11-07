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
 * ss_dms_log_output.cpp
 *  initialize for DMS shared storage.
 * 
 * 
 * IDENTIFICATION
 *        src/include/ss_output_log.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef __SS_OUTPUT_LOG_H__
#define __SS_OUTPUT_LOG_H__

#include "ss_common_attr.h"

#define GS_MAX_PATH_BUFFER_SIZE 1024
#define GS_MAX_NAME_LEN 64
typedef bool bool8;
typedef bool bool32;
#define GS_FILE_NAME_BUFFER_SIZE 1024.
/* _log_level */
#define LOG_NONE              0x00000000
#define LOG_RUN_ERR_LEVEL     0x00000001
#define LOG_RUN_WAR_LEVEL     0x00000002
#define LOG_RUN_INF_LEVEL     0x00000004
#define LOG_DEBUG_ERR_LEVEL   0x00000010
#define LOG_DEBUG_WAR_LEVEL   0x00000020
#define LOG_DEBUG_INF_LEVEL   0x00000040
 
void DMSLogOutput(uint32 ss_log_level, const char *code_file_name, uint32 code_line_num, char buf[]);
int32 DMSLogLevelCheck(dms_log_id_t dms_log_id, dms_log_level_t dms_log_level, uint32 *log_level);
 
typedef enum en_log_level {
    SS_LEVEL_ERROR = 0,  // error conditions
    SS_LEVEL_WARN,       // warning conditions
    SS_LEVEL_INFO,       // informational messages
} log_level_t;
 
typedef enum en_log_id {
    LOG_RUN = 0,
    LOG_DEBUG,
    LOG_COUNT,  // LOG COUNT = 2
} log_id_t;

#endif
