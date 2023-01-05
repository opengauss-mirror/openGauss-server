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
 * dss_log.h
 * 
 * IDENTIFICATION
 *        src/include/storage/dss/dss_log.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSS_LOG_H
#define DSS_LOG_H

#define DMS_LOGGER_BUFFER_SIZE 2048

void DSSInitLogger();
void DSSRefreshLogger(char *log_field, unsigned long long *value);

typedef enum dss_log_level {
    LOG_RUN_ERR_LEVEL = 0,
    LOG_RUN_WAR_LEVEL,
    LOG_RUN_INF_LEVEL,
    LOG_DEBUG_ERR_LEVEL,
    LOG_DEBUG_WAR_LEVEL,
    LOG_DEBUG_INF_LEVEL
} dss_log_level;

#endif /* DSS_LOG_H */