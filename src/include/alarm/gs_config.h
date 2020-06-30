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
 * gs_config.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/alarm/gs_config.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef __GS_CONFIG_H__
#define __GS_CONFIG_H__

#include "alarm/gs_warn_common.h"

#define WARN_MAX_NAME_LEN 64
#define MAX_LEN_USERNAME WARN_MAX_NAME_LEN
#define MAX_LEN_CLUSTERNAME WARN_MAX_NAME_LEN

typedef enum tagConfigType {
    CONFIG_PORT,
    CONFIG_GAUSS_HOST,
    CONFIG_GAUSS_PORT,
    CONFIG_GAUSS_DBNAME,
    CONFIG_GAUSS_USERNAME,
    CONFIG_GAUSS_PASSWORD,
    CONFIG_LOG_FILENAME,

    CONFIG_BUTT = 0xffffffff
} CONFIG_TYPE;

typedef struct tagGsConfig {
    int32 portNum;
    char dbHost[MAX_LEN_SERVADDR];
    int32 dbPort;
    char username[MAX_LEN_USERNAME];
    char dbname[MAX_LEN_USERNAME];
    char clustername[MAX_LEN_CLUSTERNAME];
} GS_CONFIG, *LP_GS_CONFIG;

WARNERRCODE processConfiguration(int32 argc, char* argv[], char** password);
void fillDefaultGaussInfo();

#endif  //__GS_CONFIG_H__
