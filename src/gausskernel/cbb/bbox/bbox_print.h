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
 * bbox_print.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_print.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __BBOX_PRINT_H_
#define __BBOX_PRINT_H_

#include "bbox_types.h"

#define BBOX_DBG 1

#ifdef BBOX_DBG
#define BBOX_LOG_SIZE (64 * 1024)
#else
#define BBOX_LOG_SIZE (32 * 1024)
#endif

typedef enum {
    PRINT_DBG = 1,
    PRINT_LOG = 2, /* Print log with function name */
    PRINT_TIP = 3, /* Print tip message to screen */
    PRINT_ERR = 4, /* Print log with function name and line number */
    LOG_TYPE_COUNT,
} EN_PRINT_TYPE;

extern char g_acBBoxLog[BBOX_LOG_SIZE];
extern int g_iLastLogLen;

/* init log */
extern void bbox_initlog(int iLogScreen);

/* set print level. */
extern s32 bbox_set_log_level(EN_PRINT_TYPE enLevel);

/* set screen print level. */
extern s32 bbox_set_screen_log_level(EN_PRINT_TYPE enLevel);

/* print */
extern s32 bbox_snprintf(char* pszBuff, s32 iSize, const char* pFmt, ...);

/* standard print function */
extern void bbox_printf(const char* pFmt, ...) __attribute__((format(printf, 1, 2)));

extern void bbox_print(EN_PRINT_TYPE enType, const char* pFmt, ...) __attribute__((format(printf, 2, 3)));

#endif
