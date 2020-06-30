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
 * gs_utils.h
 *        Header file for Gauss warning module utility functions.
 * 
 * 
 * IDENTIFICATION
 *        src/include/alarm/gs_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __GS_UTILS_H__
#define __GS_UTILS_H__

#include "alarm/gs_warn_common.h"

#define BASE_DECIMAL 10

#define MAX_STR_LEN_OF_UINT32 11

#define MAX_STR_LEN_OF_UINT64 22

WARNERRCODE stringToLong(char* str, int32 base, long* value);

WARNERRCODE validateIPAddress(char* pucIp, char* key, uint32 slLen);

WARNERRCODE validatePort(char* str, char* key, int32* port);

void stringCopy(char* dest, char* src, uint32 iMaxLen);

bool compareString(char* str1, char* str2);

void uint32ToStr(uint32 intVal, char* strVal);

void uint64ToStr(uint64 longVal, char* strVal);

void writeUIntToBuffer(char* buf, uint32 value, int32* iBufPos);

void writeULongToBuffer(char* buf, uint64 value, int32* iBufPos);

void writeNULLToBuffer(char* buf, int32* iBufPos);

void writeStringToBuffer(char* buf, char* str, int32* iBufPos);

#endif  //__GS_UTILS_H__
