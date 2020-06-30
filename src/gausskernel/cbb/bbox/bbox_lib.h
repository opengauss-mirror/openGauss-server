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
 * bbox_lib.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_lib.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __BBOX_LIB_H_
#define __BBOX_LIB_H_

#include "bbox_types.h"

#define BBOX_PATH_LEN 256

extern s32 bbox_get_exec_path(pid_t pid, char* buff, int buff_len);

extern s32 bbox_skip_atoi(const char* pszString);

extern char* bbox_strncat(char* pszDest, const char* pszSrc, s32 count);

extern s32 bbox_strlen(const char* pszString);

extern s32 bbox_strnlen(const char* pszString, s32 count);

extern s32 bbox_strncmp(const char* pszSrc, const char* pszTarget, s32 count);

extern s32 bbox_strcmp(const char* pszSrc, const char* pszTarget);

extern s32 bbox_memcmp(const void* cs, const void* ct, s32 count);

extern char* bbox_strstr(const char* s1, const char* s2);

extern s32 bbox_atoi(const char* pszString);

extern s32 sys_popen(char* command, const char* mode);

extern s32 sys_pclose(s32 fd);

extern s32 bbox_mkdir(const char* pszDir);

extern long bbox_pread(s32 fd, void* buf, unsigned long count, unsigned long offset);

typedef s32 (*BBOX_LIST_DIR_CALLBACK)(const char* pszPath, const char* pszName, void* pArgs);
extern s32 bbox_listdir(const char* pstPath, BBOX_LIST_DIR_CALLBACK callback, void* pArgs);

#endif
