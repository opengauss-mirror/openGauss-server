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
 * bbox_threads.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_threads.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __BBOX_THREAD_H__
#define __BBOX_THREAD_H__

#include "bbox_syscall_support.h"

#define PAGE_SIZE 4096
#define BBOX_PROC_PATH_LEN 128

#define BBOX_ALT_STACKSIZE (MINSIGSTKSZ + 4096 * 32)

struct BBOX_ListDirParam {
    void* pArg1;
    void* pArg2;
    void* pArg3;
};

/* get thread type */
typedef enum {
    GET_TYPE_DUMP, /* export information */
    GET_TYPE_SNAP, /* snapshoot */
    GET_TYPE_BUTT,
} GET_THREAD_TYPE;

typedef void (*BBOX_GetAllThreadDone)(void* pArgs);

typedef s32 (*BBOX_GetAllThreadsCallBack)(
    BBOX_GetAllThreadDone pDone, void* pDoneHandle, s32 iThreadNum, pid_t* parThreads, va_list ap);

extern s32 BBOX_GetAllThreads(
    GET_THREAD_TYPE enType, BBOX_GetAllThreadDone pDone, void* pDoneArgs, BBOX_GetAllThreadsCallBack pCallback, ...);

#endif
