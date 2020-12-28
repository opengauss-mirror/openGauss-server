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
 * hashfuncs.h
 *        Head file for streaming engine launcher.
 *
 *
 * IDENTIFICATION
 *        src/include/streaming/launcher.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_STREAMING_LAUNCHER_H_
#define SRC_INCLUDE_STREAMING_LAUNCHER_H_

#include "gs_thread.h"

typedef enum {
    STREAMING_BACKEND_INIT = 1,
    STREAMING_BACKEND_REAP,
    STREAMING_BACKEND_SIGTERM,
    STREAMING_BACKEND_SIGHUP,
    STREAMING_BACKEND_SHUTDOWN
} StreamingBackendManagerType;

typedef void (*StreamingBackendServerLoopFunc)();
typedef void (*StreamingBackendShutdownFunc)();

bool is_streaming_engine();
int streaming_get_thread_count();
void streaming_backend_main(knl_thread_arg* arg);
extern bool streaming_backend_manager(StreamingBackendManagerType mtype, const void *info = NULL);
bool is_streaming_backend_terminating();

#endif /* SRC_INCLUDE_STREAMING_LAUNCHER_H_ */
