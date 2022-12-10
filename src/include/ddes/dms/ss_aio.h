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
 * ss_aio.h
 *        aio interface.
 *
 * IDENTIFICATION
 *        src/include/ddes/dms/ss_aio.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __SS_AIO_H__
#define __SS_AIO_H__

#include "libaio.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*aio_callback)(struct io_event *event);
#define DSS_AIO_BATCH_SIZE 128
#define DSS_AIO_UTIL_NUM 2
typedef struct AioUtil {
    io_context_t handle;
    struct iocb iocbs[DSS_AIO_BATCH_SIZE];
    struct iocb *iocbs_ptr[DSS_AIO_BATCH_SIZE];
    struct io_event events[DSS_AIO_BATCH_SIZE];
    int iocount;
} AioUtil;

typedef struct DSSAioCxt {
    bool initialized;
    aio_callback aiocb;
    int index;
    AioUtil aio[DSS_AIO_UTIL_NUM];
} DSSAioCxt;

void DSSAioInitialize(DSSAioCxt *aio_cxt, aio_callback callback);
void DSSAioDestroy(DSSAioCxt *aio_cxt);
struct iocb* DSSAioGetIOCB(DSSAioCxt *aio_cxt);
int DSSAioGetIOCBIndex(DSSAioCxt *aio_cxt);
void DSSAioAppendIOCB(DSSAioCxt *aio_cxt, struct iocb *iocb_ptr);
void DSSAioFlush(DSSAioCxt *aio_cxt);

#ifdef __cplusplus
}
#endif

#endif /* __SS_AIO_H__ */