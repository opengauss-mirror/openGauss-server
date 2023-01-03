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
 * ss_aio.cpp
 *        aio implementation
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_aio.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "ddes/dms/ss_aio.h"
#include "utils/elog.h"

#ifndef ENABLE_LITE_MODE
static void WaitDSSAioComplete(DSSAioCxt *aio_cxt, int index)
{
    AioUtil *aio = &aio_cxt->aio[index];
    errno_t ret;
    struct timespec timeout = { 0, 200 };
    int event_num = aio->iocount;
    while (event_num > 0) {
        ret = memset_s(aio->events, sizeof(struct io_event) * event_num,
            0, sizeof(struct io_event) * event_num);
        securec_check_c(ret, "\0", "\0");

        int num = io_getevents(aio->handle, 1, event_num, aio->events, &timeout);
        if (num < 0) {
            if (errno == EINTR || num == -EINTR) {
                continue;
            }
            ereport(WARNING, (errmsg("failed to getevent by aio, errno = %d, retCode = %d", errno, num)));
            _exit(0);
        }

        for (int i = 0; i < num; i++) {
            aio_cxt->aiocb(&aio->events[i]);
        }
        event_num -= num;
    }

    aio->iocount = 0;
    ret = memset_s(aio->iocbs_ptr, sizeof(struct iocb *) * DSS_AIO_BATCH_SIZE,
        0, sizeof(struct iocb *) * DSS_AIO_BATCH_SIZE);
    securec_check_c(ret, "\0", "\0");
    ret = memset_s(aio->events, sizeof(struct io_event) * DSS_AIO_BATCH_SIZE,
        0, sizeof(struct io_event) * DSS_AIO_BATCH_SIZE);
    securec_check_c(ret, "\0", "\0");
}
#endif

void DSSAioFlush(DSSAioCxt *aio_cxt)
{
#ifndef ENABLE_LITE_MODE
    bool need_wait = false;
    AioUtil *aio = &aio_cxt->aio[aio_cxt->index];
    if (aio->iocount > 0) {
        if (io_submit(aio->handle, aio->iocount, aio->iocbs_ptr) != aio->iocount) {
            ereport(WARNING, (errmsg("io_submit failed, errno = %d", errno)));
            _exit(0);
        }
        need_wait = true;
        /* wait aio result at last to improve performance */
    }

    aio = &aio_cxt->aio[1 - aio_cxt->index];
    if (aio->iocount > 0) {
        WaitDSSAioComplete(aio_cxt, 1 - aio_cxt->index);
    }

    if (need_wait) {
        WaitDSSAioComplete(aio_cxt, aio_cxt->index);
    }
#endif
}

void DSSAioAppendIOCB(DSSAioCxt *aio_cxt, struct iocb *iocb_ptr)
{
#ifndef ENABLE_LITE_MODE
    AioUtil *aio = &aio_cxt->aio[aio_cxt->index];
    aio->iocbs_ptr[aio->iocount] = iocb_ptr;
    aio->iocount++;

    if (aio->iocount >= DSS_AIO_BATCH_SIZE) {
        if (io_submit(aio->handle, aio->iocount, aio->iocbs_ptr) != aio->iocount) {
            ereport(PANIC, (errmsg("io_submit failed, errno = %d", errno)));
        }

        aio_cxt->index = 1 - aio_cxt->index;
        aio = &aio_cxt->aio[aio_cxt->index];
        if (aio->iocount > 0) {
            WaitDSSAioComplete(aio_cxt, aio_cxt->index);
        }
    }
#endif
}

struct iocb* DSSAioGetIOCB(DSSAioCxt *aio_cxt)
{
    AioUtil *aio = &aio_cxt->aio[aio_cxt->index];
    return &aio->iocbs[aio->iocount];
}

int DSSAioGetIOCBIndex(DSSAioCxt *aio_cxt)
{
    AioUtil *aio = &aio_cxt->aio[aio_cxt->index];
    return (aio_cxt->index * DSS_AIO_BATCH_SIZE + aio->iocount);
}

void DSSAioInitialize(DSSAioCxt *aio_cxt, aio_callback callback)
{
#ifndef ENABLE_LITE_MODE
    errno_t err = memset_s(aio_cxt, sizeof(DSSAioCxt), 0, sizeof(DSSAioCxt));
    securec_check_ss(err, "\0", "\0");

    if (io_setup(DSS_AIO_BATCH_SIZE, &aio_cxt->aio[0].handle) < 0) {
        ereport(WARNING, (errmsg("io_setup failed for DSS AIO, errno=%d", errno)));
        _exit(0);
    }

    if (io_setup(DSS_AIO_BATCH_SIZE, &aio_cxt->aio[1].handle) < 0) {
        ereport(WARNING, (errmsg("io_setup failed for DSS AIO, errno=%d", errno)));
        _exit(0);
    }
    aio_cxt->initialized = true;
    aio_cxt->aiocb = callback;
    aio_cxt->index = 0;
#endif
}

void DSSAioDestroy(DSSAioCxt *aio_cxt)
{
#ifndef ENABLE_LITE_MODE
    if (aio_cxt->initialized) {
        (void)io_destroy(aio_cxt->aio[0].handle);
        (void)io_destroy(aio_cxt->aio[1].handle);
        aio_cxt->initialized = false;
        errno_t err = memset_s(aio_cxt, sizeof(DSSAioCxt), 0, sizeof(DSSAioCxt));
        securec_check_ss(err, "\0", "\0");
    }
#endif
}

