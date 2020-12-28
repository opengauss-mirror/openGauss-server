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
 * sctp_queue.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_queue.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "libcomm_queue.h"
#include "libcomm_common.h"

int mc_queue_pop(struct mc_queue* q, int* e)
{
    if (q == NULL) {
        return -1;
    }

    LIBCOMM_PTHREAD_MUTEX_LOCK(&(q->lock));
    if (q->is_empty == 1) {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&(q->lock));
        return 0;
    }
    *e = q->data[q->head];
    q->data[q->head] = -1;  // unnormal value

    ++(q->head);

    if (q->head == q->size) {
        q->head = 0;
    }
    if (q->head == q->tail) {
        q->is_empty = 1;
    }

    q->is_full = 0;
    q->count--;

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&(q->lock));
    return 1;
}

int mc_queue_push(struct mc_queue* q, int e)
{
    if (q == NULL) {
        return -1;
    }

    LIBCOMM_PTHREAD_MUTEX_LOCK(&(q->lock));
    if (q->is_full) {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&(q->lock));
        return 0;
    }
    q->data[q->tail] = e;
    ++(q->tail);

    if (q->tail == q->size) {
        q->tail = 0;
    }
    if (q->tail == q->head) {
        q->is_full = 1;
    }

    q->is_empty = 0;
    q->count++;
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&(q->lock));
    return 1;
}

int mc_queue_init(struct mc_queue* q, int size)
{
    if (q == NULL || size <= 1) {
        return -1;
    }

    q->size = size - 1;
    q->data = NULL;
    LIBCOMM_MALLOC(q->data, (q->size * sizeof(int)), int);
    if (q->data == NULL) {
        return -1;
    }

    LIBCOMM_PTHREAD_MUTEX_INIT(&(q->lock), 0);

    // we desgin this for getting streamid, we do not use stream 0
    //
    q->is_empty = 1;
    q->is_full = 0;
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->pop = mc_queue_pop;
    q->push = mc_queue_push;

    return 0;
}

struct mc_queue* mc_queue_clear(struct mc_queue* q)
{
    if (q == NULL) {
        return NULL;
    }

    int data = 0;

    while (!q->is_empty) {
        (void)q->pop(q, &data);
    }
    return q;
}

struct mc_queue* mc_queue_destroy(struct mc_queue* q)
{
    if (q == NULL) {
        return NULL;
    }

    if (q->data == NULL) {
        return q;
    }

    q = mc_queue_clear(q);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&(q->lock));
    mc_free(q->data);
    q->data = NULL;
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&(q->lock));
    LIBCOMM_PTHREAD_MUTEX_DESTORY(&(q->lock));

    return q;
}
