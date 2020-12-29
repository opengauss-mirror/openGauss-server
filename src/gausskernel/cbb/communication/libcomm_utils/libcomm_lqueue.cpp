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
 * sctp_lqueue.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_lqueue.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "libcomm_utils/libcomm_util.h"
#include "libcomm_cont.h"
#include "libcomm_lqueue.h"
#include "libcomm_common.h"

inline int mc_lqueue_item_size(struct mc_lqueue_item* q_item)
{
    return q_item->element.data->iov_len;
}

int mc_lqueue_add(struct mc_lqueue* q, struct mc_lqueue_item* q_item)
{
    if (q == NULL || q_item == NULL) {
        return -1;
    }

    Assert(&(q_item->item) != NULL);

    int data_size = mc_lqueue_item_size(q_item);
    Assert(data_size > 0);

    mc_list_push(&q->list, &(q_item->item));

    if (q->is_empty == 1) {
        q->is_empty = 0;
    }

    q->u_size += (unsigned long)(unsigned)data_size;
    q->count++;

    if (q->u_size >= q->size) {
        q->is_full = 1;
    }

    return 1;
}

struct mc_lqueue_item* mc_lqueue_remove(struct mc_lqueue* q, struct mc_lqueue_item* q_item)
{
    if (q == NULL) {
        return NULL;
    }

    if (q->count > 0) {
        q->count--;
    }

    struct mc_list_item* l_item = mc_list_pop(&q->list);
    if (l_item == NULL) {
        return NULL;
    }

    q_item = mc_cont(l_item, struct mc_lqueue_item, item);

    q->u_size -= (unsigned long)(unsigned)mc_lqueue_item_size(q_item);
    if (q->u_size < q->size) {
        q->is_full = 0;
    }

    if (q->count == 0) {
        q->is_empty = 1;
    }

    return q_item;
}

struct mc_lqueue* mc_lqueue_init(unsigned long size)
{
    if (size == 0) {
        return NULL;
    }

    struct mc_lqueue* q = NULL;
    LIBCOMM_MALLOC(q, sizeof(struct mc_lqueue), mc_lqueue);
    if (q == NULL) {
        return NULL;
    }

    mc_list_init(&q->list);

    q->count = 0;
    q->is_empty = 1;
    q->is_full = 0;
    q->size = size;
    q->u_size = 0;

    return (q);
}

extern void libcomm_free_iov_item(struct mc_lqueue_item** iov_item, int size);
struct mc_lqueue* mc_lqueue_clear(struct mc_lqueue* q)
{
    if (q == NULL) {
        return NULL;
    }

    struct mc_lqueue_item* q_item = NULL;

    while (q->count > 0) {
        q_item = mc_lqueue_remove(q, q_item);
        libcomm_free_iov_item(&q_item, IOV_DATA_SIZE);
    }

    return q;
}

void mc_lqueue_print(int n, struct mc_lqueue* q)
{
    LIBCOMM_ELOG(LOG, "q:%d ", n);

    if (q == NULL) {
        LIBCOMM_ELOG(LOG, "\t(null)");
        return;
    }

    LIBCOMM_ELOG(LOG, "\tsize\t	= %lu", q->size);
    LIBCOMM_ELOG(LOG, "\tcount\t	= %u", q->count);
    LIBCOMM_ELOG(LOG, "\tis_empty\t= %d", q->is_empty);
    LIBCOMM_ELOG(LOG, "\tis_full\t	= %d", q->is_full);
    LIBCOMM_ELOG(LOG, "\tu_size\t	= %lu", q->u_size);

    struct mc_list_item* l_item = NULL;
    struct mc_lqueue_item* q_item = NULL;
    struct mc_list* self = &q->list;
    // traversing the list
    //
    int i = 0;
    for (l_item = self->head; l_item; l_item = l_item->next) {
        i++;
        q_item = mc_cont(l_item, struct mc_lqueue_item, item);
        LIBCOMM_ELOG(LOG,
            "\tdata[%d]\t	= %s(%d)(%d)\n",
            i,
            (char*)((q_item->element.data)->iov_base),
            (int)((q_item->element.data)->iov_len),
            (int)(mc_lqueue_item_size(q_item)));
    }
}
