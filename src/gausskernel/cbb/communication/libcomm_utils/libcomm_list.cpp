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
 * sctp_list.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_list.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stddef.h>

#include "libcomm_list.h"

void mc_list_init(struct mc_list* self)
{
    self->head = NULL;
    self->tail = NULL;
}

void mc_list_term(struct mc_list* self)
{
    self->head = NULL;
    self->tail = NULL;
}

void mc_list_push(struct mc_list* self, struct mc_list_item* item)
{
    item->next = NULL;
    if (self->head == NULL) {
        self->head = item;
    }
    if (self->tail != NULL) {
        self->tail->next = item;
    }
    self->tail = item;
}

struct mc_list_item* mc_list_pop(struct mc_list* self)
{
    struct mc_list_item* result = NULL;

    if (self->head == NULL) {
        return NULL;
    }
    result = self->head;
    self->head = result->next;
    if (self->head == NULL) {
        self->tail = NULL;
    }
    result->next = NULL;
    return result;
}
