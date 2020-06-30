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
 * sctp_list.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_list.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _MC_UTILS_LIST_H_
#define _MC_UTILS_LIST_H_

struct mc_list_item {
    struct mc_list_item* next;
};

struct mc_list {
    struct mc_list_item* head;
    struct mc_list_item* tail;
};

//  Initialise the list.
void mc_list_init(struct mc_list* self);

//  Terminate the list. Note that list must be manually emptied before the
//		termination.
void mc_list_term(struct mc_list* self);

/*
//  Returns 1 if there are no items in the list, 0 otherwise.
*/
//  Inserts one element into the list.
void mc_list_push(struct mc_list* self, struct mc_list_item* item);

//  Retrieves one element from the list. The element is removed
//		from the list. Returns NULL if the list is empty.
struct mc_list_item* mc_list_pop(struct mc_list* self);

#endif  //_MC_UTILS_LIST_H_
