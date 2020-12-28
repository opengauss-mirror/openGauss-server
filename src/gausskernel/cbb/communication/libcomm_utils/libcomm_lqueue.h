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
 * sctp_lqueue.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_lqueue.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _UTILS_MC_LQUEUE_H_
#define _UTILS_MC_LQUEUE_H_
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <sys/uio.h>
#include "libcomm_common.h"
#include "libcomm_list.h"

typedef struct iovec* element_ptr;

struct mc_2int_item {
    int nid;
    int sid;
    struct mc_list_item item;
};

// A structure of list queue element,
//	consis of data pointer and data size.
//
struct mc_lqueue_element {
    element_ptr data;  // message data

    // Add data to the element.
    //
    void add(element_ptr e)
    {
        data = e;
    }
};

// A structure of list queue item,
//	consist of a queue element and a linked list item link itself to list.
//
struct mc_lqueue_item {
    // queue element
    //
    struct mc_lqueue_element element;
    // linked list item
    //
    struct mc_list_item item;
};

// A structure of fifo queue, constucted by a linked list,
//	we use it as buffer to cache message data received from sctp channel.
//
struct mc_lqueue {
    int8 is_empty;         // 1: no item in the queue, 0: item in the queue
    int8 is_full;          // 1: the queue is full (u_size >= size), 0: not full (u_size < size)
    uint32 count;          // number of queue item
    mc_list list;          // linked list
    unsigned long size;    // capacity of the queue
    unsigned long u_size;  // total size of items in the queue
};

// Get the size of queue item from queue item.
//
inline int mc_lqueue_item_size(struct mc_lqueue_item*);

// Get the size of queue item from list item.
//
int mc_lqueue_item_size(struct mc_list_item*);

// Add queue item element to list queue q.
//
int mc_lqueue_add(struct mc_lqueue* q, struct mc_lqueue_item* element);

// Remove queue item element from list queue q.
//
struct mc_lqueue_item* mc_lqueue_remove(struct mc_lqueue* q, struct mc_lqueue_item* element);

// Initialize list queue, set queue size equals to parameter size.
//
struct mc_lqueue* mc_lqueue_init(unsigned long size);

// Clear the list queue, remove all queue items in the queue.
//
struct mc_lqueue* mc_lqueue_clear(struct mc_lqueue* q);

// Print all data elements in the queue.
//
void mc_lqueue_print(int n, struct mc_lqueue* q);

#endif  //_UTILS_MC_QUEUE_H_
