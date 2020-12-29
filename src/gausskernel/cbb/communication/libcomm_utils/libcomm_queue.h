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
 * sctp_queue.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_queue.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _UTILS_MC_QUEUE_H_
#define _UTILS_MC_QUEUE_H_
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <sys/uio.h>
#include <pthread.h>

// A structure of filo queue,
// for caching avaliable stream index for each sctp channel (each node).
//
struct mc_queue {
    int* data;
    int size;
    int count;
    int is_empty;
    int is_full;
    int head;
    int tail;
    pthread_mutex_t lock;

    int (*pop)(struct mc_queue*, int*);
    int (*push)(struct mc_queue*, int);
};

// initilaized the queue
//
int mc_queue_init(struct mc_queue* q, int size);

// pop the current usable stream index from the queue
//
int mc_queue_pop(struct mc_queue* q, int*);

// push an used stream index into the queue
//
int mc_queue_push(struct mc_queue*, int);

int mc_queue_add(struct mc_queue* q, int* e);

int* mc_queue_remove(struct mc_queue* q, int* e);

// destroy the queue
//
struct mc_queue* mc_queue_destroy(struct mc_queue* q);

// clear the queue
//
struct mc_queue* mc_queue_clear(struct mc_queue* q);

// print all the elements in the queue
//
void mc_queue_print(int n, struct mc_queue* q);

#endif  //_UTILS_MC_QUEUE_H_
