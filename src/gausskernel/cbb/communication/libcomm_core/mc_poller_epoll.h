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
 * mc_poller_epoll.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_core/mc_poller_epoll.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _MC_POLLER_EPOLL_H_
#define _MC_POLLER_EPOLL_H_
#include <sys/epoll.h>
#include <stdint.h>
#include "libcomm_utils/libcomm_list.h"
#include "libcomm_utils/libcomm_cont.h"
#include "libcomm_common.h"

#define MC_POLLER_HAVE_ASYNC_ADD 1

#define MC_POLLER_MAX_EVENTS 512

#define MC_POLLER_FD_ID_OFFSET 32

#define MC_POLLER_FD_ID_MASK 0xffff

// Item of epoller list, including poller handler and list item.
//
struct mc_poller_hndl_item {
    struct mc_poller_hndl hndl;
    struct mc_list_item item;
};

struct mc_poller {
    int ep;                                           //  Current pollset.
    int nevents;                                      //  Number of events being processed at the moment.
    int index;                                        // Index of the event being processed at the moment.
    struct epoll_event events[MC_POLLER_MAX_EVENTS];  //  Events being processed at the moment.
};

#endif
