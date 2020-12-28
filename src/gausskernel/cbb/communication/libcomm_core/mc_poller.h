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
 * mc_poller.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_core/mc_poller.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _MC_POLLER_H_
#define _MC_POLLER_H_

#define MC_POLLER_IN 1
#define MC_POLLER_OUT 2
#define MC_POLLER_ERR 3

#include "libcomm_utils/libcomm_err.h"
#include "mc_poller_epoll.h"

//  Initialize poller.
//
int mc_poller_init(struct mc_poller* self);
//  Terminate poller.
//
void mc_poller_term(struct mc_poller* self);
//   Add fd to poller.
//
void mc_poller_add(struct mc_poller* self, int fd, int id);
//   Remove item from poller
//
int mc_poller_rm(struct mc_poller* self, int fd);
//   Set poller event as POLLIN.
//
void mc_poller_set_in(struct mc_poller* self, struct mc_poller_hndl* hndl);
//   Wait poller events happens.
//
int mc_poller_wait(struct mc_poller* self, int timeout);

// declaration of Poller handler list.
//
class mc_poller_hndl_list {
public:
    mc_poller_hndl_list();
    ~mc_poller_hndl_list();
    struct mc_poller* get_poller();
    int get_socket_count();
    int init();
    int add_fd(struct sock_id* fd_id);
    int del_fd(struct sock_id* fd_id);
    // Disable copy construction and assignment.
    mc_poller_hndl_list(const mc_poller_hndl_list&) = delete;
    const mc_poller_hndl_list& operator=(const mc_poller_hndl_list&) = delete;
    struct mc_poller* m_poller;

private:
    pthread_mutex_t m_lock;
    int m_socket_count;
};

#endif  // _MC_POLLER_H_
