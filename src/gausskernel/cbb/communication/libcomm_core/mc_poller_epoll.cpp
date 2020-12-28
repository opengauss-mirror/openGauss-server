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
 * mc_poller_epoll.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_core/mc_poller_epoll.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <string.h>
#include <fcntl.h>
#include "mc_poller.h"
#include "libcomm_common.h"

int mc_poller_init(struct mc_poller* self)
{
#ifndef EPOLL_CLOEXEC
    int rc;
#endif

#ifdef EPOLL_CLOEXEC
    self->ep = epoll_create1(EPOLL_CLOEXEC);
#else
    // Size parameter is unused, we can safely set it to 1.
    //
    self->ep = epoll_create(1);
    rc = fcntl(self->ep, F_SETFD, FD_CLOEXEC);
    errno_assert(rc != -1);
#endif
    if (self->ep == -1) {
        if (errno == ENFILE || errno == EMFILE) {
            return -EMFILE;
        }
        errno_assert(false);
    }
    self->nevents = 0;
    self->index = 0;

    return 0;
}

void mc_poller_term(struct mc_poller* self)
{
    close(self->ep);
}

void mc_poller_add(struct mc_poller* self, int fd, int id)
{
    int rc;
    struct epoll_event ev;
    errno_t ss_rc = 0;

    //  Initialise the handle and add the file descriptor to the pollset.
    //
    ss_rc = memset_s(&ev, sizeof(ev), 0, sizeof(struct epoll_event));
    securec_check(ss_rc, "\0", "\0");
    ev.events = EPOLLIN;
    ev.data.u64 = (((uint64)(unsigned)(fd)) << MC_POLLER_FD_ID_OFFSET) + id;
    rc = epoll_ctl(self->ep, EPOLL_CTL_ADD, fd, &ev);
    errno_assert(rc == 0);
}

int mc_poller_rm(struct mc_poller* self, int fd)
{
    //  Remove the file descriptor from the pollset.
    //
    return epoll_ctl(self->ep, EPOLL_CTL_DEL, fd, NULL);
}

int mc_poller_wait(struct mc_poller* self, int timeout)
{
    int nevents;

    //  Clear all existing events.
    //
    self->nevents = 0;
    self->index = 0;

    //  Wait for new events.
    //
    for (;;) {
        nevents = epoll_wait(self->ep, self->events, MC_POLLER_MAX_EVENTS, timeout);
        if (mc_slow(nevents == -1 && errno == EINTR)) {
            continue;
        } else {
            break;
        }
    }
    errno_assert(self->nevents != -1);
    self->nevents = nevents;
    return 0;
}

// implementation of mc_poller_hndl_list
//
mc_poller_hndl_list::mc_poller_hndl_list()
{
    m_poller = NULL;
    m_lock = PTHREAD_MUTEX_INITIALIZER;
    m_socket_count = 0;
}
mc_poller_hndl_list::~mc_poller_hndl_list()
{
    LIBCOMM_FREE(m_poller, sizeof(struct mc_poller));
}
int mc_poller_hndl_list::init()
{
    LIBCOMM_PTHREAD_MUTEX_INIT(&m_lock, 0);

    LIBCOMM_MALLOC(m_poller, (sizeof(struct mc_poller)), mc_poller);

    if (NULL == m_poller) {
        return -1;
    }

    if (mc_poller_init(m_poller)) {
        return -1;
    }

    m_socket_count = 0;

    return 0;
}
struct mc_poller* mc_poller_hndl_list::get_poller()
{
    return m_poller;
}

int mc_poller_hndl_list::get_socket_count()
{
    int cnt = 0;
    LIBCOMM_PTHREAD_MUTEX_LOCK(&m_lock);
    cnt = m_socket_count;
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&m_lock);
    return cnt;
}

int mc_poller_hndl_list::add_fd(struct sock_id* fd_id)
{
    if (fd_id == NULL) {
        return -1;
    }
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_POLLER_ADD_FD_FAILED)) {
        LIBCOMM_ELOG(
            WARNING, "(poller add fd)\t[FAULT INJECTION]Failed to save socket[%d] version[%d].", fd_id->fd, fd_id->id);
        return -1;
    }
#endif

    LIBCOMM_PTHREAD_MUTEX_LOCK(&m_lock);
    // add the the socket to do epoll
    //
    mc_poller_add(m_poller, fd_id->fd, fd_id->id);
    m_socket_count++;

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&m_lock);
    return 0;
}

int mc_poller_hndl_list::del_fd(struct sock_id* fd_id)
{
    if (fd_id == NULL) {
        return -1;
    }

    LIBCOMM_PTHREAD_MUTEX_LOCK(&m_lock);

    int rc = mc_poller_rm(m_poller, fd_id->fd);

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&m_lock);

    return rc;
}
