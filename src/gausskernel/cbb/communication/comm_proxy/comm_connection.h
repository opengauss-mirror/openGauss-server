/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * comm_socket.h
 *     Include all param data structure for Socket API invokation in proxy mode
 *     example:
 *        @api: socket()
 *        @data structure: CommSocketParam
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_connection.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef COMM_CONNECTION_H
#define COMM_CONNECTION_H
#include <sys/eventfd.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include "communication/commproxy_interface.h"

typedef int (*Sys_socket)(int domain, int type, int protocol);
typedef int (*Sys_accept)(int s, struct sockaddr*, socklen_t*);
typedef int (*Sys_accept4)(int s, struct sockaddr*, socklen_t*, int flags);
typedef int (*Sys_bind)(int s, const struct sockaddr*, socklen_t);
typedef int (*Sys_listen)(int s, int backlog);
typedef int (*Sys_connect)(int s, const struct sockaddr* name, socklen_t namelen);
typedef int (*Sys_getpeername)(int s, struct sockaddr* name, socklen_t* namelen);
typedef int (*Sys_getsockname)(int s, struct sockaddr* name, socklen_t* namelen);
typedef int (*Sys_setsockopt)(int s, int level, int optname, const void* optval, socklen_t optlen);
typedef int (*Sys_getsockopt)(int s, int level, int optname, void* optval, socklen_t* optlen);
typedef int (*Sys_close)(int fd);
typedef int (*Sys_shutdown)(int fd, int how);
typedef pid_t (*Sys_fork)(void);
typedef ssize_t (*Sys_read)(int fd, void* mem, size_t len);
typedef ssize_t (*Sys_write)(int fd, const void* data, size_t len);
typedef int (*Sys_fcntl)(int fd, int cmd, ...);
typedef int (*Sys_epoll_create)(int size);
typedef int (*Sys_epoll_create1)(int flags);
typedef int (*Sys_epoll_ctl)(int epfd, int op, int fd, struct epoll_event* event);
typedef int (*Sys_epoll_wait)(int epfd, struct epoll_event* events, int maxevents, int timeout);
typedef int (*Sys_epoll_pwait)(int epfd, struct epoll_event* events, int maxevents, int timeout, const sigset_t *sigmask);
typedef int (*Sys_sigaction)(int signum, const struct sigaction* act, struct sigaction* oldact);
typedef int (*Sys_poll)(struct pollfd* fds, nfds_t nfds, int timeout);
typedef ssize_t (*Sys_send)(int sockfd, const void* buf, size_t len, int flags);
typedef ssize_t (*Sys_recv)(int sockfd, void* buf, size_t len, int flags);

extern int gs_api_init(gs_api_t* gs_api, phy_proto_type_t proto);
extern void comm_init_api_hook();


template <typename Req_T, typename Fn_T> int comm_socket_call(Req_T* param, Fn_T fn);
typedef int (*CommApiCheckHookPre)(void *param, CommConnType* type);
typedef int (*CommApiCheckHookEnd)(void *ret, CommConnType* type);
extern CommApiCheckHookPre g_comm_api_hooks_pre[sockreq_max];
extern CommApiCheckHookEnd g_comm_api_hooks_end[sockreq_max];

#define COMM_API_HOOK_PRE(x, p, c) \
    do { \
        if (g_comm_api_hooks_pre[x] != NULL) { \
            int err = g_comm_api_hooks_pre[x](&p, &c); \
            if (err < 0) { \
                errno = -(err); \
                return -1; \
            } \
        } \
    } while(0);

#define COMM_API_HOOK_END(x, r, c) \
    do { \
        if (g_comm_api_hooks_end[x] != NULL) { \
            int err = g_comm_api_hooks_end[x](&r, &c); \
            if (err < 0) { \
                errno = -(err); \
                return -1; \
            } \
        } \
    } while(0);


#endif /* end tag of COMM_CONNECTION_H */
