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
 * comm_proxy.h
 *     Include all param data structure for Socket API invokation in proxy mode
 *     example:
 *        @api: socket()
 *        @data structure: CommSocketParam
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_proxy.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef COMM_PROXY_H
#define COMM_PROXY_H

#include <unistd.h>

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/epoll.h>
#include <poll.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <mutex>
#include <condition_variable>
#include "comm_connection.h"
#include "utils/guc.h"

/*--------------------------Global Variables Declaretion-------------------------------*/
#define COMM_PROXY_DEBUG_LEVEL COMM_DEBUG1


/*--------------------------Data Structure Definition----------------------------------*/
/* - Unix accept() parameter & results */
typedef struct CommAcceptParam {
    int s_fd;
    struct sockaddr* s_addr;
    socklen_t* s_addrlen;

    int caller(Sys_accept fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd, s_addr, s_addrlen);

        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s comm_accept: accept new fd[%d]  from comm_server_fd[%d] errno:%d, %m",
            t_thrd.proxy_cxt.identifier, ret, s_fd, errno)));

       return ret;
    }
} CommAcceptParam;

typedef struct CommAccept4Param {
    int s_fd;
    struct sockaddr* s_addr;
    socklen_t* s_addrlen;
    int s_flags;

    int caller(Sys_accept4 fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd, s_addr, s_addrlen, s_flags);

        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s comm_accept: accept new fd[%d]  from comm_server_fd[%d] errno:%d, %m",
            t_thrd.proxy_cxt.identifier, ret, s_fd, errno)));

       return ret;
    }
} CommAccept4Param;

/* - Unix connect() parameter & results */
typedef struct CommConnectParam {
    int s_sockfd;
    const struct sockaddr *s_addr;
    socklen_t s_addrlen;

    int caller(Sys_connect fn, unsigned long int reqid = 0) {
        int ret = 0;

        ret = fn(s_sockfd, s_addr, s_addrlen);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("%s comm_connect: connect fd[%d], ret:%d, errno:%d, %m",
            t_thrd.proxy_cxt.identifier, s_sockfd, ret, errno)));

        return ret;
    }
} CommConnectParam;


/* - Unix send() parameter & results */
typedef struct CommSendParam {
    int s_fd;
    void *buff;
    size_t len;
    int flags;

    int s_ret;

    int caller(Sys_send fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd, buff, len, flags);
        if ((SECUREC_UNLIKELY(log_min_messages <= DEBUG1))) {
            ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                errmsg("%s reqid[%016lu] comm_send: send comm_server_fd[%d], buff_char:%c, ret:%d, errno:%d, %m",
                t_thrd.proxy_cxt.identifier, reqid, s_fd, ((char *)buff)[0], ret, errno)));
        }

        s_ret = ret;
        return ret;
    }
} CommSendParam;

/* - Unix recv() parameter & results */
typedef struct CommRecvParam {
    int s_fd;
    void *buff;
    size_t len;
    int flags;

    int s_ret;

    int caller(Sys_recv fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd, buff, len, flags);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] comm_recv: recv comm_server_fd[%d], buff_char:%c, ret:%d, errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, s_fd, ((char *)buff)[0], ret, errno)));

        s_ret = ret;
        return ret;
    }
} CommRecvaram;

/*--------------------------Comm Proxy data structure ---------------------------------*/

/* - Unix socket() parameter & results */
typedef struct CommSocketInvokeParam {
    int s_domain;
    int s_type;
    int s_protocol;

    int s_group_id;
    bool s_iserver;
    int s_fd; /* not use, only for template define */
    bool s_block;

    int caller(Sys_socket fn, unsigned long int reqid = 0) {
        int ret = fn(s_domain, s_type, s_protocol);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_socket: create new fd[%d], errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, ret, errno)));
        return ret;
    }
} CommSocketInvokeParam;

/* - Unix close() parameter & results */
typedef struct CommCloseInvokeParam {
    int s_fd;

    int caller(Sys_close fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_close: close fd[%d], ret:[%d], errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, s_fd, ret, errno)));
        return ret;
    }
} CommCloseInvokeParam;

/* - Unix shutdown() parameter & results */
typedef struct CommShutdownInvokeParam {
    int s_fd;
    int s_how;

    int caller(Sys_shutdown fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd, s_how);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_shutdown: shutdown fd[%d] how[%d], ret:[%d], errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, s_fd, s_how, ret, errno)));
        return ret;
    }
} CommShutdownInvokeParam;

/* - Unix accept() parameter & results */
typedef struct CommAcceptInvokeParam {
    int s_fd;
    struct sockaddr* s_addr;
    socklen_t* s_addrlen;

    /* for debug info */
    ThreadPoolCommunicator *comm;

    int caller(Sys_accept fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd, s_addr, s_addrlen);

        /* CommProxy Dfx support */
        if (g_comm_proxy_config.s_enable_dfx) {
            int save_err = errno;
            struct sockaddr_in local_sock;
            socklen_t len = sizeof(local_sock);
            comm->m_comm_api.getsockname_fn(s_fd, (struct sockaddr *)&local_sock, &len);
            char local_ip_str[INET_ADDRSTRLEN];
            int local_port = ntohs(local_sock.sin_port);
            inet_ntop(AF_INET, &local_sock.sin_addr, local_ip_str, sizeof(local_ip_str));

            struct sockaddr_in *sock = (struct sockaddr_in*)s_addr;
            char ip_str[INET_ADDRSTRLEN] = "uninitip";
            int port = -1;
            if (ret > 0) {
                port = ntohs(sock->sin_port);
                inet_ntop(AF_INET, &sock->sin_addr, ip_str, sizeof(ip_str));
            }
            errno = save_err;

            ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                errmsg("%s reqid[%016lu] sockreq_accept: accept new fd[%d]  from comm_server_fd[%d]:[%s:%d] "
                "with client:[%s:%d], errno:%d, %m",
                t_thrd.proxy_cxt.identifier, reqid, ret, s_fd, local_ip_str, local_port, ip_str, port, errno)));
        }

        return ret;
    }
} CommAcceptInvokeParam;

/* - Unix connect() parameter & results */
typedef struct CommConnectInvokeParam {
    int s_sockfd;
    const struct sockaddr *s_addr;
    socklen_t s_addrlen;

    /* for debug info */
    ThreadPoolCommunicator *comm;

    int caller(Sys_connect fn, unsigned long int reqid = 0) {
        /*
         * we need to identify whether it's local or remote
         * if local, we connect direct, and create a linux kernel fd
         * if remote, we set it to unblock, and crete a libos fd
         */
        int ret = 0;

        struct sockaddr_in *sockaddr = (struct sockaddr_in*)s_addr;
        uint32 ip = sockaddr->sin_addr.s_addr;
        IPAddrType ip_type = CommLibNetGetIPType(ip);

        /* GaussDB
         * in libnet, will both call posix api and libnet api
         * but we cannot do this because 
         * it will send 2 type protocol packet, with different port
         * then, connection build success depende which ack received first
         * it's an uncertain result
         */
        if (ip_type == IpAddrTypeOther) {
            comm->SetBlockMode(s_sockfd, false);
            /* now, the fd is a libos fd, we don't want to call the kernel interface on it, so remove host type */
            CommLibNetSetFdType(s_sockfd, CommFdTypeDelHost);
            ret = fn(s_sockfd, s_addr, s_addrlen);
        } else {
            CommLibNetSetFdType(s_sockfd, CommFdTypeSetHost);
            ret = g_comm_controller->m_default_comm_api.connect_fn(s_sockfd, s_addr, s_addrlen);
        }

        /* CommProxy Debug support */
        if (g_comm_proxy_config.s_enable_dfx) {
            int save_err = errno;
            struct sockaddr_in *sock = (struct sockaddr_in*)s_addr;
            int port = ntohs(sock->sin_port);
            char ip_str[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &sock->sin_addr, ip_str, sizeof(ip_str));

            struct sockaddr_in local_sock;
            socklen_t len = sizeof(local_sock);
            if (ip_type == IpAddrTypeOther) {
                comm->m_comm_api.getsockname_fn(s_sockfd, (struct sockaddr *)&local_sock, &len);
            } else {
                g_comm_controller->m_default_comm_api.getsockname_fn(s_sockfd, (struct sockaddr *)&local_sock, &len);
            }
            char local_ip_str[INET_ADDRSTRLEN] = "uninit";
            int local_port = -1;
            local_port = ntohs(local_sock.sin_port);
            inet_ntop(AF_INET, &local_sock.sin_addr, local_ip_str, sizeof(local_ip_str));

            errno = save_err;

            ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                errmsg("%s reqid[%016lu] sockreq_connect: connect fd[%d]  to server[%s:%d] with client[%s:%d], "
                "ret:%d, errno:%d, %m",
                t_thrd.proxy_cxt.identifier, reqid, s_sockfd, ip_str, port, local_ip_str, local_port, ret, errno)));
        }

        return ret;
    }
} CommConnectInvokeParam;

/* - Unix bind() parameter & results */
typedef struct CommBindInvokeParam {
    int s_fd;
    struct sockaddr* s_addr;
    socklen_t addrlen;

    int caller(Sys_bind fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd, s_addr, addrlen);

        if (g_comm_proxy_config.s_enable_dfx) {
            struct sockaddr_in *sock = (struct sockaddr_in*)s_addr;
            int port = ntohs(sock->sin_port);
            char ip_str[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &sock->sin_addr, ip_str, sizeof(ip_str));

            ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                errmsg("%s reqid[%016lu] sockreq_bind: bind comm_server_fd[%d] to[%s:%d] ret:%d errno:%d, %m",
                t_thrd.proxy_cxt.identifier, reqid, s_fd, ip_str, port, ret, errno)));
        }

        return ret;
    }
} CommBindInvokeParam;

/* - Unix listen() parameter & results */
typedef struct CommListenInvokeParam {
    int s_fd;
    int backlog;

    int caller(Sys_listen fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd, backlog);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_listen: listen comm_server_fd[%d] ret:%d, errno:%d, %m\n",
            t_thrd.proxy_cxt.identifier, reqid, s_fd, ret, errno)));

        return ret;
    }
} CommListenInvokeParam;

/* - Unix setsockopt() parameter & results */
typedef struct CommSetsockoptInvokeParam {
    int s_fd;
    int level;
    int optname;
    const void* optval;
    socklen_t optlen;

    int caller(Sys_setsockopt fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd, level, optname, optval, optlen);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_setsockopt: setsockopt fd[%d], level:%d, optname:%d ret:%d, errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, s_fd, level, optname, ret, errno)));

        return ret;
    }
} CommSetsockoptInvokeParam;

typedef struct CommGetsockoptInvokeParam {
    int s_fd;
    int level;
    int optname;
    void* optval;
    socklen_t* optlen;

    int caller(Sys_getsockopt fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd, level, optname, optval, optlen);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_getsockopt: getsockopt fd[%d], level:%d, optname:%d ret:%d, errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, s_fd, level, optname, ret, errno)));

        return ret;
    }
} CommGetsockoptInvokeParam;

typedef struct CommGetsocknameInvokeParam {
    int s_fd;
    struct sockaddr* s_server_addr;
    socklen_t* s_addrlen;

    int caller(Sys_getsockname fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd, s_server_addr, s_addrlen);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_getsockname: getsockname fd[%d] ret:%d, errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, s_fd, ret, errno)));

        return ret;
    }
} CommGetsocknameInvokeParam;

typedef struct CommGetpeernameInvokeParam {
    int s_fd;
    struct sockaddr* s_server_addr;
    socklen_t* s_addrlen;

    int caller(Sys_getpeername fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd, s_server_addr, s_addrlen);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_getpeername: getpeername fd[%d] ret:%d, errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, s_fd, ret, errno)));

        return ret;
    }
} CommGetpeernameInvokeParam;

typedef struct CommFcntlInvokeParam {
    int s_fd;
    int s_cmd;
    long s_arg;

    int caller(Sys_fcntl fn, unsigned long int reqid = 0) {
        int ret = fn(s_fd, s_cmd, s_arg);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_fcntl: fcntl fd[%d], cmd:%d, arg:%ld, ret:%d, errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, s_fd, s_cmd, s_arg, ret, errno)));

        return ret;
    }
} CommFcntlInvokeParam;

typedef struct CommEpollCtlInvokeParam {
    int s_epfd;
    int s_op;
    int s_fd;
    struct epoll_event* s_event;

    int caller(Sys_epoll_ctl fn, unsigned long int reqid = 0) {
        int ret = fn(s_epfd, s_op, s_fd, s_event);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_epollctl: epoll_ctl epoll_fd[%d], op[%d], fd[%d], ret:[%d], errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, s_epfd, s_op, s_fd, ret, errno)));

        return ret;
    }
}CommEpollCtlInvokeParam;

typedef struct CommPollInternalInvokeParam {
    int s_fd;
    int s_logical_fd;
} CommPollInternalInvokeParam;

/*
 * driven by comm's epoll, currently we have tree socket api need polling from comm-proxy thread
 * - 1. poll()
 * - 2. epoll_wait()
 */
typedef struct CommWaitPollParam {
    SocketRequestType s_wait_type;
    struct pollfd* s_fdarray;
    unsigned long s_nfds;
    int s_timeout;
    bool s_ctx_switch;

    /* result return code */
    int s_ret;

    int caller(Sys_poll fn, unsigned long int reqid = 0) {
        int ret = fn(s_fdarray, s_nfds, s_timeout);

    /*
     * comm_poll is only called in the PM thread and involved frequent interrupt signal response, so,
     * there is no need for printing log debugging information.
     */

        return ret;
    }

} CommWaitPollParam;

typedef struct CommWaitEpollWaitParam {
    SocketRequestType s_wait_type;
    int s_epollfd;
    struct epoll_event* s_events;
    int s_maxevents;
    int s_timeout;

    /* for epoll wakeup mode */
    boost::lockfree::queue<struct epoll_event*>* m_epoll_queue;
    sem_t* s_queue_sem;

    /* result return nevents */
    int s_nevents;

    int caller(Sys_epoll_wait fn, unsigned long int reqid = 0) {
        int ret = fn(s_epollfd, s_events, s_maxevents, s_timeout);
        ereport(DEBUG5, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_epoll_wait: epoll fd[%d], ret:[%d], errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, s_epollfd, ret, errno)));

        return ret;
    }

} CommWaitEpollWaitParam;

typedef struct CommEpollPWaitParam {
    int s_epollfd;
    struct epoll_event* s_events;
    int s_maxevents;
    int s_timeout;
    const sigset_t *s_sigmask;

    int caller(Sys_epoll_pwait fn, unsigned long int reqid = 0) {
        int ret = fn(s_epollfd, s_events, s_maxevents, s_timeout, s_sigmask);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_epoll_pwait: epoll fd[%d], ret:[%d], errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, s_epollfd, ret, errno)));

        return ret;
    }

} CommEpollPWaitParam;

typedef struct CommEpollCreateParam {
    int size;
    int groupid;
    int caller(Sys_epoll_create fn, unsigned long int reqid = 0) {
        int ret = fn(size);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_epoll_create: ret:[%d], errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, ret, errno)));

        return ret;
    }
} CommEpollCreateParam;

typedef struct CommEpollCreate1Param {
    int flag;
    int groupid;
    int caller(Sys_epoll_create1 fn, unsigned long int reqid = 0) {
        int ret = fn(flag);
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s reqid[%016lu] sockreq_epoll_create1: ret:[%d], errno:%d, %m",
            t_thrd.proxy_cxt.identifier, reqid, ret, errno)));

        return ret;
    }
} CommEpollCreate1Param;

#endif /* COMM_PROXY_H */
