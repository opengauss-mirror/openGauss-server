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
 * -------------------------------------------------------------------------
 *
 * comm_proxy.cpp
 *        TODO add contents
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_proxy.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <stdio.h>
#include <string.h>
#ifndef WIN32
#include <unistd.h>   /* for write() */
#include <sys/mman.h> /* for mmap/munmap */
#else
#include <io.h> /* for _write() */
#include <win32.h>
#endif
#ifdef __USE_NUMA
#include <numa.h>
#endif
#include "communication/commproxy_interface.h"
#include "communication/commproxy_dfx.h"
#include "comm_core.h"
#include "comm_proxy.h"
#include "comm_connection.h"


/* local function decleration */
static void comm_broadcast(SocketRequest* req, BroadcastRequest* br);
static bool is_need_broadcast(int fd, BroadcastRequest* br);
static bool is_uninit_request(SocketRequest* req);
static void comm_wait_broadcast_end(SocketRequest** req_arr, int num);

/*
 ************************************************************************************
 * export function definition
 ************************************************************************************
 */
int comm_proxy_socket(int domain, int type, int protocol)
{
    SocketRequest req;
    CommSocketInvokeParam param;
    CommProxyInvokeResult result;

    /* socket need some option infos */
    CommSocketOption *sock_opt = CommGetConnOption();
    if (sock_opt == NULL) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("sock_opt is NULL in comm_proxy_socket."),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));

        return -1;
    }

    /* init & set parameter */
    param.s_domain = domain;
    param.s_type = type;
    param.s_protocol = protocol;
    param.s_group_id = sock_opt->group_id;
    param.s_block = false;
    result.s_ret = -1;

    /* create invokation request packet */
    if (sock_opt->conn_role == CommConnSocketServer) {
        req.s_fd = SET_PROXY_GROUP(sock_opt->group_id);
    } else {
        req.s_fd = UNINIT_CONNECT_FD;
    }
    req.s_sockreq_type = sockreq_socket;
    req.s_request_data = (void*)&param;
    req.s_response_data = &result;
    req.s_done = false;
    SET_REQ_DFX_START(req);

    /* Blocked until we get the response of proxy side's execution */
    SubmitSocketRequest(&req);

    SET_REQ_DFX_DONE(req);

    return result.s_ret;
}

/*
 * GaussDB send/recv function in communicator mode, in communicator mode the caller
 * does not invoke OS send/recv directly, instead we put a package into tx/rx queue
 *
 * Important point:
 *    - invoked in worker thread
 */
ssize_t comm_proxy_send(int socketfd, const void* buffer, size_t length, int flags)
{
    Assert(length > 0);

    /* Do send work at proxy thread side */
    CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(socketfd);
    if (comm_sock == NULL) {
        Assert(0);
        errno = 2; /* ENOENT: No such file or directory */
        ereport(WARNING, (errmodule(MOD_COMM_PROXY),
            errmsg("fd[%d] is already close, CommSockDesc is NULL, send fail.", socketfd)));
        return -1;
    }

    int len = comm_worker_transport_send(comm_sock, (const char*)buffer, length);
    if (g_comm_proxy_config.s_enable_dfx && len > 0) {
        comm_sock->UpdateDataStatic(len, ChannelTX);
    }

    return len;
}

ssize_t comm_proxy_recv(int socketfd, void* buffer, size_t length, int flags)
{
    int recv_nbytes = -1;

    CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(socketfd);
    if (comm_sock == NULL) {
        recv_nbytes = -1;
        errno = 2; /* ENOENT: No such file or directory */
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("fd[%d] is already close, CommSockDesc is NULL, recv fail.",
            socketfd)));
        return recv_nbytes;
    }

    recv_nbytes = comm_worker_transport_recv(comm_sock, (char *)buffer, length);
    if (g_comm_proxy_config.s_enable_dfx && recv_nbytes > 0) {
        comm_sock->UpdateDataStatic(recv_nbytes, ChannelRX);
    }

    return recv_nbytes;
}
ssize_t comm_proxy_addr_recv(int sockfd, void *buf, size_t len, int flags)
{
    return comm_proxy_recv(sockfd, buf, len, flags);
}

int comm_proxy_close(int fd)
{
    SocketRequest req;
    CommCloseInvokeParam param;
    CommProxyInvokeResult result;

    /* set parameter */
    param.s_fd = fd;

    if (fd < 0) {
        return 0;
    }

    /* create invokation request packet */
    req.s_fd = fd;
    req.s_sockreq_type = sockreq_close;
    req.s_request_data = (void*)&param;
    req.s_response_data = &result;
    req.s_done = false;
    SET_REQ_DFX_START(req);
    
    CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(fd);
    (void)is_null_getcommsockdesc(comm_sock, "comm_proxy_close", fd);
    /* before close fd, must clear all recv queue and send queue */
    comm_sock->ClearSockQueue();

    if (comm_sock->m_fd_type == CommSockEpollFd) {
        /*
         * if fd is dummy epoll fd, we only need close real epfd
         * this sock communicator is NULL, we can not submit to proxy
         */
        CommReleaseSockDesc(&comm_sock);
        comm_sock = NULL;
        SET_REQ_DFX_DONE(req);
        return 0;
    }

    
    if (comm_sock->m_event_header.s_listener != NULL) {
        CommSockDesc* comm_epoll_sock = comm_sock->m_event_header.s_listener;
        comm_epoll_sock->m_epoll_list->delCommSock(comm_sock);
    }

    /* Blocked until we get the response of proxy side's execution */
    SubmitSocketRequest(&req);

    SET_REQ_DFX_DONE(req);

    return result.s_ret;
}

int comm_proxy_shutdown(int fd, int how)
{
    SocketRequest req;
    CommShutdownInvokeParam param;
    CommProxyInvokeResult result;

    /* set parameter */
    param.s_fd = fd;
    param.s_how = how;

    if (fd < 0) {
        return 0;
    }

    /* create invokation request packet */
    req.s_fd = fd;
    req.s_sockreq_type = sockreq_shutdown;
    req.s_request_data = (void*)&param;
    req.s_response_data = &result;
    req.s_done = false;
    SET_REQ_DFX_START(req);
    
    CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(fd);
    (void)is_null_getcommsockdesc(comm_sock, "comm_proxy_shutdown", fd);

    /* before close fd, must clear all recv queue and send queue */
    comm_sock->ClearSockQueue();

    if (comm_sock->m_fd_type == CommSockEpollFd) {
        /*
         * if fd is dummy epoll fd, we only need close real epfd
         * this sock communicator is NULL, we can not submit to proxy
         */
        close(comm_sock->m_epoll_real_fd);
        CommReleaseSockDesc(&comm_sock);
        comm_sock = NULL;
        SET_REQ_DFX_DONE(req);
        return 0;
    }

    
    if (comm_sock->m_event_header.s_listener != NULL) {
        CommSockDesc* comm_epoll_sock = comm_sock->m_event_header.s_listener;
        comm_epoll_sock->m_epoll_list->delCommSock(comm_sock);
    }

    /* Blocked until we get the response of proxy side's execution */
    SubmitSocketRequest(&req);

    SET_REQ_DFX_DONE(req);

    return result.s_ret;
}

int comm_proxy_accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen)
{
    SocketRequest req;
    CommAcceptInvokeParam param;
    CommProxyInvokeResult result;

    int real_fd = -1;
    CommSockDesc* sock_desc = g_comm_controller->FdGetCommSockDesc(sockfd);
    (void)is_null_getcommsockdesc(sock_desc, "comm_proxy_accept", sockfd);

    CommSockDesc* comm_sock_internal = NULL;
    if (sock_desc->m_listen_type == CommSockListenByEpoll || sock_desc->m_listen_type == CommSockListenByPoll) {
        /* loop all communicator to find incoming event */
        /* start with last communicator */
        int start_index = sock_desc->m_last_index + 1;
        if (start_index >= sock_desc->m_server_fd_cnt) {
            start_index = 0;
        }
        int real_index = 0;
        bool find = false;
        for (int k = start_index; k < sock_desc->m_server_fd_cnt + start_index; k++) {

            if (k >= sock_desc->m_server_fd_cnt) {
                real_index = k - sock_desc->m_server_fd_cnt;
            } else {
                real_index = k;
            }

            int current_fd = sock_desc->m_server_fd_arrays[real_index];
            comm_sock_internal = g_comm_controller->FdGetCommSockDesc(current_fd);
            (void)is_null_getcommsockdesc(comm_sock_internal, "comm_proxy_accept", current_fd);
            if (comm_sock_internal->m_server_income_ready_num > 0) {
                comm_atomic_fetch_sub_u32(&sock_desc->m_server_income_ready_num, 1);
                comm_atomic_fetch_sub_u32(&comm_sock_internal->m_server_income_ready_num, 1);
                find = true;
                sock_desc->m_last_index = real_index;
                real_fd = current_fd;
                break;
            }
            if (find) {
                break;
            }
        }

        if (!find) {
            Assert(0);
            ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("comm_proxy_accept: find error.")));
        }
    } else {
        Assert(0);
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("comm_proxy_accept: m_listen_type error.")));
    }

    /* set parameter */
    param.s_fd = real_fd;
    param.s_addr = addr;
    param.s_addrlen = addrlen;

    /* create invokation request packet */
    req.s_fd = real_fd;
    req.s_sockreq_type = sockreq_accept;
    req.s_request_data = (void*)&param;
    req.s_response_data = &result;
    req.s_done = false;
    SET_REQ_DFX_START(req);

    comm_static_inc(g_comm_controller->m_accept_static.s_prepare_num);
    /* Blocked until we get the response of proxy side's execution */
    SubmitSocketRequest(&req);

    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("comm_proxy_accept: get new fd[%d].", result.s_ret)));

    comm_static_inc(g_comm_controller->m_accept_static.s_endnum);

    SET_REQ_DFX_DONE(req);

    return result.s_ret;
}

int comm_proxy_accept4(int sockfd, struct sockaddr* addr, socklen_t* addrlen, int flags)
{
    return comm_proxy_accept(sockfd, addr, addrlen);
}

int comm_proxy_connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen)
{
    SocketRequest req;
    CommConnectInvokeParam param;
    CommProxyInvokeResult result;

    /* set parameter */
    param.s_sockfd = sockfd;
    param.s_addr = (const struct sockaddr*)addr;
    param.s_addrlen = addrlen;

    /* create invokation request packet */
    req.s_fd = sockfd;
    req.s_sockreq_type = sockreq_connect;
    req.s_request_data = (void*)&param;
    req.s_response_data = &result;
    req.s_done = false;
    SET_REQ_DFX_START(req);

    /* Blocked until we get the response of proxy side's execution */
    SubmitSocketRequest(&req);

    CommSockDesc *comm_sock = g_comm_controller->FdGetCommSockDesc(sockfd);
    if (comm_sock != NULL && comm_sock->m_block && req.s_errno == EINPROGRESS) {
        /* because we use noblock mode always, so need wait until connect success */
        while (!ConnectEnd(comm_sock->m_connect_status)) {
            pg_usleep(10);
        }

        if (comm_sock->m_connect_status == CommSockConnectSuccess) {
            result.s_ret = 0;
            errno = 0;
        } else {
            result.s_ret = -1;
            errno = comm_sock->m_errno;
        }
    }

    SET_REQ_DFX_DONE(req);

    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("comm_proxy_connect: fd[%d] connect end.", sockfd)));

    /*
     * connect fail:
     * if err is normal, we try again at comm-proxy
     * if other err, process by caller
     */

    return result.s_ret;
}

int comm_proxy_bind(int sockfd, const struct sockaddr* ServerAddr, socklen_t addrlen)
{
    SocketRequest req;
    CommBindInvokeParam param;
    CommProxyInvokeResult result;

    /* set parameter */
    param.s_fd = sockfd;
    param.s_addr = (struct sockaddr*)ServerAddr;
    param.addrlen = addrlen;

    /* create invokation request packet */
    req.s_fd = sockfd;
    req.s_sockreq_type = sockreq_bind;
    req.s_request_data = (void*)&param;
    req.s_response_data = &result;
    req.s_done = false;
    SET_REQ_DFX_START(req);

    /* Blocked until we get the response of proxy side's execution */
    SubmitSocketRequest(&req);

    SET_REQ_DFX_DONE(req);

    return result.s_ret;
}

int comm_proxy_listen(int sockfd, int backlog)
{
    SocketRequest req;
    CommListenInvokeParam param;
    CommProxyInvokeResult result;

    /* set parameter */
    param.s_fd = sockfd;
    param.backlog = backlog;

    /* create invokation request packet */
    req.s_fd = sockfd;
    req.s_sockreq_type = sockreq_listen;
    req.s_request_data = (void*)&param;
    req.s_response_data = &result;
    req.s_done = false;
    SET_REQ_DFX_START(req);

    /* Blocked until we get the response of proxy side's execution */
    SubmitSocketRequest(&req);

    SET_REQ_DFX_DONE(req);

    return result.s_ret;
}

int comm_proxy_setsockopt(int sockfd, int level, int optname, const void* optval, socklen_t optlen)
{
    SocketRequest req;
    CommSetsockoptInvokeParam param;
    CommProxyInvokeResult result;

    /* set parameter */
    param.s_fd = sockfd;
    param.level = level;
    param.optname = optname;
    param.optval = optval;
    param.optlen = optlen;

    /* create invokation request packet */
    req.s_fd = sockfd;
    req.s_sockreq_type = sockreq_setsockopt;
    req.s_request_data = (void*)&param;
    req.s_response_data = &result;
    req.s_done = false;
    SET_REQ_DFX_START(req);

    /* Blocked until we get the response of proxy side's execution */
    SubmitSocketRequest(&req);

    SET_REQ_DFX_DONE(req);

    return result.s_ret;
}

int comm_proxy_getsockopt(int sockfd, int level, int optname, void* optval, socklen_t* optlen)
{
    SocketRequest req;
    CommGetsockoptInvokeParam param;
    CommProxyInvokeResult result;

    /* set parameter */
    param.s_fd = sockfd;
    param.level = level;
    param.optname = optname;
    param.optval = optval;
    param.optlen = optlen;

    /* create invokation request packet */
    req.s_fd = sockfd;
    req.s_sockreq_type = sockreq_getsockopt;
    req.s_request_data = (void*)&param;
    req.s_response_data = &result;
    req.s_done = false;
    SET_REQ_DFX_START(req);

    /* Blocked until we get the response of proxy side's execution */
    SubmitSocketRequest(&req);
    SET_REQ_DFX_DONE(req);

    return result.s_ret;
}

int comm_proxy_getsockname(int sockfd, struct sockaddr* addr, socklen_t* addrlen)
{
    SocketRequest req;
    CommGetsocknameInvokeParam param;
    CommProxyInvokeResult result;

    /* set parameter */
    param.s_fd = sockfd;
    param.s_server_addr = addr;
    param.s_addrlen = addrlen;

    /* create invokation request packet */
    req.s_fd = sockfd;
    req.s_sockreq_type = sockreq_getsockname;
    req.s_request_data = (void*)&param;
    req.s_response_data = &result;
    req.s_done = false;
    SET_REQ_DFX_START(req);

    /* Blocked until we get the response of proxy side's execution */
    SubmitSocketRequest(&req);
    SET_REQ_DFX_DONE(req);

    return result.s_ret;
}

int comm_proxy_getpeername(int sockfd, struct sockaddr* addr, socklen_t* addrlen)
{
    SocketRequest req;
    CommGetpeernameInvokeParam param;
    CommProxyInvokeResult result;

    /* set parameter */
    param.s_fd = sockfd;
    param.s_server_addr = addr;
    param.s_addrlen = addrlen;

    /* create invokation request packet */
    req.s_fd = sockfd;
    req.s_sockreq_type = sockreq_getpeername;
    req.s_request_data = (void*)&param;
    req.s_response_data = &result;
    req.s_done = false;
    SET_REQ_DFX_START(req);

    /* Blocked until we get the response of proxy side's execution */
    SubmitSocketRequest(&req);
    SET_REQ_DFX_DONE(req);

    return result.s_ret;
}

int comm_proxy_fcntl(int sockfd, int cmd, ...)
{
    SocketRequest req;
    CommFcntlInvokeParam param;
    CommProxyInvokeResult result;
    long arg;
    va_list ap;

    va_start(ap, cmd);
    arg = va_arg(ap, long);
    va_end(ap);

    /* set parameter */
    param.s_fd = sockfd;
    param.s_cmd = cmd;
    param.s_arg = arg;

    /* create invokation request packet */
    req.s_fd = sockfd;
    req.s_sockreq_type = sockreq_fcntl;
    req.s_request_data = (void*)&param;
    req.s_response_data = &result;
    req.s_done = false;
    SET_REQ_DFX_START(req);

    /* HACK: all F_SETFD set to close exec, gaussdb is multi threads, not multi process, ignore it */
    if (cmd == F_SETFD || cmd == F_GETFD) {
        SET_REQ_DFX_DONE(req);
        return 0;
    }

    /*
     * HACK:
     *     all F_SETFL in gaussdb is used to set block or noblock
     *     we use m_block to simulate user layer conn block mode, and proxy conn real is noblock
     *     so we return now
     */
    if (cmd == F_SETFD || cmd == F_SETFL) {
        CommSockDesc *comm_sock = g_comm_controller->FdGetCommSockDesc(sockfd);
        (void)is_null_getcommsockdesc(comm_sock, "comm_proxy_fcntl", sockfd);
        bool save_block = comm_sock->m_block;
        /* set flag is set by get flag value, so flag is real block value */
        if (arg & O_NONBLOCK) {
            comm_sock->m_block = false;
        } else {
            comm_sock->m_block = true;
        }
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("comm_proxy_fcntl change fd[%d] from %s -> %s.",
            sockfd,
            save_block ? "block" : "nonblock", 
            comm_sock->m_block ? "block" : "nonblock")));
        save_block = comm_sock->m_block;

        SET_REQ_DFX_DONE(req);
        return 0;
    }

    /* Blocked until we get the response of proxy side's execution */
    SubmitSocketRequest(&req);


    SET_REQ_DFX_DONE(req);

    return result.s_ret;
}

int comm_proxy_poll(struct pollfd* fdarray, unsigned long nfds, int timeout)
{
    CommWaitPollParam param;
    param.s_wait_type = sockreq_poll;
    param.s_fdarray = fdarray;
    param.s_nfds = nfds;
    param.s_timeout = timeout;
    param.s_ret = -1;

    param.s_ctx_switch = true;

    CommPollOption *option = CommGetPollOption();
    if (option != NULL) {
        param.s_ctx_switch = option->ctx_switch;
        if (param.s_nfds == 1) {
            /* now, we only support 1 fd to check threshold, if more than one, we need a set of thresholds */
            CommSockDesc *comm_sock = g_comm_controller->FdGetCommSockDesc(param.s_fdarray[0].fd);
            (void)is_null_getcommsockdesc(comm_sock, "comm_proxy_poll", param.s_fdarray[0].fd);
            comm_sock->SetPollThreshold(ChannelRX, option->threshold);
        }
    }

    g_comm_controller->ProxyWaitProcess((SocketRequestType*)&param);

    if (option != NULL) {
        if (param.s_nfds == 1) {
            /* now, we only support 1 fd to check threshold, if more than one, we need a set of thresholds */
            CommSockDesc *comm_sock = g_comm_controller->FdGetCommSockDesc(param.s_fdarray[0].fd);
            (void)is_null_getcommsockdesc(comm_sock, "comm_proxy_poll", param.s_fdarray[0].fd);
            comm_sock->SetPollThreshold(ChannelRX, 0);
        }
    }

    return param.s_ret;
}

int comm_proxy_epoll_create(int size)
{
    /*
     * This parameter is used only for identification  and does not provide real epoll processing.
     * The fds that added to this epfd will find their own communicator's epfd to add and listened.
     */
    int fd = g_comm_controller->GetNextDummyFd();

    SockDescCreateAttr sock_attr = {
        .type = CommSockEpollFd,
        .comm = NULL,
        .parent = NULL
    };
    sock_attr.extra_data.epoll.size = size;

    CommSockDesc* comm_sock = CommCreateSockDesc(fd, &sock_attr);

    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("create epoll fd:[logical:%d, real:%d].",
        fd, comm_sock->m_epoll_real_fd)));

    comm_sock = NULL;
    return fd;
}

int comm_proxy_epoll_create1(int flag)
{
    return comm_proxy_epoll_create(1);
}

int comm_proxy_epoll_ctl(int epfd, int op, int fd, struct epoll_event* event)
{
    SocketRequest req;
    CommEpollCtlInvokeParam param;
    CommProxyInvokeResult result;
    bool need_setevent = false;

    /* set parameter */
    param.s_epfd = epfd;
    param.s_op = op;
    param.s_fd = fd;
    param.s_event = event;

    struct epoll_event new_ev = {0};
    CommSockDesc* comm_sock_fd = g_comm_controller->FdGetCommSockDesc(fd);
    (void)is_null_getcommsockdesc(comm_sock_fd, "comm_proxy_epoll_ctl", fd);
    CommSockDesc* comm_sock_epollfd = g_comm_controller->FdGetCommSockDesc(epfd);

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    if (comm_sock_epollfd != NULL && comm_sock_epollfd->m_fd_type == CommSockEpollFd) {

        if (g_comm_controller->m_epoll_mode == CommEpollModeWakeup) {
            param.s_epfd = comm_sock_fd->m_communicator->m_epoll_fd;

            if (op == EPOLL_CTL_DEL) {
                if (comm_sock_fd->m_not_need_del_from_epoll) {
                    comm_sock_fd->m_event_header.s_event_type = EventUknown;
                    return 0;
                }
                need_setevent = true;
            } else if (op == EPOLL_CTL_ADD) {
                comm_sock_fd->m_event_header.s_event_type = EventThreadPoolListen;
                comm_sock_fd->m_event_header.s_origin_event_data_ptr = event->data;
                comm_sock_fd->m_event_header.s_listener = comm_sock_epollfd;
                if (comm_sock_fd->GetQueueCnt(ChannelRX) > 0) {
                    /* sock already recv data, we can direct wakeup epollwait */
                    struct epoll_event* listen_event = NULL;
                    listen_event = (struct epoll_event*)comm_malloc(sizeof(struct epoll_event));
                    listen_event->events = EPOLLIN;
                    listen_event->data.ptr = &comm_sock_fd->m_event_header;
                    bool need_post = comm_sock_epollfd->m_epoll_queue->empty();
                    comm_sock_epollfd->m_epoll_queue->push(listen_event);
                    if (need_post) {
                        sem_post(&comm_sock_epollfd->m_epoll_queue_sem);
                    }
                    comm_sock_fd->m_event_header.s_event_type = EventRecv;
                }
                new_ev.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
                new_ev.data.ptr = &comm_sock_fd->m_event_header;
                param.s_event = &new_ev;
            }
        } else {    
            /* replace epoll fd with really communicator epoll fd of data fd */
            if (op == EPOLL_CTL_DEL) {
                comm_sock_epollfd->m_epoll_list->delCommSock(comm_sock_fd);
                comm_sock_fd->m_listen_type = CommSockListenNone;
                switch (comm_sock_fd->m_fd_type)
                {
                    case CommSockServerFd: {
                    } break;

                    case CommSockCommServerFd: {
                        Assert(0);
                    } break;

                    case CommSockFakeLibosFd: {
                        Assert(0);
                    } break;

                    /* thread pool data fd */
                    case CommSockServerDataFd:
                    case CommSockClientDataFd: {
                        comm_sock_fd->m_event_header.s_listener = NULL;
                        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("comm_proxy_epoll epoll fd[%d] del fd[%d].",
                            epfd, fd)));
                        return 0;
                    } break;

                    default : {
                        Assert(0);
                        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("unknown request.")));
                    }
                }
            } else if (op == EPOLL_CTL_ADD) {
                comm_sock_epollfd->m_epoll_list->addCommSock(comm_sock_fd);

                /*
                 * we use listen type to distinguish whether it is a user epoll listen or comm_proxy internal listen
                 * for comm_proxy layer, all fd are added to epoll
                 * for user layer, fds are only operate by epoll_list
                 * normal case, we listen with EPOLLIN
                 * specially, if connect is building, we need add listen with EPOLLOUT by user, once we get EPOLLOUT and build success,
                 *   immediately we mod it to EPOLLIN
                 */
                comm_sock_fd->m_listen_type = CommSockListenByEpoll;
                switch (comm_sock_fd->m_fd_type)
                {
                    case CommSockServerFd: {
                        /* all sub serverfd use logical serverfd event */
                        comm_sock_fd->m_event_header.s_event_type = EventListen;
                        comm_sock_fd->m_event_header.s_origin_event_data_ptr = event->data;
                        comm_sock_fd->m_event_header.s_origin_events = event->events;
                        comm_sock_fd->m_event_header.s_listener = comm_sock_epollfd;
                        if (event->events & EPOLLONESHOT) {
                            comm_sock_fd->m_event_header.s_epoll_oneshot_active = true;
                        }
                        new_ev.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
                        new_ev.data.ptr = &comm_sock_fd->m_event_header;
                        param.s_event = &new_ev;

                        /*
                         * for server fd add listen event, we need add communicator served cnt
                         * but now comm_sock is logical, so we need add in broadcast process
                         */
                    } break;

                    /* TODO: not use, libcomm server fd */
                    case CommSockCommServerFd: {
                        Assert(0);
                    } break;

                    case CommSockFakeLibosFd: {
                        Assert(0);
                    } break;

                    /* thread pool data fd */
                    case CommSockServerDataFd:
                    case CommSockClientDataFd: {
                        param.s_epfd = comm_sock_fd->m_communicator->m_epoll_fd;
                        /* we only modify event header because of we add epoll in accept/connect with EventRecv */
                        comm_sock_fd->m_event_header.s_origin_event_data_ptr = event->data;
                        comm_sock_fd->m_event_header.s_origin_events = event->events;
                        comm_sock_fd->m_event_header.s_listener = comm_sock_epollfd;
                        if (event->events & EPOLLONESHOT) {
                            comm_sock_fd->m_event_header.s_epoll_oneshot_active = true;
                        }

                        if (event->events & EPOLLIN) {
                            ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                                errmsg("comm_proxy_epoll epoll fd[%d] add fd[%d] with EPOLLIN\n", epfd, fd)));
                            return 0;
                        }
                        if (event->events & EPOLLOUT) {
                            /*
                             * we need add epoll event EPOLLOUT
                             * NOTICE: only one case, noblock connect inprogress, we add epoll out
                             * if other cases, we need redesign...
                             **/
                            if (comm_sock_fd->m_fd_type == CommSockClientDataFd && comm_sock_fd->m_connect_status == CommSockConnectInprogressNonBlock) {
                                comm_sock_fd->m_event_header.s_event_type = EventOut;
                                new_ev.events = EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
                                new_ev.data.ptr = &comm_sock_fd->m_event_header;
                                param.s_event = &new_ev;

                                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                                    errmsg("comm_proxy_epoll epoll fd[%d] add fd[%d] with connect EPOLLOUT.",
                                    epfd, fd)));
                            } else {
                                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                                    errmsg("comm_proxy_epoll epoll fd[%d] add fd[%d] with EPOLLOUT.", epfd, fd)));
                                return 0;
                            }

                        }
                    } break;

                    default : {
                        Assert(0);
                        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("unknown request.")));
                    }
                }
            } else if (op == EPOLL_CTL_MOD) {
                switch (comm_sock_fd->m_fd_type)
                {
                    case CommSockServerFd: {
                        Assert(0);
                    } break;

                    case CommSockCommServerFd: {
                        Assert(0);
                    } break;

                    case CommSockFakeLibosFd: {
                        Assert(0);
                    } break;

                    case CommSockServerDataFd:
                    case CommSockClientDataFd: {
                        param.s_epfd = comm_sock_fd->m_communicator->m_epoll_fd;
                        /* this step because of we add epoll in accept/connect with EventRecv */
                        comm_sock_fd->m_event_header.s_origin_event_data_ptr = event->data;
                        comm_sock_fd->m_event_header.s_origin_events = event->events;
                        comm_sock_fd->m_event_header.s_listener = comm_sock_epollfd;
                        if (event->events & EPOLLONESHOT) {
                            comm_sock_fd->m_event_header.s_epoll_oneshot_active = true;
                        }

                        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                            errmsg("comm_proxy_epoll epoll fd[%d] mod fd[%d]\n", epfd, fd)));
                        return 0;
                    } break;

                    default : {
                        Assert(0);
                        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("unknown request.")));
                    }
                }

            }
        }
    } else {
        /*
         * when we call communicator->AddCommEpoll, the epoll fd is internal
         * this epfd type is CommSockCommEpollFd
         * we do not need to use comm sock list
         */
    }

    /* create invokation request packet */
    req.s_fd = fd; /* if fd is serverfd, will enter broadcast */
    req.s_sockreq_type = sockreq_epollctl;
    req.s_request_data = (void*)&param;
    req.s_response_data = &result;
    req.s_done = false;
    SET_REQ_DFX_START(req);

    /* Blocked until we get the response of proxy side's execution */
    SubmitSocketRequest(&req);

    SET_REQ_DFX_DONE(req);

    if (need_setevent) {
	    comm_sock_fd->m_event_header.s_event_type = EventUknown;
    }

    return result.s_ret;
}

int comm_proxy_epoll_wait(int epfd, struct epoll_event* events, int maxevents, int timeout)
{
    CommWaitEpollWaitParam param;
    param.s_wait_type = sockreq_epollwait;
    param.s_epollfd = epfd;
    param.s_events = events;
    param.s_maxevents = maxevents;
    param.s_timeout = timeout;
    param.s_nevents = -1;
    param.m_epoll_queue = NULL;
    param.s_queue_sem = NULL;

    if (g_comm_controller->m_epoll_mode == CommEpollModeWakeup) {
        CommSockDesc* comm_sock_epollfd = g_comm_controller->FdGetCommSockDesc(epfd);
        if (comm_sock_epollfd != NULL) {
            if (comm_sock_epollfd->m_fd_type == CommSockEpollFd) {
                /* special fields for proxy communication mode */
                param.m_epoll_queue = comm_sock_epollfd->m_epoll_queue;
                param.s_queue_sem = &comm_sock_epollfd->m_epoll_queue_sem;
            }
        }
    }

    /* submit the request and polling the result */
    g_comm_controller->ProxyWaitProcess((SocketRequestType*)&param);

    if (param.s_nevents > 0) {
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("comm_proxy_epoll_wait[%d] return events[%d].",
            epfd, param.s_nevents)));
    }
    return param.s_nevents;
}

int comm_proxy_epoll_pwait(int epfd, struct epoll_event* events, int maxevents, int timeout, const sigset_t *sigmask)
{
    return comm_proxy_epoll_wait(epfd, events, maxevents, timeout);
}

pid_t comm_proxy_fork(void)
{
    return 0;
}

ssize_t comm_proxy_read(int s, void* mem, size_t len)
{
    return 0;
}

ssize_t comm_proxy_write(int s, const void* mem, size_t size)
{
    return 0;
}

int comm_proxy_sigaction(int signum, const struct sigaction* act, struct sigaction* oldact)
{
    return 0;
}

/*
 ****************************************************************************************
 * Proxy submit request/broadcast support
 ****************************************************************************************
 */
void SubmitSocketRequest(SocketRequest* req)
{
    BroadcastRequest broadcast_req = {
        .s_is_broadcast = false,
        .s_broadcast_comms = NULL,
        .s_broadcast_num = 0
    };

    /*
     * for sockreq_accept:
     * fd is CommSockCommServerFd, it dose not matc broadcast conditions
     */
    if (is_need_broadcast(req->s_fd, &broadcast_req)) {
        comm_broadcast(req, &broadcast_req);
        comm_free(broadcast_req.s_broadcast_comms);
    } else if (is_uninit_request(req)) {
        /* process socket for connect scene */
        CommSocketInvokeParam* param = (CommSocketInvokeParam*)req->s_request_data;
        CommProxyInvokeResult* result = (CommProxyInvokeResult*)req->s_response_data;
        int group_id = param->s_group_id;
        ThreadPoolCommunicator *communicator = g_comm_controller->GetCommunicatorByGroup(0, group_id);
        communicator->PutSockRequest(req);
        WaitCondPositive(req->s_done, 10);

        if (result->s_ret > 0) {
            SockDescCreateAttr sock_attr = {
                .type = CommSockClientDataFd,
                .comm = communicator,
                .parent = NULL,
            };
            sock_attr.extra_data.other = 0;
            CommCreateSockDesc(result->s_ret, &sock_attr);
        }
    } else {
        CommSockDesc* sock_desc = g_comm_controller->FdGetCommSockDesc(req->s_fd);
        Assert(sock_desc != NULL);
        (void)is_null_getcommsockdesc(sock_desc, "SubmitSocketRequest", req->s_fd);

        sock_desc->m_communicator->PutSockRequest(req);
        WaitCondPositive(req->s_done, 10);
    }

    errno = req->s_errno;
}

static inline void set_broadcast_comms(BroadcastRequest* br, ThreadPoolCommunicator** comms, int comm_nums)
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    br->s_broadcast_num = comm_nums;
    br->s_broadcast_comms =
            (ThreadPoolCommunicator **)comm_malloc(sizeof(ThreadPoolCommunicator *) * comm_nums);
    for (int i = 0; i < comm_nums; i++) {
        br->s_broadcast_comms[i] = comms[i];
    }
}

static bool is_need_broadcast(int fd, BroadcastRequest* br)
{
    /* we can support many type comm later, one speical fd one communicators type */
    if (AmIUnInitServerFd(fd)) {
        set_broadcast_comms(br,
            g_comm_controller->m_communicators[GET_PROXY_GROUP(fd)],
            g_comm_controller->m_communicator_nums[GET_PROXY_GROUP(fd)]);

        return true;
    }

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(fd);
    if (comm_sock != NULL && comm_sock->m_fd_type == CommSockServerFd) {
        if (comm_sock->m_server_fd_cnt > 0) {
            br->s_broadcast_num = comm_sock->m_server_fd_cnt;
            br->s_broadcast_comms =
                (ThreadPoolCommunicator **)comm_malloc(sizeof(ThreadPoolCommunicator *) * comm_sock->m_server_fd_cnt);

            for (int i = 0; i < comm_sock->m_server_fd_cnt; i++) {
                int fd_server = comm_sock->m_server_fd_arrays[i];
                CommSockDesc* comm_sock_server = g_comm_controller->FdGetCommSockDesc(fd_server);
                (void)is_null_getcommsockdesc(comm_sock_server, "is_need_broadcast", fd_server);
                br->s_broadcast_comms[i] = comm_sock_server->m_communicator;
            }

            return true;
        }
    }

    return false;
}

static bool is_uninit_request(SocketRequest* req) {
    /* other reqtype cases, fd < 0 is invalid */
    return (req->s_fd < 0) && (req->s_sockreq_type == sockreq_socket);
}

template <typename Req_T>
void comm_copy_request(SocketRequest* req_parent, SocketRequest* req, int i)
{
    req->s_response_data = (CommProxyInvokeResult*)comm_malloc(sizeof(CommProxyInvokeResult));
    req->s_request_data = (Req_T*)comm_malloc(sizeof(Req_T));
    int rc = memcpy_s(req->s_request_data, sizeof(Req_T), req_parent->s_request_data, sizeof(Req_T));
    securec_check(rc, "\0", "\0");
    if (req->s_sockreq_type != sockreq_socket) {
        Req_T* param = (Req_T*)req->s_request_data;
        CommSockDesc* sock_desc_server = g_comm_controller->FdGetCommSockDesc(req_parent->s_fd);
        (void)is_null_getcommsockdesc(sock_desc_server, "comm_copy_request", req_parent->s_fd);
        param->s_fd = sock_desc_server->m_server_fd_arrays[i];
    }
}

void comm_wait_broadcast_end(SocketRequest** req_arr, int num)
{
    int done_cnt;
    do {
        done_cnt = 0;
        for (int i = 0; i < num; i++) {
            if (req_arr[i]->s_done) {
                done_cnt++;
            } else {
                WaitCondPositive(req_arr[i]->s_done, 10);
                break;
            }
        }
    } while (done_cnt < num);

    return;
}

void comm_broadcast(SocketRequest* req, BroadcastRequest* br)
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    CommSockDesc* sock_desc_server = g_comm_controller->FdGetCommSockDesc(req->s_fd);
    
    int num = br->s_broadcast_num;
    SocketRequest** req_arr = NULL;
    /* for epoll_ctl */
    struct epoll_event *new_evs = NULL;

    if (req->s_sockreq_type == sockreq_epollctl) {
        new_evs = (struct epoll_event *)comm_malloc(sizeof(struct epoll_event) * num);
    }

    req_arr = (SocketRequest**)comm_malloc(sizeof(SocketRequest*) * num);
    for (int i = 0; i < num; i++) {
        req_arr[i] = (SocketRequest*)comm_malloc(sizeof(SocketRequest));
        int rc = memcpy_s(req_arr[i], sizeof(SocketRequest), req, sizeof(SocketRequest));
        securec_check(rc, "\0", "\0");
        req_arr[i]->s_request_data = NULL;
        req_arr[i]->s_response_data = NULL;
    }

    for (int i = 0; i < num; i++) {
        switch (req_arr[i]->s_sockreq_type) {
            case sockreq_close: {
                comm_copy_request<CommCloseInvokeParam>(req, req_arr[i], i);
            } break;
            case sockreq_socket: {
                comm_copy_request<CommSocketInvokeParam>(req, req_arr[i], i);
            } break;
            case sockreq_accept: {
                comm_copy_request<CommAcceptInvokeParam>(req, req_arr[i], i);
            } break;
            case sockreq_bind: {
                comm_copy_request<CommBindInvokeParam>(req, req_arr[i], i);
            } break;
            case sockreq_listen: {
                comm_copy_request<CommListenInvokeParam>(req, req_arr[i], i);
            } break;
            case sockreq_setsockopt: {                
                comm_copy_request<CommSetsockoptInvokeParam>(req, req_arr[i], i);
            } break;
            case sockreq_getsockopt: {                
                comm_copy_request<CommGetsockoptInvokeParam>(req, req_arr[i], i);
            } break;
            case sockreq_getsockname: {
                comm_copy_request<CommGetsocknameInvokeParam>(req, req_arr[i], i);
            } break;
            case sockreq_fcntl: {
                comm_copy_request<CommFcntlInvokeParam>(req, req_arr[i], i);
            } break;
            case sockreq_epollctl: {
                comm_copy_request<CommEpollCtlInvokeParam>(req, req_arr[i], i);

                /* this case only valid at epoll fd not serverfd */
                CommEpollCtlInvokeParam* param = (CommEpollCtlInvokeParam*)req_arr[i]->s_request_data;
                CommEpollCtlInvokeParam* origin_param = (CommEpollCtlInvokeParam*)req->s_request_data;
                (void)is_null_getcommsockdesc(sock_desc_server, "comm_broadcast", req->s_fd);
                int fd_server = sock_desc_server->m_server_fd_arrays[i];
                CommSockDesc *comm_inter_server = g_comm_controller->FdGetCommSockDesc(fd_server);
                (void)is_null_getcommsockdesc(comm_inter_server, "comm_broadcast", fd_server);
                param->s_epfd = comm_inter_server->m_communicator->m_epoll_fd;
                comm_inter_server->m_event_header.s_event_type = sock_desc_server->m_event_header.s_event_type;
                comm_inter_server->m_event_header.s_listener = sock_desc_server->m_event_header.s_listener;
                comm_inter_server->m_event_header.s_origin_event_data_ptr =
                    sock_desc_server->m_event_header.s_origin_event_data_ptr;
                comm_inter_server->m_listen_type = CommSockListenByEpoll;
                new_evs[i].events = origin_param->s_event->events;
                new_evs[i].data.ptr = &comm_inter_server->m_event_header;
                param->s_event = &new_evs[i];
                /* TODO: if del fd from epoll fd, we can check fd status now, skip submit */
            } break;
            case sockreq_poll: {                
                comm_copy_request<CommPollInternalInvokeParam>(req, req_arr[i], i);
                (void)is_null_getcommsockdesc(sock_desc_server, "comm_broadcast", req->s_fd);
                int fd_server = sock_desc_server->m_server_fd_arrays[i];
                CommSockDesc *comm_inter_server = g_comm_controller->FdGetCommSockDesc(fd_server);
                (void)is_null_getcommsockdesc(comm_inter_server, "comm_broadcast", fd_server);
                comm_inter_server->m_listen_type = CommSockListenByPoll;
                if (comm_inter_server->m_poll_status != CommSockPollUnRegist) {
                    req_arr[i]->s_done = true;
                    req_arr[i]->s_response_data->s_ret = 0;
                    continue;
                }
            } break;
            case sockreq_epollwait:
            default: {
                ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("unknown request.")));
            }
        }
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("broadcast request type[%d] to comm[%d]\n",
            req_arr[i]->s_sockreq_type, i)));
        br->s_broadcast_comms[i]->PutSockRequest(req_arr[i]);
    }

    comm_wait_broadcast_end(req_arr, num);

    /*
     * TODO: if some proxy have error???
     *
     * now we should create logical broadcast server fd
     */
    if (req->s_sockreq_type == sockreq_socket) {
        int fd = g_comm_controller->GetNextDummyFd();

        SockDescCreateAttr sock_attr = {
            .type = CommSockServerFd,
            .comm = NULL,
            .parent = NULL,
        };
        sock_attr.extra_data.server.comm_num = num;
        CommSockDesc* sock_desc = CommCreateSockDesc(fd, &sock_attr);

        ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("logical server[%d], with real fd:", fd)));
        int real_fd = 0;
        for (int i = 0; i < num; i++) {
            CommProxyInvokeResult* resp = (CommProxyInvokeResult*)req_arr[i]->s_response_data;
            real_fd = resp->s_ret;
            sock_desc->m_server_fd_arrays[i] = real_fd;
            SockDescCreateAttr sock_attr = {
                .type = CommSockCommServerFd,
                .comm = br->s_broadcast_comms[i],
                .parent = sock_desc,
            };
            sock_attr.extra_data.other = 0;
            CommCreateSockDesc(real_fd, &sock_attr);

            ereport(DEBUG1, (errmodule(MOD_COMM_PROXY), errmsg("{%d:%d}.", i, real_fd)));
        }

        req->s_done = req_arr[0]->s_done;
        req->s_errno = req_arr[0]->s_errno;
        CommProxyInvokeResult* resp_origin = (CommProxyInvokeResult*)req->s_response_data;
        resp_origin->s_ret = fd;
    } else {
        /* use first comm as logical comm */
        req->s_done = req_arr[0]->s_done;
        req->s_errno = req_arr[0]->s_errno;
        /* now, all socket api result is fd */
        if (req_arr[0]->s_response_data != NULL) {
            int rc = memcpy_s(req->s_response_data, sizeof(CommProxyInvokeResult),
                            req_arr[0]->s_response_data, sizeof(CommProxyInvokeResult));
            securec_check(rc, "\0", "\0");
        }
    }

    /* release memory */
    if (req->s_sockreq_type == sockreq_epollctl) {
        comm_free(new_evs);
    }

    for (int i = 0; i < num; i++) {
        if (req_arr[i]->s_response_data != NULL) {
            comm_free(req_arr[i]->s_response_data);
        }
        if (req_arr[i]->s_request_data != NULL) {
            comm_free(req_arr[i]->s_request_data);
        }
        comm_free(req_arr[i]);
    }
    comm_free(req_arr);

    if (req->s_sockreq_type == sockreq_close) {
        (void)is_null_getcommsockdesc(sock_desc_server, "comm_broadcast", req->s_fd);
        int sock_num = sock_desc_server->m_server_fd_cnt;
        for (int i = 0; i < sock_num; i++) {
            CommSockDesc *comm_sock = g_comm_controller->FdGetCommSockDesc(sock_desc_server->m_server_fd_arrays[i]);
            CommReleaseSockDesc(&comm_sock);
            sock_desc_server->m_server_fd_arrays[i] = -1;
        }
    }

    return;
}

void comm_change_event(CommSockDesc* comm_sock, EventType event_type, struct epoll_event* ev)
{
    comm_sock->m_event_header.s_event_type = event_type;

    switch (event_type) {
        case EventRecv: {
            ev->events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
            ev->data.ptr = &comm_sock->m_event_header;
        } break;

        case EventListen: {
            ev->events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
            ev->data.ptr = &comm_sock->m_event_header;
        } break;

        case EventOut: {
            ev->events = EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
            ev->data.ptr = &comm_sock->m_event_header;
        } break;

        default: {
            ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("unknown event_type.")));
        }
    }

    return;
}

static inline void comm_refresh_libnet_buffer(ThreadPoolCommunicator *comm)
{
#ifdef USE_LIBNET
    if(comm->m_proto_type == phy_proto_libnet){
        eth_dev_poll();
    }
#endif
}

/*
 *****************************************************************************************
 ** Proxy transport support
 *****************************************************************************************
 **/
SendStatus comm_one_pack_send_by_doublequeue(Packet* pack)
{
    Assert(pack != NULL);
    int nbytes = 0;
    int retry_loop = 0;
    const int max_retry_loop = 10;
    CommSockDesc *comm_sock = g_comm_controller->FdGetCommSockDesc(pack->sockfd);
    (void)is_null_getcommsockdesc(comm_sock, "comm_one_pack_send_by_doublequeue", pack->sockfd);
    ThreadPoolCommunicator *comm = comm_sock->m_communicator;

    while (pack->cur_off < pack->exp_size) {
        nbytes = comm->m_comm_api.send_fn(pack->sockfd, pack->outbuff + pack->cur_off, pack->exp_size - pack->cur_off, 0);
        if (nbytes == 0) {
            ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("%s fd:[%d] send data fail( 0 bytes):%s",
                t_thrd.proxy_cxt.identifier, pack->sockfd, pack->outbuff)));
            return SendStatusError;
        }
        if (nbytes < 0 && errno != EINTR && errno != EAGAIN) {
            ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("%s fd:[%d] send data(%s) fail: errno:%d, %m",
                t_thrd.proxy_cxt.identifier, pack->sockfd, pack->outbuff, errno)));
            return SendStatusError;
        }

        if (nbytes < 0) {
            comm_refresh_libnet_buffer(comm);
            retry_loop++;
            if (retry_loop > max_retry_loop) {
                ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("%s fd:[%d] send data(%s) half: errno:%d, %m",
                    t_thrd.proxy_cxt.identifier, pack->sockfd, pack->outbuff, errno)));
                return SendStatusPart;
            }
        }

        nbytes = (nbytes < 0) ? 0 : nbytes;
        pack->cur_off += nbytes;
    }

    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("%s fd[%d] send data length:%d (explen: %d) data:%s.",
        t_thrd.proxy_cxt.identifier,
        pack->sockfd,
        pack->cur_off,
        pack->exp_size,
        pack->outbuff)));

    return SendStatusSuccess;
}

/* transport */
int comm_worker_transport_recv(CommSockDesc* sock_desc, char *buffer, int exp_length)
{
    CommTransPort *ring_buffer = sock_desc->m_transport[ChannelRX];
    int recv_len = -1;
    errno = 0;

    while ((recv_len = ring_buffer->GetDataFromBuffer(buffer, exp_length)) == 0) {
        if (sock_desc->m_status == CommSockStatusClosedByPeer) {
            /* if connecttion is close by remote normal, errno is 0 */
            recv_len = sock_desc->m_errno == 0 ? 0 : -1;
            errno = sock_desc->m_errno;
            ring_buffer->PrintBufferDetail("worker recv fail, connection need close.");
            break;
        }

        if (!ring_buffer->NeedWait(sock_desc->m_block)) {    
            recv_len = -1;
            errno = EAGAIN;
            break;
        }
    }

    return recv_len;
}

void comm_proxy_transport_recv(CommSockDesc* sock_desc)
{
    SockRecvStatus status = RecvStatusReady;
    CommTransPort *buffer = sock_desc->m_transport[ChannelRX];
    ThreadPoolCommunicator *comm = sock_desc->m_communicator;

    /*
     * block and nonblock, recv return value are same.
     * < 0, error occurred,
     * = 0, remote close conn,
     * > 0, recv data
     */
    status = buffer->RecvDataToBuffer(sock_desc->m_fd, comm);
    if (status == RecvStatusSuccess || status == RecvStatusRetry) {
        COMM_DEBUG_EXEC(
            if (sock_desc->m_status == CommSockStatusUninit) {
                sock_desc->m_status = CommSockStatusProcessedData;
            }
        )

        return;
    }
    
    /* if connect has error, we process it in EPOLLERR */
    if (status == RecvStatusClose && sock_desc->m_status == CommSockStatusClosedByPeer) {
        /* conn is del from poll by recv process */
        sock_desc->m_errno = errno;

        return;
    }

    /* 
     * now, status must be RecvStatusError/RecvStatusClose
     * 
     * if (status == RecvStatusError || status == RecvStatusClose) {
     */
    /* TEMP: assume this connection has recv/send data, we catch error, or ignore */
    ereport(WARNING, (errmodule(MOD_COMM_PROXY),
        errmsg("%s fd:[%d] recv fail, recv status:%d, sock status:%d, errno:%d, %m\n",
        t_thrd.proxy_cxt.identifier, sock_desc->m_fd, status, sock_desc->m_status, errno)));

    sock_desc->m_errno = errno;
    comm->DelCommEpollInternal(sock_desc->m_fd);
    sock_desc->m_not_need_del_from_epoll = true;
    sock_desc->m_status = CommSockStatusClosedByPeer;

    return;
}

int comm_worker_transport_send(CommSockDesc* comm_sock, const char* buffer, int length)
{
    ThreadPoolCommunicator *comm = comm_sock->m_communicator;

    if (comm_sock->m_status == CommSockStatusClosedByPeer) {        
        /* 
         * if proxy discovers connection is closed will set status and errno, we return error. 
         * other case, recv error, that caller worker will close immediately.
         */
        errno = comm_sock->m_errno;
        return -1;
    }

    CommTransPort *transport = comm_sock->m_transport[ChannelTX];
    int send_len = transport->PutDataToBuffer(buffer, length);

    if (send_len < length) {
        if (comm_sock->m_block) {
            int temp_len = 0;
            send_len = send_len > 0 ? send_len : 0;
            transport->PrintBufferDetail("block mode, we wait all data send over");
            do {                
                pg_usleep(10);
                temp_len = transport->PutDataToBuffer(buffer + send_len, length - send_len);
                send_len += temp_len > 0 ? temp_len : 0;
                if (send_len != 0 && comm_sock->m_send_idle) {
                    comm_sock->m_send_idle = false;
                    comm->AddTailSock(comm_sock);
                }
            } while (send_len < length);            
            comm_sock->m_transport[ChannelTX]->PrintBufferDetail("block mode, we send all data over");
        } else {
            if (send_len == 0) {
                errno = EAGAIN;
                return -1;
            }
        }
    }

    if (g_comm_controller->m_ready_send_mode == CommProxySendModeReadyQueue) {
        if (comm_sock->m_send_idle) {
            comm_sock->m_send_idle = false;
            comm->AddTailSock(comm_sock);
        }
    }

    return send_len;
}


void comm_proxy_transport_send(CommSockDesc* sock_desc, ThreadPoolCommunicator *comm)
{
    SendStatus status = SendStatusReady;
    errno = 0;

    sock_desc->m_send_idle = true;

    status = sock_desc->m_transport[ChannelTX]->SendDataFromBuffer(sock_desc->m_fd, comm);
    if (status == SendStatusSuccess) {            
        COMM_DEBUG_EXEC(
            if (sock_desc->m_status == CommSockStatusUninit) {
                sock_desc->m_status = CommSockStatusProcessedData;
            }
        );
        return;
    }

    if (status == SendStatusPart) {
        /*
         * if send buffer overflow, comm proxy need retry send after epoll_wait()
         * must handle sock add to queue tail , because user may donot call comm_send anymore
         */
        if (g_comm_controller->m_ready_send_mode == CommProxySendModeReadyQueue) {
            if (sock_desc->m_send_idle) {
                sock_desc->m_send_idle = false;
                comm->AddTailSock(sock_desc);
            }
        }

        comm_refresh_libnet_buffer(comm);

        COMM_DEBUG_EXEC(
            if (sock_desc->m_status == CommSockStatusUninit) {
                sock_desc->m_status = CommSockStatusProcessedData;
            }
        )

        return;
    }

    sock_desc->m_status = CommSockStatusClosedByPeer;
    sock_desc->m_errno = errno;

    return;
}

void comm_proxy_send_ready_queue(ThreadPoolCommunicator* comm)
{
    CommSockDesc* sock_desc = NULL;

    /*
     * null: no any packet wait send
     * not null:
     *      sock send over: remove
     *      sock send part: readd next
     */
    uint32 ready_sock_num = comm->GetSockCnt();

    if (ready_sock_num == 0) {
        return;
    }

    uint32 current_cnt = 0;
    while ((sock_desc = comm->GetHeadSock()) != NULL) {
        sock_desc->m_process = CommSockProcessProxySending;
        comm_proxy_transport_send(sock_desc, comm);
        sock_desc->m_process = CommSockProcessProxySendend;
        current_cnt++;
        if (current_cnt >= ready_sock_num) {
            /* once send loop only process queue size sock to avoid deadloop */
            break;
        }
    }

    return;
}

void comm_proxy_send_polling(ThreadPoolCommunicator* comm)
{
    CommSockDesc* sock_desc = NULL;
    int fd;
    CommFdInfoNode *curFdInfo = comm->m_comm_fd_node_head.next;
    while (curFdInfo) {
        fd = curFdInfo->fd;
        curFdInfo = curFdInfo->next;

        sock_desc = g_comm_controller->FdGetCommSockDesc(fd);
        if (sock_desc == NULL || sock_desc->m_communicator != comm) {
            continue;
        }

        sock_desc->m_process = CommSockProcessProxySending;
        comm_proxy_transport_send(sock_desc, comm);
        sock_desc->m_process = CommSockProcessProxySendend;
    }

    return;
}

