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
 * comm_core.cpp
 *        TODO add contents
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_core.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <time.h>
#include <sys/time.h>
#ifdef __USE_NUMA
#include <numa.h>
#endif
#include <arpa/inet.h>

#include "communication/commproxy_interface.h"
#include "communication/commproxy_dfx.h"
#include "comm_core.h"
#include "comm_proxy.h"
#include "comm_connection.h"
#include "executor/executor.h"

#ifdef ENABLE_UT
#define static
#endif

static void ProcessControlRequest(ThreadPoolCommunicator* comm);
static void ProcessRecvRequest(ThreadPoolCommunicator* comm);
static void ProcessSendRequest(ThreadPoolCommunicator* comm);
static void ProcessControlRequestInternal(ThreadPoolCommunicator* comm, SocketRequest* sockreq);
static int GetSysCommErrno(ThreadPoolCommunicator* comm, int fd);

#define COMM_DEBUG COMM_DEBUG1

bool is_null_getcommsockdesc(const CommSockDesc* comm_sock, const char* fun_name, const int fd)
{
    if (comm_sock != NULL) {
        return false;
    }

    ereport(ERROR, (errmodule(MOD_COMM_PROXY),
        errcode(ERRCODE_SYSTEM_ERROR),
        errmsg("Failed to obtain the comm_sock in %s, fd:%d.", fun_name, fd),
        errdetail("N/A"),
        errcause("System error."),
        erraction("Contact Huawei Engineer.")));

    return true;
}


/*
 * @global variables definition
 */
CommController* g_comm_controller;

/*****************************************************************************
 *
 *       Primary communication proxy entry point
 *
 * To support proxy-communication work, we start serveral communication threads as "proxy" to do
 * network operations like send()/recv()/poll(), we devides this the work into three steps(looply),
 * (1). control request, like accept/bind/listen
 * (2). send request
 * (3). recv request
 *
 *****************************************************************************/
void ProcessProxyWork(ThreadPoolCommunicator* comm)
{
    int i;
    // 1. process control class socket request
    ProcessControlRequest(comm);

    // 2. process recv
    for (i = 0; i < g_comm_controller->m_proxy_recv_loop_cnt; i++) {
        ProcessRecvRequest(comm);
    }

    // 3. process send
    for (i = 0; i < g_comm_controller->m_proxy_send_loop_cnt; i++) {
        ProcessSendRequest(comm);
    }

}

/*
 * - brief. process control socket request
 */
static void ProcessControlRequest(ThreadPoolCommunicator* comm)
{
    SocketRequest* req = NULL;

    int init = 0;
    while (true) {

        req = comm->GetSockRequest();
        if (req == NULL) {
            break;
        }
        init++;
        /* process request */
        ProcessControlRequestInternal(comm, req);
    }

    return;
}

/*
 * - brief. process recv requst
 */
static void ProcessRecvRequest(ThreadPoolCommunicator* comm)
{
    int nevents = 0;

    struct epoll_event* recv_event = NULL;
    EventDataHeader* event = NULL;
    bool has_error = false;
    bool has_indata = false;
    bool has_out = false;


    nevents = comm->m_comm_api.epoll_wait_fn(comm->m_epoll_fd, comm->m_epoll_events, 2048, 0);

    for (int i = 0; i < nevents; i++) {
        has_error = false;
        has_indata = false;
        has_out = false;
        recv_event = &comm->m_epoll_events[i];
        if (recv_event->events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
            has_error = true;
        }

        if (recv_event->events & EPOLLIN) {
            has_indata = true;
        } else if (recv_event->events & EPOLLOUT) {
            has_out = true;
        } else {
            comm_static_inc(g_comm_controller->m_epoll_all_static.s_skipped_num);
            continue;
        }

        event = (EventDataHeader*)recv_event->data.ptr;
        switch (event->s_event_type) {
            case EventListen: {
                comm->ProcessOneRecvListenEvent(recv_event, has_indata, has_error);
            } break;

            case EventThreadPoolListen:
            case EventRecv: {
                comm->ProcessOneRecvDataEvent(recv_event, has_indata, has_error);
            } break;

            case EventOut: {
                comm->ProcessOneConnectDataEvent(recv_event, has_out, has_error);
            } break;

            default: {
                ereport(ERROR, (errmodule(MOD_COMM_PROXY),
                    errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("Unknown events %d in comm proxy.", event->s_event_type),
                    errdetail("N/A"),
                    errcause("System error."),
                    erraction("Contact Huawei Engineer.")));
                exit(-1);
            }
        }

    }

    comm_static_add(g_comm_controller->m_epoll_all_static.s_endnum, nevents);

    return;
}

/*
 * - brief. process send requst
 */
static void ProcessSendRequest(ThreadPoolCommunicator* comm)
{
    g_comm_controller->m_process_send_func(comm);

    return;
}

template <typename Req_T, typename Fn_T>
void comm_process_request(SocketRequest* sockreq, Fn_T fn)
{
    Req_T* param = (Req_T*)sockreq->s_request_data;
    CommProxyInvokeResult* result = (CommProxyInvokeResult*)sockreq->s_response_data;
    int ret = 0;
    ret = param->caller(fn, sockreq->s_req_id);

    /* Attach result */
    result->s_ret = ret;
    if (ret < 0) {
        sockreq->s_errno = errno;
    } else {
        sockreq->s_errno = 0;
    }
}

static void ProcessControlRequestInternal(ThreadPoolCommunicator* comm, SocketRequest* sockreq)
{
    Assert(sockreq != NULL);
    if (sockreq == NULL) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("sockreq is null in ProcessControlRequestInternal."),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
        return;
    }

    SET_REQ_DFX_PROCESS(*sockreq);
    ErrorLevel edge_debug_level = COMM_DEBUG1;
    ErrorLevel mid_debug_level = COMM_DEBUG1;
    /* reset errno */
    errno = 0;

    switch (sockreq->s_sockreq_type) {
        case sockreq_close: {
            CommCloseInvokeParam* param = (CommCloseInvokeParam*)sockreq->s_request_data;
            /* TODO:
             * if the fd is accept by epoll listen server
             * first, remove from epoll
             * second, close fd
             */
            CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(param->s_fd);
            if (comm_sock == NULL) {
                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("%s sockreq_close: fd[%d] had closed already.",
                    t_thrd.proxy_cxt.identifier, param->s_fd)));
                break;
            }
            switch (comm_sock->m_fd_type) {
                case CommSockServerFd: {
                    /* comm_proxy_close will call broadcast to CommSockCommServerFd */
                } break;

                case CommSockCommServerFd: {
                    if (comm_sock->m_listen_type == CommSockListenByEpoll) {
                        comm->DelCommEpollInternal(comm_sock->m_fd);
                    }
                } break;

                case CommSockEpollFd: {
                    /* close kernel epfd */
                    comm->m_comm_api.close_fn(comm_sock->m_epoll_real_fd);
                } break;

                case CommSockCommEpollFd: {
                    /* proxy end */
                } break;


                case CommSockServerDataFd:
                case CommSockClientDataFd: {
                    comm->RemoveServicedFd(comm_sock->m_fd);
                    if (comm_sock->m_not_need_del_from_epoll) {
                        break;
                    }
                    comm->DelCommEpollInternal(comm_sock->m_fd);
                } break;

                case CommSockFakeLibosFd: {

                } break;
                default: {
                    Assert(0);
                }
            }

            /* comm_sock need release before close api */
            CommReleaseSockDesc(&comm_sock);

            errno = 0;
            comm_process_request<CommCloseInvokeParam, Sys_close>(sockreq, comm->m_comm_api.close_fn);
        } break;
        case sockreq_socket: {
            comm_process_request<CommSocketInvokeParam, Sys_socket>(sockreq, comm->m_comm_api.socket_fn);

            CommProxyInvokeResult* result = (CommProxyInvokeResult*)sockreq->s_response_data;
            int fd = result->s_ret;
            if (fd == INVALID_FD) {
                break;
            }

            bool set_ret = comm->SetBlockMode(fd, false);
            if (!set_ret) {
                sockreq->s_errno = errno;
                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                    errmsg("%s sockreq_socket: set new fd[%d] noblock fail, errno: %d %m.",
                    t_thrd.proxy_cxt.identifier, fd, errno)));
                break;
            }

        } break;
        case sockreq_accept: {
            CommAcceptInvokeParam* param = (CommAcceptInvokeParam*)sockreq->s_request_data;
            CommProxyInvokeResult* result = (CommProxyInvokeResult*)sockreq->s_response_data;

            param->comm = comm;
            comm_process_request<CommAcceptInvokeParam, Sys_accept>(sockreq, comm->m_comm_api.accept_fn);

            /*
             * server fd build new connection success, need reset status to listen
             * if serverfd listen by poll, user will call poll() to add next
             * if serverfd listen by epoll, we need readd now, and del in epoll handle process before
             */
            CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(param->s_fd);
            (void)is_null_getcommsockdesc(comm_sock, "ProcessControlRequestInternal_accept", param->s_fd);
            if (comm_sock->m_listen_type == CommSockListenByPoll) {
                comm_sock->m_poll_status = CommSockPollUnRegist;
            } else if (comm_sock->m_listen_type == CommSockListenByEpoll) {
                /* first, re-add serverfd to epoll list for listen */
                if (comm->AddCommEpollInternal(comm_sock, EventListen)) {
                    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                        errmsg("sockreq_accept regist epoll event poll_ctl(add) fd:%d to epollfd:%d fail errno:%d, %m",
                        param->s_fd,
                        comm->m_epoll_fd,
                        errno)));
                } else {
                    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                        errmsg("sockreq_accept regist epoll event --epoll_ctl(add) fd:%d to epollfd:%d ...success.",
                        param->s_fd,
                        comm->m_epoll_fd)));
                }
            } else {
                Assert(0);
                ereport(ERROR, (errmodule(MOD_COMM_PROXY),
                    errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("sockreq_accept error, listen_type:%d.", comm_sock->m_listen_type),
                    errdetail("N/A"),
                    errcause("System error."),
                    erraction("Contact Huawei Engineer.")));
            }

            comm_static_inc(g_comm_controller->m_accept_static.s_ready_num);

            int fd = result->s_ret;
            if (fd == INVALID_FD) {
                break;
            }

            SockDescCreateAttr sock_attr = {
                .type = CommSockServerDataFd,
                .comm = comm,
                .parent = NULL,
            };
            sock_attr.extra_data.other = 0;
            CommSockDesc* data_comm_sock = CommCreateSockDesc(fd, &sock_attr);

            if (!CommLibNetIsLibosFd(comm->m_proto_type, fd)) {
                data_comm_sock->m_fd_type = CommSockFakeLibosFd;
                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                    errmsg("%s sockreq_accept: get new fd[%d] is a linux kernel fd.",
                    t_thrd.proxy_cxt.identifier, fd)));
                break;
            }

            ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("%s sockreq_accept: get new fd[%d] is a libos fd.",
                t_thrd.proxy_cxt.identifier, fd)));

            /* set no-block mode to use with libnet proxy */
            bool set_ret = comm->SetBlockMode(fd, false);
            if (!set_ret) {
                sockreq->s_errno = errno;
                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                    errmsg("%s sockreq_accept: set new fd[%d] noblock fail, errno: %d %m\n",
                    t_thrd.proxy_cxt.identifier, fd, errno)));
                break;
            }

            /* add to epoll list for recv */
            if (comm->AddCommEpollInternal(data_comm_sock, EventRecv)) {
                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                    errmsg("sockreq_accept regist recv event --epoll_ctl(add) fd:%d to epollfd:%d fail, errno:%d, %m.",
                    fd,
                    comm->m_epoll_fd,
                    errno)));
            } else {
                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                    errmsg("sockreq_accept regist recv event --epoll_ctl(add) fd:%d to epollfd:%d ...success.",
                    fd,
                    comm->m_epoll_fd)));
            }

            comm->AddServicedFd(fd);

        } break;
        case sockreq_connect: {
            CommConnectInvokeParam* param = (CommConnectInvokeParam*)sockreq->s_request_data;
            CommProxyInvokeResult* result = (CommProxyInvokeResult*)sockreq->s_response_data;

            param->comm = comm;
            comm_process_request<CommConnectInvokeParam, Sys_connect>(sockreq, comm->m_comm_api.connect_fn);

            int ret = result->s_ret;
            /*
             * if ret >= 0, connect must build success
             * if ret < 0, connect is not build success, this has two cases:
             *     block: need check errno
             *          1. INGRESS/INTR/WOULDBLOCK, these are normal result, further processing is required
             *          2. other case, means connection build failed, return
             *     noblock: it's normal if errno is INPROGRESS , other is fail. now we directly return with ret and errno.
             */
            int fd = param->s_sockfd;
            CommSockDesc *comm_sock = g_comm_controller->FdGetCommSockDesc(fd);
            (void)is_null_getcommsockdesc(comm_sock, "ProcessControlRequestInternal_connect", fd);
            if (ret >= 0) {
                /* if connection is linux channel, we use it as no proxy fd */
                if (!CommLibNetIsLibosFd(comm->m_proto_type, fd)) {
                    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("%s connect fd[%d] done, is a linux fd.",
                        t_thrd.proxy_cxt.identifier,
                        fd)));
                    comm_sock->m_fd_type = CommSockFakeLibosFd;
                    break;
                }

                /* if connection is libos channel, we add it to epoll list to recv */
                if (comm->AddCommEpollInternal(comm_sock, EventRecv)) {
                    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                        errmsg("sockreq_connect regist recv event epoll_ctl_add fd:%d to epollfd:%d fail errno:%d, %m",
                        fd,
                        comm->m_epoll_fd,
                        errno)));

                    comm_sock->m_connect_status = CommSockConnectFail;
                } else {
                    comm_sock->m_connect_status = CommSockConnectSuccess;
                    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                        errmsg("sockreq_connect regist recv event --epoll_ctl(add) fd:%d to epollfd:%d ...success.",
                        fd,
                        comm->m_epoll_fd)));
                }

                comm->AddServicedFd(fd);
            } else {
                if (comm_sock->m_block) {
                    /*
                     * block mode, we have to return a definite result
                     * if inprogress, we need add it to epoll list, and monitor ConnectEvent in proxy recv process
                     *      here we return a half status, so comm_proxy_connect will block to wait proxying result
                     * if other, we directly return fail
                     */
                    if (IgnoreErrorNo(sockreq->s_errno)) {
                        /* block mode, we need add epoll to monitor connect build success and notify user by condition */
                        comm_sock->m_connect_status = CommSockConnectInprogressBlock;
                        if (comm->AddCommEpollInternal(comm_sock, EventOut)) {
                            ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                                errmsg("sockreq_connect regist connect event --epoll_ctl(add) fd:%d to epollfd:%d fail"
                                "errno:%d, %m.",
                                fd,
                                comm->m_epoll_fd,
                                errno)));
                        } else {
                            ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                                errmsg("sockreqconnect regist connect event epoll_ctl_add fd:%d to epollfd:%d success",
                                fd,
                                comm->m_epoll_fd)));
                        }
                    } else {
                        comm_sock->m_connect_status = CommSockConnectFail;
                    }
                } else {
                    /*
                     * noblock mode, we return and monitor connect status by user self
                     * generally, POLLOUT/EPOLLOUT are used to do this by obtaining whether the link is writable
                     * TODO: current implementation, we assume all OUT events are used to check connect
                     */
                    comm_sock->m_connect_status = CommSockConnectInprogressNonBlock;
                }
            }
        } break;
        case sockreq_poll: {
            /*
             * NOTICE: only server fd will enter the case
             * for data fd, if use poll to monitor POLLIN/POLLOUT, we only check transport buffer
             * for server fd, we use epoll to simulate the implementation of the poll
             */
            CommPollInternalInvokeParam* param = (CommPollInternalInvokeParam*)sockreq->s_request_data;
            CommProxyInvokeResult* result = (CommProxyInvokeResult*)sockreq->s_response_data;

            int fd = param->s_fd;
            CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(fd);
            (void)is_null_getcommsockdesc(comm_sock, "ProcessControlRequestInternal_poll", fd);
            /*
             * server fd is logical fd, contains some phy fd
             * one accept action only build a connectin, and consume one phy server fd status
             * so other fds of logical server fd are still Registed status, we skip them
             * TODO: we can do this judgment at worker thread RegistPoll process
             */
            if (comm_sock->m_poll_status != CommSockPollUnRegist) {
                result->s_ret = 0;
                sockreq->s_errno = 0;
                break;
            }

            if (comm->AddCommEpollInternal(comm_sock, EventListen)) {
                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                    errmsg("sockreq_poll regist poll event --epoll_ctl(add) fd:%d to epollfd:%d fail, errno:%d, %m.",
                    fd,
                    comm->m_epoll_fd,
                    errno)));
                result->s_ret = -1;
                sockreq->s_errno = errno;
            } else {
                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                    errmsg("sockreq_poll regist poll event --epoll_ctl(add) fd:%d to epollfd:%d ...success.",
                    fd,
                    comm->m_epoll_fd)));
                comm_sock->m_poll_status = CommSockPollRegisted;
                result->s_ret = 0;
                sockreq->s_errno = 0;
            }
        } break;
        case sockreq_bind: {
            CommBindInvokeParam* param = (CommBindInvokeParam*)sockreq->s_request_data;

            /*
             * for server fd, we set REUSEADDR | REUSEPORT default
             * we listen one {ip:port} on four dies different threads
             */
            CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(param->s_fd);
            (void)is_null_getcommsockdesc(comm_sock, "ProcessControlRequestInternal_bind", param->s_fd);
            if (comm_sock->m_fd_type == CommSockCommServerFd) {
                int on = 1;
                comm->m_comm_api.setsockopt_fn(param->s_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &on, sizeof(on));
                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("sockreq_bind set fd[%d] reuseaddr |reuseport.",
                    param->s_fd)));
            }

            comm_process_request<CommBindInvokeParam, Sys_bind>(sockreq, comm->m_comm_api.bind_fn);
        } break;
        case sockreq_listen: {
            comm_process_request<CommListenInvokeParam, Sys_listen>(sockreq, comm->m_comm_api.listen_fn);
            CommListenInvokeParam* param = (CommListenInvokeParam*)sockreq->s_request_data;
            CommProxyInvokeResult* result = (CommProxyInvokeResult*)sockreq->s_response_data;
            int fd = param->s_fd;
            if (sockreq->s_errno == 98) {
                sockreq->s_errno = 0;
                result->s_ret = 0;
            }

            /*
             * listen addresses have some cases
             * for local address, like 127.0.0.1/localhost, they only respond to local connections, we use kernel listen
             * for local address, 192.168.x.x, this will be connected by local or remote, we use libos listen(also call kernel listen)
             * if connection is linux kernel channel, we use it as no proxy fd
             */
            if (!CommLibNetIsLibosFd(comm->m_proto_type, fd)) {
                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("%s listen fd[%d] done, change to linux fd.",
                    t_thrd.proxy_cxt.identifier,
                    fd)));
                CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(fd);
                (void)is_null_getcommsockdesc(comm_sock, "ProcessControlRequestInternal_listen", fd);
                comm_sock->m_fd_type = CommSockFakeLibosFd;
                break;
            }
        } break;
        case sockreq_shutdown: {
            comm_process_request<CommShutdownInvokeParam, Sys_shutdown>(sockreq, comm->m_comm_api.shutdown_fn);
        } break;
        case sockreq_setsockopt: {
            comm_process_request<CommSetsockoptInvokeParam, Sys_setsockopt>(sockreq, comm->m_comm_api.setsockopt_fn);
        } break;
        case sockreq_getsockopt: {
            comm_process_request<CommGetsockoptInvokeParam, Sys_getsockopt>(sockreq, comm->m_comm_api.getsockopt_fn);
        } break;
        case sockreq_getsockname: {
            comm_process_request<CommGetsocknameInvokeParam, Sys_getsockname>(sockreq, comm->m_comm_api.getsockname_fn);
        } break;
        case sockreq_fcntl: {
            comm_process_request<CommFcntlInvokeParam, Sys_fcntl>(sockreq, comm->m_comm_api.fcntl_fn);
        } break;
        case sockreq_epollctl: {
            comm_process_request<CommEpollCtlInvokeParam, Sys_epoll_ctl>(sockreq, comm->m_comm_api.epoll_ctl_fn);
            CommEpollCtlInvokeParam* param = (CommEpollCtlInvokeParam*)sockreq->s_request_data;
            /* now, for serverfd, we need add/sbu served cnt */
            if (param->s_op == EPOLL_CTL_ADD) {
                comm->AddServedFdCnt();
            } else if (param->s_op == EPOLL_CTL_DEL){
                comm->SubServedFdCnt(param->s_fd);
            } else {

            }
        } break;
        case sockreq_epollwait:
        default: {
            ereport(ERROR, (errmodule(MOD_COMM_PROXY),
                errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("unknown request %d.", sockreq->s_sockreq_type),
                errdetail("N/A"),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
        }
    }

    /* mark socket()-request is done to wake up client */
    sockreq->s_done = true;
    SET_REQ_DFX_READY(*sockreq);

    edge_debug_level = COMM_DEBUG1;
    mid_debug_level = COMM_DEBUG_DESC;
    return;
}

static int GetSysCommErrno(ThreadPoolCommunicator* comm, int fd)
{
    int optval;
    int save_err = 0;
    socklen_t optlen = sizeof(optval);
    int err = comm->m_comm_api.getsockopt_fn(fd, SOL_SOCKET, SO_ERROR, &optval, &optlen);
    {
        if (err < 0 || optval) {
            save_err = err < 0 ? errno : optval;
        }
    }
    return save_err;
}

/*
 *****************************************************************************************
 * CLASS -- ThreadPoolCommunicator
 *****************************************************************************************
 */
ThreadPoolCommunicator::ThreadPoolCommunicator(
    int group_id, int comm_id, int proxy_group_id, phy_proto_type_t proto_type)
{
    m_comm_id = comm_id;
    m_group_id = group_id;
    m_proxy_group_id = proxy_group_id;
    m_proto_type = proto_type;

    m_epoll_fd = INVALID_FD;
    m_epoll_events = NULL;
    m_serve_fd_num = 0;

    m_bind_core = -1;

    m_proxyer_status = CommProxyerUninit;

    /* we use comm_api to replace posix api */
    int ret = gs_api_init(&m_comm_api, m_proto_type);
    if (ret < 1) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("Communicator gs api init fail:%d, %m.", errno),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
    }
    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("Communicator gs api init success:%d.", m_proto_type)));

    comm_static_set(m_served_workers_num, 0);

    int rc = memset_s(&m_thread_status, sizeof(CommProxyThreadStatus),
            0, sizeof(CommProxyThreadStatus));

    m_thread_status.s_start_time = GetCurrentTimestamp();
    securec_check(rc, "\0", "\0");
}

ThreadPoolCommunicator::~ThreadPoolCommunicator()
{
    Assert(m_sockreq_queue == NULL);
    Assert(m_epoll_events == NULL);
}

void ThreadPoolCommunicator::ParseBindCores(int core_id)
{
    m_bind_core = core_id;
    return;
}

void ThreadPoolCommunicator::DownToDeInit()
{
    if (m_sockreq_queue != NULL) {
        delete m_sockreq_queue;
    }

    if (m_ready_sock_queue != NULL) {
        delete m_ready_sock_queue;
    }
}

void ThreadPoolCommunicator::StartUp(CommTransportMode transmode)
{

    m_sockreq_queue = new boost::lockfree::queue<SocketRequest*>(INIT_QUEUE_SIZE);
    m_ready_sock_queue = new boost::lockfree::queue<int>(INIT_QUEUE_SIZE);
    m_ready_sock_num = 0;
    m_trans_mode = transmode;

    m_comm_fd_node_head = {-1, 0 , NULL};

    if (m_trans_mode == CommModeDoubleQueue) {
        m_packet_buf[ChannelRX] = new boost::lockfree::stack<Packet*>(INIT_QUEUE_SIZE);
        m_packet_buf[ChannelTX] = new boost::lockfree::stack<Packet*>(INIT_QUEUE_SIZE);

        for (int i = 0; i < INIT_QUEUE_SIZE; i++) {
            Packet* pkt = CreatePacket(INVALID_FD, NULL, NULL, 512, PKT_TYPE_RECV);
            m_packet_buf[ChannelRX]->push(pkt);
            pkt = CreatePacket(INVALID_FD, NULL, NULL, 0, PKT_TYPE_SEND);
            m_packet_buf[ChannelTX]->push(pkt);
        }
    }

    NotifyStatus(CommProxyerStart);
    CommStartProxyer(&m_comm_proxy_thread_id, this);
}

void ThreadPoolCommunicator::NotifyStatus(CommProxyerStatus status)
{
    m_proxyer_status = status;
    m_proxyer_status_callback(this, status);
}

void ThreadPoolCommunicator::SetStatusCallback(ProxyerStatusCallbackFunc callback)
{
    m_proxyer_status_callback = callback;
}

SocketRequest* ThreadPoolCommunicator::GetSockRequest()
{
    SocketRequest* p = NULL;
    m_sockreq_queue->pop(p);
    return p;
}
void ThreadPoolCommunicator::PutSockRequest(SocketRequest* req)
{
    m_sockreq_queue->push(req);
    SET_REQ_DFX_PREPARE(*req);
}

void ThreadPoolCommunicator::CeateCommEpoll(int group_id)
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    int max_event_num = MAX_EVENT_NUM;
    m_epoll_fd = m_comm_api.epoll_create_fn(max_event_num);
    if (m_epoll_fd == INVALID_FD) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("Fail to create epoll for communicator[%d], check if the system is out of memory or "
                "limit on the total number of open files has been reached.", m_comm_id),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
        exit(-1);
    }

    SockDescCreateAttr sock_attr = {
        .type = CommSockCommEpollFd,
        .comm = this,
        .parent = NULL
    };
    sock_attr.extra_data.other = 0;
    CommCreateSockDesc(m_epoll_fd, &sock_attr);

    m_epoll_events = (struct epoll_event*)comm_malloc(sizeof(struct epoll_event) * max_event_num);
    if (m_epoll_events == NULL) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("Not enough memory for listener epoll."),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
        exit(-1);
    }
}

void ThreadPoolCommunicator::AddActiveWorkerCommSock(CommSockDesc* comm_sock)
{
    comm_static_inc(comm_sock->m_wakeup_cnt);
    comm_static_inc(m_served_workers_num);
    comm_static_atomic_inc(&g_comm_controller->m_active_worker_num);
    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
        errmsg("%s attach session[%d] to commproxy[%d-%d] woker[%d], now m_served_workers_num:[%d], "
        "all active worker[%d].",
        t_thrd.proxy_cxt.identifier,
        comm_sock->m_fd,
        m_group_id,
        m_comm_id,
        comm_sock->GetWorkerId(),
        m_served_workers_num,
        comm_static_atomic_get(&g_comm_controller->m_active_worker_num))));
}

void ThreadPoolCommunicator::RemoveActiveWorkerCommSock(CommSockDesc* comm_sock)
{
    comm_static_dec(m_served_workers_num);
    comm_static_atomic_inc(&g_comm_controller->m_active_worker_num);
    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
        errmsg("%s detach session[%d] from commproxy[%d-%d] woker[%d], now m_served_workers_num:[%d], "
        "all active worker[%d].",
        t_thrd.proxy_cxt.identifier,
        comm_sock->m_fd,
        m_group_id,
        m_comm_id,
        comm_sock->GetWorkerId(),
        m_served_workers_num,
        comm_static_atomic_get(&g_comm_controller->m_active_worker_num))));
}

void ThreadPoolCommunicator::AddServicedFd(int fd)
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    if (g_comm_controller->m_ready_send_mode == CommProxySendModePolling) {
        CommFdInfoNode *curFdInfo = (CommFdInfoNode*)comm_malloc(sizeof(CommFdInfoNode));
        if (curFdInfo == NULL) {
            ereport(ERROR, (errmodule(MOD_COMM_PROXY),
                errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("Not enough memory for CommFdInfoNode."),
                errdetail("N/A"),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
        }

        curFdInfo->fd = fd;
        curFdInfo->next = m_comm_fd_node_head.next;
        m_comm_fd_node_head.next = curFdInfo;
        m_comm_fd_node_head.fdNums++;
    }

}

void ThreadPoolCommunicator::RemoveServicedFd(int fd) {
    if (g_comm_controller->m_ready_send_mode == CommProxySendModePolling) {
        delFdFromFdList(fd);
    }
}

/*
 * - Brief:  Function to regist event on communicator side, normally it is an epoll-driven case
 * (1). poll(), e.g. invoked at postmaster
 * (2). epoll_wait(), e.g. invoked at listener thread
 * (3). recv(), e.g. invoked at normal worker process
 */
int ThreadPoolCommunicator::AddCommEpoll(CommSockDesc* comm_sock, EventType event_type)
{
    struct epoll_event ev = {0};
    int ret;

    comm_change_event(comm_sock, event_type, &ev);

    /* Add event */
    int fd = comm_sock->m_fd;
    ret = comm_epoll_ctl(m_epoll_fd, EPOLL_CTL_ADD, fd, &ev);
    if (ret) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY),
            errmsg("%s epoll_ctl_add fd:%d to epollfd:%d type:[%d] fail %s, %m.",
            t_thrd.proxy_cxt.identifier,
            fd,
            m_epoll_fd,
            event_type,
            __FUNCTION__)));
    } else {
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s epoll_ctl(add) fd:%d to epollfd:%d type:[%d] success %s.",
            t_thrd.proxy_cxt.identifier,
            fd,
            m_epoll_fd,
            event_type,
            __FUNCTION__)));
    }

    return ret;
}

int ThreadPoolCommunicator::AddCommEpollInternal(CommSockDesc* comm_sock, EventType event_type)
{
    struct epoll_event ev = {0};
    int ret;

    comm_change_event(comm_sock, event_type, &ev);

    /* Add event */
    int fd = comm_sock->m_fd;
    ret = m_comm_api.epoll_ctl_fn(m_epoll_fd, EPOLL_CTL_ADD, fd, &ev);

    if (!ret) {
        AddServedFdCnt();
    }

    return ret;
}

void ThreadPoolCommunicator::DelCommEpollInternal(int fd)
{
    if (m_comm_api.epoll_ctl_fn(m_epoll_fd, EPOLL_CTL_DEL, fd, NULL)) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY),
            errmsg("%s epoll_ctl(del fd:[%d]) form epfd:[%d] fail, errno:%d, %m.",
            __FUNCTION__, fd, m_epoll_fd, errno)));
    } else {
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("%s epoll_ctl(del fd:[%d]) form epfd:[%d] succeed.",
            __FUNCTION__, fd, m_epoll_fd)));
        SubServedFdCnt(fd);
    }
}

bool ThreadPoolCommunicator::SetBlockMode(int sock, bool is_block)
{
    int flg, rc;

    if ((flg = m_comm_api.fcntl_fn(sock, F_GETFL, 0)) < 0) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("fcntl get fd[%d] block mode failed! errno:%d, %m.",
            sock, errno)));
        return false;
    }

    if (is_block) {
        flg = flg & ~O_NONBLOCK;
    } else {
        flg |= O_NONBLOCK;
    }

    if ((rc = m_comm_api.fcntl_fn(sock, F_SETFL, flg)) < 0) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("fcntl set fd[%d] %s failed! errno:%d, %m.",
            sock, is_block ? "BLOCK" : "O_NONBLOCK", errno)));
        return false;
    }

    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("set fd:[%d] %s success.",
        sock, is_block ? "BLOCK" : "O_NONBLOCK")));

    return true;
}

void ThreadPoolCommunicator::ProcessOneRecvListenEvent(struct epoll_event* recv_event, bool has_indata, bool has_error)
{

    EventDataHeader* event = NULL;
    event = (EventDataHeader*)recv_event->data.ptr;
    int save_err = 0;

    CommSockDesc* comm_sock_internal = event->s_sock;
    CommSockDesc* comm_sock_logical = comm_sock_internal->m_parent_comm_sock;

    if (has_error) {
        /* errno maybe is 0, because epoll wait is success */
        save_err = GetSysCommErrno(this, comm_sock_internal->m_fd);
        if (IgnoreErrorNo(save_err)) {
            has_error = false;
        }
    }

    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
        errmsg("%s-wakeup handle \"EventListen\" from server_fd:[%d], logical_fd:[%d],"
        "error:%d, %s event error, events:%d.",
        t_thrd.proxy_cxt.identifier,
        comm_sock_internal->m_fd,
        comm_sock_logical->m_fd,
        save_err,
        save_err == 0 ? "no" : "has",
        recv_event->events)));

    /* no-user epoll, need internal del */
    DelCommEpollInternal(comm_sock_internal->m_fd);
    if (has_error) {
        comm_sock_internal->m_errno = save_err;
        comm_sock_internal->m_not_need_del_from_epoll = true;
        comm_sock_internal->m_status = CommSockStatusClosedByPeer;
    }

    if (comm_sock_internal->m_listen_type == CommSockListenByPoll) {
        comm_sock_internal->m_poll_status = CommSockPollReady;
    }



    if (has_indata) {
        /* wake up any polled threads */
        comm_atomic_add_fetch_u32(&comm_sock_internal->m_server_income_ready_num, 1);
        comm_atomic_add_fetch_u32(&comm_sock_logical->m_server_income_ready_num, 1);
        comm_static_inc(g_comm_controller->m_poll_static.s_ready_num);
    }

    return;
}

void ThreadPoolCommunicator::ProcessOneRecvDataEvent(struct epoll_event* recv_event, bool has_indata, bool has_error)
{
    EventDataHeader* event = NULL;
    int save_err = 0;

    /* Handle case of wake up Data Recv */
    event = (EventDataHeader*)recv_event->data.ptr;
    CommSockDesc* sock_desc = (CommSockDesc*)(event->s_sock);

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    if (event->s_event_type == EventThreadPoolListen) {
        /* Handle case of wake up listener */
        struct epoll_event* listen_event = NULL;

        listen_event = (struct epoll_event*)comm_malloc(sizeof(struct epoll_event));
        if (listen_event == NULL) {
            ereport(ERROR, (errmodule(MOD_COMM_PROXY),
                errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("Not enough memory for epoll_event."),
                errdetail("N/A"),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
        }
        listen_event->events = recv_event->events;
        listen_event->data = recv_event->data;

        CommSockDesc* listen_dfd = event->s_listener;

        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s -wakeup handle \"EventThreadPoolListen\" from client fd:%d, epoll fd:%d, occur error:%u.",
            t_thrd.proxy_cxt.identifier,
            sock_desc->m_fd,
            listen_dfd->m_fd,
            has_error ? recv_event->events : 0)));

        /* we only have to notify consumer if needed */
        bool need_post = listen_dfd->m_epoll_queue->empty();

        listen_dfd->m_epoll_queue->push(listen_event);
        if (need_post) {
            sem_post(&listen_dfd->m_epoll_queue_sem);
        }

        comm_static_inc(g_comm_controller->m_epoll_listen_static.s_ready_num);

        event->s_event_type = EventRecv;
    }


    if (has_error) {
        /*
         * errinfo set by recv return value
         * worker parse packet->errno get errinfo
         */
        save_err = GetSysCommErrno(this, sock_desc->m_fd);

        /*
         * connection is close by remote normal, this case errno is 0, and we can not recv anything
         * recv event is trigger by FIN packet
         */
        if (save_err == 0 && (recv_event->events | EPOLLRDHUP)) {
            has_error = true;
        } else {
            if (IgnoreErrorNo(save_err)) {
                has_error = false;
            } else {
                sock_desc->m_errno = save_err;
            }
        }

    }

    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
        errmsg("%s -wakeup handle \"EventRecv\" from client fd:[%d], error:%d, %s event error, events:%d.",
        t_thrd.proxy_cxt.identifier,
        sock_desc->m_fd,
        save_err,
        save_err == 0 ? "no" : "has",
        recv_event->events)));

    if (has_indata) {
        /* do recv both noral and error case, errno get by recv() */
        sock_desc->m_process = CommSockProcessProxyRecving;
        comm_proxy_transport_recv(sock_desc);
        sock_desc->m_process = CommSockProcessProxyRecvend;
    }

    if (has_error) {
        if (sock_desc->m_status == CommSockStatusClosedByPeer) {
            CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(sock_desc->m_fd);
            if (comm_sock != NULL && comm_sock->m_event_header.s_listener != NULL) {
                CommSockDesc* comm_epoll_sock = comm_sock->m_event_header.s_listener;
                comm_epoll_sock->m_epoll_list->delCommSock(comm_sock);
            }
            ereport(WARNING, (errmodule(MOD_COMM_PROXY),
                errmsg("%s fd:[%d] recv epollerr with recvevent, errno:%d, and recv fail.",
                t_thrd.proxy_cxt.identifier, sock_desc->m_fd, sock_desc->m_errno)));
        } else {
            ereport(WARNING, (errmodule(MOD_COMM_PROXY),
                errmsg("%s fd:[%d] recv epollerr with recvevent, errno:%d, but recv success, we wait next trigger.",
                t_thrd.proxy_cxt.identifier, sock_desc->m_fd, sock_desc->m_errno)));
        }
    }

    comm_static_inc(g_comm_controller->m_epoll_recv_static.s_ready_num);

    return;
}

void ThreadPoolCommunicator::ProcessOneConnectDataEvent(struct epoll_event* recv_event, bool has_out, bool has_error)
{
    EventDataHeader* event = NULL;

    /* Handle case of wake up Data Recv */
    event = (EventDataHeader*)recv_event->data.ptr;
    CommSockDesc* sock_desc = (CommSockDesc*)(event->s_sock);
    int save_err = 0;
    bool need_notify = false;

    if (has_error) {
        /*
         * errinfo set by recv return value
         * worker parse packet->errno get errinfo
         */
        save_err = GetSysCommErrno(this, sock_desc->m_fd);
        if (IgnoreErrorNo(save_err)) {
            /* wake up by INTR or other no-error case, we need monitor continue */
            has_error = false;
            return;
        }
    }

    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
        errmsg("%s -wakeup handle \"EventOut\" from client fd:[%d], error:%d, %s event error, events:%d.",
        t_thrd.proxy_cxt.identifier,
        sock_desc->m_fd,
        save_err,
        save_err == 0 ? "no" : "has",
        recv_event->events)));

    /*
     * for comm_proxy call: m_connect_status is start, and we need notify wprker
     * for user call: m_connect_status is uninit, we only set status
     */
    /* Use getsockopt and SO_ERROR to get the pending error on the socket both EPOLLOUT/EPOLLERR */
    if (has_error) {
        sock_desc->m_connect_status = CommSockConnectFail;
        sock_desc->m_errno = save_err;
    } else {
        if (sock_desc->m_connect_status == CommSockConnectInprogressBlock) {
            need_notify = true;
        }
        sock_desc->m_connect_status = CommSockConnectSuccess;
        sock_desc->m_errno = 0;
        if (need_notify) {
            sock_desc->m_connect_cv.notify_one();
        }
        AddServicedFd(sock_desc->m_fd);
    }

    if (!CommLibNetIsLibosFd(m_proto_type, sock_desc->m_fd)) {
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("%s connect fd[%d] done, is a linux fd.",
            t_thrd.proxy_cxt.identifier,
            sock_desc->m_fd)));
        DelCommEpollInternal(sock_desc->m_fd);
        sock_desc->m_not_need_del_from_epoll = true;
        sock_desc->m_fd_type = CommSockFakeLibosFd;
        return;
    }

    if (sock_desc->m_connect_status == CommSockConnectSuccess) {
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s connect fd[%d] done, is a libos fd, then mod event from connect to recv.",
            t_thrd.proxy_cxt.identifier,
            sock_desc->m_fd)));
        /*
         * connection build success
         * Notice:
         * the new connection maybe kernel or libos fd.
         * if connect dest is local ip, libos api return kernel fd,
         * if connect dest is remote ip, libos api return user fd.
         *
         */
        EventDataHeader* event_data_ptr = &sock_desc->m_event_header;

        /* Add event */
        struct epoll_event ev = {0};
        ev.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
        ev.data.ptr = event_data_ptr;
        if (m_comm_api.epoll_ctl_fn(m_epoll_fd, EPOLL_CTL_MOD, sock_desc->m_fd, &ev)) {
            ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                errmsg("%s regist recv event from connect success --epoll_ctl(mod) fd:%d to epollfd:%d fail, "
                "errno:%d, %m.",
                t_thrd.proxy_cxt.identifier,
                sock_desc->m_fd,
                m_epoll_fd,
                errno)));
        } else {
            ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                errmsg("%s regist recv event from connect success --epoll_ctl(mod) fd:%d to epollfd:%d success.",
                t_thrd.proxy_cxt.identifier,
                sock_desc->m_fd,
                m_epoll_fd)));

            event_data_ptr->s_event_type = EventRecv;
        }
    } else {
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("%s connect fd[%d] fail, is a libos fd, delete from epoll[%d] list.",
            t_thrd.proxy_cxt.identifier,
            sock_desc->m_fd,
            m_epoll_fd)));
        DelCommEpollInternal(sock_desc->m_fd);
        sock_desc->m_not_need_del_from_epoll = true;
        sock_desc->m_status = CommSockStatusClosedByPeer;
    }

    comm_static_inc(g_comm_controller->m_epoll_recv_static.s_ready_num);

    return;
}
