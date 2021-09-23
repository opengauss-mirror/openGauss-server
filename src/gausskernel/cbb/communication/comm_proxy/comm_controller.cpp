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
 * comm_controller.cpp
 *        TODO add contents
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_controller.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <time.h>
#include <sys/time.h>
#include <arpa/inet.h>

#include "communication/commproxy_interface.h"
#include "communication/commproxy_dfx.h"
#include "comm_core.h"
#include "comm_proxy.h"
#include "comm_connection.h"
#include "executor/executor.h"

static void ProxyerStatusCallBack(ThreadPoolCommunicator* comm, CommProxyerStatus status)
{
    if (status == CommProxyerReady) {
        g_comm_controller->ProxyerStatusReadyCallBack(CommProxyerReady);
    }

    return;
}

int CommGetEpollEventByWakeup(CommWaitEpollWaitParam* ewp, int events_num)
{
    struct epoll_event* ev = NULL;
    int ret;
    while ((ret = ewp->m_epoll_queue->pop(ev))) {
        Assert(ev != NULL);
        ewp->s_events[events_num].events = ev->events;
        EventDataHeader* ed = (EventDataHeader*)ev->data.ptr;
        ewp->s_events[events_num].data = ed->s_origin_event_data_ptr;
        events_num++;
        comm_free(ev);
        ev = NULL;
    }

    return events_num;
}

CommController::CommController()
{
    /* 64 max fd num is 52712462 -->67,108,863 */
    m_dummy_fd_min = 0x3ffffff + 1;
    m_dummy_fd_next = 0x3ffffff + 1;
    m_dummy_fd_max = 0x4ffffff;

    m_transport_mode = CommModeRingBuffer;

    m_ready_comm_cnt = 0;

    if (gs_api_init(&m_default_comm_api, phy_proto_tcp) == 0) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("Failed to initialize gs_api_init in CommController."),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
    }

    comm_static_set(m_active_worker_num, 0);
    init_req_static(&m_accept_static);
    init_req_static(&m_poll_static);
    init_req_static(&m_epoll_recv_static);
    init_req_static(&m_epoll_listen_static);
    init_req_static(&m_epoll_all_static);
}

CommController::~CommController()
{
    for (int k = 0; k < MAX_COMM_PROXY_GROUP_CNT; k++) {
        for (int i = 0; i < m_communicator_nums[k]; i++) {
            m_communicators[k][i]->DownToDeInit();
            comm_free_ext(m_communicators[k][i]);
        }
        comm_free_ext(m_communicators[k]);
    }
}

int CommController::Init()
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    /*
     * we cannot apply for all fds resources, but we can assume the range of fds values that can be obtained
     * for each process, the fd starts from 0. 0, 1, and 2 are preset as three special values.
     * each time the process applies, the minimum fd value is used
     * therefore, we can apply for 1024 FDs in advance for the first batch of connections
     * when the fd is close, these resources are not released but marked as unavailable
     */
    m_sockcomm_map_arr = (CommSockDesc**)comm_malloc(sizeof(CommSockDesc*) * m_dummy_fd_max);
    int rc = memset_s(m_sockcomm_map_arr, sizeof(CommSockDesc*) * m_dummy_fd_max,
                      0x0, sizeof(CommSockDesc*) * m_dummy_fd_max);
    securec_check(rc, "\0", "\0");

    m_epoll_mode = CommEpollModeSockList;
    m_notify_mode = CommProxyNotifyModeSpinLoop;
    m_ready_send_mode = CommProxySendModePolling;

    m_proxy_recv_loop_cnt = 5;
    m_proxy_send_loop_cnt = 1;

    switch (m_ready_send_mode) {
        case CommProxySendModePolling: {
            m_process_send_func = comm_proxy_send_polling;
        } break;

        case CommProxySendModeReadyQueue: {
            m_process_send_func = comm_proxy_send_ready_queue;
        } break;

        default: {
            m_process_send_func = comm_proxy_send_polling;
        }
    }

    comm_init_api_hook();

    return 0;
}

int CommController::InitCommProxyGroup(int proxy_id, CommProxyInstance *proxy_instance)
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);
    m_communicator_nums[proxy_id] = proxy_instance->s_comm_nums;

    /* allocate communicator array */
    m_communicators[proxy_id] = 
        (ThreadPoolCommunicator**)comm_malloc(sizeof(ThreadPoolCommunicator*) * m_communicator_nums[proxy_id]);

    /*
     * Invoke numa bind before starting worker thread to make more memory allocation
     * local to worker thread.
     * numa_bind may be too tough. Consider numa_set_preferred(numa_id)?
     */
    int index = 0;
    for (int i = 0; i < THREADPOOL_TOTAL_GROUP; i++) {
        if (i >= g_comm_proxy_config.s_numa_num) {
            continue;
        }

        for (int j = 0; j < MAX_PROXY_CORE_NUM; j++) {
            if (proxy_instance->s_comms[i][j] >= 0) {
                m_communicators[proxy_id][index] = New(CurrentMemoryContext) ThreadPoolCommunicator(i, j, proxy_id,
                    (phy_proto_type_t)proxy_instance->s_comm_type);
                m_communicators[proxy_id][index]->SetStatusCallback(ProxyerStatusCallBack);
                m_communicators[proxy_id][index]->ParseBindCores(proxy_instance->s_comms[i][j]);
                m_communicators[proxy_id][index]->StartUp(m_transport_mode);
                index++;
            } else {
                break;
            }
        }
    }

    int ret = WaitCommReady(proxy_id);

    return ret;
}

int CommController::WaitCommReady(int proxy_id)
{
    /* wait 10*1000*6 = 1 min */
    WaitCondPositiveWithTimeout((m_ready_comm_cnt == (m_communicator_nums[proxy_id])), 10, 10 * 1000 *6);
    if (m_ready_comm_cnt == (m_communicator_nums[proxy_id])) {
        ereport(DEBUG2, (errmodule(MOD_COMM_PROXY),
            errmsg("CommController wait comm_groups[%d] ready success, current ready:%d/%d.",
            proxy_id, m_ready_comm_cnt, m_communicator_nums[proxy_id])));

        m_ready_comm_cnt = 0;

        return 1;
    }

    ereport(ERROR, (errmodule(MOD_COMM_PROXY),
        errcode(ERRCODE_SYSTEM_ERROR),
        errmsg("CommController wait comm_groups[%d] ready fail, current ready:%d/%d.",
            proxy_id, m_ready_comm_cnt, m_communicator_nums[proxy_id]),
        errdetail("N/A"),
        errcause("System error."),
        erraction("Contact Huawei Engineer.")));

    return 0;
}

void CommController::ProxyerStatusReadyCallBack(CommProxyerStatus status)
{
    m_ready_comm_cnt++;

    return;
}

int CommController::GetNextDummyFd()
{
    int fd = m_dummy_fd_next;
    m_dummy_fd_next++;
    return fd;
}

void CommController::RegistPollEvents(    struct pollfd* fdarray, unsigned long nfds)
{
    for (int i = 0; i < (int)nfds; i++) {
        /* skip the slot in poll-array that we do not use */
        if (fdarray[i].fd == INVALID_FD) {
            continue;
        }

        CommSockDesc* sock_desc = FdGetCommSockDesc(fdarray[i].fd);
        /* regist an EventListen to communicators */
        if (sock_desc != NULL && sock_desc->m_fd_type != CommSockFakeLibosFd && (fdarray[i].events & POLLIN)) {
            sock_desc->m_listen_type = CommSockListenByPoll;
            switch (sock_desc->m_fd_type) {
                case CommSockServerFd: {
                    /* for listen fd */
                    SocketRequest req;

                    CommPollInternalInvokeParam param_new;
                    CommProxyInvokeResult poll_ret;
                    param_new.s_fd = INVALID_FD;
                    param_new.s_logical_fd = fdarray[i].fd;

                    req.s_fd = fdarray[i].fd;
                    req.s_sockreq_type = sockreq_poll;
                    req.s_request_data = (void *)&param_new;
                    req.s_response_data = &poll_ret;
                    req.s_done = false;

                    /* Blocked until we get the response of proxy side's execution */
                    SubmitSocketRequest(&req);
                } break;

                case CommSockCommServerFd: {
                    Assert(0);
                } break;

                case CommSockEpollFd:
                case CommSockCommEpollFd: {
                } break;

                case CommSockClientDataFd:
                case CommSockServerDataFd: {
                    /* data fd is added to proxy_epoll, we check recv buffer here */
                } break;

                case CommSockFakeLibosFd: {

                } break;
                default: {
                    Assert(0);
                }
            }
        }

        if (sock_desc != NULL && sock_desc->m_fd_type != CommSockFakeLibosFd && (fdarray[i].events & POLLOUT)) {
            sock_desc->m_listen_type = CommSockListenByPoll;
                switch (sock_desc->m_fd_type) {
                    case CommSockServerFd: {
                    } break;

                    case CommSockCommServerFd: {
                        Assert(0);
                    } break;

                    case CommSockEpollFd:
                    case CommSockCommEpollFd: {
                    } break;

                    case CommSockClientDataFd: {
                    if (sock_desc->m_connect_status == CommSockConnectInprogressNonBlock) {
                        sock_desc->m_connect_status = CommSockConnectInprogressNonBlockPollListen;
                        sock_desc->m_communicator->AddCommEpoll(sock_desc, EventOut);
                        ereport(DEBUG2, (errmodule(MOD_COMM_PROXY),
                            errmsg("%s fd:[%d] regist to epoll fd to simulate POLLOUT event.",
                            t_thrd.proxy_cxt.identifier, sock_desc->m_fd)));
                    }
                    } break;

                    case CommSockServerDataFd: {
                        /* data fd is added to proxy_epoll, we check recv buffer here */
                    } break;

                    case CommSockFakeLibosFd: {

                    } break;
                    default: {
                        Assert(0);
                    }
                }
        }
    }
}

int CommController::GetCommPollEvents(    struct pollfd* fdarray, unsigned long nfds, int poll_ready_num)
{
    bool trigged = false;
    for (int i = 0; i < (int)nfds; i++) {
        if (fdarray[i].fd == INVALID_FD) {
            continue;
        }

        trigged = false;
        CommSockDesc* sock_desc = FdGetCommSockDesc(fdarray[i].fd);

        if (sock_desc != NULL && sock_desc->m_fd_type != CommSockFakeLibosFd) {
            if (sock_desc->m_status == CommSockStatusClosedByPeer) {
                fdarray[i].revents |= POLLERR;
                trigged = true;
            } else {
                sock_desc->m_listen_type = CommSockListenByPoll;
                if ((fdarray[i].events & POLLIN)) {
                    switch (sock_desc->m_fd_type) {
                        case CommSockServerFd: {
                            if (sock_desc->m_errno != 0) {
                                fdarray[i].revents |= POLLERR;
                                trigged = true;
                            } else if (sock_desc->m_server_income_ready_num > 0) {
                                /*
                                 * maybe need distinguish between in and out,
                                 * but we only use epoll_in to listen
                                 */
                                fdarray[i].revents |= POLLIN;
                                trigged = true;
                                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                                    errmsg("comm_proxy_poll: server fd[%d] get event POLLIN, now events nums:%d.",
                                    sock_desc->m_fd, poll_ready_num + 1)));
                            }
                        } break;

                        case CommSockCommServerFd: {
                            /**/
                            Assert(0);
                        } break;

                        case CommSockEpollFd:
                        case CommSockCommEpollFd: {
                            /* maybe */
                        } break;

                        case CommSockClientDataFd:
                        case CommSockServerDataFd: {
                            /* for in: recv buffer is not empty
                             * for out: always true
                             */
                            if (sock_desc->GetQueueCnt(ChannelRX) <= sock_desc->m_data_threshold[ChannelRX]) {
                                /* no any new data incoming */
                            } else {
                                fdarray[i].revents |= POLLIN;
                                trigged = true;
                                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                                    errmsg("comm_proxy_poll: data fd[%d] get event POLLIN, now events nums:%d.",
                                    sock_desc->m_fd, poll_ready_num + 1)));
                            }
                        } break;

                        case CommSockFakeLibosFd: {

                        } break;
                        default: {
                            Assert(0);
                        }
                    }
                }

                if ((fdarray[i].events & POLLOUT)) {
                    switch (sock_desc->m_fd_type) {
                        case CommSockServerFd: {
                            fdarray[i].revents |= POLLOUT;
                            trigged = true;
                        } break;

                        case CommSockCommServerFd: {
                            /**/
                            Assert(0);
                        } break;

                        case CommSockEpollFd:
                        case CommSockCommEpollFd: {
                            /* maybe */
                        } break;

                        case CommSockClientDataFd: {
                            if (sock_desc->m_connect_status == CommSockConnectFail) {
                                fdarray[i].revents |= POLLERR;
                                trigged = true;
                            } else if (sock_desc->m_connect_status == CommSockConnectSuccess) {
                                if (sock_desc->m_transport[ChannelTX]->CheckBufferFull()) {
                                    /* no any data incoming */
                                } else {
                                    fdarray[i].revents |= POLLOUT;
                                    trigged = true;
                                    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                                        errmsg("comm_proxy_poll: data fd[%d] is writenable, now events nums:%d.",
                                        sock_desc->m_fd, poll_ready_num + 1)));
                                }
                            } else {

                            }

                        } break;
                        case CommSockServerDataFd: {
                            /* for in: recv buffer is not empty
                             * for out: always true
                             */
                            if (sock_desc->m_transport[ChannelTX]->CheckBufferFull()) {
                                /* no any data incoming */
                            } else {
                                fdarray[i].revents |= POLLOUT;
                                trigged = true;
                                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                                    errmsg("comm_proxy_poll: data fd[%d] is writenable, now events nums:%d.",
                                    sock_desc->m_fd, poll_ready_num + 1)));
                            }
                        } break;

                        case CommSockFakeLibosFd: {

                        } break;

                        default: {
                            Assert(0);
                        }
                    }

                }

            }
        }
        poll_ready_num = poll_ready_num + trigged ? 1 : 0;
    }

    return poll_ready_num;
}

void CommController::ProxyWaitProcess(SocketRequestType* param)
{
    SocketRequestType sockreq = *(SocketRequestType*)param;
    struct timeval st;
    gettimeofday(&st, NULL);
    int ready_event_nums = 0;

    switch (sockreq) {
        case sockreq_poll: /* postmaster thread will get into this btranch */
        {
            CommWaitPollParam* pp = (CommWaitPollParam*)param;
            int kernel_fd_cnt;

            RegistPollEvents(pp->s_fdarray, pp->s_nfds);

            while (true) {
                /*
                 * if fd is a big value though greater than max fd, poll will return 1 or greater number
                 * so comm_sock fd should remove from poll array
                 * after poll, we need comm_sock fd to get comm sock desc
                 * here, we convert fd by doing (0 - fd) twice, to make poll ignore and then restore
                 */
                ready_event_nums = 0;
                kernel_fd_cnt = CommPollFdMarkSwitch(pp->s_fdarray, pp->s_nfds);
                if (kernel_fd_cnt > 0) {
                    /* native poll must call first, then rewrite fdarray by commsock */
                    ready_event_nums = m_default_comm_api.poll_fn(pp->s_fdarray, pp->s_nfds, 0);
                    if (ready_event_nums < 0) {
                        /*
                         * if poll return -1, the errors contains:
                         * EFAULT, EINTR, EINVAL, ENOMEM
                         * we return early to ask the worker thread to handle these errors.
                         */
                        (void)CommPollFdMarkSwitch(pp->s_fdarray, pp->s_nfds);
                        break;
                    }
                }
                (void)CommPollFdMarkSwitch(pp->s_fdarray, pp->s_nfds);

                ready_event_nums = GetCommPollEvents(pp->s_fdarray, pp->s_nfds, ready_event_nums);
                if (ready_event_nums > 0) {
                    break;
                }

                CommWaitNextStatus ns = comm_proxy_process_wait(st, pp->s_timeout, pp->s_ctx_switch);
                if (ns == CommWaitNextBreak) {
                    break;
                }
           }

            pp->s_ret = ready_event_nums;
            comm_static_add(m_poll_static.s_endnum, ready_event_nums);
        } break;

        case sockreq_epollwait: /* normally listener thread waittask() get into this btranch */
        {
            CommWaitEpollWaitParam* ewp = (CommWaitEpollWaitParam*)param;
            int logic_epfd = ewp->s_epollfd;
            CommSockDesc* comm_epoll_sock = FdGetCommSockDesc(logic_epfd);
            if (comm_epoll_sock == NULL) {
                ereport(ERROR, (errmodule(MOD_COMM_PROXY),
                    errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("Failed to obtain the comm_epoll_sock in ProxyWaitProcess_epollwait, fd:%d.", logic_epfd),
                    errdetail("N/A"),
                    errcause("System error."),
                    erraction("Contact Huawei Engineer.")));
                break;
            }
            int real_epfd = comm_epoll_sock->m_epoll_real_fd;
            int origin_timeout = ewp->s_timeout;
            int fake_sig_timeout = 1000;

            while (true) {
                ready_event_nums = 0;
                if (real_epfd != INVALID_FD && comm_epoll_sock->m_need_kernel_call) {
                    ready_event_nums = m_default_comm_api.epoll_wait_fn(real_epfd, ewp->s_events, ewp->s_maxevents, 0);
                }
                if (ready_event_nums < 0) {
                    ereport(WARNING, (errmodule(MOD_COMM_PROXY),
                        errmsg("epfd[%d:%d] call real epoll wait catch error return:%d.",
                        logic_epfd, real_epfd, ready_event_nums)));
                    ready_event_nums = 0;
                }

                if (m_epoll_mode == CommEpollModeWakeup) {
                    ready_event_nums = CommGetEpollEventByWakeup(ewp, ready_event_nums);
                } else {
                    ready_event_nums = comm_epoll_sock->m_epoll_list->getCommEpollEvents(ewp->s_events, ready_event_nums);
                }
                if (ready_event_nums > 0) {
                    break;
                }

                CommWaitNextStatus ns = comm_proxy_process_wait(st, fake_sig_timeout, true, ewp->s_queue_sem);
                if (ns == CommWaitNextBreak) {
                    break;
                }
            }
            if (ready_event_nums == 0) {
                ewp->s_nevents = -1;
                errno = EINTR;
                origin_timeout = 0;
            } else {
                ewp->s_nevents = ready_event_nums;
            }
        } break;

        default: {
            ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("unkown wait type %d.", *param)));
        }
    }

    return;
}

bool CommController::IsServerfd(int fd)
{
    if (AmIUnInitServerFd(fd)) {
        return true;
    }

    CommSockDesc* sock_desc = FdGetCommSockDesc(fd);
    if (sock_desc != NULL && sock_desc->m_fd_type == CommSockServerFd) {
        return true;
    }

    return false;
}

int CommController::GetCommSockGroupId(int sockfd_key)
{
    CommSockDesc* comm_sock = FdGetCommSockDesc(sockfd_key);
    if (comm_sock == NULL) {
        return -1;
    }

    return comm_sock->m_group_id;
}

void CommController::SetCommSockNotifyMode(int fd, CommProxyNotifyMode mode)
{
    CommSockDesc* comm_sock = FdGetCommSockDesc(fd);
    if (comm_sock == NULL) {
        return;
    }

    if (comm_sock->m_trans_mode == CommModeRingBuffer) {
        CommRingBuffer *ringbuff = (CommRingBuffer *)comm_sock->m_transport[ChannelRX];
        ringbuff->SetNotifyMode(mode);
    }

    return;
}

void CommController::SetCommSockIdle(int sockfd_key)
{
    CommSockDesc* comm_sock = FdGetCommSockDesc(sockfd_key);
    if (comm_sock == NULL) {
        return;
    }

    if (comm_sock->m_worker_id < 0) {
        /* attach session fail, session is not bind with worker */
        return;
    }
    comm_sock->m_communicator->RemoveActiveWorkerCommSock(comm_sock);
    comm_sock->m_worker_id = -1;
}

void CommController::SetCommSockActive(int sockfd_key, int workerid)
{
    CommSockDesc* comm_sock = FdGetCommSockDesc(sockfd_key);
    if (comm_sock == NULL) {
        return;
    }
    comm_sock->m_worker_id = workerid;
    comm_sock->m_communicator->AddActiveWorkerCommSock(comm_sock);
    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
        errmsg("worker:(%d,%d) assign task in WaitNextSession() with client_fd:%d.",
        comm_sock->m_group_id,
        workerid, sockfd_key)));
}

ThreadPoolCommunicator* CommController::GetCommunicator(int fd)
{
    CommSockDesc* comm_sock = FdGetCommSockDesc(fd);
    if (comm_sock == NULL) {
        return NULL;
    }
    return comm_sock->m_communicator;
}

ThreadPoolCommunicator* CommController::GetCommunicatorByGroup(int proxy_id, int group_id)
{
    /* find communicator with least fd */
    int least_fd_comm_id = -1;
    uint32 least_fd_cnt = 0xfffffff;
    for (int i = 0; i < m_communicator_nums[proxy_id]; i++) {
        if (m_communicators[proxy_id][i]->m_group_id == group_id) {
            if (m_communicators[proxy_id][i]->m_serve_fd_num < least_fd_cnt) {
                least_fd_cnt = m_communicators[proxy_id][i]->m_serve_fd_num;
                least_fd_comm_id = i;
            }
        }
    }
    Assert(least_fd_comm_id != -1);
    return m_communicators[proxy_id][least_fd_comm_id];
}

int CommController::CommPollFdMarkSwitch(struct pollfd* fdarray, int nfds)
{
    /*
     * remove adn reset fd from poll list if fd is commsock, must be called in pairs
     * for some special fd value, 0, 1, 2 is reserved value, -1 always is invalid.
     * fd 0: stdin
     * fd 1: stdout
     * fd 2: stderr
     * poll fd must greater than 2
     */
    int kernel_fd_cnt = 0;
    for (int i = 0; i < nfds; i++) {
        if (fdarray[i].fd == INVALID_FD) {
            continue;
        }

        /* exec before poll, negate fds created by commproxy to ignored */
        if (fdarray[i].fd > 0 && FdGetCommSockDesc(fdarray[i].fd) != NULL && FdGetCommSockDesc(fdarray[i].fd)->m_fd_type != CommSockFakeLibosFd) {
            fdarray[i].fd = 0 - fdarray[i].fd;
            continue;
        }

        /* exec after poll, reset comm fd to get comm_sock */
        if (fdarray[i].fd < 0 && FdGetCommSockDesc(-fdarray[i].fd) != NULL && FdGetCommSockDesc(-fdarray[i].fd)->m_fd_type != CommSockFakeLibosFd) {
            fdarray[i].fd = 0 - fdarray[i].fd;
            continue;
        }
        kernel_fd_cnt++;
    }

    return kernel_fd_cnt;
}

void CommController::BindThreadToGroupExcludeProxy(int fd)
{
    CommSockDesc *comm_sock = FdGetCommSockDesc(fd);
    if (comm_sock == NULL) {
        return;
    }

    int group_id = comm_sock->m_group_id;
    if (group_id == -1) {
        return;
    }
    return;
}