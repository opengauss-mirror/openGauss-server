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
 * comm_sock.cpp
 *        TODO add contents
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_sock.cpp
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

int CommDropTransportPair(CommSockDesc* comm_sock);
int CommCreateTransportPair(CommSockDesc* comm_sock);

template<CommSockType type>
int comm_init_commsock(CommSockDesc *comm_sock)
{
    return 0;
}

template<CommSockType type>
int comm_init_commsock(CommSockDesc *comm_sock, int comm_nums)
{
    return 0;
}

template<CommSockType type>
int comm_init_commsock(CommSockDesc *comm_sock, int size, int group_id)
{
    return 0;
}

template<CommSockType type>
int comm_deinit_commsock(CommSockDesc *comm_sock)
{
    return 0;
}

template<>
int comm_init_commsock<CommSockServerFd>(CommSockDesc *comm_sock, int comm_nums)
{
    comm_sock->m_server_fd_cnt = comm_nums;
    comm_sock->m_server_fd_arrays = (int*)comm_malloc(sizeof(int) * comm_nums);
    comm_sock->m_server_income_ready_num = 0;
    comm_sock->m_last_index = 0;

    return 0;
}

template<>
int comm_deinit_commsock<CommSockServerFd>(CommSockDesc *comm_sock)
{
    comm_free(comm_sock->m_server_fd_arrays);

    return 0;
}

template<>
int comm_init_commsock<CommSockCommServerFd>(CommSockDesc *comm_sock)
{
    comm_sock->m_server_income_ready_num = 0;
    comm_sock->m_poll_status = CommSockPollUnRegist;

    return 0;
}

template<>
int comm_init_commsock<CommSockClientDataFd>(CommSockDesc *comm_sock)
{
    comm_sock->m_connect_status = CommSockConnectUninit;
    comm_sock->m_worker_id = -1;
    comm_sock->m_event_header.s_event_type = EventRecv;
    CommCreateTransportPair(comm_sock);

    return 0;
}

template<>
int comm_deinit_commsock<CommSockClientDataFd>(CommSockDesc *comm_sock)
{
    comm_sock->m_connect_status = CommSockConnectUninit;
    comm_sock->m_worker_id = -1;
    comm_sock->m_event_header.s_event_type = EventRecv;
    CommDropTransportPair(comm_sock);

    return 0;
}

template<>
int comm_init_commsock<CommSockEpollFd>(CommSockDesc *comm_sock, int size)
{
    comm_sock->m_communicator = NULL;
    comm_sock->m_group_id = -1;
    int epfd = epoll_create(size);
    comm_sock->m_epoll_real_fd = epfd;
    comm_sock->m_need_kernel_call = false;

    comm_sock->m_epoll_queue = new boost::lockfree::queue<struct epoll_event*>(1024);
    sem_init(&comm_sock->m_epoll_queue_sem, 0, 0);
    comm_sock->m_epoll_list = New(CurrentMemoryContext) CommSockList(comm_sock);

    return 0;
}

template<>
int comm_deinit_commsock<CommSockEpollFd>(CommSockDesc *comm_sock)
{
    g_comm_controller->m_default_comm_api.close_fn(comm_sock->m_epoll_real_fd);
    delete comm_sock->m_epoll_queue;
    comm_sock->m_epoll_queue = NULL;
    sem_destroy(&comm_sock->m_epoll_queue_sem);
    if (comm_sock->m_epoll_list != NULL) {
        delete comm_sock->m_epoll_list;
        comm_sock->m_epoll_list = NULL;
    }

    return 0;
}

inline CommTransPort *CommCreateTransport(CommTransportMode mode, phy_proto_type_t m_proto_type)
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    if (mode == CommModeRingBuffer) {
#ifdef USE_LIBNET
        if (m_proto_type == phy_proto_libnet) {
            return New(CurrentMemoryContext) CommRingBufferLibnet();
        } else {
#endif
        return New(CurrentMemoryContext) CommRingBuffer();
#ifdef USE_LIBNET
    }
#endif
    }
    if (mode == CommModeDoubleQueue) {
        return New(CurrentMemoryContext) CommPacketBuffer();
    }

    return NULL;
}

int CommCreateTransportPair(CommSockDesc* comm_sock)
{
    if (comm_sock->m_transport[ChannelTX] == NULL) {
        comm_sock->m_transport[ChannelTX] = CommCreateTransport(comm_sock->m_trans_mode,
            phy_proto_tcp);
    }

    comm_sock->m_transport[ChannelTX]->Init(comm_sock->m_fd, ChannelTX);


    if (comm_sock->m_transport[ChannelRX] == NULL) {
        comm_sock->m_transport[ChannelRX] = CommCreateTransport(comm_sock->m_trans_mode,
            comm_sock->m_communicator->m_proto_type);
    }

    comm_sock->m_transport[ChannelRX]->Init(comm_sock->m_fd, ChannelRX);
    return 0;
}

inline void CommDropTransport(CommTransPort **transport)
{
    if (transport == NULL || *transport == NULL) {
        return;
    }

    delete *transport;
    *transport = NULL;
}

int CommDropTransportPair(CommSockDesc* comm_sock)
{
    comm_sock->m_transport[ChannelRX]->DeInit();
    comm_sock->m_transport[ChannelTX]->DeInit();

    CommDropTransport(&comm_sock->m_transport[ChannelRX]);
    CommDropTransport(&comm_sock->m_transport[ChannelTX]);

    return 0;
}

/*
 * For create a comm_sock, we need 3 step
 * 1. create comm_sock object
 * 2. initialize all common variables
 * 3. initialize resources based on different types of requirements
 *
 * so, drop for reuse, we only free specific resources by step 3 alloced
 */
CommSockDesc* CommCreateSockDesc(int fd, SockDescCreateAttr* attr)
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    CommSockDesc* comm_sock = NULL;
    comm_sock = g_comm_controller->FdGetCommSockDescOrigin(fd);
    if (comm_sock == NULL) {
        comm_sock = New(CurrentMemoryContext) CommSockDesc();
    } else {
        /* we can reuse comm_sock */
    }

    comm_sock->Init(fd, attr->type, attr->comm, attr->parent);
    switch (attr->type) {
        case CommSockServerFd: {
            comm_init_commsock<CommSockServerFd>(comm_sock, attr->extra_data.server.comm_num);
        } break;
        case CommSockCommServerFd: {
            comm_init_commsock<CommSockCommServerFd>(comm_sock);
        } break;

        case CommSockFakeLibosFd:
        case CommSockServerDataFd:
        case CommSockClientDataFd: {
            comm_init_commsock<CommSockClientDataFd>(comm_sock);
        } break;

        case CommSockEpollFd: {
            comm_init_commsock<CommSockEpollFd>(comm_sock, attr->extra_data.epoll.size);
        } break;

        default: {
            comm_init_commsock<CommSockUnknownType>(comm_sock);
        }
    }

    g_comm_controller->AddCommSockMap(comm_sock);

    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("create new sockdesc:[%d], type:%d", fd, attr->type)));

    return comm_sock;
}

void CommReleaseSockDesc(CommSockDesc **comm_sock)
{
    if (comm_sock == NULL || *comm_sock == NULL) {
        ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("comm_sock is null in CommReleaseSockDesc.")));
        return;
    }

    /*
     * In most cases, only data connections are frequently created and destroyed.
     * The lifecycles of listen fd and epoll fd (except check_connection_status) are long.
     * However, the overhead for creating and applying for the ring buffer of the data fd is large.
     * Therefore, when the connection is closed, resources are not released for reuse
     *
     * Generally, it is not wasteful to retain these resources
     * because the fd allocation of each process starts from the smallest FD
     * that is, reused as much as possible.
     * This situation perfectly complies with the resource reuse logic
     */
    (*comm_sock)->DeInit();

    return;
}

void CommDropSockDesc(CommSockDesc **comm_sock)
{
    if (comm_sock == NULL || *comm_sock == NULL) {
        ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("comm_sock is null in CommDropSockDesc.")));
        return;
    }

    g_comm_controller->DelCommSockMap(*comm_sock);

    (*comm_sock)->DeInit();
    delete (*comm_sock);
    (*comm_sock) = NULL;

    return;
}

CommSockDesc::CommSockDesc()
{
    /* first alloc, need clear transport */
    m_transport[ChannelRX] = NULL;
    m_transport[ChannelTX] = NULL;

    return;
}

CommSockDesc::~CommSockDesc()
{
    ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("delete CommSockDesc with  fd[%d]", m_fd)));

    /*
     * if we release commsock, we need delete transport for data fd
     * Constraint: call DeInit before (delete comm_sock)
     */
    switch (m_fd_type) {
        case CommSockServerFd: {
        } break;
        case CommSockEpollFd: {
        } break;

        case CommSockFakeLibosFd:
        case CommSockServerDataFd:
        case CommSockClientDataFd: {
            CommDropTransportPair(this);
        } break;

        default: {
        }
    }
}

void CommSockDesc::Init(int fd, CommSockType type, ThreadPoolCommunicator* comm, CommSockDesc* parent)
{
    /* Set common fields and leave other type-specific fields set later */
    m_fd_type = type;
    m_fd = fd;
    m_communicator = comm;
    m_parent_comm_sock = parent;
    m_group_id = m_communicator == NULL ? -1 : m_communicator->m_group_id;
    m_trans_mode = m_communicator == NULL ? CommModeDefault : m_communicator->m_trans_mode;
    m_errno = 0;
    m_status = CommSockStatusUninit;
    m_worker_id = -1;
    m_not_need_del_from_epoll = false;
    m_process = CommSockProcessInit;
    m_event_header = {0};
    m_event_header.s_sock = this;

    /* for CommServerFd */
    m_server_fd_cnt = 0;
    m_server_fd_arrays = NULL;
    m_server_income_ready_num = 0;
    m_last_index = -1;
    m_fds = NULL;
    m_nfds = 0;
    m_listen_type = CommSockListenNone;
    m_poll_status = CommSockPollUnRegist;

    /* for CommSockEpollFd */
    m_epoll_queue = 0;
    m_epoll_real_fd = -1;
    m_epoll_list = NULL;
    m_epoll_catch_error = false;

    /* for data fd */
    m_send_idle = true;
    m_block = true;
    m_connect_status = CommSockConnectUninit;

    m_data_threshold[ChannelRX] = 0;
    m_data_threshold[ChannelTX] = 0;

    comm_static_set(m_wakeup_cnt, 0);
    comm_static_set(m_enable_packet_static, false);

}

void CommSockDesc::DeInit()
{
    ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("deinit CommSockDesc with  fd[%d]", m_fd)));

    /*
     * in deinit, we need release all resources can not be reused
     * and these resource will alloc in next comm init
     */
    switch (m_fd_type) {
        case CommSockServerFd: {
            comm_deinit_commsock<CommSockServerFd>(this);
        } break;
        case CommSockEpollFd: {
            comm_deinit_commsock<CommSockEpollFd>(this);
        } break;
        case CommSockClientDataFd: {
            comm_deinit_commsock<CommSockClientDataFd>(this);
        } break;
        default: {
            comm_deinit_commsock<CommSockUnknownType>(this);
        }
    }

    m_fd_type = CommSockUnknownType;
}

void CommSockDesc::WaitQueueEmpty(int channel)
{
    if (!AmCommDataFd(this)) {
        return;
    }

    m_transport[channel]->WaitBufferEmpty();
};

void CommSockDesc::ClearSockQueue()
{
    if (!AmCommDataFd(this)) {
        return;
    }

    WaitQueueEmpty(ChannelRX);
    WaitQueueEmpty(ChannelTX);
}

void CommSockDesc::SetPollThreshold(int channel, int threshold)
{
    m_data_threshold[channel] = (unsigned int)threshold;
}

void CommSockDesc::InitDataStatic()
{
    m_static_pkt_cnt_by_length[ChannelTX] = (uint64 *)comm_malloc(sizeof(uint64) * MAX_STAT_PACKET_LENGTH);
    m_static_pkt_cnt_by_length[ChannelRX] = (uint64 *)comm_malloc(sizeof(uint64) * MAX_STAT_PACKET_LENGTH);

    int rc = memset_s(m_static_pkt_cnt_by_length[ChannelTX], sizeof(uint64) * MAX_STAT_PACKET_LENGTH,
                    0, sizeof(uint64) * MAX_STAT_PACKET_LENGTH);
    securec_check(rc, "\0", "\0");
    rc = memset_s(m_static_pkt_cnt_by_length[ChannelRX], sizeof(uint64) * MAX_STAT_PACKET_LENGTH,
                    0, sizeof(uint64) * MAX_STAT_PACKET_LENGTH);
    securec_check(rc, "\0", "\0");
    m_enable_packet_static = true;
};

void CommSockDesc::UpdateDataStatic(size_t len, int type)
{
    if (!m_enable_packet_static) {
        return;
    }
    if (len >= MAX_STAT_PACKET_LENGTH) {
        /* we use 0 to record max length packet */
        if (len > m_static_pkt_cnt_by_length[type][0]) {
            m_static_pkt_cnt_by_length[type][0] = len;
        }

        return;
    }
    m_static_pkt_cnt_by_length[type][len]++;
};

void CommSockDesc::PrintDataStatic()
{
    ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("fd:[%d] packet static:", m_fd)));
    for (int i = 0; i < MaxPacketLength; i++) {
        if (m_static_pkt_cnt_by_length[ChannelTX][i] != 0 ||
            m_static_pkt_cnt_by_length[ChannelRX][i] != 0) {
            ereport(DEBUG1, (errmodule(MOD_COMM_PROXY), errmsg("fd:[%d], length:[%d], send:%lu, recv:%lu\n",
                m_fd,
                i,
                m_static_pkt_cnt_by_length[ChannelTX][i],
                m_static_pkt_cnt_by_length[ChannelRX][i])));
        }
    }
    ereport(DEBUG1, (errmodule(MOD_COMM_PROXY), errmsg("fd:[%d], process transaction counts:%lu\n",
        m_fd, m_wakeup_cnt)));
};

CommSockList::CommSockList(CommSockDesc *epoll_sock)
{
    m_sock_list = NIL;
    m_comm_epoll_sock = epoll_sock;
    (void)pthread_mutex_init(&m_mutex, 0);
}
CommSockList::~CommSockList()
{
    (void)pthread_mutex_destroy(&m_mutex);
}

void CommSockList::addCommSock(CommSockDesc *comm_sock)
{
    MemoryContext old_context;
    AutoMutexLock sock_list_lock(&m_mutex);
    sock_list_lock.lock();
    old_context = MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);
    m_sock_list = lappend(m_sock_list, (void*)comm_sock);
    (void)MemoryContextSwitchTo(old_context);
}
void CommSockList::delCommSock(CommSockDesc *_comm_sock)
{
    MemoryContext old_context;
    AutoMutexLock sock_list_lock(&m_mutex);
    sock_list_lock.lock();
    old_context = MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);

    CommSockDesc* comm_sock = NULL;
    ListCell* cell = NULL;
    ListCell* prev = NULL;

    foreach (cell, m_sock_list) {
        comm_sock = (CommSockDesc*)lfirst(cell);
        if (comm_sock->m_fd == _comm_sock->m_fd) {
            m_sock_list = list_delete_cell(m_sock_list, cell, prev);
            break;
        }
        prev = cell;
    }

    (void)MemoryContextSwitchTo(old_context);
}

bool CommSockList::findCommSock(bool with_lock, int fd)
{
    MemoryContext old_context;
    bool exist = false;
    CommSockDesc *comm_sock = NULL;
    ListCell* cell = NULL;
    ListCell* prev = NULL;
    AutoMutexLock sock_list_lock(&m_mutex);

    if (with_lock) {
        sock_list_lock.lock();
    }
    old_context = MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);

    foreach (cell, m_sock_list) {
        comm_sock = (CommSockDesc*)lfirst(cell);
        if (comm_sock->m_fd == fd) {
            exist = true;
            break;
        }
        prev = cell;
    }

    (void)MemoryContextSwitchTo(old_context);

    return exist;
}
int CommSockList::getCommEpollEvents(struct epoll_event* events, int events_num)
{
    CommSockDesc *comm_sock = NULL;
    ListCell* cell = NULL;

    AutoMutexLock sock_list_lock(&m_mutex);
    sock_list_lock.lock();
    foreach (cell, m_sock_list) {
        comm_sock = (CommSockDesc*)lfirst(cell);
        Assert(AmCommDataFd(comm_sock) || AmCommServerFd(comm_sock));

        EventDataHeader* ed = &comm_sock->m_event_header;

        if (comm_sock->m_event_header.s_origin_events & EPOLLIN) {
            if ((AmCommDataFd(comm_sock) && !comm_sock->m_transport[ChannelRX]->CheckBufferEmpty()) ||
                (AmCommServerFd(comm_sock) && comm_sock->m_server_income_ready_num > 0)) {
                    if ((ed->s_origin_events & EPOLLONESHOT)) {
                        if (ed->s_epoll_oneshot_active) {
                            ed->s_epoll_oneshot_active = false;
                        } else {
                            continue;
                        }
                    }
                    events[events_num].events = EPOLLIN;
                    events[events_num].data = ed->s_origin_event_data_ptr;
                    events_num++;
                    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                        errmsg("comm_proxy_epoll_wait: epfd[%d] get event EPOLLIN from fd[%d] type[%d], "
                        "now events nums:%d",
                        m_comm_epoll_sock->m_fd, comm_sock->m_fd, comm_sock->m_fd_type, events_num)));
            } else {
                /* we have to sure all received data have been processed */
                if (comm_sock->m_status == CommSockStatusClosedByPeer && !comm_sock->m_epoll_catch_error) {
                    if ((ed->s_origin_events & EPOLLONESHOT)) {
                        if (ed->s_epoll_oneshot_active) {
                            ed->s_epoll_oneshot_active = false;
                        } else {
                            continue;
                        }
                    }
                    /* if catch error, this is last process */
                    comm_sock->m_epoll_catch_error = true;
                    /* we set event with EPOLLERR thougn real event is RDHUP or other */
                    events[events_num].events = (EPOLLERR);
                    events[events_num].data = ed->s_origin_event_data_ptr;
                    events_num++;
                    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                        errmsg("comm_proxy_epoll_wait: epfd[%d] get event EPOLLERR from fd[%d] type[%d], "
                        "now events nums:%d",
                        m_comm_epoll_sock->m_fd, comm_sock->m_fd, comm_sock->m_fd_type, events_num)));
                    continue;
                }
            }
        } else if (comm_sock->m_event_header.s_origin_events & EPOLLOUT) {
            /*
             * if we close by self, comm_proxy will wait send buffer empty or conn excption
             * if remote close, data will send fail, so we don't care tx buffer data length
             */
            if (comm_sock->m_status == CommSockStatusClosedByPeer && !comm_sock->m_epoll_catch_error) {
                if ((ed->s_origin_events & EPOLLONESHOT)) {
                    if (ed->s_epoll_oneshot_active) {
                        ed->s_epoll_oneshot_active = false;
                    } else {
                        continue;
                    }
                }
                comm_sock->m_epoll_catch_error = true;
                /* we set event with EPOLLERR thougn real event is RDHUP or other */
                events[events_num].events = (EPOLLERR);
                events[events_num].data = ed->s_origin_event_data_ptr;
                events_num++;
                ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                    errmsg("comm_proxy_epoll_wait: epfd[%d] get event EPOLLERR from fd[%d] type[%d], "
                    "now events nums:%d",
                    m_comm_epoll_sock->m_fd, comm_sock->m_fd, comm_sock->m_fd_type, events_num)));
                continue;
            }
            /* now we not scene use EPOLLOUT with ONESHOT */
            if (comm_sock->m_fd_type == CommSockClientDataFd) {
                if (comm_sock->m_connect_status == CommSockConnectSuccess && !comm_sock->m_transport[ChannelTX]->CheckBufferFull()) {
                    events[events_num].events = EPOLLOUT;
                    events[events_num].data = ed->s_origin_event_data_ptr;
                    events_num++;

                    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                        errmsg("comm_proxy_epoll_wait: epfd[%d] get event EPOLLOUT from fd[%d] type[%d], "
                        "now events nums:%d",
                        m_comm_epoll_sock->m_fd, comm_sock->m_fd, comm_sock->m_fd_type, events_num)));
                    continue;
                } else if (comm_sock->m_connect_status == CommSockConnectFail) {
                    events[events_num].events = EPOLLERR;
                    events[events_num].data = ed->s_origin_event_data_ptr;
                    events_num++;

                    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                        errmsg("comm_proxy_epoll_wait: epfd[%d] get event EPOLLERR from fd[%d] type[%d], "
                        "now events nums:%d",
                        m_comm_epoll_sock->m_fd, comm_sock->m_fd, comm_sock->m_fd_type, events_num)));
                    continue;
                }
            } else if (comm_sock->m_fd_type == CommSockServerDataFd) {
                if (!comm_sock->m_transport[ChannelTX]->CheckBufferFull()) {
                    events[events_num].events = EPOLLOUT;
                    events[events_num].data = ed->s_origin_event_data_ptr;
                    events_num++;

                    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
                        errmsg("comm_proxy_epoll_wait: epfd[%d] get event EPOLLOUT from fd[%d] type[%d], "
                        "now events nums:%d",
                        m_comm_epoll_sock->m_fd, comm_sock->m_fd, comm_sock->m_fd_type, events_num)));
                    continue;
                }
            }
        } else {
            /* catch error??? */
        }

    }

    return events_num;
}