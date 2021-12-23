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
 * comm_core.h
 *     Include all param data structure for Socket API invokation in proxy mode
 *     example:
 *        @api: socket()
 *        @data structure: CommSocketParam
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_core.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef COMM_CORE_H
#define COMM_CORE_H

#include <unistd.h>

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/epoll.h>
#include <poll.h>
#include <fcntl.h>

#include "comm_adapter.h"
#include "comm_connection.h"
#include "communication/commproxy_interface.h"
#include "utils/palloc.h"

/* other proxy interface added here */
#define INVALID_FD (-1)
#define MAX_EVENT_NUM 1024

#define INIT_QUEUE_SIZE 1024
#define MAX_PACKET_SIZE 8192*2
typedef enum PacketType {
    PKT_TYPE_INVALID = 0,
    PKT_TYPE_SEND,
    PKT_TYPE_RECV,
} PacketType;

typedef struct Packet {
    /* Package header information */
    int sockfd;

    /* data is appened here */
    char* data;
    char* outbuff;

    /* expected size that to read */
    int exp_size;

    /* actual size at current */
    int cur_size;

    int cur_off;

    /* actual read data */
    int comm_errno;

    PacketType type;

    volatile bool done;

    Packet* next_pkt;
} Packet;

extern Packet* CreatePacket(int socketfd, const char* sendbuff, char* recvbuff, int length, PacketType type);
extern void DropPacket(Packet** pack);
extern bool EndPacket(Packet* pack);
extern bool CheckPacketReady(Packet* pack);
extern void ClearPacket(Packet** pack);

#ifdef ENABLE_UT
void ProcessControlRequest(ThreadPoolCommunicator* comm);
void ProcessRecvRequest(ThreadPoolCommunicator* comm);
void ProcessSendRequest(ThreadPoolCommunicator* comm);
void ProcessControlRequestInternal(ThreadPoolCommunicator* comm, SocketRequest* sockreq);
int GetSysCommErrno(ThreadPoolCommunicator* comm, int fd);
#endif

/*
 * Special values indicate different proxy combinations
 */
#define UNINIT_SERVER_OFFSET (10)
#define UNINIT_SERVER_FD_START (-(UNINIT_SERVER_OFFSET))
#define UNINIT_SERVER_FD_MAX (UNINIT_SERVER_FD_START - MAX_COMM_PROXY_GROUP_CNT)
/*
 * -10 + 10 = 0 -> 0
 * -11 + 10 = -1 -> 1
 */
#define GET_PROXY_GROUP(x) (-(x + UNINIT_SERVER_OFFSET))
#define SET_PROXY_GROUP(x) (-(x) - UNINIT_SERVER_OFFSET)
#define AmIUnInitServerFd(fd) (fd <= UNINIT_SERVER_FD_START && fd > UNINIT_SERVER_FD_MAX)

#define UNINIT_CONNECT_FD (-20)

typedef struct SocketRequest {
    SocketRequestType s_sockreq_type;
    void* s_request_data;
    CommProxyInvokeResult* s_response_data;
    bool s_done;
    int s_errno;
    int s_fd; /* use to find communicator */

    CommExecStep s_step;
    unsigned long int s_req_id;

    void printf_req() {
        ereport(DEBUG4, (errmodule(MOD_COMM_PROXY),
            errmsg("user worker  : reqid[%016lu] fd:[%d] request type:[%d], ret:[%d], step:[%d]\n",
            s_req_id, s_fd, s_sockreq_type, s_response_data->s_ret, s_step)));
    }
} SocketRequest;

class ThreadPoolCommunicator;

typedef struct BroadcastRequest {
    bool s_is_broadcast;
    ThreadPoolCommunicator** s_broadcast_comms;
    int s_broadcast_num;
} BroadcastRequest;

typedef void (*ProxyerStatusCallbackFunc)(ThreadPoolCommunicator* comm, CommProxyerStatus status);

typedef struct CommProxyThreadStatus CommProxyThreadStatus;

/*
 *****************************************************************************************
 * CLASS -- ThreadPoolCommunicator
 *****************************************************************************************
 */
typedef struct CommProxyThreadStatus {
    int64               s_proxy_thread_id;
    char                s_thread_affinity[32];
    TimestampTz         s_start_time;
    volatile uint64     s_recv_packet_num;
    volatile uint64     s_send_packet_num;
    uint64              s_previous_recv_packet_num;
    uint64              s_previous_send_packet_num;
    float8              s_recv_pps;
    float8              s_send_pps;
} CommProxyThreadStatus;

class ThreadPoolCommunicator : public BaseObject {
public:
    ThreadPoolCommunicator(
        int group_id, int comm_id, int proxy_group_id, phy_proto_type_t proto_type = phy_proto_tcp);
    ~ThreadPoolCommunicator();

public:
    void StartUp(CommTransportMode transmode = CommModeRingBuffer);
    void ParseBindCores(int core_id);

    /* epoll functions */
    void CeateCommEpoll(int group_id);
    int AddCommEpoll(CommSockDesc* comm_sock, EventType event_type);
    void DelCommEpollInternal(int fd);
    int AddCommEpollInternal(CommSockDesc* comm_sock, EventType event_type);
    bool SetBlockMode(int sock, bool is_block);
    void AddActiveWorkerCommSock(CommSockDesc* comm_sock);
    void RemoveActiveWorkerCommSock(CommSockDesc* comm_sock);
    bool GetWorkerIsActive(int worker_id);
    void NotifyStatus(CommProxyerStatus status);
    void SetStatusCallback(ProxyerStatusCallbackFunc callback);
    void AddServicedFd(int fd);
    void RemoveServicedFd(int fd);
    void DownToDeInit();

public:
    int m_proxy_group_id;
    int m_comm_id;
    pthread_t m_thread_id;
    int m_epoll_fd;
    struct epoll_event* m_epoll_events;
    int m_group_id;

    int m_bind_core;

    volatile CommProxyerStatus m_proxyer_status;
    ProxyerStatusCallbackFunc m_proxyer_status_callback;

    /* DFX support */
    CommProxyThreadStatus   m_thread_status;

    /*
     * fd cnt created by this communicator
     * when create connect fd, we need select a communicator that servered least fd
     * one fd is created by 2 apis, but we need distinguish between libos/kernel fd
     *      socket:
     *              the fd created by the socket can be listened or connected. 
     *              it is uncertain whether the fd is libos fd
     *      accept: after accepting, we can check whether the fd is libos fd
     * so, we take the time to add to epoll list and add one to the count
     *             the time to del from epoll list and sub one to the count
     */
    volatile uint32 m_serve_fd_num;

    gs_api_t m_comm_api;
    CommTransportMode m_trans_mode;

    boost::lockfree::queue<SocketRequest*>* m_sockreq_queue;

    boost::lockfree::queue<int>* m_ready_sock_queue;
    volatile uint32 m_ready_sock_num;

    /*
     * record all fd be served by this communicator
     * for single node hack, connection is fix when tpcc start
     */
    CommFdInfoNode m_comm_fd_node_head;

#if COMM_SEND_QUEUE_WITH_DPDP_MPSC
    void *m_ready_sock_ring;
#endif

    boost::lockfree::stack<Packet*>* m_packet_buf[TotalChannel];

    ThreadId m_comm_proxy_thread_id;
    struct epoll_event* m_listen_epoll_events;
    int* m_listen_epoll_events_num;

    phy_proto_type_t m_proto_type;

    /*
     * CommProxy Dfx Support
     * - brief:for served workers number support
     */
    volatile uint32 m_served_workers_num;

    SocketRequest* GetSockRequest();
    void PutSockRequest(SocketRequest* req);

    /* comm-thread invoked function */
    virtual void ProcessOneRecvListenEvent(struct epoll_event* recv_event, bool has_indata, bool has_error);
    virtual void ProcessOneRecvDataEvent(struct epoll_event* recv_event, bool has_indata, bool has_error);
    virtual void ProcessOneConnectDataEvent(struct epoll_event* recv_event, bool has_indata, bool has_error);

    inline CommSockDesc* GetHeadSock()
    {
#if COMM_SEND_QUEUE_WITH_DPDP_MPSC
         CommSockDesc* p = NULL;
         MpScRingQueueDeque(m_ready_sock_ring, (void **)&p);
 
         return p;
#else
        CommSockDesc* p = NULL;
        int fd = -1;
        bool ret = m_ready_sock_queue->pop(fd);
        if (ret) {
            gaussdb_read_barrier();
            p = g_comm_controller->FdGetCommSockDesc(fd);
            comm_atomic_fetch_sub_u32(&m_ready_sock_num, 1);
        }

        return p;
#endif
    };

    inline void AddTailSock(CommSockDesc* p)
    {
    #if COMM_SEND_QUEUE_WITH_DPDP_MPSC
         int ret = MpScRingQueueEnque(m_ready_sock_ring, (void * const*)g_comm_controller->FdGetCommSockDescPtr(p->m_fd));
         if (ret < 0) {
             ereport(ERROR, (errmodule(MOD_COMM_PROXY),
                errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("fd[%d] AddTailSock fail", p->m_fd),
                errdetail("N/A"),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
         }
    #else
        m_ready_sock_queue->push(p->m_fd);
        gaussdb_write_barrier();
        comm_atomic_add_fetch_u32(&m_ready_sock_num, 1);
    #endif
    };

    inline uint32 GetSockCnt()
    {
    #if COMM_SEND_QUEUE_WITH_DPDP_MPSC
        return MpScRingQueueCount(m_ready_sock_ring);
    #else
        return comm_atomic_read_u32(&m_ready_sock_num);
    #endif
    };

    inline uint32 AddServedFdCnt()
    {
        return comm_atomic_add_fetch_u32(&m_serve_fd_num, 1);
    };

    inline uint32 SubServedFdCnt(int fd)
    {
        uint32 ret = comm_atomic_fetch_sub_u32(&m_serve_fd_num, 1);

        return ret;
    };

    inline void delFdFromFdList(int fd)
    {
        CommFdInfoNode *lastFdInfo = &m_comm_fd_node_head;
        CommFdInfoNode *curFdInfo = m_comm_fd_node_head.next;

        while (curFdInfo) {
            if (curFdInfo->fd == fd) {
                lastFdInfo->next = curFdInfo->next;
                comm_free(curFdInfo);
                curFdInfo = NULL;
                (m_comm_fd_node_head.fdNums)--;
                break;
            }
            lastFdInfo = curFdInfo;
            curFdInfo = curFdInfo->next;
        }

    }
};

extern void ProcessProxyWork(ThreadPoolCommunicator* comm);
extern void SubmitSocketRequest(SocketRequest* req);

extern bool CommSetBlockMode(Sys_fcntl fn, int sock, bool is_block);

extern void SetCPUAffinity(int cpu_id);
extern int gs_api_init(gs_api_t* gs_api, phy_proto_type_t proto);

/*
 * Send client
 */
extern int comm_worker_transport_send(CommSockDesc* comm_sock, const char* buffer, int length);
extern void comm_proxy_transport_send(CommSockDesc* sock_desc, ThreadPoolCommunicator *comm);

/*
 * Recv client
 */
/* Mode: pack queue */
extern int comm_worker_transport_recv(CommSockDesc* sock_desc, char *buffer, int exp_length);
extern void comm_proxy_transport_recv(CommSockDesc* sock_desc);

extern SendStatus comm_one_pack_send_by_doublequeue(Packet* pack);

extern void comm_proxy_send_ready_queue(ThreadPoolCommunicator* comm);
extern void comm_proxy_send_polling(ThreadPoolCommunicator* comm);

extern bool is_null_getcommsockdesc(const CommSockDesc* comm_sock, const char* fun_name, const int fd);

#endif /* COMM_CORE_H */
