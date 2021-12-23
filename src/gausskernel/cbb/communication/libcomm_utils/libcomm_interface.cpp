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
 * libcomm_interface.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/libcomm_utils/libcomm_interface.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <libcgroup.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <net/if.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/param.h>
#include <sys/time.h>
#include <unistd.h>

#include "../libcomm_core/mc_tcp.h"
#include "../libcomm_core/mc_poller.h"
#include "../libcomm_utils/libcomm_thread.h"
#include "../libcomm_common.h"
#include "libcomm_lqueue.h"
#include "libcomm_queue.h"
#include "libcomm_lock_free_queue.h"
#include "distributelayer/streamCore.h"
#include "distributelayer/streamProducer.h"
#include "pgxc/poolmgr.h"
#include "libpq/auth.h"
#include "libpq/pqsignal.h"
#include "storage/ipc.h"
#include "utils/ps_status.h"
#include "utils/dynahash.h"

#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecnodes.h"
#include "executor/exec/execStream.h"
#include "miscadmin.h"
#include "gssignal/gs_signal.h"
#include "pgxc/pgxc.h"

#ifdef ENABLE_UT
#define static
#endif

// the beginnig of global variable definition
int g_ackchk_time = 2000;           // default 2s

extern LibcommAdaptLayer g_libcomm_adapt;
extern int gs_check_cmailbox_data(const gsocket* gs_sock_array,  int nproducer, int* producer,
                                  bool close_expected, check_cmailbox_option opt);

bool is_tcp_mode()
{
    return g_libcomm_adapt.recv_data == libcomm_tcp_recv;
}

static void recv_ackchk_msg(int sock)
{
    /* receive ack package */
    struct pollfd input_fd;

    input_fd.fd = sock;
    input_fd.events = POLLIN;
    input_fd.revents = 0;
    if (poll(&input_fd, 1, 0) > 0) {
        char buf[1024];

        /* Receive ack check message, we don't care the content */
        (void)recv(input_fd.fd, buf, sizeof(buf), 0);
    }
}

// send control message to remoter without_lock
// the parameter sock and the fd_id are result parameters
//
int gs_send_ctrl_msg_without_lock(struct node_sock* ns, FCMSG_T* msg, int node_idx, int role)
{
    int ctrl_sock = -1;
    int ctrl_sock_id = -1;
    int rc = -1;
    int send_bytes = 0;
    uint64 time_enter, time_now;
    time_enter = mc_timers_ms();

    ctrl_sock = ns->get_nl(CTRL_TCP_SOCK, &ctrl_sock_id);
    if (ctrl_sock >= 0) {
        while (1) {
            /* we send data in non-block mode,
             * but we will assure the data will
             * be sent out if the network is ok
             */
            rc = mc_tcp_write_noblock(ctrl_sock, (char*)msg + send_bytes, sizeof(FCMSG_T) - send_bytes);

            if (rc < 0) {
                break;
            }

            /* check if other thread has closed current connection, for producer only */
            if ((role == ROLE_PRODUCER) && (g_instance.comm_cxt.g_s_node_sock[node_idx].ip_changed == true)) {
                errno = ECOMMTCPPEERCHANGED;
                rc = -1;
                shutdown(ctrl_sock, SHUT_RDWR);
                break;
            }

            send_bytes += rc;
            if ((uint32)send_bytes >= sizeof(FCMSG_T)) {
                break;
            }
            time_now = mc_timers_ms();
            if (((time_now - time_enter) >
                    (((uint64)(unsigned)g_instance.comm_cxt.counters_cxt.g_comm_send_timeout) * SEC_TO_MICRO_SEC)) &&
                (time_now > time_enter)) {
                errno = ECOMMTCPSENDTIMEOUT;
                rc = -1;
                shutdown(ctrl_sock, SHUT_RDWR);
                break;
            }
        }
    }

    // if tcp send failed, close tcp connction
    if (rc <= 0) {
        LIBCOMM_ELOG(WARNING,
            "(SendCtrlMsg)\tFailed to send type[%s] message to node:%s with socket[%d,%d]:%s.",
            ctrl_msg_string(msg->type),
            ns->remote_nodename,
            ctrl_sock,
            ctrl_sock_id,
            mc_strerror(errno));
    }

    COMM_DEBUG_CALL(printfcmsg("SendCtrlMsg", msg));

    return rc;
}  // gs_r_send_ctrl_msg

// send control message to remoter
// the parameter sock and the fd_id are result parameters
//
int gs_send_ctrl_msg(struct node_sock* ns, FCMSG_T* msg, int role)
{
    int rc = -1;
    if (msg->node_idx == 0 && msg->streamid == 0) {
        return 0;
    }
    ns->lock();
    rc = gs_send_ctrl_msg_without_lock(ns, msg, msg->node_idx, role);
    ns->unlock();
    return rc;
}  // gs_r_send_ctrl_msg

// send control message to remoter
// by sokcet
//
int gs_send_ctrl_msg_by_socket(LibCommConn *conn, FCMSG_T* msg, int node_idx)
{
    int rc = LibCommClientWriteBlock(conn, (void*)msg, sizeof(FCMSG_T));  // do send message

    // if tcp send failed, close tcp connction
    if (rc <= 0) {
        LIBCOMM_ELOG(WARNING,
            "(SendCtrlMsg)\tFailed to send type[%s] message to node[%d]:%s with socket[%d]:%s.",
            ctrl_msg_string(msg->type),
            msg->node_idx,
            msg->nodename,
            conn->socket,
            mc_strerror(errno));
    }

    COMM_DEBUG_CALL(printfcmsg("SendCtrlMsg", msg));

    return rc;
}  // gs_r_send_ctrl_msg

static int getRecvError(const sock_id fdId, int nodeIdx, struct mc_lqueue_item* iovItem)
{
    int error = RECV_NEED_RETRY;
    struct iovec* iov = iovItem->element.data;
    uint16 msgType = *(uint16*)iov->iov_base;
    
    switch (msgType) {
        case LIBCOMM_PKG_TYPE_CONNECT:
            error = gs_accept_data_conntion(iov, fdId);
            break;

        case LIBCOMM_PKG_TYPE_DELAY_REQUEST:
        case LIBCOMM_PKG_TYPE_DELAY_REPLY:
            error = gs_handle_data_delay_message(nodeIdx, iovItem, msgType);
            if (0 == error) {
                /* iov save to cmailbox[idx][0], no need free */
                error = RECV_NEED_RETRY;
            }
            break;

        default:
            struct libcomm_delay_package* delayMsg = (struct libcomm_delay_package*)iov->iov_base;
            COMM_DEBUG_LOG("[DELAY_INFO]recv invalid type[%d] sn=%d\n", msgType, delayMsg->sn);
            error = RECV_NET_ERROR;
            break;
    }
    return error;
}

void WakeupSession(SessionInfo *session_info, bool err_occurs, const char* caller_name)
{
    CommEpollFd *comm_epfd = NULL;

    if (session_info == NULL) {
        LIBCOMM_ELOG(LOG, "Trace: CommEpollFd NotifyListener(%s), no register", caller_name);
        return;
    }

    if (gs_compare_and_swap_32(&session_info->is_idle, (int)true, (int)false) == false) {
        LIBCOMM_ELOG(LOG, "Trace: CommEpollFd NotifyListener(%s), no event "
            "detail: idx[%d], streamid[%d], is_idle[false]",
            caller_name,
            session_info->commfd.logic_fd.idx,
            session_info->commfd.logic_fd.sid);
        return;
    }

    comm_epfd = session_info->comm_epfd_ptr;
    comm_epfd->PushReadyEvent(session_info, err_occurs);
    LIBCOMM_ELOG(LOG, "Trace: CommEpollFd DoWakeup(%s), normal event detail:epfd[%d], idx[%d], streamid[%d]",
        caller_name,
        comm_epfd->m_epfd,
        session_info->commfd.logic_fd.idx,
        session_info->commfd.logic_fd.sid);
    comm_epfd->DoWakeup();
}

void NotifyListener(struct c_mailbox* cmailbox, bool err_occurs, const char* caller_name)
{
    Assert(cmailbox);
    SessionInfo *session_info = NULL;

    if (cmailbox->session_info_ptr) {
        session_info = cmailbox->session_info_ptr;
    } else {
        LogicFd logic_fd = {cmailbox->idx, cmailbox->streamid};
        session_info = g_instance.comm_logic_cxt.comm_fd_collection->GetSessionInfo(&logic_fd);
        cmailbox->session_info_ptr = session_info;
    }

    WakeupSession(session_info, err_occurs, caller_name);
}


// the broker: receive message from give socket and put it into
//      the corressponding mailbox in g_c_mailbox, then wake up Consumer of executor to fetch the data
// sock        : the data socket which has epoll events
// node_idx    : the node index in the global variables
// flag        : flag for recv interface
// step1: initialize local variables and the message buffer
// step2: receive the data message
// setp3: get the stream id
// setp4: check the quota of the mailboxreport error or put the message into the mailbox
// step5: notify the Consumer to take away the data
// step6: check the quota and state of the c_mailbox and notify auxiliary thread to set the state
//
int gs_internal_recv(const sock_id fd_id, int node_idx, int flags)
{
    int recvsk = fd_id.fd;
    int idx = node_idx;
    int error = 0;
    struct c_mailbox* cmailbox = NULL;
    // Initialize inmessage with enough space for DATA, and control message.
    COMM_TIMER_INIT();

    for (;;) {
        LibcommRecvInfo recv_info = {0};
        COMM_TIMER_LOG("(r|inner recv)\tInternal receive start.");

        int64 curr_rcv_time = 0;
        int64 last_rcv_time = 0;
        if (idx >= 0) {
            last_rcv_time = g_instance.comm_cxt.g_receivers->receiver_conn[idx].last_rcv_time;
        }

        recv_info.socket = recvsk;
        recv_info.node_idx = idx;
        if (idx >= 0) {
            LIBCOMM_PTHREAD_RWLOCK_WRLOCK(&g_instance.comm_cxt.g_receivers->receiver_conn[idx].rwlock);
        }
        error = g_libcomm_adapt.recv_data(&recv_info);
        if (idx >= 0) {
            LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_receivers->receiver_conn[idx].rwlock);
        }

        COMM_TIMER_LOG("(r|inner recv)\tInternal receive something.");

        // not real network error, it can be resolved by trying again
        if (unlikely((error == RECV_NEED_RETRY))) {
            COMM_DEBUG_LOG("(r|inner recv)\tReceiver from node[%d] socket[%d]:%s.", idx, recvsk, strerror(errno));
            return RECV_NEED_RETRY;
        }

        // real network errors, we should report it
        // errno is not EAGAIN/EWOULDBLOCK/EINTR
        if (unlikely(error < 0)) {
            LIBCOMM_ELOG(WARNING,
                "(r|inner recv)\tFailed to receive sock[%d], "
                "return %d, from node[%d]:%s, %s.",
                recvsk,
                error,
                idx,
                REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx),
                strerror(errno));
            return error;
        }

        int streamid = recv_info.streamid;
        int version = recv_info.version;
        struct mc_lqueue_item* iov_item = recv_info.iov_item;
        struct iovec* iov = iov_item->element.data;

        /*
         * We recv a small message and the last message is very early,
         * we want to send a ack message in order to avoid tcp ack package missing.
         * The message should be less than MTU, so we use 1024 byte.
         */
        if (is_tcp_mode() && streamid > 0 && g_ackchk_time > 0 && iov->iov_len < 1024 && idx >= 0) {
            /* send ack message. */
            curr_rcv_time = g_instance.comm_cxt.g_receivers->receiver_conn[idx].last_rcv_time;
            if (last_rcv_time > 0 && curr_rcv_time - last_rcv_time > g_ackchk_time) {
#ifdef USE_SSL
                if (g_instance.attr.attr_network.comm_enable_SSL) {
                    SSL* ssl = LibCommClientGetSSLForSocket(recvsk);
                    if (ssl != NULL) {
                        LIBCOMM_ELOG(LOG, "gs_internal_recv start\n");
                        LibCommClientSSLWrite(ssl, "ACK", strlen("ACK"));
                    } else {
                        return RECV_NEED_RETRY;
                    }
                } else
#endif
                {
                    send(recvsk, "ACK", strlen("ACK"), 0);
                }
            }
        }

        /* Speicail message for connection request */
        if (streamid == 0) {
            error = getRecvError(fd_id, node_idx, iov_item);
            if (error == RECV_NEED_RETRY) {
                return RECV_NEED_RETRY;
            }

            libcomm_free_iov_item(&iov_item, IOV_DATA_SIZE);
            return error;
        } else if (streamid > 0 && idx >= 0) { // if the sid is not 0, we should receive data
            cmailbox = &C_MAILBOX(idx, streamid);
            if (gs_push_cmailbox_buffer(cmailbox, iov_item, version) < 0) {
                libcomm_free_iov_item(&iov_item, IOV_DATA_SIZE);
            }

            if (ENABLE_THREAD_POOL_DN_LOGICCONN) {
                NotifyListener(cmailbox, false, __FUNCTION__);
            }
            COMM_TIMER_LOG("(r|inner recv)\tInternal receive finish for [%d,%d].", idx, streamid);
            return RECV_NEED_RETRY;
        } else {
            COMM_DEBUG_LOG("(r|inner recv)\tWrong stream id %d:%d from Node %d, sock[%d], error[%d]:%s.",
                streamid,
                version,
                idx,
                recvsk,
                error,
                strerror(errno));
            libcomm_free_iov_item(&iov_item, IOV_DATA_SIZE);
            return RECV_NEED_RETRY;
        }
    }
}  // gs_internal_recv

// take data from the cmailbox, called by Consumer thread in executor
// step1: get mailbox by node index and stream index
// step2: check if there is data in the buffer of mailbox
// step3: take away the data in the buffer queue
// step4: notify auxiliary thread to check the state of the mailbox, then copy the data into buff, then release the
// internal buffer step5: return the length of received data
//
int gs_recv(
    gsocket* gs_sock,  // array of logic conn index, which labeled the logic_conn will recv data from this logic_conn
    void* buff,        // buffer to copy data message, allocated by caller
    int buff_size)     // size of buffer
{
    if ((gs_sock == NULL) || (buff == NULL) || (buff_size <= 0)) {
        LIBCOMM_ELOG(WARNING,
            "(r|recv)\tInvalid argument: "
            "%s%sbuff size:%d.",
            gs_sock == NULL ? "gs_sock is NULL, " : "",
            buff == NULL ? "buff is NULL, " : "",
            buff_size);
        errno = ECOMMTCPARGSINVAL;
        return -1;
    }

    uint64 time_enter = COMM_STAT_TIME();
    uint64 time_now = time_enter;
    int idx = gs_sock->idx;
    int streamid = gs_sock->sid;
    int version = gs_sock->ver;
    struct FCMSG_T fcmsgs = {0x0};

    if ((idx < 0) || (idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num) || (streamid <= 0) ||
        (streamid >= g_instance.comm_cxt.counters_cxt.g_max_stream_num)) {
        LIBCOMM_ELOG(WARNING, "(r|recv)\tInvalid argument: node idx:%d, stream id:%d.", idx, streamid);
        errno = ECOMMTCPARGSINVAL;
        return -1;
    }
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    int ret = -1;
    struct c_mailbox* cmailbox = NULL;
    struct iovec* iov = NULL;
    struct mc_lqueue_item* q_item = NULL;
    bool is_notify_quota = false;
    bool TempImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = false;
    errno_t ss_rc = 0;
    errno = 0;

    PGSTAT_INIT_TIME_RECORD();
    PGSTAT_START_TIME_RECORD();
    COMM_TIMER_INIT();

    // step1: get idx and streamid
    COMM_TIMER_LOG("(r|recv)\tReceive start for node[%d,%d]:%s.",
        idx,
        streamid,
        REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx));

    // step2: check mailbox
    cmailbox = &(C_MAILBOX(idx, streamid));
    LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);

    // check the state of the mailbox is correct
    if (false == gs_check_mailbox(cmailbox->local_version, version)) {
        MAILBOX_ELOG(
            cmailbox, WARNING, "(r|recv)\tStream has already closed, detail:%s.", mc_strerror(cmailbox->close_reason));
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
        errno = cmailbox->close_reason;
        ret = -1;
        goto return_result;
    }

    if (cmailbox->buff_q->is_empty == 1) {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
        errno = ECOMMTCPNODATA;
        ret = -1;
        goto return_result;
    }

    // step3: get data block from mailbox
    q_item = mc_lqueue_remove(cmailbox->buff_q, q_item);
    if (q_item != NULL) {
        iov = q_item->element.data;
    } else {
        iov = NULL;
    }

    COMM_TIMER_LOG("(r|recv)\tReceived data for node[%d,%d]:%s.",
        idx,
        streamid,
        REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx));

    // there is no data
    if (iov == NULL || iov->iov_len == 0) {
        // release the internal buffer
        libcomm_free_iov_item(&q_item, IOV_DATA_SIZE);

        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
        errno = ECOMMTCPNODATA;
        ret = -1;
        goto return_result;
    }
    // step4: update quota
    if (g_instance.comm_cxt.quota_cxt.g_having_quota) {
        // notify auxiliary thread to check the state of the mailbox
        is_notify_quota = gs_r_quota_notify(cmailbox, &fcmsgs);
        COMM_TIMER_LOG("(r|recv)\tSend quota to node[%d,%d]:%s.",
            idx,
            streamid,
            REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx));
    }

    COMM_DEBUG_LOG("(r|recv)\tNode[%d]:%s stream[%d] recv %zu msg:%c. "
                   "bufCAP[%lu] and buff_q->u_size[%lu].",
        idx,
        g_instance.comm_cxt.g_r_node_sock[idx].remote_nodename,
        streamid,
        iov->iov_len,
        ((char*)iov->iov_base)[0],
        cmailbox->bufCAP,
        cmailbox->buff_q->u_size);

    time_now = COMM_STAT_TIME();
    COMM_STAT_CALL(cmailbox, cmailbox->statistic->gs_recv_time += ABS_SUB(time_now, time_enter));

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
    // send control message without cmailbox lock
    if (is_notify_quota && (gs_send_ctrl_msg(&g_instance.comm_cxt.g_r_node_sock[idx], &fcmsgs, ROLE_CONSUMER) <= 0)) {
        // failed to send control message, clear the information of the node
        MAILBOX_ELOG(cmailbox, WARNING, "(r|quota notify)\tFailed to send quota:%s.", mc_strerror(errno));
    }

    // step5: copy data then return
    // get the need data length
    // warning: if the buffer length is smaller than real data length,
    // the redundant data will be dropped
    //
    LIBCOMM_ASSERT((bool)(buff_size >= (int)iov->iov_len), idx, streamid, ROLE_CONSUMER);
    ret = (int)iov->iov_len;
    // copy the datat to the buffer of executor
    ss_rc = memcpy_s(buff, buff_size, iov->iov_base, iov->iov_len);
    securec_check(ss_rc, "\0", "\0");

    // release the internal buffer
    libcomm_free_iov_item(&q_item, IOV_DATA_SIZE);

    COMM_TIMER_LOG("(r|recv)\tReceive finish for node[%d,%d]:%s.",
        idx,
        streamid,
        REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx));

return_result:
    LIBCOMM_INTERFACE_END(false, TempImmediateInterruptOK);

    END_NET_STREAM_RECV_INFO(ret);
    return ret;
}  // gs_recv

/*
 * function name    : gs_receivers_flow_handle_tid_request
 * description    : save peer thread id when received CTRL_PEER_TID message.
 * arguments        : _in_ fcmsgr: the message that receivers_flow thread received.
 * return value    : void
 */
void gs_receivers_flow_handle_tid_request(FCMSG_T* fcmsgr)
{
    int streamid = fcmsgr->streamid;
    int node_idx = fcmsgr->node_idx;
    int version = fcmsgr->version;
    struct c_mailbox* cmailbox = NULL;

    cmailbox = &(C_MAILBOX(node_idx, streamid));
    LIBCOMM_PTHREAD_MUTEX_LOCK(&(cmailbox->sinfo_lock));
    // check stream key and mailbox state
    if (true == gs_check_mailbox(cmailbox->local_version, version)) {
        cmailbox->peer_thread_id = fcmsgr->extra_info;
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&(cmailbox->sinfo_lock));
}

// Sender use the function to connect to the receiver.
// 1. connect to the remote datanode
// 2. get stream index for Producer
//
static int gs_internal_connect(libcommaddrinfo* libcomm_addrinfo)
{
    /*
     * Step 1: Initialize local variable
     */
    if (libcomm_addrinfo == NULL) {
        LIBCOMM_ELOG(WARNING, "(s|connect)\tInvalid argument: libcomm addr info is NULL");
        errno = ECOMMTCPARGSINVAL;
        return -1;
    }

    if (unlikely((libcomm_addrinfo->host == NULL) || (libcomm_addrinfo->ctrl_port <= 0) ||
                 (libcomm_addrinfo->listen_port <= 0))) {
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tInvalid argument: %s"
            "tcp port:%d, libcomm port:%d.",
            libcomm_addrinfo->host == NULL ? "host is NULL, " : "",
            libcomm_addrinfo->ctrl_port,
            libcomm_addrinfo->listen_port);
        errno = ECOMMTCPARGSINVAL;
        return -1;
    }

    int rc = 0;
    int node_idx = -1;
    int streamid = -1;
    errno_t ss_rc;
    uint32 cpylen;
    int to_ctrl_tcp_port = libcomm_addrinfo->ctrl_port;

    uint64 time_enter = COMM_STAT_TIME();
    uint64 time_now = time_enter;

    /*
     * Step 1: Modify localhost to IP
     */
    if (IS_LOCAL_HOST(libcomm_addrinfo->host)) {
        libcomm_addrinfo->host = "127.0.0.1";
    }

    /*
     * Step 2: Get or set node index by remote nodename
     */
    COMM_TIMER_INIT();

    node_idx = gs_get_node_idx(libcomm_addrinfo->nodename);
    if (unlikely(node_idx < 0)) {
        LIBCOMM_ELOG(WARNING,
            "(s|send connect)\tFailed to get node index for %s: %s.",
            libcomm_addrinfo->nodename,
            mc_strerror(errno));
        return -1;
    }

    COMM_TIMER_LOG("(s|send connect)\tConnect start for node[%d]:%s.",
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    /* Step 3: Check and build ctrl connection with the remote point (if need) */
    COMM_TIMER_LOG("(s|send connect)\tBuild ctrl channel connect for node[%d]:%s.",
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    if (unlikely(gs_s_check_connection(libcomm_addrinfo, node_idx, false, CTRL_CHANNEL) == false)) {
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tFailed to connect to host:port[%s:%d], node name[%s].",
            libcomm_addrinfo->host,
            to_ctrl_tcp_port,
            libcomm_addrinfo->nodename);
        return -1;
    }

    /* Step 4: Check and build data connection with the remote point (if need) */
    COMM_TIMER_LOG("(s|send connect)\tBuild data connection for node[%d]:%s.",
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    // failed to build data connection
    if (unlikely(gs_s_check_connection(libcomm_addrinfo, node_idx, false, DATA_CHANNEL) == false)) {
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tFailed to build data connection "
            "to %s:%d for node[%d]:%s, detail:%s.",
            libcomm_addrinfo->host,
            to_ctrl_tcp_port,
            node_idx,
            libcomm_addrinfo->nodename,
            mc_strerror(errno));
        errno = ECOMMTCPCONNFAIL;
        return -1;
    }

    COMM_TIMER_LOG("(s|send connect)\tBuild data logical connection for node[%d]:%s.",
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    /*
     * Step 6: Get stream id, initialize pmailbox and save information keys -> stream index in
     * hash table (g_s_htab_nodeid_skey_to_stream)
     */
    streamid = gs_get_stream_id(node_idx);
    if (streamid < 0) {
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tFailed to obtain logic stream idx for connect %s:%d, node:%s.",
            libcomm_addrinfo->host,
            to_ctrl_tcp_port,
            libcomm_addrinfo->nodename);
        return -1;
    }

    struct p_mailbox* pmailbox = &P_MAILBOX(node_idx, streamid);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
    Assert(pmailbox->state == MAIL_CLOSED);

    int version = pmailbox->local_version + 1;
    if (version >= MAX_MAILBOX_VERSION) {
        version = 0;
    }

    pmailbox->local_version = version;
    pmailbox->ctrl_tcp_sock = g_instance.comm_cxt.g_s_node_sock[node_idx].get_nl(CTRL_TCP_SOCK, NULL);
    pmailbox->state = MAIL_READY;
    pmailbox->bufCAP = 0;
    pmailbox->stream_key = libcomm_addrinfo->streamKey;
    pmailbox->query_id = DEBUG_QUERY_ID;
    pmailbox->local_thread_id = 0;
    pmailbox->peer_thread_id = 0;
    pmailbox->close_reason = 0;
    if (g_instance.comm_cxt.commutil_cxt.g_stat_mode && (pmailbox->statistic == NULL)) {
        LIBCOMM_MALLOC(pmailbox->statistic, sizeof(struct pmailbox_statistic), pmailbox_statistic);
        if (NULL == pmailbox->statistic) {
            errno = ECOMMTCPRELEASEMEM;
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&(pmailbox->sinfo_lock));
            return -1;
        }
    }

    /* update the statistic information of the mailbox */
    if (pmailbox->statistic != NULL) {
        time_now = COMM_STAT_TIME();
        pmailbox->statistic->start_time = time_now;
        pmailbox->statistic->connect_time = time_enter;
    }

    /*
     * Step 7: Send connection request to consumer with stream id
     * Send ready control message
     */
    COMM_TIMER_LOG("(s|send connect)\tSend ready message to node[%d, %d]:%s.",
        node_idx,
        streamid,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    struct FCMSG_T fcmsgs = {0x0};

    // for connect between cn and dn, channel is duplex
    fcmsgs.type = (IS_PGXC_COORDINATOR) ? CTRL_CONN_DUAL : CTRL_CONN_REQUEST;
    fcmsgs.extra_info = (IS_PGXC_COORDINATOR && (u_sess != NULL)) ? 
                        (unsigned long)(unsigned)u_sess->pgxc_cxt.NumDataNodes : 0;

    fcmsgs.node_idx = node_idx;
    fcmsgs.streamid = streamid;
    // send pmailbox version to cmailbox,
    // and cmailbox will save as cmailbox->remote_version.
    fcmsgs.version = version;
    fcmsgs.stream_key = libcomm_addrinfo->streamKey;
    fcmsgs.query_id = DEBUG_QUERY_ID;
    cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
    ss_rc = memset_s(fcmsgs.nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(fcmsgs.nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    fcmsgs.nodename[cpylen] = '\0';

    // streamcap contain the ctrl port and data port of remote process.
    // we use streamcap to send these ports
    // becuase we do not want to add too much members to FCMSG_T
    // so we usually use streamcap to send some extra msgs
    fcmsgs.streamcap = ((unsigned long)(unsigned)g_instance.attr.attr_network.comm_control_port << 32) +
                       (long)g_instance.attr.attr_network.comm_sctp_port;

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&(pmailbox->sinfo_lock));
    rc = gs_send_ctrl_msg(&g_instance.comm_cxt.g_s_node_sock[node_idx], &fcmsgs, ROLE_PRODUCER);
    if (rc <= 0) {
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tFailed to send ready msg to node[%d]:%s, detail:%s.",
            node_idx,
            REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx),
            mc_strerror(errno));

        errno = ECOMMTCPTCPDISCONNECT;
        return -1;
    }

    /*
     * Step 8: set the node index and stream index for caller, and return the stream index
     */
    libcomm_addrinfo->gs_sock.idx = node_idx;
    libcomm_addrinfo->gs_sock.sid = streamid;
    libcomm_addrinfo->gs_sock.ver = version;
    libcomm_addrinfo->gs_sock.type = IS_PGXC_COORDINATOR ? GSOCK_DAUL_CHANNEL : GSOCK_PRODUCER;

    COMM_DEBUG_LOG("(s|send connect)\tConnect finish for node[%d]:%s.",
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    return streamid;
}  // gs_internal_connect

void gs_close_timeout_connections(libcommaddrinfo** libcomm_addrinfo, int addr_num,
                                         int node_idx, int streamid)
{
    int i;
    struct p_mailbox* pmailbox = NULL;

    for (i = 0; i < addr_num; i++) {
        pmailbox = &P_MAILBOX(libcomm_addrinfo[i]->gs_sock.idx, libcomm_addrinfo[i]->gs_sock.sid);
        LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
        if ((gs_check_mailbox(pmailbox->local_version, libcomm_addrinfo[i]->gs_sock.ver)) &&
            (pmailbox->state == MAIL_READY)) {
            LIBCOMM_ELOG(WARNING,
                "(s|parallel connect)\t wait ready response timeout node[%d] stream[%d], node "
                "name[%s].",
                node_idx,
                streamid,
                g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename);
            gs_s_close_logic_connection(pmailbox, ECOMMTCPEPOLLTIMEOUT, NULL);
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
    }
}

/*
 * function name   : gs_s_check_connection
 * description     : sender check that is receiver changed.
 *                   if the destination ip is changed,
 *                   which means the primary and the standby are reverted,
 *                   then we close tcp connection and rebuild later.
 * arguments       :
 *                   _in_ libcomm_addrinfo: remote infomation.
 *                   _in_ node_idx: remote node index.
 * return value    :
 *                   false: failed.
 *                   true : succeed.
 */
bool gs_s_check_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx, bool is_reply, int type)
{
    ip_key addr;
    errno_t ss_rc;
    uint32 cpylen;

    struct sock_id fd_id = {-1, -1};

    cpylen = comm_get_cpylen(libcomm_addrinfo->host, HOST_LEN_OF_HTAB);
    ss_rc = memset_s(addr.ip, HOST_LEN_OF_HTAB, 0x0, HOST_LEN_OF_HTAB);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(addr.ip, HOST_LEN_OF_HTAB, libcomm_addrinfo->host, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    addr.ip[cpylen] = '\0';

    if (type == CTRL_CHANNEL) {
        addr.port = libcomm_addrinfo->ctrl_port;
    } else {
        addr.port = libcomm_addrinfo->listen_port;
    }

retry:

    int rc = gs_s_get_connection_state(addr, node_idx, type);

    if (likely(rc == CONNSTATESUCCEED)) {
        /* data channel need to check socket even if the connection state in htap is succeed */
        if (type == DATA_CHANNEL) {
            LIBCOMM_PTHREAD_RWLOCK_WRLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
            if (LibCommClientCheckSocket(g_instance.attr.attr_network.comm_data_channel_conn[node_idx - 1]) != 0) {
                fd_id.fd = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket;
                fd_id.id = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id;
                LIBCOMM_ELOG(WARNING,
                    "(s|connect)\tFailed to check libcomm socket "
                    "node[%d]:%s, errno[%d]:%s, close socket[%d,%d].",
                    node_idx,
                    g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
                    errno,
                    mc_strerror(errno),
                    fd_id.fd,
                    fd_id.id);
                gs_s_close_bad_data_socket(&fd_id, ECOMMTCPDISCONNECT, node_idx);
                LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
                /* retry to build new connection, next rc must be CONNSTATECONNECTING */
                goto retry;
            }
            LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
        }

        COMM_DEBUG_LOG("(s|connect)\tAlready has connection:port[%s:%d], node name[%s]].",
            addr.ip,
            addr.port,
            libcomm_addrinfo->nodename);
        return true;
    } else if (rc == CONNSTATECONNECTING) {
        if (type == CTRL_CHANNEL) {
            rc = gs_s_build_tcp_ctrl_connection(libcomm_addrinfo, node_idx, is_reply);
        } else {
            rc = g_libcomm_adapt.connect(libcomm_addrinfo, node_idx);
        }
        /* update connection state to fail when connect failed */
        if (rc < 0) {
            gs_update_connection_state(addr, CONNSTATEFAIL, true, node_idx);
            return false;
        }
        return true;
    } else {
        COMM_DEBUG_LOG("(s|connect)\tFailed checking connection state:port[%s:%d], node name[%s] error[%s].",
            libcomm_addrinfo->host,
            libcomm_addrinfo->ctrl_port,
            libcomm_addrinfo->nodename,
            mc_strerror(errno));
        return false;
    }
}  // gs_s_check_connection

int gs_clean_connection(libcommaddrinfo** libcomm_addrinfo, int addr_num, int error_index,
                             int re, bool TempImmediateInterruptOK)
{
    int i = 0;
    libcommaddrinfo* addr_info = NULL;

    LIBCOMM_ELOG(WARNING,
        "(s|parallel connect)\tFailed to connect node:%s, detail:%s.",
        libcomm_addrinfo[error_index]->nodename,
        mc_strerror(errno));

    for (i = 0; i < addr_num; i++) {
        addr_info = libcomm_addrinfo[i];
        gs_close_gsocket(&(addr_info->gs_sock));
    }

    // For timeout, we need to check the interruption
    LIBCOMM_INTERFACE_END((re == ETIMEDOUT), TempImmediateInterruptOK);

    // when error_index is 0, return 1; 0 means succeed
    return (error_index + 1);
}

/*
* function name    : gs_connect
* description    : build connects one or multiple address.
* arguments        : _in_ libcomm_addrinfo: the address info list.
*                  _in_ addr_num: the number of address info list.
*                  _in_ error_index: error index of address info list wher connect failed.
* return value    :  0: succeed
*            : -1: all connection failed
            : other value: failed connection index
* call gs_internal_connect, cause we want to receive a MAIL_READY message,
* which means the logic connection has been build successfully.
*/
int gs_connect(libcommaddrinfo** libcomm_addrinfo, int addr_num, int timeout)
{
    int re = -1;
    int i;
    int ret;
    if (timeout == -1) {
        timeout = CONNECT_TIMEOUT;
    }
    libcommaddrinfo* addr_info = NULL;
    int error_index = -1;

    if (libcomm_addrinfo == NULL || addr_num == 0) {
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tInvalid argument: %saddress number is %d.",
            libcomm_addrinfo == NULL ? "libcomm addr info is NULL, " : "",
            addr_num);
        errno = ECOMMTCPARGSINVAL;
        return -1;
    }

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    bool TempImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = false;
    errno = 0;

    if (gs_poll_create() != 0) {
        LIBCOMM_ELOG(WARNING, "(s|parallel connect)\tFailed to malloc for create poll!");
        LIBCOMM_INTERFACE_END(false, TempImmediateInterruptOK);
        return -1;
    }
#ifdef USE_SSL
    if (g_instance.attr.attr_network.comm_enable_SSL) {
        ret = LibCommClientSecureInit();
        if (ret != 0) {
            LIBCOMM_ELOG(ERROR, "(s|connect|SSL)\tSet libcomm ssl context initialize failed!\n");
            return -1;
        }
    }
#endif
    // producer build connections with address list,
    // and send MAIL_READY message to consumers.
    for (i = 0; i < addr_num; i++) {
        addr_info = libcomm_addrinfo[i];
        addr_info->gs_sock = GS_INVALID_GSOCK;

        pgstat_report_waitstatus_comm(STATE_STREAM_WAIT_CONNECT_NODES,
            addr_info->nodeIdx,
            addr_num - i,
            -1,
            global_node_definition ? global_node_definition->num_nodes : -1);

        re = gs_internal_connect(addr_info);
        // errno set in gs_connect
        if (re < 0) {
            if (IS_PGXC_COORDINATOR) {
                continue;
            } else {
                error_index = i;
                return gs_clean_connection(libcomm_addrinfo, addr_num, error_index,
                                           re, TempImmediateInterruptOK);
            }
        }
    }

    ret = gs_check_all_mailbox(libcomm_addrinfo, addr_num, re, TempImmediateInterruptOK, timeout);
    if (ret > 0) {
        return ret;
    }

    LIBCOMM_INTERFACE_END(false, TempImmediateInterruptOK);

    // if gs_check_mailbox return false, means consumer close it, not need report error
    return 0;
}

static int GsWaitPmailboxReady(
    struct p_mailbox* pmailbox, SendOptions* options, TimeProfile* time, WaitResult* wr)
{
    int node_idx = pmailbox->idx;
    int streamid = pmailbox->streamid;

    int total_wait_time = 0;
    int single_timeout = SINGLE_WAITQUOTA;

    for (;;) {
        // stream state is right and quota size is enough
        if (pmailbox->bufCAP >= options->need_send_len) {
            pmailbox->state = MAIL_RUN;
            return 0;
        }

        // if the quota is not enough for next sending, the state should be changed
        pmailbox->state = MAIL_HOLD;
        pmailbox->semaphore = t_thrd.comm_cxt.libcomm_semaphore;
        COMM_DEBUG_LOG("(s|send)\tNode[%d] stream[%d], node name[%s] in MAIL_HOLD, cap[%lu].",
            node_idx,
            streamid,
            g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
            pmailbox->bufCAP);

        // if no need wait quota, we return -2, you can resend
        if (options->block_mode == FALSE) {
            wr->ret = BROADCAST_WAIT_QUOTA;
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
            return -1;
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);

        pgstat_report_waitstatus_phase(PHASE_WAIT_QUOTA);

        /* wait quota start */
        StreamTimeWaitQuotaStart(t_thrd.pgxc_cxt.GlobalNetInstr);
        time->start = mc_timers_ms();
        // wait quota 3 seconds, then wake up and check mailbox
        wr->ret = gs_poll(single_timeout);
        time->end = mc_timers_ms();
        StreamTimeWaitQuotaEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
        /* wait quota end */
        LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
        // the mailbox is closed or reused by other query, we should return error
        if (gs_check_mailbox(pmailbox->local_version, options->local_version) == false) {
            COMM_DEBUG_LOG("(s|send)\tStream has already closed by remote:%s, detail:%s.",
                REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx),
                mc_strerror(pmailbox->close_reason));

            errno = pmailbox->close_reason;
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
            wr->ret = -1;
            return -1;
        }

        pmailbox->semaphore = NULL;

        // stream state is right and quota size is enough, can send now
        if (pmailbox->bufCAP >= options->need_send_len) {
            pmailbox->state = MAIL_RUN;
            return 0;
        }

        // if it is waked up normal, and quota size is not enough,
        // there must be interruption, we should return and CHECK_FOR_INTERRUPT
        if (wr->ret == 0) {
            wr->ret = 0;
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
            return -1;
        }

        // wait quota timedout, if total_wait_time > time_out,
        // we should return and CHECK_FOR_INTERRUPT
        if (wr->ret == ETIMEDOUT) {
            total_wait_time += single_timeout;
            if (total_wait_time >= options->time_out) {
                errno = ECOMMTCPEPOLLTIMEOUT;
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
                wr->ret = 0;
                return -1;
            }
        }

        // wait quota error (not timedout)
        if (wr->ret != 0 && wr->ret != ETIMEDOUT) {
            MAILBOX_ELOG(pmailbox, WARNING, "(s|send)\tStream waked up by error[%d]:%s.", wr->ret, mc_strerror(errno));

            gs_s_close_logic_connection(pmailbox, ECOMMTCPWAITQUOTAFAIL, options->fcmsgs);
            wr->notify_remote = true;
            errno = ECOMMTCPWAITQUOTAFAIL;
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
            wr->ret = -1;
            return -1;
        }
    }

    return -1;
}

static int GsInternalSendRemote(LibcommSendInfo *sendInfo, struct p_mailbox* pmailbox, int nodeIdx)
{
    int ret = 0;
    do {
        ret = g_libcomm_adapt.send_data(sendInfo);
        // Maybe no socket buffer in kernel or get EAGAIN, we do retry
        if (ret == 0) {
            (void)usleep(100);
        }
    } while (ret == 0);

    // if we failed to send message, we will close and rebuild the socket, but the current query will fail
    if (ret < 0) {
        MAILBOX_ELOG(pmailbox,
            WARNING,
            "(s|send)\tFailed to send data message on socket[%d], error[%d]:%s.",
            g_instance.comm_cxt.g_senders->sender_conn[nodeIdx].socket,
            errno,
            mc_strerror(errno));
    
        errno = ECOMMTCPSND;
        ret = -1;
        return ret;
    }

    if (is_tcp_mode() && g_ackchk_time > 0) {
        recv_ackchk_msg(sendInfo->socket);
    }

    return ret;
}

static void GsUpdatePmailboxStatics(struct p_mailbox* pmailbox, int sentSize, TimeProfile* waitQuota, TimeProfile* sendProfile, TimeProfile* timeProfile)
{
    // update quota
    if (g_instance.comm_cxt.quota_cxt.g_having_quota) {
        pmailbox->bufCAP -= sentSize;
    }

    if (pmailbox->state != MAIL_RUN) {
        LIBCOMM_ELOG(WARNING, "(s|send)\tsend state is wrong[%d].", pmailbox->state);
    }

    /* update the statistic information of the mailbox */
    if (pmailbox->statistic != NULL) {
        if (pmailbox->statistic->first_send_time == 0) {
            pmailbox->statistic->first_send_time = sendProfile->start;
            /* first time, calculate time difference between connect finish time and first send time  */
            pmailbox->statistic->producer_elapsed_time += (uint32)ABS_SUB(timeProfile->start, pmailbox->statistic->start_time);
        } else {
            /* other time, calculate time difference between last send time and this send time  */
            pmailbox->statistic->producer_elapsed_time +=
                (uint32)ABS_SUB(timeProfile->start, t_thrd.comm_cxt.g_producer_process_duration);
        }
        pmailbox->statistic->last_send_time = sendProfile->start;
        pmailbox->statistic->wait_quota_overhead += (uint32)ABS_SUB(waitQuota->end, waitQuota->start);
        pmailbox->statistic->os_send_overhead += ABS_SUB(sendProfile->end, sendProfile->start);

        timeProfile->end = COMM_STAT_TIME();
        pmailbox->statistic->total_send_time += ABS_SUB(timeProfile->end, timeProfile->start);
        pmailbox->statistic->send_bytes += (uint64)(unsigned)sentSize;
        pmailbox->statistic->call_send_count++;
    }

}

static int GsSendCheckParams(gsocket* gsSock, char* message, int mLen)
{
    if (gsSock == NULL) {
        LIBCOMM_ELOG(WARNING, "(s|send)\tInvalid argument: gsSock is NULL");
        errno = ECOMMTCPARGSINVAL;
        return -1;
    }

    int node_idx = gsSock->idx;
    int streamid = gsSock->sid;

    if ((message == NULL) || (mLen <= 0) || (node_idx < 0) ||
        (node_idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num) || (streamid <= 0) ||
        (streamid >= g_instance.comm_cxt.counters_cxt.g_max_stream_num)) {
        LIBCOMM_ELOG(WARNING,
            "(s|send)\tInvalid argument: %s"
            "len=%d, "
            "node idx=%d, stream id=%d.",
            message == NULL ? "message is NULL, " : "",
            mLen,
            node_idx,
            streamid);

        errno = ECOMMTCPARGSINVAL;
        return -1;
    }

    return 0;
}

/*
 * @Description:        Communication library external interface for send.
 * @IN gs_sock:        all information libcomm needed.
 * @IN message:            data.
 * @IN m_len:            data len.
 * @Return:-1:            -1:        send failed.
 *                           m_len:     send succsessed.
 * @See also:
 * send message to the destination datanode through logic channel
 * 1. check if stream info (quota and state), sending messages whenever there is enough quota
 * 2. if no quota, wait on gs_poll
 * 3. decrease quota after successful send,  if quota is used up, set stream in HOLD state and wait on gs_poll
 * 4. return error or sent message size
 */
int gs_send(gsocket* gs_sock, char* message, int m_len, int time_out, bool block_mode)
{
    if (GsSendCheckParams(gs_sock, message, m_len) < 0) {
        return -1;
    }

    PGSTAT_INIT_TIME_RECORD();
    PGSTAT_START_TIME_RECORD();

    // step 1: get node index, stream index, and connection information
    //
    int node_idx = gs_sock->idx;
    int streamid = gs_sock->sid;
    int local_version = gs_sock->ver;
    int remote_version = -1;

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);
    bool TempImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = false;
    errno = 0;

    COMM_TIMER_INIT();

    int ret = 0;
    int res = 0;
    int sent_size = 0;
    bool send_msg = false;

    TimeProfile wait_quota = {0};
    TimeProfile send_proile = {0};
    TimeProfile time_profile = {0};

    time_profile.start = COMM_STAT_TIME();
    time_profile.end = time_profile.start;

    unsigned long need_send_len = 0;
    struct FCMSG_T fcmsgs = {0x0};
    struct sock_id fd_id = {0, 0};
    bool notify_remote = false;
    struct p_mailbox* pmailbox = NULL;

    WaitResult wr = {
        .ret = 0,
        .notify_remote = notify_remote
    };
    SendOptions options = {0};

    // set default time out to 600 seconds(10 minutes)
    if (time_out == -1) {
        time_out = WAITQUOTA_TIMEOUT;
    }

    char* node_name = NULL;
    WaitStatePhase oldPhase = pgstat_report_waitstatus_phase(PHASE_NONE, true);

    // regist current thread id, so we can wait in poll when need quota
    if (gs_poll_create() != 0) {
        LIBCOMM_ELOG(WARNING, "(s|send)\tFailed to malloc for create poll!");
        return -1;
    }

    // step 2: find the pmailbox[node idx][stream id], to check quota and stream state, prepare to do send data
    //
    pmailbox = &P_MAILBOX(node_idx, streamid);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
    // the mailbox is closed already, we should return error
    if (gs_check_mailbox(pmailbox->local_version, local_version) == false) {
        COMM_DEBUG_LOG("(s|send)\tStream already closed, remote:%s, detail:%s.",
            REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx),
            mc_strerror(pmailbox->close_reason));

        errno = pmailbox->close_reason;
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
        ret = -1;
        goto return_result;
    }

    pmailbox->local_thread_id = t_thrd.comm_cxt.MyPid;

    if ((unsigned long)(unsigned)m_len > DEFULTMSGLEN) {
        need_send_len = DEFULTMSGLEN;
    } else {
        need_send_len = (unsigned long)(unsigned)m_len;
    }

    fd_id.fd = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket;
    fd_id.id = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id;

    // check the stream state and quota size, if the state is not RESUME, we should wait
    //
    COMM_TIMER_LOG("(s|send)\tWait quota start for node[%d,%d]:%s.",
        node_idx,
        streamid,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));


    options.need_send_len = need_send_len;
    options.block_mode = block_mode;
    options.time_out = time_out;
    options.fcmsgs = &fcmsgs;
    options.local_version = local_version;

    res = GsWaitPmailboxReady(pmailbox, &options, &wait_quota, &wr);

    COMM_TIMER_LOG("(s|send)\tWait quota end for node[%d,%d]:%s.",
        node_idx,
        streamid,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    ret = wr.ret;
    notify_remote = wr.notify_remote;
    if (res < 0) {
        goto return_result;
    }

    send_proile.start = COMM_STAT_TIME();
    remote_version = pmailbox->remote_version;

    send_msg = gs_s_form_start_ctrl_msg(pmailbox, &fcmsgs);

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
    // send local thread id to remote without cmailbox lock
    if (send_msg && (gs_send_ctrl_msg(&g_instance.comm_cxt.g_s_node_sock[node_idx], &fcmsgs, ROLE_PRODUCER) <= 0)) {
        errno = ECOMMTCPTCPDISCONNECT;
        ret = -1;
        goto return_result;
    }

    // step 3: if the state is RESUME and quota is enough, we do send
    //
    StreamTimeOSSendStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    COMM_TIMER_LOG("(s|send)\tSend message start for node[%d,%d]:%s.",
        node_idx,
        streamid,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    node_name = g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename;
    if (0 == strcmp(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, node_name)) {
        LIBCOMM_PTHREAD_RWLOCK_RDLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
        // remote_version is the cmailbox version, consumer use it to check.
        ret = gs_push_local_buffer(streamid, message, need_send_len, remote_version);
        LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
        if (ret < 0) {
            LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
            gs_s_close_logic_connection(pmailbox, errno, &fcmsgs);
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
            if (IS_NOTIFY_REMOTE(errno)) {
                notify_remote = true;
            }
            MAILBOX_ELOG(
                pmailbox, WARNING, "(s|send)\tFailed to push local cmailbox, error[%d]:%s.", errno, mc_strerror(errno));
            ret = -1;
            goto return_result;
        }
    } else {
        LibcommSendInfo send_info;
        send_info.socket = fd_id.fd;
        send_info.socket_id = fd_id.id;
        send_info.node_idx = node_idx;
        send_info.streamid = streamid;
        send_info.version = remote_version;
        send_info.msg = message;
        send_info.msg_len = need_send_len;

        ret = GsInternalSendRemote(&send_info, pmailbox, node_idx);
        if (ret < 0) {
            goto return_result;
        }
    }

    // step 4: if we send successfully, change the state and quota of the stream
    //
    sent_size = ret;
    send_proile.end = COMM_STAT_TIME();
    StreamTimeOSSendEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
    COMM_TIMER_LOG("(s|send)\tSend message end for node[%d,%d]:%s.",
        node_idx,
        streamid,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
    // the mailbox is closed or reused by other query, we should return error
    if (gs_check_mailbox(pmailbox->local_version, local_version) == false) {
        errno = pmailbox->close_reason;

        COMM_DEBUG_LOG("(s|send)\tStream has already closed by remote:%s, detail:%s.",
            REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx),
            mc_strerror(errno));
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
        ret = -1;
        goto return_result;
    }

    // this should not happen
    LIBCOMM_ASSERT((bool)(pmailbox->bufCAP >= (unsigned long)(unsigned)sent_size), node_idx, streamid, ROLE_PRODUCER);

    GsUpdatePmailboxStatics(pmailbox, sent_size, &wait_quota, &send_proile, &time_profile);

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);

    COMM_DEBUG_LOG("(s|send)\tSend to node[%d]:%s on stream[%d] with msg:%c, m_len[%d] @ bufCAP[%lu].",
        node_idx,
        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
        streamid,
        message[0],
        sent_size,
        pmailbox->bufCAP);

    COMM_TIMER_LOG("(s|send)\tSend finish for node[%d,%d]:%s.",
        node_idx,
        streamid,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

return_result:
    if (notify_remote) {
        (void)gs_send_ctrl_msg(&g_instance.comm_cxt.g_s_node_sock[node_idx], &fcmsgs, ROLE_PRODUCER);
    }

    /* Do not check interruption to maintain the atomicity of data sending */
    LIBCOMM_INTERFACE_END(false, false);
    t_thrd.int_cxt.ImmediateInterruptOK = TempImmediateInterruptOK;
    t_thrd.comm_cxt.g_producer_process_duration = time_profile.end;
    pgstat_report_waitstatus_phase(oldPhase);
    END_NET_STREAM_SEND_INFO(ret);

    // step 5: send successfully, reture the sent size
    return ret;
}  // gs_send

int gs_broadcast_check_paras(struct libcommaddrinfo* libcomm_addr_head, char* message, int m_len)
{
    if ((libcomm_addr_head == NULL) || (message == NULL) || (m_len <= 0)) {
        LIBCOMM_ELOG(WARNING,
            "(s|send)\tInvalid argument: %s%s, len=%d.",
            libcomm_addr_head == NULL ? "addr list is NULL, " : "",
            message == NULL ? "message is NULL, " : "",
            m_len);
        errno = ECOMMTCPARGSINVAL;
        return -1;
    }

    return 0;
}

void gs_clean_addrinfo(struct libcommaddrinfo* libcomm_addr_head)
{
    struct libcommaddrinfo* addr_info = libcomm_addr_head;
    struct p_mailbox* wait_quota_pmailbox = NULL;
    int addr_num = libcomm_addr_head->addr_list_size;
    int i;

    for (i = 0; i < addr_num; i++) {
        if (addr_info->status == BROADCAST_WAIT_QUOTA) {
            addr_info->status = BROADCAST_NEED_SEND;
            wait_quota_pmailbox = &P_MAILBOX(addr_info->gs_sock.idx, addr_info->gs_sock.sid);
            LIBCOMM_PTHREAD_MUTEX_LOCK(&wait_quota_pmailbox->sinfo_lock);
            if (true == gs_check_mailbox(wait_quota_pmailbox->local_version, addr_info->gs_sock.ver)) {
                wait_quota_pmailbox->semaphore = NULL;
            }
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&wait_quota_pmailbox->sinfo_lock);
        }
        addr_info = addr_info->addr_list_next;
    }

    return;
}

/*
 * @Description:        send the message to all connection in address info list.
 * @IN libcomm_addr_head:    the head of address info list.
 * @IN message:            data.
 * @IN m_len:            data len.
 * @Return:                -1:        broadcast send failed.
 *                           m_len:     broadcast send succsessed.
 * @See also:
 */
int gs_broadcast_send(struct libcommaddrinfo* libcomm_addr_head, char* message, int m_len, int time_out)
{
    int ret = gs_broadcast_check_paras(libcomm_addr_head, message, m_len);
    if (ret != 0) {
        return ret;
    }

    int i = 0;
    int send_re = -1;
    int wait_quota = 0;

    if (time_out == -1) {
        time_out = WAITQUOTA_TIMEOUT;
    }

    struct libcommaddrinfo* addr_info = libcomm_addr_head;
    int addr_num = libcomm_addr_head->addr_list_size;
    int error_conn_count = 0;
    struct libcommaddrinfo* wait_addr_info = NULL;
    struct p_mailbox* wait_quota_pmailbox = NULL;

    if (addr_num == 0) {
        LIBCOMM_ELOG(WARNING, "(s|broad case)\tNo one need send in addr list.");
        errno = ECOMMTCPARGSINVAL;
        return -1;
    }

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    COMM_TIMER_INIT();

    COMM_DEBUG_LOG("(s|broad case)\tStart to send to %d datanodes, msg_len=%d.", addr_num, m_len);

    // send to all connections that needed send
    COMM_TIMER_LOG("(s|broad case)\tBroad cast send start.");

    // regist current thread id, so we can wait in poll when no data arrived
    if (gs_poll_create() != 0) {
        LIBCOMM_ELOG(WARNING, "(r|wait poll)\tFailed to malloc for create poll!");
        return -1;
    }

    wait_quota = 0;
    error_conn_count = 0;
    addr_info = libcomm_addr_head;
    for (i = 0; i < addr_num; i++) {
        // this connection need to send
        if (addr_info->status == BROADCAST_NEED_SEND) {
            // really send without waiting quota
            send_re = gs_send(&addr_info->gs_sock, message, m_len, time_out, FALSE);
            // this connection send successed
            if (send_re > 0) {
                addr_info->status = BROADCAST_SEND_FINISH;
            } else if (send_re == BROADCAST_WAIT_QUOTA) { // this connection need wait quota
                addr_info->status = BROADCAST_WAIT_QUOTA;
                wait_quota_pmailbox = &P_MAILBOX(addr_info->gs_sock.idx, addr_info->gs_sock.sid);
                wait_quota = 1;
                wait_addr_info = addr_info;
                COMM_DEBUG_LOG("(s|broad case)\tNeed wait quota for %s.", addr_info->nodename);
            } else if (send_re == 0) {  // we should return and CHECK_FOR_INTERRUPT
                LIBCOMM_ELOG(WARNING, "(s|broad case)\trecv EINTR.");
                goto clean_return;
            } else if (send_re < 0) { // this connection send failed, continue to send other connecions
                // close this connection in libcomm side
                (void)gs_s_close_stream(&addr_info->gs_sock);
                addr_info->gs_sock = GS_INVALID_GSOCK;

                // close this connection in app side
                addr_info->listen_port = 0;
                addr_info->status = BROADCAST_CONNECT_CLOSED;

                // set error connection flag
                error_conn_count++;
                COMM_DEBUG_LOG("(s|broad case)\tFail to send to %s.", addr_info->nodename);
            }
        } else if (addr_info->status == BROADCAST_CONNECT_CLOSED) {
            error_conn_count++;
        }
        addr_info = addr_info->addr_list_next;
    }

    // all connections that needed send has already closed, broadcast send failed!
    if (error_conn_count == addr_num) {
        COMM_DEBUG_LOG("(s|broad case)\tFail to send to all connection.");
        COMM_TIMER_LOG("(s|broad case)\tStart broad cast error.");
        libcomm_addr_head->addr_list_size = 0;
        return -1;
    }

    // some connections need wait quota
    if (wait_quota == 1) {
        StreamTimeWaitQuotaStart(t_thrd.pgxc_cxt.GlobalNetInstr);

        COMM_DEBUG_LOG("(s|broad case)\tWait quota start.");

        if (wait_addr_info != NULL) {
            pgstat_report_waitstatus_comm(STATE_WAIT_FLUSH_DATA,
                wait_addr_info->nodeIdx,
                -1,
                u_sess->stream_cxt.producer_obj->getParentPlanNodeId(),
                global_node_definition ? global_node_definition->num_nodes : -1);
        }

        COMM_TIMER_LOG("(s|broad case)\tWait quota start.");
        uint64 wait_quota_start = mc_timers_ms();
        (void)gs_poll(time_out);
        uint64 wait_quota_end = mc_timers_ms();
        COMM_TIMER_LOG("(s|broad case)\tWait quota end.");

        // add wait quota overhead time
        LIBCOMM_PTHREAD_MUTEX_LOCK(&wait_quota_pmailbox->sinfo_lock);
        COMM_STAT_CALL(wait_quota_pmailbox,
            wait_quota_pmailbox->statistic->wait_quota_overhead += ABS_SUB(wait_quota_end, wait_quota_start));
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&wait_quota_pmailbox->sinfo_lock);

        StreamTimeWaitQuotaEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

        // we should return and CHECK_FOR_INTERRUPT
        goto clean_return;
    }

    addr_info = libcomm_addr_head;
    while (addr_info != NULL) {
        // if this connection send finish, ready to send next data package
        if (addr_info->status == BROADCAST_SEND_FINISH) {
            addr_info->status = BROADCAST_NEED_SEND;
        }
        addr_info = addr_info->addr_list_next;
    }

    COMM_TIMER_LOG("(s|broad case)\tBroad cast send end.");
    // all connection send successed, exception error connections
    return m_len;

clean_return:
    gs_clean_addrinfo(libcomm_addr_head);
    return 0;
}

// Consumer thread (at executor) use gs_wait_poll for waiting for data,
// just like waiting data from network using system function poll()
//
int gs_wait_poll(gsocket* gs_sock_array,  // array of producers node index
    int nproducer,                        // number of producers
    int* producer,                        // producers number triggers poll
    int timeout,                          // time out in seconds, -1 for block mode
    bool close_expected)                  // is logic connection closed by remote is an expected result
{
    if ((gs_sock_array == NULL) || (producer == NULL) || (nproducer <= 0)) {
        LIBCOMM_ELOG(WARNING,
            "(r|wait poll)\tInvalid argument: %s%s"
            "nproducer=%d.",
            gs_sock_array == NULL ? "gs_sock_array is NULL, " : "",
            producer == NULL ? "producer is NULL, " : "",
            nproducer);
        errno = ECOMMTCPARGSINVAL;
        return -1;
    }

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    if (gs_poll_create() != 0) {
        LIBCOMM_ELOG(WARNING, "(r|wait poll)\tPoll create failed! Detail:%s.", mc_strerror(errno));
        return -1;
    }

    // step 1: initialize local variables
    //
    int ret = 0;
    const int interruptTimeOut = 60;
    // if waked by other threads, the flag is set to 1
    struct libcomm_time_record time_record = {0};
    time_record.time_enter = COMM_STAT_TIME();
    time_record.time_now = time_record.time_enter;
    bool TempImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = false;
    errno = 0;
    check_cmailbox_option opt = {&time_record, 1, 0};

    COMM_TIMER_INIT();
    time_record.t_begin = t_begin;
    COMM_TIMER_LOG("(r|wait poll)\tStart timer log.");

    for (;;) {
        time_record.wait_lock_start = COMM_STAT_TIME();
        // step 2: check if cmailbox already has data
        //
        ret = gs_check_cmailbox_data(gs_sock_array, nproducer, producer, close_expected, opt);
        if (ret != 0) {
            goto return_result;
        }
        opt.first_cycle = 0;
        // step 3: if no data found and waked up flag is not set, we should wait on CV
        //
        int pool_re = 0;
        int time_out = -1;
        if (timeout > 0) {
            time_out = timeout;
        }

        /* If a cancel signal is received, we set gs_poll the default timeout 60s to avoid falling into wait(). */
        if (InterruptPending || t_thrd.int_cxt.ProcDiePending) {
            time_out = (timeout > 0) ? timeout : interruptTimeOut;
            LIBCOMM_ELOG(WARNING, "(r|wait poll)\t The gs_wait_poll receive Cancel Interrupt.");
        }

        time_record.wait_data_start = COMM_STAT_TIME();
        COMM_TIMER_LOG("(r|wait poll)\tWait poll start.");
        pool_re = gs_poll(time_out);
        COMM_TIMER_LOG("(r|wait poll)\tWait poll end.");
        time_record.wait_data_end = COMM_STAT_TIME();

        // step 4: if it is waked up normal, return 0, then receive again
        // it is used to deal with cancel and interruption
        //
        if (pool_re == 0) {
            /* we set the return value to 0, the caller will check this return value
             * if there is no interrupt, the caller will call this function again
             */
            if (InterruptPending || t_thrd.int_cxt.ProcDiePending) {
                ret = 0;
                goto return_result;
            } else if (t_thrd.int_cxt.ProcDiePending) {
                /* t_thrd.int_cxt.ProcDiePending when process startup packet return -1 then thread will exit */
                ret = -1;
            } else {
                continue;
            }
        }

        // if poll has already closed, we continue to check closed mailbox
        if (-1 == pool_re) {
            opt.poll_error_flag = 1;
            continue;
        }

        // timed out, in fact, we do not set the timeout now !!!
        if (pool_re == ETIMEDOUT) {
            LIBCOMM_ELOG(WARNING, "(r|wait poll)\tFailed to wait for notify, timeout:%ds.", time_out);
            errno = ECOMMTCPEPOLLTIMEOUT;
        } else { // waked up abnormal, unknown error
            LIBCOMM_ELOG(WARNING, "(r|wait poll)\tFailed to wait poll, detail: %s.", mc_strerror(errno));
            errno = ECOMMTCPWAITPOLLERROR;
        }
        ret = -1;
        goto return_result;
    }

return_result:
    gs_check_all_producers_mailbox(gs_sock_array, nproducer, producer);

    // ret 0 means poll is waked unexpected, so we check interruption
    LIBCOMM_INTERFACE_END((ret == 0), TempImmediateInterruptOK);
    t_thrd.comm_cxt.g_consumer_process_duration = COMM_STAT_TIME();

    return ret;
}  // gs_wait_poll

