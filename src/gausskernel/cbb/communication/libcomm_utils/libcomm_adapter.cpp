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
 * libcomm_adapter.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/libcomm_utils/libcomm_adapter.cpp
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
#include "executor/execStream.h"
#include "miscadmin.h"
#include "gssignal/gs_signal.h"
#include "pgxc/pgxc.h"

#ifdef ENABLE_UT
#define static
#endif

#define MSG_HEAD_TEMP_CHECKSUM 0xCE3BA6CE

#define MSG_HEAD_MAGIC_NUM 0x9D
#define MSG_HEAD_MAGIC_NUM2 0x3E

// libcomm delay message number
static int libcomm_delay_no = 0;

LibcommAdaptLayer g_libcomm_adapt;
extern HTAB* g_htab_fd_id_node_idx;
extern pthread_mutex_t g_htab_fd_id_node_idx_lock;

static int gs_tcp_write_noblock(int node_idx, int sock, const char* msg, int msg_len, int *send_count);
static int libcomm_build_tcp_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx);
int gs_s_build_tcp_ctrl_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx, bool is_reply);

static int libcomm_tcp_listen()
{
    return mc_tcp_listen(g_instance.comm_cxt.localinfo_cxt.g_local_host,
        g_instance.comm_cxt.g_receivers->server_listen_conn.port,
        NULL);
}

/*
 * function name    : libcomm_malloc_iov_item
 * description      : malloc iov item
 * arguments        :   iov_item: the pointer of malloc memory.
                        size: size of iov->iov_base
 * return value     :   0: malloc succeed
 *                      -1: malloc failed
 */
int libcomm_malloc_iov_item(struct mc_lqueue_item** iov_item, int size)
{
    struct mc_lqueue_item* item = NULL;
    struct iovec* iov = NULL;

    /* get iov_item from memory pool  */
    *iov_item = gs_memory_pool_queue_pop((char*)iov);
    if (*iov_item != NULL) {
        return 0;
    }

    /*
     * IF libcomm_used_memory + size is more than g_total_usable_memory
     * then errno = ECOMMTCPMEMALLOC and return -1
     */
    /* if memory pool is empty, malloc iov_item */
    LIBCOMM_MALLOC(iov, sizeof(struct iovec), iovec);
    if (iov == NULL) {
        errno = ECOMMTCPMEMALLOC;
        return -1;
    }

    LIBCOMM_MALLOC(iov->iov_base, (unsigned)size, void);
    if (iov->iov_base == NULL) {
        LIBCOMM_FREE(iov, sizeof(struct iovec));
        errno = ECOMMTCPMEMALLOC;
        return -1;
    }
    iov->iov_len = 0;

    LIBCOMM_MALLOC(item, sizeof(struct mc_lqueue_item), mc_lqueue_item);
    if (item == NULL) {
        LIBCOMM_FREE(iov->iov_base, size);
        LIBCOMM_FREE(iov, sizeof(struct iovec));
        errno = ECOMMTCPMEMALLOC;
        return -1;
    }
    item->element.add(iov);

    *iov_item = item;
    return 0;
}

/*
 * function name    : libcomm_free_iov_item
 * description      : free iov item
 * arguments        :   iov_item: the pointer of free memory.
 *                        size: size of iov->iov_base
 */
void libcomm_free_iov_item(struct mc_lqueue_item** iov_item, int size)
{
    struct mc_lqueue_item* item = *iov_item;
    struct iovec* iov = NULL;
    bool rc = false;

    if (unlikely(item == NULL)) {
        return;
    }

    iov = item->element.data;

    Assert(iov != NULL && iov->iov_base != NULL);

    iov->iov_len = 0;

    /* push pointer to  memory pool   */
    rc = gs_memory_pool_queue_push((char*)item);
    /* if memory pool is full, free iov_item */
    if (!rc) {
        LIBCOMM_FREE(iov->iov_base, size);
        LIBCOMM_FREE(iov, sizeof(struct iovec));
        LIBCOMM_FREE(item, sizeof(struct mc_lqueue_item));
    }

    *iov_item = NULL;
    return;
}

/*
 * function name    : gs_tcp_write_noblock
 * description        : loop send msg by tcp with noblock mode
 * arguments        :   node_idx: sender node id.
 *                        sock: socket
 *                        msg: send message content
 *                        msg_len: msg length
 * return value        : length of msg had be sent
 */
static int gs_tcp_write_noblock(int node_idx, int sock, const char* msg, int msg_len, int *send_count)
{
    uint64 time_enter, time_now;
    int send_bytes = 0;
    int error = -1;

    time_enter = mc_timers_ms();

    do {
        /*
         * we send data in non-block mode,
         * but we will assure the data will
         * be sent out if the network is ok
         */
        error = mc_tcp_write_noblock(sock, msg + send_bytes, msg_len - send_bytes);

        if (error < 0) {
            errno = ECOMMTCPDISCONNECT;
            break;
        }

        if (send_count != NULL) {
            (*send_count)++;
        }

        /*
         * when primary and the standby is switchover,
         * the old connection is broken
         */
        if (g_instance.comm_cxt.g_senders->sender_conn[node_idx].ip_changed == true) {
            errno = ECOMMTCPPEERCHANGED;
            break;
        }

        time_now = mc_timers_ms();
        if (((time_now - time_enter) >
                ((uint64)(unsigned)g_instance.comm_cxt.counters_cxt.g_comm_send_timeout * SEC_TO_MICRO_SEC)) &&
            (time_now > time_enter)) {
            errno = ECOMMTCPSENDTIMEOUT;
            break;
        }

        send_bytes += error;
    } while (send_bytes != msg_len);

    return send_bytes;
}
static int libcomm_tcp_send(LibcommSendInfo* send_info)
{
    int sock = send_info->socket;
    int sock_id = send_info->socket_id;
    int version = send_info->version;
    int streamid = send_info->streamid;
    int node_idx = send_info->node_idx;
    int msg_len = send_info->msg_len;
    char* msg = send_info->msg;
    int error = -1;
    int send_bytes;
    int send_count = 0;
    struct sock_id fd_id = {0, 0};
    MsgHead msg_head;
    msg_head.type = 'D';
    msg_head.magic_num = MSG_HEAD_MAGIC_NUM;
    msg_head.version = version;
    msg_head.logic_id = streamid;
    msg_head.msg_len = msg_len;
    msg_head.checksum = MSG_HEAD_TEMP_CHECKSUM;

    LIBCOMM_PTHREAD_RWLOCK_WRLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);

    /* check socket version saved before, to prevent send msg to wrong remote node */
    if ((sock != g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket) ||
        (sock_id != g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id)) {
        COMM_DEBUG_LOG("(s|send)\tsocket version of node%d:%s mismatch old[%d,%d], new[%d,%d].",
            node_idx,
            REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx),
            sock,
            sock_id,
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket,
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id);
        LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
        return -1;
    }

    send_bytes = gs_tcp_write_noblock(node_idx, sock, (char*)&msg_head, sizeof(MsgHead), NULL);
    if (send_bytes != sizeof(MsgHead)) {
        /* close the bad socket when send failed */
        fd_id.fd = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket;
        fd_id.id = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id;
        gs_s_close_bad_data_socket(&fd_id, ECOMMTCPDISCONNECT, node_idx);
        LIBCOMM_ELOG(WARNING,
            "(s|send)\tsend msghead failed send_bytes[%d] errno[%d:%s].",
            send_bytes,
            errno,
            mc_strerror(errno));
        LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
        return -1;
    }

    COMM_DEBUG_LOG("(s|send)\tsend to dn[%d]:%s head[%d, %d] on socket[%d].",
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx),
        (int)sizeof(MsgHead),
        error,
        g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket);

    send_bytes = gs_tcp_write_noblock(node_idx, sock, msg, msg_len, &send_count);
    if (send_bytes > 0) {
        g_instance.comm_cxt.g_senders->sender_conn[node_idx].comm_bytes += send_bytes;
    }
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].comm_count += send_count;

    COMM_DEBUG_LOG("(s|send)\tsend to dn[%d]:%s data[%d, %d] on socket[%d].",
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx),
        msg_len,
        error,
        g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket);

    if (send_bytes != msg_len) {
        /* close the bad socket when send failed */
        fd_id.fd = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket;
        fd_id.id = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id;
        gs_s_close_bad_data_socket(&fd_id, errno, node_idx);
        LIBCOMM_ELOG(WARNING,
            "(s|send)\tsend length mismatch send_bytes[%d] msg_len[%d] errno[%d:%s].",
            send_bytes,
            msg_len,
            errno,
            mc_strerror(errno));
        send_bytes = -1;
    }

    LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
    return send_bytes;
}

static int libcomm_tcp_recv_noidx(LibcommRecvInfo* recv_info)
{
    int sock = recv_info->socket;
    MsgHead msg_head = {0};
    struct mc_lqueue_item* iov_item = NULL;
    struct iovec* iov = NULL;
    int error = -1;

    // malloc 64 bytes for recv when node_idx < 0, need free
    if (0 != libcomm_malloc_iov_item(&iov_item, IOV_DATA_SIZE)) {
        return RECV_MEM_ERROR;
    }

    iov = iov_item->element.data;

    // recv poll event, recv msg head in block mode
    error = mc_tcp_read_block(sock, &msg_head, sizeof(MsgHead), 0);
    // must be a connect msg when node_idx < 0
    if (error < 0 || msg_head.type != 'C' || msg_head.magic_num != MSG_HEAD_MAGIC_NUM ||
        msg_head.checksum != MSG_HEAD_TEMP_CHECKSUM || msg_head.msg_len > IOV_DATA_SIZE) {
        LIBCOMM_ELOG(WARNING,
            "(r|inner recv)\tReceiver error msg head[%d] "
            "from socket[%d] lid:%d type[%d], magic_num[%d], len[%u].",
            error,
            sock,
            msg_head.logic_id,
            msg_head.type,
            msg_head.magic_num,
            msg_head.msg_len);

        libcomm_free_iov_item(&iov_item, IOV_DATA_SIZE);
        return RECV_NET_ERROR;
    }

    // recv msg head finish
    error = mc_tcp_read_block(sock, iov->iov_base, msg_head.msg_len, 0);
    if (error <= 0) {
        libcomm_free_iov_item(&iov_item, IOV_DATA_SIZE);
        return RECV_NET_ERROR;
    }

    iov->iov_len = error;
    recv_info->iov_item = iov_item;
    recv_info->streamid = msg_head.logic_id;
    recv_info->version = msg_head.version;

    return error;
}

int libcomm_tcp_recv(LibcommRecvInfo* recv_info)
{
    MsgHead* msg_head = NULL;
    struct iovec* iov = NULL;
    struct mc_lqueue_item* iov_item = NULL;
    int sock = recv_info->socket;
    int node_idx = recv_info->node_idx;
    int recv_bytes = -1;
    int* head_read_cursor = 0;
    int unread_head_len = 0;
    int unread_body_len = 0;

    // first READY message, no have idx, can not buffer, block recv
    if (node_idx < 0) {
        return libcomm_tcp_recv_noidx(recv_info);
    }

    msg_head = &g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].msg_head;
    iov_item = g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].iov_item;

    head_read_cursor = &(g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].head_read_cursor);
    unread_head_len = sizeof(MsgHead) - *head_read_cursor;

    if (unread_head_len > 0) {
        // recv poll event, recv msg head in block mode
        recv_bytes = mc_tcp_read_nonblock(sock, (char*)msg_head + *head_read_cursor, unread_head_len, 0);
        if (recv_bytes < 0) {
            return RECV_NET_ERROR;
        }

        if (recv_bytes == 0) {
            return RECV_NEED_RETRY;
        }

        *head_read_cursor = *head_read_cursor + recv_bytes;

        /* msg head not received complete, return to epoll_wait */
        if (recv_bytes < unread_head_len) {
            return RECV_NEED_RETRY;
        }
    }

    /* recv msg head finish */
    Assert(*head_read_cursor == sizeof(MsgHead));

    if (msg_head->magic_num != MSG_HEAD_MAGIC_NUM || msg_head->checksum != MSG_HEAD_TEMP_CHECKSUM ||
        msg_head->msg_len > IOV_DATA_SIZE) {
        LIBCOMM_ELOG(WARNING,
            "(r|inner recv)\tReceiver error msg head[%d] "
            "from socket[%d] node[%d]:%s lid:%d len=%u, magic_num[%d].",
            recv_bytes,
            sock,
            node_idx,
            REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, node_idx),
            msg_head->logic_id,
            msg_head->msg_len,
            msg_head->magic_num);
        return RECV_NET_ERROR;
    }

    if (iov_item == NULL) {
        if (0 != libcomm_malloc_iov_item(&iov_item, IOV_DATA_SIZE)) {
            return RECV_MEM_ERROR;
        }

        // save new malloc iov point
        g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].iov_item = iov_item;
    }
    iov = iov_item->element.data;

    if (msg_head->msg_len <= iov->iov_len) {
        Assert(msg_head->msg_len > iov->iov_len);
        LIBCOMM_ELOG(WARNING,
            "(r|inner recv)\tReceiver error msg_len %u iov_len %lu.",
            msg_head->msg_len, iov->iov_len);
        return RECV_NET_ERROR;
    }

    unread_body_len = msg_head->msg_len - iov->iov_len;

#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if ((is_comm_fault_injection(LIBCOMM_FI_R_PACKAGE_SPLIT))) {
        if (iov->iov_len == 0) {
            unread_body_len = unread_body_len / 2;
        }
    }
#endif

    recv_bytes = mc_tcp_read_nonblock(sock, (char*)iov->iov_base + iov->iov_len, unread_body_len, 0);
    g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].comm_bytes += recv_bytes;
    g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].comm_count += 1;

    // real network errors, we should report it
    // errno is not EAGAIN/EWOULDBLOCK/EINTR
    if (recv_bytes < 0) {
        return RECV_NET_ERROR;
    }

    if (recv_bytes == 0) {
        return RECV_NEED_RETRY;
    }

    iov->iov_len += recv_bytes;

    /* msg body not received complete, return to epoll_wait */
    if (iov->iov_len < msg_head->msg_len) {
        return RECV_NEED_RETRY;
    }

    /* recv msg body finish */
    Assert(iov->iov_len == msg_head->msg_len);

    recv_info->iov_item = iov_item;
    recv_info->streamid = msg_head->logic_id;
    recv_info->version = msg_head->version;

    COMM_DEBUG_LOG("(r|inner recv)\tReceiver msg head[%d] "
                   "from socket[%d] node[%d]:%s logic id:%d len=%u.",
        recv_bytes,
        sock,
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, node_idx),
        msg_head->logic_id,
        msg_head->msg_len);

    // give up iov point that g_receivers had
    // iov point must return and save to cmailbox
    g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].iov_item = NULL;
    g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].head_read_cursor = 0;
    g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].msg_head.type = MSG_NULL;
    if (g_ackchk_time) {
        g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].last_rcv_time = mc_timers_ms();
    }

    return iov->iov_len;
}

/*
 * function name    : gs_accept_data_conntion
 * description      : accept a logic connection and save some info to global variable
 * arguments        : iov: provide libcomm_connect_package
 *                    sock: phycial sock of logic connection.
 * return value     : 0: succeed
 *                   -1: net error
 *                   -2: mem error
 */
int gs_accept_data_conntion(struct iovec* iov, const sock_id fd_id)
{
    int node_idx = -1;
    struct sock_id old_fd_id;
    struct libcomm_connect_package* connect_pkg = (struct libcomm_connect_package*)iov->iov_base;

    if (iov->iov_len < sizeof(struct libcomm_connect_package)) {
        LIBCOMM_ELOG(WARNING,
            "(r|inner recv)\tIov len[%zu] is less than libcomm_connect_package[%zu].",
            iov->iov_len, sizeof(struct libcomm_connect_package));
        Assert(iov->iov_len == sizeof(struct libcomm_connect_package));
        return RECV_NET_ERROR;
    }

    /* Network data is not trusted */
    if (connect_pkg->magic_num != MSG_HEAD_MAGIC_NUM2) {
        return RECV_NET_ERROR;
    }

    connect_pkg->node_name[NAMEDATALEN - 1] = '\0';
    connect_pkg->host[HOST_ADDRSTRLEN - 1] = '\0';

    node_idx = gs_get_node_idx(connect_pkg->node_name);
    if (unlikely(node_idx < 0)) {
        LIBCOMM_ELOG(WARNING,
            "(r|inner recv)\tFailed to get node index for %s: %s.",
            connect_pkg->node_name,
            mc_strerror(errno));

        return RECV_NET_ERROR;
    }

    if (gs_map_sock_id_to_node_idx(fd_id, node_idx) < 0) {
        LIBCOMM_ELOG(WARNING, "(r|inner recv)\tFailed to save sock and sockid.");
        return RECV_NET_ERROR;
    }

    LIBCOMM_PTHREAD_RWLOCK_WRLOCK(&g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].rwlock);
    // step6: if the old socket is ok, maybe the primary is changed, we should close the old connection
    if (g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].socket >= 0) {
        LIBCOMM_ELOG(WARNING,
            "(r|inner recv)\tOld connection exist, maybe the primary is changed, old address of "
            "node[%d] is:%s, new is:%s, the connection will be reset.",
            node_idx,
            g_instance.comm_cxt.g_r_node_sock[node_idx].remote_host,
            connect_pkg->host);
        old_fd_id.fd = g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].socket;
        old_fd_id.id = g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].socket_id;
        gs_r_close_bad_data_socket(node_idx, old_fd_id, ECOMMTCPPEERCHANGED, false);
    }

    g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].socket = fd_id.fd;
    g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].socket_id = fd_id.id;
    g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].msg_head.type = MSG_NULL;
    g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].head_read_cursor = 0;
    if (g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].iov_item) {
        struct iovec* iov_data = g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].iov_item->element.data;
        iov_data->iov_len = 0;
    }

    LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].rwlock);

    // step8: set the socket information
    int ss_rc = strcpy_s(g_instance.comm_cxt.g_r_node_sock[node_idx].remote_host, HOST_ADDRSTRLEN, connect_pkg->host);
    securec_check(ss_rc, "\0", "\0");

    // step 7: send back ack to tell the sender continue
    struct libcomm_accept_package ack_msg;
    ack_msg.type = LIBCOMM_PKG_TYPE_ACCEPT;
    ack_msg.result = 1;
    if (g_libcomm_adapt.send_ack(fd_id.fd, (char*)&ack_msg, sizeof(ack_msg)) < 0) {
        gs_r_close_bad_data_socket(node_idx, fd_id, ECOMMTCPDISCONNECT, true);
        return RECV_NET_ERROR;
    }

#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_GSS_SCTP_FAILED)) {
        errno = ECOMMTCPGSSAUTHFAIL;
        LIBCOMM_ELOG(WARNING,
            "(r|recv loop)\t[FAULT INJECTION]Data channel GSS authentication failed, listen socket[%d]:%s.",
            fd_id.fd,
            mc_strerror(errno));
        return RECV_NET_ERROR;
    }
#endif

    LIBCOMM_ELOG(LOG,
        "(r|recv loop)\tAccept data connection for "
        "node[%d]:%s with socket[%d,%d].",
        node_idx,
        g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename,
        fd_id.fd,
        fd_id.id);

    return 0;
}

/*
 * function name    : gs_accept_ctrl_conntion
 * description      : check connection, if connection is new, close old one.
 * arguments        : _in_ t_fd_id: the fd and fd id for this connection.
 *                    _in_ fcmsgr: the message we received.
 */
void gs_accept_ctrl_conntion(struct sock_id* t_fd_id, struct FCMSG_T* fcmsgr)
{
    int current_mode;
    int rc = -1;
    char ack;
    errno_t ss_rc;
    uint32 cpylen;

    struct sock_id old_fd_id = {-1, -1};
    uint16 idx = fcmsgr->node_idx;

    Assert(idx < g_instance.comm_cxt.counters_cxt.g_cur_node_num);

    /* Network data is not trusted */
    fcmsgr->nodename[NAMEDATALEN - 1] = '\0';

    g_instance.comm_cxt.g_r_node_sock[idx].lock();

    old_fd_id.fd = g_instance.comm_cxt.g_r_node_sock[idx].ctrl_tcp_sock;
    old_fd_id.id = g_instance.comm_cxt.g_r_node_sock[idx].ctrl_tcp_sock_id;

    // if the two sockets are the same, we need not close the socket, it is ok
    if (old_fd_id.fd == t_fd_id->fd && old_fd_id.id == t_fd_id->id) {
        g_instance.comm_cxt.g_r_node_sock[idx].unlock();
    } else {
        g_instance.comm_cxt.g_r_node_sock[idx].unlock();

        if (old_fd_id.fd >= 0) {
            /* close the old tcp socket, or it will be error and leak */
            gs_r_close_bad_ctrl_tcp_sock(&old_fd_id, ECOMMTCPPEERCHANGED);
            LIBCOMM_ELOG(WARNING,
                "(r|flow ctrl)\tOld connection exist, maybe the primary is changed, old address of "
                "node[%d] is:%s, the connection will be reset.",
                idx,
                g_instance.comm_cxt.g_r_node_sock[idx].remote_host);
        }

        /* regist new sock and sock id to node idx */
        if (gs_map_sock_id_to_node_idx(*t_fd_id, idx) < 0) {
            LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tFailed to save sock and sockid.");
            gs_r_close_bad_ctrl_tcp_sock(t_fd_id, ECOMMTCPTCPDISCONNECT);
            return;
        }
    }

    g_instance.comm_cxt.g_r_node_sock[idx].lock();
    g_instance.comm_cxt.g_r_node_sock[idx].set_nl(t_fd_id->fd, CTRL_TCP_SOCK);
    g_instance.comm_cxt.g_r_node_sock[idx].set_nl(t_fd_id->id, CTRL_TCP_SOCK_ID);

    cpylen = comm_get_cpylen(fcmsgr->nodename, NAMEDATALEN);
    ss_rc = memset_s(g_instance.comm_cxt.g_r_node_sock[idx].remote_nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc =
        strncpy_s(g_instance.comm_cxt.g_r_node_sock[idx].remote_nodename, NAMEDATALEN, fcmsgr->nodename, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    g_instance.comm_cxt.g_r_node_sock[idx].remote_nodename[cpylen] = '\0';

    g_instance.comm_cxt.g_r_node_sock[idx].unlock();

    /* send response to remote, thus ready control msg arrived after connection has established */
    if (IS_PGXC_COORDINATOR) {
        ack = 'o';
        rc = mc_tcp_write_block(t_fd_id->fd, &ack, sizeof(ack));
        // if tcp send failed, close tcp connction
        if (rc <= 0) {
            LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tFailed to send ack, error:%s.", mc_strerror(errno));
            gs_r_close_bad_ctrl_tcp_sock(t_fd_id, ECOMMTCPTCPDISCONNECT);

            return;
        }
    } else if (fcmsgr->type == CTRL_CONN_REGIST_CN) {
        if (g_instance.comm_cxt.g_ha_shm_data) {
            current_mode = g_instance.comm_cxt.g_ha_shm_data->current_mode;
        } else {
            current_mode = UNKNOWN_MODE;
            LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tCannot get current mode, postmaster exit.");
        }

        ack =   (current_mode != STANDBY_MODE && current_mode != PENDING_MODE && current_mode != UNKNOWN_MODE) ?
                'o' : 'r';

        rc = mc_tcp_write_block(t_fd_id->fd, &ack, sizeof(ack));
        // if tcp send failed, close tcp connction
        if (rc <= 0 || (ack == 'r')) {
            if (current_mode == STANDBY_MODE) {
                LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tCannot accept connection in standby mode.");
            } else if (current_mode == PENDING_MODE) {
                // sleep 1 second to wait process starting
                (void)sleep(1);
                LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tCannot accept connection in pending mode.");
            } else {
                LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tCannot accept connection in unknown mode.");
            }

            if (rc <= 0) {
                LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tFailed to send ack, error:%s.", mc_strerror(errno));
            }

            gs_r_close_bad_ctrl_tcp_sock(t_fd_id, ECOMMTCPTCPDISCONNECT);

            return;
        }
    }

    LIBCOMM_ELOG(LOG,
        "(r|flow ctrl)\tAccept control connection for "
        "node[%d]:%s with socket[%d,%d].",
        idx,
        g_instance.comm_cxt.g_r_node_sock[idx].remote_nodename,
        t_fd_id->fd,
        t_fd_id->id);

    return;
}

/*
 * function name    : gs_connect_by_unix_domain
 * description      : connect with postmaster thread by unix domain
 * arguments        : void
 * return value     : -1: error
 *                  : 0: succeed
 */
static int gs_connect_by_unix_domain()
{
    errno_t ss_rc;
    uint32 cpylen, maxlen;

    // STEP1 create new socket
    if ((g_instance.comm_cxt.localinfo_cxt.sock_to_server_loop = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
        LIBCOMM_ELOG(WARNING, "(SendUnixDomainMsg)\tCould not create socket.");
        return -1;
    }

    // STEP2 set unix addr
    struct sockaddr_un unp;
    ss_rc = memset_s(&unp, sizeof(unp), 0x0, sizeof(struct sockaddr_un));
    securec_check(ss_rc, "\0", "\0");
    unp.sun_family = AF_UNIX;

    maxlen = sizeof(unp.sun_path);
    cpylen = comm_get_cpylen(g_instance.comm_cxt.g_unix_path, maxlen);
    ss_rc = memset_s(unp.sun_path, maxlen, 0x0, maxlen);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(unp.sun_path, maxlen, g_instance.comm_cxt.g_unix_path, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    unp.sun_path[cpylen] = '\0';

    // STEP3 connecting, server loop will be waked up
    if (connect(g_instance.comm_cxt.localinfo_cxt.sock_to_server_loop,
                (struct sockaddr*)&unp,
                sizeof(struct sockaddr_un)) == -1) {
        LIBCOMM_ELOG(WARNING, "(SendUnixDomainMsg)\tFailed to connect by unix socket, error: %s", mc_strerror(errno));
        close(g_instance.comm_cxt.localinfo_cxt.sock_to_server_loop);
        g_instance.comm_cxt.localinfo_cxt.sock_to_server_loop = INVALID_SOCK;
        return -1;
    }
    return 0;
}

/*
 * function name    : gs_send_msg_by_unix_domain
 * description      : send gs_sock to postmaster, as serverloop listen on the unix domain sock.
 *                    we need to connect first then send the gs_sock
 * arguments        : msg: the message that we want to send, gs_sock in this case
 * return value     : -1: error
 *                  : other postive value: sent bytes
 */
int gs_send_msg_by_unix_domain(const void* msg, int msg_len)
{
    int error = 0;
    bool is_retry = true;

retry:
    if (g_instance.comm_cxt.localinfo_cxt.sock_to_server_loop == INVALID_SOCK) {
        error = gs_connect_by_unix_domain();
    }

    if (error < 0) {
        return error;
    }

    error = mc_tcp_write_block(g_instance.comm_cxt.localinfo_cxt.sock_to_server_loop, (const void*)msg, msg_len);
    if (error <= 0) {
        LIBCOMM_ELOG(WARNING, "(s|unix domain)\tFailed to send through unix socket, error: %s", mc_strerror(errno));
        close(g_instance.comm_cxt.localinfo_cxt.sock_to_server_loop);
        g_instance.comm_cxt.localinfo_cxt.sock_to_server_loop = INVALID_SOCK;
        // send fail may due to socket is closed by postmaster
        // try to make a new connection then send one more time
        // if failed in second time, return error
        if (is_retry) {
            is_retry = false;
            goto retry;
        }
    }

    return error;
}

/*
* function name    : gs_recv_msg_by_unix_domain
* description      : recv gs_sock from receiver flow ctrl
* arguments        : fd: unix domain socket fd
                     gs_sock:output pointer
* return value     : -1: recv failed or value of gsocket is invalid
*                  : sizeof(gsocket):succeed
*/
int gs_recv_msg_by_unix_domain(int fd, gsocket* gs_sock)
{
    int error;
    int size = (int)sizeof(gsocket);
    error = mc_tcp_read_block(fd, gs_sock, size, 0);
    // recv failed
    if (error != size) {
        LIBCOMM_ELOG(WARNING,
            "(r|unix domain)\tfailed to recv gs_sock from unix domain, result: %d, error:%s.",
            error,
            gs_comm_strerror());
        mc_tcp_close(fd);
        return -1;
    }

    // check the value of receiver gs_sock
    if ((gs_sock->type != GSOCK_DAUL_CHANNEL) || (gs_sock->idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num) ||
        (gs_sock->sid == 0) || (gs_sock->sid >= g_instance.comm_cxt.counters_cxt.g_max_stream_num)) {
        LIBCOMM_ELOG(WARNING,
            "(r|unix domain)\tinvalid gs_sock from unix domain, idx: %d, sid: %d, ver: %d, type: %d.",
            gs_sock->idx,
            gs_sock->sid,
            gs_sock->ver,
            gs_sock->type);
        mc_tcp_close(fd);
        return -1;
    }

    return error;
}

static int libcomm_build_tcp_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx)
{
    struct sock_id fd_id = {-1, -1};
    ip_key addr;
    int msg_len = NAMEDATALEN;
    int error = -1;
    errno_t ss_rc = 0;
    uint32 cpylen;

    /*
     * Historical residual problem!
     * comm_control_port and comm_sctp_port is the same,
     * it is well on sctp mode, because we use two protocol.
     * and it is conflict when we only use tcp protocol on tcp mode.
     * so we use sctp_port+1 for data connection for tcp mode.
     */
    int sock = mc_tcp_connect(libcomm_addrinfo->host, libcomm_addrinfo->listen_port);
    if (sock < 0) {
        LIBCOMM_ELOG(WARNING,
            "(s|build tcp connection)\tFailed to build data connection "
            "to %s:%d for node[%d]:%s, error[%d:%d]:%s.",
            libcomm_addrinfo->host,
            libcomm_addrinfo->listen_port,
            node_idx,
            g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
            error,
            errno,
            mc_strerror(errno));
        return -1;
    }

    /* Client side gss kerberos authentication for data connection. */
    if (g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile != NULL && GssClientAuth(sock, libcomm_addrinfo->host) < 0) {
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tData channel GSS authentication failed, "
            "remote:%s[%s:%d]:%s.",
            g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
            libcomm_addrinfo->host,
            libcomm_addrinfo->listen_port,
            mc_strerror(errno));
        errno = ECOMMTCPGSSAUTHFAIL;
        // Failed to build sctp connection
        mc_tcp_close(sock);
        return -1;
    }

    fd_id.fd = sock;
    fd_id.id = 0;
    if (gs_update_fd_to_htab_socket_version(&fd_id) < 0) {
        mc_tcp_close(sock);
        LIBCOMM_ELOG(WARNING, "(s|build tcp connection)\tFailed to save socket[%d,%d], close it.", fd_id.fd, fd_id.id);
        return -1;
    }

    struct libcomm_connect_package connect_package;
    connect_package.type = LIBCOMM_PKG_TYPE_CONNECT;
    connect_package.magic_num = MSG_HEAD_MAGIC_NUM2;
    ss_rc = strcpy_s(connect_package.node_name, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strcpy_s(connect_package.host, HOST_ADDRSTRLEN, g_instance.comm_cxt.localinfo_cxt.g_local_host);
    securec_check(ss_rc, "\0", "\0");
    msg_len = sizeof(struct libcomm_connect_package);

    MsgHead msg_head;
    msg_head.type = 'C';
    msg_head.magic_num = MSG_HEAD_MAGIC_NUM;
    msg_head.checksum = MSG_HEAD_TEMP_CHECKSUM;
    msg_head.logic_id = 0;
    msg_head.msg_len = msg_len;
    msg_head.version = 0;

    error = mc_tcp_write_block(sock, (char*)&msg_head, sizeof(MsgHead));
    if (error > 0) {
        error = mc_tcp_write_block(sock, (char*)&connect_package, msg_len);
    }

    if (error <= 0) {
        LIBCOMM_ELOG(WARNING,
            "(s|build tcp connection)\tFailed to send assoc id to %s:%d "
            "for node[%d]:%s on socket[%d].",
            libcomm_addrinfo->host,
            libcomm_addrinfo->listen_port,
            node_idx,
            g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
            sock);
        mc_tcp_close(sock);
        return -1;
    }

    if (gs_map_sock_id_to_node_idx(fd_id, node_idx) < 0) {
        LIBCOMM_ELOG(WARNING, "(s|build tcp connection)\tFailed to save sock and sockid.");
        mc_tcp_close(sock);
        return -1;
    }

retry_read:
    struct libcomm_accept_package ack_msg = {0, 0};
    error = mc_tcp_read_block(sock, &ack_msg, sizeof(ack_msg), 0);

    // if failed, we close the bad one and return -1
    if (error < 0 || ack_msg.result != 1 || ack_msg.type != LIBCOMM_PKG_TYPE_ACCEPT) {
        LIBCOMM_ELOG(WARNING,
            "(s|build tcp connection)\tFailed to recv assoc id from %s:%d "
            "for node[%d]:%s on socket[%d].",
            libcomm_addrinfo->host,
            libcomm_addrinfo->listen_port,
            node_idx,
            g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
            sock);
        mc_tcp_close(sock);
        return -1;
    } else if (error == 0) {
        usleep(1000);
        goto retry_read;
    }

    g_instance.comm_cxt.g_senders->sender_conn[node_idx].ip_changed = true;
    LIBCOMM_PTHREAD_RWLOCK_WRLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
    /*
     * check the host with ctrl channel
     * make sure the connection of control
     * channel and data channel is same node
     */
    if (strcmp(g_instance.comm_cxt.g_s_node_sock[node_idx].remote_host, libcomm_addrinfo->host) != 0) {
        g_instance.comm_cxt.g_senders->sender_conn[node_idx].ip_changed = false;
        LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
        return -1;
    }

    /* close old connection */
    struct sock_id libcomm_fd_id = {g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket,
        g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id};
    gs_s_close_bad_data_socket(&libcomm_fd_id, ECOMMTCPPEERCHANGED, node_idx);

    cpylen = comm_get_cpylen(libcomm_addrinfo->host, HOST_ADDRSTRLEN);
    ss_rc = memset_s(
        g_instance.comm_cxt.g_senders->sender_conn[node_idx].remote_host, HOST_ADDRSTRLEN, 0x0, HOST_ADDRSTRLEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(g_instance.comm_cxt.g_senders->sender_conn[node_idx].remote_host,
        HOST_ADDRSTRLEN,
        libcomm_addrinfo->host,
        cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].remote_host[cpylen] = '\0';

    g_instance.comm_cxt.g_senders->sender_conn[node_idx].port = libcomm_addrinfo->listen_port;
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].assoc_id = 1;
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket = fd_id.fd;
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id = fd_id.id;
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].ip_changed = false;

    /* set reply socket for g_r_node_sock */
    gs_set_reply_sock(node_idx);

    cpylen = comm_get_cpylen(libcomm_addrinfo->host, HOST_LEN_OF_HTAB);
    ss_rc = memset_s(addr.ip, HOST_LEN_OF_HTAB, 0x0, HOST_LEN_OF_HTAB);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(addr.ip, HOST_LEN_OF_HTAB, libcomm_addrinfo->host, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    addr.ip[cpylen] = '\0';

    addr.port = libcomm_addrinfo->listen_port;
    /* update connection state to succeed when connect succeed */
    gs_update_connection_state(addr, CONNSTATESUCCEED, true, node_idx);

    LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
    LIBCOMM_ELOG(LOG,
        "(s|build tcp connection)\tSucceed to connect %s:%d with socket[%d:%d] for node[%d]:%s.",
        libcomm_addrinfo->host,
        libcomm_addrinfo->listen_port,
        fd_id.fd,
        fd_id.id,
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    return 0;
}

/*
 * function name    : gs_s_build_tcp_ctrl_connection
 * description    : build tcp connection to the remote,
 *                   and then update g_s_node_sock.
 * notice        : we must get g_s_poller_list_lock lock before!
 * arguments        :
 *                   _in_ sctp_addrinfo: remote infomation.
 *                   _in_ node_idx: remote node index.
 * return value    :
 *                   -1: failed.
 *                   0: succeed.
 */
int gs_s_build_tcp_ctrl_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx, bool is_reply)
{
    int tcp_sock = -1;
    int ctrl_sock = -1;
    int ctrl_sock_id = -1;
    ip_key addr;
    int error;
    errno_t ss_rc;
    uint32 cpylen;
    char ack = 'r';
    char* remote_host = libcomm_addrinfo->host;
    int remote_tcp_port = libcomm_addrinfo->ctrl_port;
    char* remote_nodename = libcomm_addrinfo->nodename;

    // do connect to remote tcp listening port
    tcp_sock = mc_tcp_connect(remote_host, remote_tcp_port);

    // failed to connect, report error
    if (tcp_sock < 0) {
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tTCP connect failed to node:%s[%s:%d]:%s.",
            remote_nodename,
            remote_host,
            remote_tcp_port,
            mc_strerror(errno));
        errno = ECOMMTCPCONNFAIL;
        return -1;
    }

    /* Client side gss kerberos authentication for tcp connection. */
    if (g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile != NULL && GssClientAuth(tcp_sock, remote_host) < 0) {
        mc_tcp_close(tcp_sock);
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tControl channel GSS authentication failed, remote:%s[%s:%d]:%s.",
            remote_nodename,
            remote_host,
            remote_tcp_port,
            mc_strerror(errno));
        errno = ECOMMTCPGSSAUTHFAIL;
        return -1;
    } else {
        COMM_DEBUG_LOG("(s|connect)\tControl channel GSS authentication SUCC, remote:%s[%s:%d]:%s.",
            remote_nodename,
            remote_host,
            remote_tcp_port,
            mc_strerror(errno));
    }

    // wait ack from remote node, reject when the state of remote node is incorrect, such as standby mode;
    struct FCMSG_T fcmsgs = {0x0};
    if (IS_PGXC_COORDINATOR) {
        fcmsgs.type = CTRL_CONN_REGIST_CN;
    } else {
        fcmsgs.type = CTRL_CONN_REGIST;
    }

    fcmsgs.node_idx = node_idx;
    fcmsgs.streamid = 1;
    fcmsgs.extra_info = 0xEA;

    cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
    ss_rc = memset_s(fcmsgs.nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(fcmsgs.nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    fcmsgs.nodename[cpylen] = '\0';

    error = gs_send_ctrl_msg_by_socket(tcp_sock, &fcmsgs, node_idx);
    if (error < 0) {
        mc_tcp_close(tcp_sock);
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tSend ctrl msg failed remote[%s] with addr[%s:%d].",
            remote_nodename,
            remote_host,
            remote_tcp_port);
        errno = ECOMMTCPCONNFAIL;
        return -1;
    }

    // cn need to ask the remote datanode status when make connection
    // 'r' is received when remote is standby or pending mode
    // for conn between dns, skip this step, ip is given by executor
    if (IS_PGXC_COORDINATOR) {
        error = mc_tcp_read_block(tcp_sock, &ack, sizeof(char), 0);
        if (error < 0 || ack != 'o') {
            mc_tcp_close(tcp_sock);
            LIBCOMM_ELOG(WARNING,
                "(s|connect)\tControl channel connect reject by remote[%s] with addr[%s:%d], remote is not a primary "
                "node.",
                remote_nodename,
                remote_host,
                remote_tcp_port);
            errno = ECOMMTCPCONNFAIL;
            return -1;
        }
    } else if (is_reply) {
        /* when DN build reply connecion to CN
         * wait the reply of CN, to make sure CN has received ctrl connection request
         * then send ctrl msgs to CN
         */
        error = mc_tcp_read_block(tcp_sock, &ack, sizeof(char), 0);
        if (error < 0 || ack != 'o') {
            mc_tcp_close(tcp_sock);
            LIBCOMM_ELOG(WARNING,
                "(s|connect)\tControl channel connect reject by remote[%s] with addr[%s:%d].",
                remote_nodename,
                remote_host,
                remote_tcp_port);
            errno = ECOMMTCPCONNFAIL;
            return -1;
        }
    }

    struct sock_id fd_id = {tcp_sock, 0};
    // if we successfully to connect, we should record the socket(fd) and the version(id)
    if (gs_update_fd_to_htab_socket_version(&fd_id) < 0) {
        mc_tcp_close(tcp_sock);
        LIBCOMM_ELOG(WARNING, "(s|connect)\tFailed to malloc for socket.");
        errno = ECOMMTCPMEMALLOC;
        return -1;
    }

    if (gs_map_sock_id_to_node_idx(fd_id, node_idx) < 0) {
        LIBCOMM_ELOG(WARNING, "(s|connect)\tFailed to save sock and sockid.");
        mc_tcp_close(tcp_sock);
        return -1;
    }

    /* close old data connection */
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].ip_changed = true;
    LIBCOMM_PTHREAD_RWLOCK_WRLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
    struct sock_id libcomm_fd_id = {g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket,
        g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id};
    gs_s_close_bad_data_socket(&libcomm_fd_id, ECOMMTCPPEERCHANGED, node_idx);
    LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);

    /* close old ctrl connection */
    g_instance.comm_cxt.g_s_node_sock[node_idx].ip_changed = true;
    g_instance.comm_cxt.g_s_node_sock[node_idx].lock();
    ctrl_sock = g_instance.comm_cxt.g_s_node_sock[node_idx].get_nl(CTRL_TCP_SOCK, &ctrl_sock_id);
    struct sock_id ctrl_fd_id = {ctrl_sock, ctrl_sock_id};
    gs_s_close_bad_ctrl_tcp_sock(&ctrl_fd_id, ECOMMTCPPEERCHANGED, false, node_idx);

    /* save remote datanode information */
    g_instance.comm_cxt.g_s_node_sock[node_idx].set_nl(fd_id.fd, CTRL_TCP_SOCK);
    g_instance.comm_cxt.g_s_node_sock[node_idx].set_nl(fd_id.id, CTRL_TCP_SOCK_ID);

    cpylen = comm_get_cpylen(remote_host, HOST_ADDRSTRLEN);
    ss_rc = memset_s(g_instance.comm_cxt.g_s_node_sock[node_idx].remote_host, HOST_ADDRSTRLEN, 0x0, HOST_ADDRSTRLEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc =
        strncpy_s(g_instance.comm_cxt.g_s_node_sock[node_idx].remote_host, HOST_ADDRSTRLEN, remote_host, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    g_instance.comm_cxt.g_s_node_sock[node_idx].remote_host[cpylen] = '\0';

    cpylen = comm_get_cpylen(remote_nodename, NAMEDATALEN);
    ss_rc = memset_s(g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(
        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename, NAMEDATALEN, remote_nodename, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename[cpylen] = '\0';

    g_instance.comm_cxt.g_s_node_sock[node_idx].set_nl(remote_tcp_port, CTRL_TCP_PORT);
    g_instance.comm_cxt.g_s_node_sock[node_idx].ip_changed = false;
    /* add the socket to the epoll list for monitoring network events */
    if (g_instance.comm_cxt.pollers_cxt.g_s_poller_list->add_fd(&fd_id) < 0) {
        g_instance.comm_cxt.g_s_node_sock[node_idx].close_socket_nl(CTRL_TCP_SOCK);
        g_instance.comm_cxt.g_s_node_sock[node_idx].set_nl(-1, CTRL_TCP_PORT);
        g_instance.comm_cxt.g_s_node_sock[node_idx].unlock();

        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
        hash_search(g_htab_fd_id_node_idx, &fd_id, HASH_REMOVE, NULL);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

        LIBCOMM_ELOG(WARNING, "(s|connect)\tFailed to malloc for poll.");
        errno = ECOMMTCPMEMALLOC;
        return -1;
    }
    cpylen = comm_get_cpylen(libcomm_addrinfo->host, HOST_LEN_OF_HTAB);
    ss_rc = memset_s(addr.ip, HOST_LEN_OF_HTAB, 0x0, HOST_LEN_OF_HTAB);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(addr.ip, HOST_LEN_OF_HTAB, libcomm_addrinfo->host, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    addr.ip[cpylen] = '\0';

    addr.port = libcomm_addrinfo->ctrl_port;
    /* update connection state to succeed when connect succeed */
    gs_update_connection_state(addr, CONNSTATESUCCEED, true, node_idx);

    g_instance.comm_cxt.g_s_node_sock[node_idx].unlock();
    LIBCOMM_ELOG(LOG,
        "(s|connect)\tTCP connect successed to node:%s[%s:%d] on socket[%d,%d] with node id[%d].",
        remote_nodename,
        remote_host,
        remote_tcp_port,
        fd_id.fd,
        fd_id.id,
        node_idx);
    return 0;
}  // gs_s_build_tcp_ctrl_connection

// Registed a Consumer callback function to wake up the Consumer (thread at executor) when a Producer (thread at
// executor) connected successfully
//
void gs_connect_regist_callback(wakeup_hook_type wakeup_callback)
{
    g_instance.comm_cxt.gs_wakeup_consumer = wakeup_callback;
}  // gs_connect_regist_callback

void gs_init_adapt_layer()
{
    /*
     * Historical residual problem!
     * comm_control_port and comm_sctp_port is the same,
     * it is well on sctp mode, because we use two protocol.
     * and it is conflict when we only use tcp protocol on tcp mode.
     * so we use sctp_port+1 for data connection for tcp mode.
     */
    g_libcomm_adapt.recv_data = libcomm_tcp_recv;
    g_libcomm_adapt.send_data = libcomm_tcp_send;
    g_libcomm_adapt.connect = libcomm_build_tcp_connection;
    g_libcomm_adapt.accept = mc_tcp_accept;
    g_libcomm_adapt.listen = libcomm_tcp_listen;
    g_libcomm_adapt.block_send = libcomm_tcp_send;
    g_libcomm_adapt.send_ack = mc_tcp_write_block;
    g_libcomm_adapt.check_socket = mc_tcp_check_socket;
}

/*
 * function name    : gs_delay_survey
 * description      : send libcomm delay message to all connection.
 */
void gs_delay_survey()
{
    int node_idx = -1;
    int socket = -1;
    int socket_id = -1;

    errno_t ss_rc = 0;
    struct libcomm_delay_package msg;

    ss_rc = memset_s(&msg, sizeof(msg), 0, sizeof(struct libcomm_delay_package));
    securec_check(ss_rc, "\0", "\0");

    msg.type = LIBCOMM_PKG_TYPE_DELAY_REQUEST;
    for (node_idx = 0; node_idx < g_instance.comm_cxt.counters_cxt.g_cur_node_num; node_idx++) {
        // if the connection is not ready, continue
        if (g_instance.comm_cxt.g_senders->sender_conn[node_idx].assoc_id == 0) {
            continue;
        }

        socket = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket;
        socket_id = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id;
        msg.sn = libcomm_delay_no;
        msg.start_time = (uint32)mc_timers_us();

        LibcommSendInfo send_info;
        send_info.socket = socket;
        send_info.socket_id = socket_id;
        send_info.node_idx = node_idx;
        send_info.streamid = 0;
        send_info.version = 0;
        send_info.msg = (char*)&msg;
        send_info.msg_len = sizeof(struct libcomm_delay_package);

        (void)g_libcomm_adapt.block_send(&send_info);
    }
    libcomm_delay_no++;
}

