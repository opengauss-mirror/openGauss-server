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
 * libcomm_shakehands.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/libcomm_utils/libcomm_shakehands.cpp
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

#ifdef USE_SPQ
extern void gs_s_build_reply_conntion(libcommaddrinfo* addr_info, int remote_version);
#endif

/*
 * function name    : gs_r_build_reply_connection
 * description      : as the connection between cn & dn is duplex.
 *                  dn need to build an inverse connection to cn.
 *                           this func will:
 *                  1. build an inverse physical conn if needed.
 *                  2. build an inverse logic conn
 *                  3. initial pmailbox
 * arguments        : fcmsgr: provides gs_sock and remote port
 * return value     : -1: error
 *                  :   0:Succeed
 */
#ifdef USE_SPQ
int gs_r_build_reply_connection(BackConnInfo* fcmsgr, int local_version, uint16 *sid)
#else
static int gs_r_build_reply_connection(FCMSG_T* fcmsgr, int local_version)
#endif
{
    errno_t ss_rc;
    uint32 cpylen;

    int node_idx = fcmsgr->node_idx;
#ifdef USE_SPQ
    int streamid = gs_get_stream_id(node_idx);
    *sid = streamid;
#else
    int streamid = fcmsgr->streamid;
#endif
    int remote_version = fcmsgr->version;

    // get remote nodename and host from global variable
    char remote_host[HOST_ADDRSTRLEN] = {0x0};
    char remote_nodename[NAMEDATALEN] = {0x0};

    cpylen = comm_get_cpylen(g_instance.comm_cxt.g_r_node_sock[node_idx].remote_host, HOST_ADDRSTRLEN);
    ss_rc = memset_s(remote_host, HOST_ADDRSTRLEN, 0x0, HOST_ADDRSTRLEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc =
        strncpy_s(remote_host, HOST_ADDRSTRLEN, g_instance.comm_cxt.g_r_node_sock[node_idx].remote_host, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    remote_host[cpylen] = '\0';

    cpylen = comm_get_cpylen(g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename, NAMEDATALEN);
    ss_rc = memset_s(remote_nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(
        remote_nodename, NAMEDATALEN, g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    remote_nodename[cpylen] = '\0';

    // streamcap contain the ctrl port and data port of remote process.
    // we use streamcap to send these ports
    // becuase we do not want to add too much members to FCMSG_T
    // so we usually use streamcap to send some extra msgs
    int remote_ctrl_port = (int)(fcmsgr->streamcap >> 32);
    int remote_data_port = (int)(fcmsgr->streamcap);

    libcommaddrinfo libcomm_addrinfo;
    libcomm_addrinfo.host = remote_host;
    libcomm_addrinfo.ctrl_port = remote_ctrl_port;
    libcomm_addrinfo.listen_port = remote_data_port;
    cpylen = comm_get_cpylen(remote_nodename, NAMEDATALEN);
    ss_rc = memset_s(libcomm_addrinfo.nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(libcomm_addrinfo.nodename, NAMEDATALEN, remote_nodename, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    libcomm_addrinfo.nodename[cpylen] = '\0';
#ifdef USE_SPQ
    libcomm_addrinfo.gs_sock.idx = fcmsgr->node_idx;
    libcomm_addrinfo.gs_sock.sid = streamid;
    libcomm_addrinfo.gs_sock.ver = local_version;
    libcomm_addrinfo.streamKey = fcmsgr->stream_key;
#endif /* USE_SPQ */

    COMM_DEBUG_LOG("(r|build reply conn)\tBuild TCP connect for node[%d]:%s.",
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    if (unlikely(gs_s_check_connection(&libcomm_addrinfo, node_idx, true, CTRL_CHANNEL) == false)) {
        LIBCOMM_ELOG(WARNING,
            "(r|build reply conn)\tFailed to connect to host:port[%s:%d], node name[%s].",
            libcomm_addrinfo.host,
            libcomm_addrinfo.ctrl_port,
            remote_nodename);
        return -1;
    }

    /*
     * Check and build logic connection whit the remote point (if need)
     */
    COMM_DEBUG_LOG("(r|build reply conn)\tBuild data connection for node[%d]:%s.",
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    // failed to build logic connection
    if (unlikely(gs_s_check_connection(&libcomm_addrinfo, node_idx, true, DATA_CHANNEL) == false)) {
        LIBCOMM_ELOG(WARNING,
            "(r|build reply conn)\tFailed to build data connection "
            "to %s:%d for node[%d]:%s, detail:%s.",
            libcomm_addrinfo.host,
            libcomm_addrinfo.listen_port,
            node_idx,
            libcomm_addrinfo.nodename,
            mc_strerror(errno));
        errno = ECOMMTCPCONNFAIL;
        return -1;
    }

    COMM_DEBUG_LOG("(r|build reply conn)\tBuild data logical connection for node[%d]:%s.",
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

#ifdef USE_SPQ
    char* node_name = g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename;
    if (0 == strcmp(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, node_name)) {
        QCConnKey key = {
            .query_id = fcmsgr->stream_key.queryId,
            .plan_node_id = fcmsgr->stream_key.planNodeId,
            .node_id = node_idx,
            .type = SPQ_QE_CONNECTION,
        };
        bool found = false;
        QCConnEntry* entry;
        /* notice: here dont use lock, so outer function call should guaranteed adp_connects has been locked */
        entry = (QCConnEntry*)hash_search(g_instance.spq_cxt.adp_connects, (void*)&key, HASH_FIND, &found);
        if (found) {
            entry->backward = {
                .idx = node_idx,
                .sid = streamid,
                .ver = remote_version,
                .type = GSOCK_CONSUMER,
            };
            gs_s_build_reply_conntion(&libcomm_addrinfo, local_version);
        } else {
            LIBCOMM_ELOG(WARNING, "(s|connect)\tFailed to build local connection to node[%d]:%s, detail:%s.", node_idx,
                         REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx), mc_strerror(errno));
            errno = ECOMMTCPTCPDISCONNECT;
            gs_close_gsocket(fcmsgr->backward);
            return -1;
        }
    } else {
        struct FCMSG_T fcmsgs = {0x0};

        // for connect between cn and dn, channel is duplex
        fcmsgs.type = CTRL_BACKWARD_REGIST;
        fcmsgs.extra_info = 0;

        fcmsgs.node_idx = node_idx;
        fcmsgs.streamid = streamid;
        // send pmailbox version to cmailbox,
        // and cmailbox will save as cmailbox->remote_version.
        fcmsgs.version = remote_version;
        fcmsgs.stream_key = libcomm_addrinfo.streamKey;
        fcmsgs.query_id = fcmsgr->query_id;
        cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
        ss_rc = memset_s(fcmsgs.nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
        securec_check(ss_rc, "\0", "\0");
        ss_rc = strncpy_s(fcmsgs.nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
        securec_check(ss_rc, "\0", "\0");
        fcmsgs.nodename[cpylen] = '\0';
        fcmsgs.streamcap = fcmsgr->streamcap;

        int rc = gs_send_ctrl_msg(&g_instance.comm_cxt.g_s_node_sock[node_idx], &fcmsgs, ROLE_PRODUCER);
        if (rc <= 0) {
            LIBCOMM_ELOG(WARNING, "(s|connect)\tFailed to send ready msg to node[%d]:%s, detail:%s.", node_idx,
                         REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx), mc_strerror(errno));

            errno = ECOMMTCPTCPDISCONNECT;
            gs_close_gsocket(fcmsgr->backward);
            return -1;
        }
        char ack;
        do {
            rc = gs_recv(fcmsgr->backward, &ack, sizeof(char));
        } while (rc == 0 || errno == ECOMMTCPNODATA);
        if (rc < 0 || ack != 'o') {
            gs_close_gsocket(fcmsgr->backward);
            return -1;
        }
    }
#endif

    struct p_mailbox* pmailbox = &P_MAILBOX(node_idx, streamid);

    LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
    if (pmailbox->state != MAIL_CLOSED) {
        MAILBOX_ELOG(pmailbox,
            WARNING,
            "(r|build reply conn)\tFailed to get mailbox for node[%d,%d]:%s.",
            node_idx,
            streamid,
            REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));
        gs_s_close_logic_connection(pmailbox, ECOMMTCPREMOETECLOSE, NULL);
    }

    // inital pmailbox for sending msgs to cn
    pmailbox->local_version = local_version;
    pmailbox->remote_version = remote_version;
    pmailbox->ctrl_tcp_sock = g_instance.comm_cxt.g_s_node_sock[node_idx].ctrl_tcp_sock;
    pmailbox->state = MAIL_RUN;
    pmailbox->bufCAP = DEFULTMSGLEN;
    pmailbox->stream_key = fcmsgr->stream_key;
    pmailbox->query_id = fcmsgr->query_id;
    pmailbox->local_thread_id = 0;
    pmailbox->peer_thread_id = 0;
    pmailbox->close_reason = 0;
    COMM_STAT_CALL(pmailbox, pmailbox->statistic->start_time = (uint32)mc_timers_ms());
    if (g_instance.comm_cxt.commutil_cxt.g_stat_mode && (NULL == pmailbox->statistic)) {
        LIBCOMM_MALLOC(pmailbox->statistic, sizeof(struct pmailbox_statistic), pmailbox_statistic);
        if (NULL == pmailbox->statistic) {
            errno = ECOMMTCPRELEASEMEM;
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
            return -1;
        }
    }

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);

    return 0;
}

/*
 * function name    : gs_r_close_logic_connection
 * description      : consumer close logic connetion and reset mailbox,
 *                    if producer call this, we only set state is CTRL_TO_CLOSE,
 *                    then really close when consumer call this.
 * notice           : we must get mailbox lock before
 * arguments        : _in_ cmailbox: libcomm conntion info.
 *                    _in_ close_reason: close reason.
 */
void gs_r_close_logic_connection(struct c_mailbox* cmailbox, int close_reason, FCMSG_T* msg)
{
    errno_t ss_rc;
    uint32 cpylen;

    if (cmailbox->state == MAIL_CLOSED) {
        return;
    }

    if (ENABLE_THREAD_POOL_DN_LOGICCONN) {
        NotifyListener(cmailbox, true, __FUNCTION__);
    }

    // 1, if tcp disconnect, we can not send control message on tcp channel,
    //    remote can receive disconnect event when flow control thread call epoll_wait.
    // 2, close reason is ECOMMTCPREMOETECLOSE means remote send MAIL_CLOSED,
    //    we could not reply MAIL_CLOSED message.
    if (IS_NOTIFY_REMOTE(close_reason) && msg) {
        msg->type = CTRL_CLOSED;
        msg->node_idx = cmailbox->idx;
        msg->streamid = cmailbox->streamid;
        msg->streamcap = 0;
        msg->version = cmailbox->remote_version;
        msg->query_id = cmailbox->query_id;

        cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
        ss_rc = memset_s(msg->nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
        securec_check(ss_rc, "\0", "\0");
        ss_rc = strncpy_s(msg->nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
        securec_check(ss_rc, "\0", "\0");
        msg->nodename[cpylen] = '\0';
    }

    // wake up the consumer who is waiting for the data at gs_wait_poll,
    gs_poll_signal(cmailbox->semaphore);

    // At last, reset mailbox and clean hash table
    gs_r_reset_cmailbox(cmailbox, close_reason);

    return;
}

uint64 gs_get_recv_ready_time()
{
    return g_instance.comm_cxt.localinfo_cxt.g_r_first_recv_time;
}

/*
 * function name    : gs_receivers_flow_handle_ready_request
 * description      : handle ready request when received MAIL_READY.
 * arguments        :
 *                   _in_ fcmsgr: the message that receivers_flow thread received.
 *                   _in_ t_fd_id: the socket that receivers_flow thread used.
 * producer is building a logic connection,
 * we get stream index from the message,
 * and initialize consumer mailbox,
 * then reply MAIL_READY message to producer.
 */
void gs_receivers_flow_handle_ready_request(FCMSG_T* fcmsgr)
{
    uint16 streamid = fcmsgr->streamid;
    uint16 node_idx = fcmsgr->node_idx;
    // producer send pmailbox version as fcmsgr->version,
    // now save it to cmailbox->remote_version.
    int remote_verion = fcmsgr->version;
    int ctrl_socket = g_instance.comm_cxt.g_r_node_sock[node_idx].ctrl_tcp_sock;
    struct FCMSG_T fcmsgs = {0x0};
    struct c_mailbox* cmailbox = NULL;
    int rc = -1;
    uint64 time_now;
    uint64 time_callback_start = 0;
    uint64 time_callback_end = 0;
    uint64 deal_time = 0;
    errno_t ss_rc;
    uint32 cpylen;
    gsocket gs_sock = {0};
    uint16 local_version;

    // If the Consumer is ready to exist, maybe because of timeout or other errors,
    // we do not need to get the stream index for the producer, or the stream index will be leaked.
    //
    // small quota size for slow start
    long add_quota = DEFULTMSGLEN;
    StreamConnInfo connInfo = {0};
    COMM_TIMER_INIT();

#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_CONSUMER_REJECT)) {
        LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\t[FAULT INJECTION]Consumer can not accept.");
        errno = ECOMMTCPAPPCLOSE;
        goto accept_failed;
    }
#endif

    // the entry should be close, may be caused by cancel
    // node_idx < 0 means data conntion has some errors
    Assert(node_idx >= 0);

    // initialize consumer cmailbox
    cmailbox = &C_MAILBOX(node_idx, streamid);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);

    if (cmailbox->state != MAIL_CLOSED) {
        MAILBOX_ELOG(cmailbox,
            WARNING,
            "(r|flow ctrl)\tFailed to get mailbox for node[%d]:%s.",
            node_idx,
            REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, node_idx));
        gs_r_close_logic_connection(cmailbox, ECOMMTCPREMOETECLOSE, NULL);
    }

    local_version = cmailbox->local_version + 1;
    if (local_version >= MAX_MAILBOX_VERSION) {
        local_version = 0;
    }

    cmailbox->local_version = local_version;
    // producer send pmailbox version as fcmsgr->version,
    // now save it to cmailbox->remote_version.
    cmailbox->remote_version = remote_verion;
    cmailbox->ctrl_tcp_sock = ctrl_socket;
    cmailbox->state = MAIL_RUN;
    cmailbox->bufCAP += add_quota;
    cmailbox->stream_key = fcmsgr->stream_key;
    cmailbox->query_id = fcmsgr->query_id;
    cmailbox->local_thread_id = 0;
    cmailbox->peer_thread_id = 0;
    cmailbox->close_reason = 0;
    if (g_instance.comm_cxt.commutil_cxt.g_stat_mode && (cmailbox->statistic == NULL)) {
        LIBCOMM_MALLOC(cmailbox->statistic, sizeof(struct cmailbox_statistic), cmailbox_statistic);
        if (cmailbox->statistic == NULL) {
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
            errno = ECOMMTCPRELEASEMEM;
            goto accept_failed;
        }
    }
    time_now = COMM_STAT_TIME();
    COMM_STAT_CALL(cmailbox, cmailbox->statistic->start_time = (uint32)time_now);
    COMM_DEBUG_LOG("(r|flow ctrl)\tNode[%d] stream[%d], node name[%s] is in state[%s].",
        node_idx,
        streamid,
        g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename,
        stream_stat_string(cmailbox->state));
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);

    deal_time = ABS_SUB((long long int)time(NULL), (long long int)gs_get_recv_ready_time());
    if ((long long int)mc_tcp_get_connect_timeout() < (long long int)deal_time) {
        LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tIt takes %lus to process the ready message, query id:%lu.",
            deal_time, u_sess->debug_query_id);
    }

#ifndef USE_SPQ
    // for dual connection, we need to build an inverse logic conn with same gs_sock
    if (fcmsgr->type == CTRL_CONN_DUAL) {
        u_sess->pgxc_cxt.NumDataNodes = (int)(fcmsgr->extra_info);
        // build pmailbox with the same version and remote_verion as cmailbox,
        // when this connection is duplex.
        if (gs_r_build_reply_connection(fcmsgr, local_version) != 0) {
            LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);
            gs_r_close_logic_connection(cmailbox, ECOMMTCPTCPDISCONNECT, NULL);
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
            goto accept_failed;
        }
    }
#endif

    gs_sock.idx = node_idx;
    gs_sock.sid = streamid;
    gs_sock.ver = local_version;

    if (fcmsgr->type == CTRL_CONN_DUAL) {
#ifdef USE_SPQ
        bool found = false;
        QCConnKey key = {
            .query_id = fcmsgr->stream_key.queryId,
            .plan_node_id = fcmsgr->stream_key.planNodeId,
            .node_id = 0,
            .type = SPQ_QC_CONNECTION,
        };
        gs_sock.type = GSOCK_CONSUMER;
        pthread_rwlock_wrlock(&g_instance.spq_cxt.adp_connects_lock);
        QCConnEntry* entry = (QCConnEntry*)hash_search(g_instance.spq_cxt.adp_connects, (void*)&key, HASH_ENTER, &found);
        if (!found) {
            entry->backward = gs_sock;
            entry->forward.idx = 0;
            entry->streamcap = fcmsgr->streamcap;
        }
        pthread_rwlock_unlock(&g_instance.spq_cxt.adp_connects_lock);
        ereport(LOG, (errmsg("Receive dual connect request from qc, queryid: %lu, nodeid: %u",
                             fcmsgr->stream_key.queryId, fcmsgr->stream_key.planNodeId)));
#else
        // CN request this logic connection, is a dual channel
        gs_sock.type = GSOCK_DAUL_CHANNEL;

        rc = gs_send_msg_by_unix_domain((void*)&gs_sock, sizeof(gs_sock));
        if (rc <= 0) {
            LIBCOMM_ELOG(WARNING,
                "(r|flow ctrl)\t fail to notify main thread from node[%d]:%s with socket[%d].",
                node_idx,
                REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, node_idx),
                ctrl_socket);
            gs_close_gsocket(&gs_sock);
            goto accept_failed;
        }
#endif
    }

    // reply MAIL_READY message to producer
    fcmsgs.type = CTRL_CONN_ACCEPT;
    fcmsgs.node_idx = node_idx;
    fcmsgs.streamid = streamid;
    fcmsgs.streamcap = add_quota;
    fcmsgs.version = remote_verion;
    fcmsgs.query_id = fcmsgr->query_id;
    fcmsgs.extra_info = local_version;

    cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
    ss_rc = memset_s(fcmsgs.nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(fcmsgs.nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    fcmsgs.nodename[cpylen] = '\0';

    rc = gs_send_ctrl_msg(&g_instance.comm_cxt.g_r_node_sock[node_idx], &fcmsgs, ROLE_CONSUMER);
    if (rc <= 0) {
        errno = ECOMMTCPTCPDISCONNECT;
        LIBCOMM_ELOG(WARNING,
            "(r|flow ctrl)\tFailed to send ready msg to node[%d]:%s with socket[%d]:%s.",
            node_idx,
            REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, node_idx),
            ctrl_socket,
            mc_strerror(errno));
        return;
    }

    COMM_DEBUG_LOG("(r|flow ctrl)\tSuccess to send CTRL_CONN_ACCEPT msg to node[%d]:%s with socket[%d].",
        node_idx, REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, node_idx), ctrl_socket);
    deal_time = ABS_SUB((long long int)time(NULL), (long long int)gs_get_recv_ready_time());
    if ((uint64)(unsigned)mc_tcp_get_connect_timeout() < deal_time) {
        LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tIt takes %lus to process the ready message, query id:%lu.",
            deal_time, u_sess->debug_query_id);
    }

    if (fcmsgr->type == CTRL_CONN_DUAL) {
        return;
    }

#ifdef LIBCOMM_SPEED_TEST_ENABLE
    else if (fcmsgr->stream_key.queryId == LIBCOMM_PERFORMANCE_PLAN_ID) {
        ;  // do nothing
    }
#endif
    else {
        Assert(g_instance.comm_cxt.gs_wakeup_consumer != NULL);

        // DN request this logic connection, is a single channel
        gs_sock.type = GSOCK_CONSUMER;

        // wake up Conumser in executor to continue
        ss_rc = memset_s(&connInfo, sizeof(StreamConnInfo), 0x0, sizeof(StreamConnInfo));
        securec_check(ss_rc, "\0", "\0");
        connInfo.port.libcomm_layer.gsock = gs_sock;

        cpylen = comm_get_cpylen(fcmsgr->nodename, NAMEDATALEN);
        ss_rc = memset_s(connInfo.nodeName, NAMEDATALEN, 0x0, NAMEDATALEN);
        securec_check(ss_rc, "\0", "\0");
        ss_rc = strncpy_s(connInfo.nodeName, NAMEDATALEN, fcmsgr->nodename, cpylen + 1);
        securec_check(ss_rc, "\0", "\0");
        connInfo.nodeName[cpylen] = '\0';

        time_callback_start = time(NULL);
        bool wakeup_if = (*g_instance.comm_cxt.gs_wakeup_consumer)(fcmsgr->stream_key, connInfo);
        time_callback_end = time(NULL);

        deal_time = ABS_SUB(time_callback_end, time_callback_start);
        if ((uint64)(unsigned)mc_tcp_get_connect_timeout() < deal_time) {
            LIBCOMM_ELOG(WARNING,
                         "(r|flow ctrl)\tWake up consumer timeout for node[%d]:%s, it takes %lus, \
                          because the lock wait timeout, query id:%lu :%s.",
                         node_idx, REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, node_idx),
                         deal_time, u_sess->debug_query_id, mc_strerror(errno));
        }

        if (!wakeup_if) {
            COMM_DEBUG_LOG("(r|flow ctrl)\tFailed to wake up consumer "
                           "for node[%d]:%s, query maybe already quit:%s.",
                node_idx,
                REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, node_idx),
                mc_strerror(errno));

            (void)gs_r_close_stream(&gs_sock);
            // we needn't to send Reject.
            return;
        }
    }
    COMM_TIMER_LOG("(r|flow ctrl)\tWake up consumer, node[%d, %d]:%s.",
        node_idx,
        streamid,
        REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, node_idx));

    return;

accept_failed:
    // Failed to accept connction,
    // reply CTRL_INIT message to producer
    fcmsgs.type = CTRL_CONN_REJECT;
    fcmsgs.node_idx = node_idx;
    fcmsgs.streamid = streamid;
    fcmsgs.streamcap = 0;
    fcmsgs.version = remote_verion;
    fcmsgs.query_id = fcmsgr->query_id;

    cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
    ss_rc = memset_s(fcmsgs.nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(fcmsgs.nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    fcmsgs.nodename[cpylen] = '\0';

    rc = gs_send_ctrl_msg(&g_instance.comm_cxt.g_r_node_sock[node_idx], &fcmsgs, ROLE_CONSUMER);
    if (rc <= 0) {
        LIBCOMM_ELOG(WARNING,
            "(r|flow ctrl)\tFailed to send init msg to node[%d]:%s with socket[%d]:%s.",
            node_idx,
            REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, node_idx),
            ctrl_socket,
            mc_strerror(errno));
    }

    return;
}  // gs_receivers_flow_handle_ready_request

/*
 * function name    : gs_receivers_flow_handle_close_request
 * description    : handle close request when received MAIL_CLOSED.
 * arguments        : _in_ fcmsgr: the message that receivers_flow thread received.
 * Producer closed the stream, which indicate error happened when sender is sending data
 * if it is a Closed message, we should close local mailbox and tell the thread of executor to quit
 */
void gs_receivers_flow_handle_close_request(FCMSG_T* fcmsgr)
{
    int streamid = fcmsgr->streamid;
    int node_idx = fcmsgr->node_idx;
    struct c_mailbox* cmailbox = NULL;

    // set node index and get stream index from hash table (g_r_htab_nodeid_skey_to_stream)
    //
    cmailbox = &(C_MAILBOX(node_idx, streamid));
    LIBCOMM_PTHREAD_MUTEX_LOCK(&(cmailbox->sinfo_lock));
    // If the stream is closed already, may because of tcp error or upper consumer closed actively,
    // we just try to delete the entry in hash table(g_r_htab_nodeid_skey_to_stream)
    //
    if (false == gs_check_mailbox(cmailbox->local_version, fcmsgr->version)) {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&(cmailbox->sinfo_lock));
        return;
    }

    COMM_DEBUG_LOG("(r|flow ctrl)\tStream[%d] is closed "
                   "by remote node[%d]:%s, query[%lu].",
        streamid,
        node_idx,
        g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename,
        fcmsgr->query_id);

    gs_r_close_logic_connection(cmailbox, ECOMMTCPREMOETECLOSE, NULL);
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&(cmailbox->sinfo_lock));

    return;
}  // gs_receivers_flow_handle_close_request

/*
 * function name    : gs_receivers_flow_handle_assert_fail_request
 * description    : handle assert fail when receivers_flow thread received CTRL_ASSERT_FAIL.
 * arguments        : _in_ fcmsgr: the message that receivers_flow thread received.
 * return value    : void
 * if the sender assert fail and core, we will get a CTRL_ASSERT_FAIL message,
 * we should make the consumer core as well
 */
void gs_receivers_flow_handle_assert_fail_request(FCMSG_T* fcmsgr)
{

    int sidx = fcmsgr->streamid;
    int nidx = fcmsgr->node_idx;

    struct c_mailbox* cmailbox = NULL;

    cmailbox = &(C_MAILBOX(nidx, sidx));
    LIBCOMM_ELOG(WARNING,
        "(r|flow ctrl)\tNode[%d] stream[%d] assert fail, node name[%s] with state[%d] has bufCAP[%lu] and "
        "buff_q->u_size[%lu].",
        nidx,
        sidx,
        g_instance.comm_cxt.g_r_node_sock[nidx].remote_nodename,
        cmailbox->state,
        cmailbox->bufCAP,
        cmailbox->buff_q->u_size);
    MAILBOX_ELOG(cmailbox, WARNING, "(r|flow ctrl)\tMailbox Info which assert fail.");

    Assert(0 != 0);
}

/*
 * function name    : gs_senders_flow_handle_tid_request
 * description    : save peer thread id when received CTRL_PEER_TID.
 * arguments        : _in_ fcmsgr: the message that senders_flow thread received.
 * return value    : void
 */
void gs_senders_flow_handle_tid_request(FCMSG_T* fcmsgr)
{
    int streamid = fcmsgr->streamid;
    int node_idx = fcmsgr->node_idx;
    int version = fcmsgr->version;
    struct p_mailbox* pmailbox = NULL;

    pmailbox = &P_MAILBOX(node_idx, streamid);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
    // 1: check the pmailbox[idx][streamid] is correct
    //
    // check stream key
    if (gs_check_mailbox(pmailbox->local_version, version) == false) {
        MAILBOX_ELOG(pmailbox, WARNING, "(s|flow ctrl)\tStream has already closed.");
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
        return;
    }
    // 2: set the new quota and peer thread id to the pmailbox
    //
    pmailbox->peer_thread_id = fcmsgr->extra_info;
    pmailbox->bufCAP += fcmsgr->streamcap;
    // 3: set the new state to the pmailbox, then tell the executor thread who may be waiting in gs_send to continue
    //
    if (pmailbox->state == MAIL_HOLD) {
        gs_poll_signal(pmailbox->semaphore);
    }
    COMM_DEBUG_LOG("(s|flow ctrl)\tWake up mailbox[%d][%d], node[%s], bufCAP[%lu] add[%ld] type[%s].",
        node_idx,
        streamid,
        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
        pmailbox->bufCAP,
        fcmsgr->streamcap,
        stream_stat_string(pmailbox->state));
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
}

/*
 * function name    : gs_senders_flow_handle_init_request
 * description    : handle init request when received CTRL_INIT.
 * arguments        : _in_ fcmsgr: the message that senders_flow thread received.
 * return value    : void
 * if the receiver failed to get an usable stream index, we will get a CTRL_INIT message,
 * we should tell the executor thread to exit and report the error.
 */
void gs_senders_flow_handle_init_request(FCMSG_T* fcmsgr)
{
    int streamid = fcmsgr->streamid;
    int node_idx = fcmsgr->node_idx;
    struct p_mailbox* pmailbox = NULL;

    pmailbox = &P_MAILBOX(node_idx, streamid);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
    if (gs_check_mailbox(pmailbox->local_version, fcmsgr->version) == false) {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
        return;
    }
    // close logic conneion and sigal producer
    gs_s_close_logic_connection(pmailbox, ECOMMTCPREJECTSTREAM, NULL);
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
}

/*
 * function name    : gs_senders_flow_handle_ready_request
 * description    : handle ready request when received MAIL_READY.
 * arguments        : _in_ fcmsgr: the message that senders_flow thread received.
 * return value    : void
 * if the receiver succeed to get an usable stream index, we will get a MAIL_READY message,
 * we should change the state of pmailbox and tell the executor thread to continue
 */
void gs_senders_flow_handle_ready_request(FCMSG_T* fcmsgr)
{
    int streamid = fcmsgr->streamid;
    int node_idx = fcmsgr->node_idx;
    struct p_mailbox* pmailbox = NULL;
    uint64 time_now = 0;

    // 1: check the pmailbox state is CLOSED, we will retry for 10 times
    pmailbox = &P_MAILBOX(node_idx, streamid);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
    if (gs_check_mailbox(pmailbox->local_version, fcmsgr->version) == false) {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
        return;
    }

    if (pmailbox->state != MAIL_READY) {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
        return;
    }

    // 2: change the state of the mailbox
    pmailbox->state = MAIL_RUN;
    pmailbox->bufCAP += fcmsgr->streamcap;
    // consumer send cmailbox version as fcmsgr->extra_info,
    // now save it to pmailbox->remote_version.
    pmailbox->remote_version = (uint16)(fcmsgr->extra_info);
    COMM_DEBUG_LOG("(s|flow ctrl)\tnode[%d] stream[%d] is in state[%s], node name[%s], bufCAP[%lu].",
        node_idx,
        streamid,
        stream_stat_string(pmailbox->state),
        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
        pmailbox->bufCAP);

    // 3: signal the producer which should be waiting in gs_connect
    gs_poll_signal(pmailbox->semaphore);

    if (pmailbox->statistic != NULL) {
        time_now = COMM_STAT_TIME();
        pmailbox->statistic->connect_time = ABS_SUB(time_now, pmailbox->statistic->connect_time);
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
}

/*
 * function name    : gs_senders_flow_handle_resume_request
 * description    : handle resume request when received MAIL_RUN.
 * arguments        : _in_ fcmsgr: the message that senders_flow thread received.
 * return value    : void
 * if the receiver have enough buffer to continue receive, we will get a MAIL_RUN message,
 * we should change the state of pmailbox and tell the executor thread to continue
 */
void gs_senders_flow_handle_resume_request(FCMSG_T* fcmsgr)
{
    int streamid = fcmsgr->streamid;
    int node_idx = fcmsgr->node_idx;
    struct p_mailbox* pmailbox = NULL;

    // 1: check the pmailbox[idx][fcmsgr.streamid] is correct, if not wait a few seconds and retry
    //
    pmailbox = &P_MAILBOX(node_idx, streamid);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
    if (gs_check_mailbox(pmailbox->local_version, fcmsgr->version) ==
        false) { // if the pmailbox is not matched, we will break here and report error
        if (pmailbox->state != MAIL_CLOSED) {
            MAILBOX_ELOG(pmailbox, WARNING, "(s|flow ctrl)\tStream not work.");
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
        return;
    }

    // 2: set the new quota and state to the pmailbox
    //
    pmailbox->bufCAP += fcmsgr->streamcap;
    COMM_DEBUG_LOG("(s|flow ctrl)\tWake up node[%d] stream[%d], node name[%s], bufCAP[%lu] add[%ld].",
        node_idx,
        streamid,
        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
        pmailbox->bufCAP,
        fcmsgr->streamcap);
    // 3: tell the executor thread who may be waiting in gs_send to continue
    //
    if (pmailbox->state == MAIL_HOLD) {
        gs_poll_signal(pmailbox->semaphore);
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
}

/*
 * function name    : gs_senders_flow_handle_close_request
 * description    : handle close request when senders_flow thread received MAIL_CLOSED.
 * arguments        : _in_ fcmsgr: the message that senders_flow thread received.
 * return value    : void
 * if the receiver quit, we will get a MAIL_CLOSED message,
 * we should close the pmailbox and tell the executor thread to continue
 */
void gs_senders_flow_handle_close_request(FCMSG_T* fcmsgr)
{
    int streamid = fcmsgr->streamid;
    int node_idx = fcmsgr->node_idx;
    struct p_mailbox* pmailbox = NULL;

    // 1: get the pmailbox[idx][fcmsgr.streamid] and check it is in correct state
    //
    pmailbox = &P_MAILBOX(node_idx, streamid);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
    COMM_DEBUG_LOG("(s|flow ctrl)\tStream[%d] is closed "
                   "by remote node[%d]:%s, query[%lu].",
        streamid,
        node_idx,
        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
        fcmsgr->query_id);

    // 2: if the pmailbox is not matched, we will break here and report error
    //
    if (gs_check_mailbox(pmailbox->local_version, fcmsgr->version) == false) {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
        return;
    }

    // 3: if gs_connect already return correctly,  delete the record in the hash table,
    //     or, the entry will be deleted in gs_connect
    //
    gs_s_close_logic_connection(pmailbox, ECOMMTCPREMOETECLOSE, NULL);
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
}

/*
 * function name    : gs_senders_flow_handle_assert_fail_request
 * description    : handle assert fail when senders_flow thread received CTRL_ASSERT_FAIL.
 * arguments        : _in_ fcmsgr: the message that senders_flow thread received.
 * return value    : void
 * if the receiver assert fail and core, we will get a CTRL_ASSERT_FAIL message,
 * we should make the producer core as well
 */
void gs_senders_flow_handle_assert_fail_request(FCMSG_T* fcmsgr)
{
    int sidx = fcmsgr->streamid;
    int nidx = fcmsgr->node_idx;

    struct p_mailbox* pmailbox = NULL;

    pmailbox = &(P_MAILBOX(nidx, sidx));

    LIBCOMM_ELOG(WARNING,
        "(s|flow ctrl)\tNode[%d] stream[%d] assert fail, node name[%s] with state[%d] has bufCAP[%lu].",
        nidx,
        sidx,
        g_instance.comm_cxt.g_s_node_sock[nidx].remote_nodename,
        pmailbox->state,
        pmailbox->bufCAP);
    MAILBOX_ELOG(pmailbox, WARNING, "(s|flow ctrl)\tMailbox Info which assert fail.");

    Assert(0 != 0);
}

extern ThreadId getThreadIdForLibcomm(int logictid);
void gs_senders_flow_handle_stop_query_request(FCMSG_T* fcmsgr)
{
    int streamid = fcmsgr->streamid;
    int node_idx = fcmsgr->node_idx;
    struct p_mailbox* pmailbox = NULL;

    // 1: get the pmailbox[idx][fcmsgr.streamid] and check it is in correct state
    //
    pmailbox = &P_MAILBOX(node_idx, streamid);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
    COMM_DEBUG_LOG("(s|flow ctrl)\tQuery[%lu] is stop "
                   "by remote node[%d]:%s with stream[%d].",
        fcmsgr->query_id,
        node_idx,
        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
        streamid);

    // 2: if the pmailbox is not matched, we will break here and report error
    //
    if (gs_check_mailbox(pmailbox->local_version, fcmsgr->version) == false) {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
        return;
    }

    // 3: send SIGUSR1 to backend, then set the stop flag to stop query
    //
    int logictid = (int)ntohl(fcmsgr->extra_info);
    ThreadId backendTID = getThreadIdForLibcomm(logictid);
    if (0 != backendTID) {
        StreamNodeGroup::stopAllThreadInNodeGroup(backendTID, fcmsgr->query_id);
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
}

