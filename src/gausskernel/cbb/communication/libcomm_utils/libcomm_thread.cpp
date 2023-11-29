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
 * libcomm_thread.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/libcomm_utils/libcomm_thread.cpp
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
#include <signal.h>

#include "../libcomm_core/mc_tcp.h"
#include "../libcomm_core/mc_poller.h"
#include "libcomm_thread.h"
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
#include "libcomm/libcomm.h"
#include "libcomm_common.h"

#ifdef ENABLE_UT
#define static
#endif

#define THREAD_FREE_TIME_10S 10000000
#define THREAD_INTSERVAL_60S 60000000
#define THREAD_WORK_PERCENT 0.8

static int g_print_interval_time = 60000;  // 60s
static knl_session_context g_comm_session;
extern pthread_mutex_t g_htab_fd_id_node_idx_lock;
extern HTAB* g_htab_fd_id_node_idx;
extern LibcommAdaptLayer g_libcomm_adapt;

static void gs_pmailbox_init();
static void gs_cmailbox_init();

extern int gs_update_fd_to_htab_socket_version(struct sock_id* fd_id);
extern void gs_s_close_bad_ctrl_tcp_sock(struct sock_id* fd_id, int close_reason, bool clean_epoll, int node_idx);
extern void gs_r_close_bad_ctrl_tcp_sock(struct sock_id* fd_id, int close_reason);
extern void gs_s_close_bad_data_socket(struct sock_id* fd_id, int close_reason, int node_idx);
extern void gs_r_close_bad_data_socket(int node_idx, sock_id fd_id, int close_reason, bool is_lock);

extern void gs_delay_survey();

void mc_thread_block_signal()
{
    sigset_t sigs;
    (void)sigemptyset(&sigs);
    (void)sigaddset(&sigs, SIGHUP);
    (void)sigaddset(&sigs, SIGINT);
    (void)sigaddset(&sigs, SIGTERM);
    (void)sigaddset(&sigs, SIGUSR1);
    (void)sigaddset(&sigs, SIGUSR2);
    (void)pthread_sigmask(SIG_BLOCK, &sigs, NULL);
}

static void print_stream_sock_info()
{
    int i;
    tcp_info info;
    socklen_t tcp_info_len = sizeof(info);
    errno_t ss_rc = memset_s(&info, tcp_info_len, 0x0, tcp_info_len);
    securec_check(ss_rc, "\0", "\0");

    if (g_instance.comm_cxt.g_senders->sender_conn != NULL) {
        for (i = 0; i < MAX_CN_DN_NODE_NUM; i++) {
            int sock = g_instance.comm_cxt.g_senders->sender_conn[i].socket;
            if (sock > 0) {
                int ret = getsockopt(sock, SOL_TCP, TCP_INFO, &info, &tcp_info_len);
                if (ret == 0) {
                    print_socket_info(sock, &info, true);
                }
            }
        }
    }

    if (g_instance.comm_cxt.g_receivers->receiver_conn != NULL) {
        for (i = 0; i < MAX_CN_DN_NODE_NUM; i++) {
            int sock = g_instance.comm_cxt.g_receivers->receiver_conn[i].socket;
            if (sock > 0) {
                int ret = getsockopt(sock, SOL_TCP, TCP_INFO, &info, &tcp_info_len);
                if (ret == 0) {
                    print_socket_info(sock, &info, false);
                }
            }
        }
    }
}

void gs_set_local_host(const char* host)
{
    uint32 cpylen;
    errno_t ss_rc;
    const char* real_host = NULL;

    if (IS_LOCAL_HOST(host)) {
        real_host = "127.0.0.1";
    } else {
        real_host = host;
    }

    cpylen = comm_get_cpylen(real_host, HOST_ADDRSTRLEN);
    ss_rc = memset_s(g_instance.comm_cxt.localinfo_cxt.g_local_host, HOST_ADDRSTRLEN, 0x0, HOST_ADDRSTRLEN);
    securec_check(ss_rc, "\0", "\0");

    ss_rc = strncpy_s(g_instance.comm_cxt.localinfo_cxt.g_local_host, HOST_ADDRSTRLEN, real_host, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");

    g_instance.comm_cxt.localinfo_cxt.g_local_host[cpylen] = '\0';
}  // gs_set_local_host

void gs_update_recv_ready_time()
{
    g_instance.comm_cxt.localinfo_cxt.g_r_first_recv_time = time(NULL);
}

static void gs_set_quota(unsigned long quota, int quotanotify_ratio)
{
    if (quota == 0) { // do not using quota mechanism
        g_instance.comm_cxt.quota_cxt.g_having_quota = false;
        g_instance.comm_cxt.quota_cxt.g_quota = DEFULTMSGLEN;
    } else {
        g_instance.comm_cxt.quota_cxt.g_having_quota = true;
        g_instance.comm_cxt.quota_cxt.g_quota = quota * 1024;  // Byte

        /* Minimum value is DEFULTMSGLEN */
        if (g_instance.comm_cxt.quota_cxt.g_quota < DEFULTMSGLEN) {
            g_instance.comm_cxt.quota_cxt.g_quota = DEFULTMSGLEN;
        }
    }
    g_instance.comm_cxt.quota_cxt.g_quotanofify_ratio = quotanotify_ratio;
}  // gs_set_quota

void init_comm_buffer_size()
{
    LIBCOMM_BUFFER_SIZE = IOV_DATA_SIZE = DEFULTMSGLEN = (unsigned long)g_instance.comm_cxt.commutil_cxt.g_comm_sender_buffer_size * 1024;
    IOV_ITEM_SIZE = IOV_DATA_SIZE + sizeof(struct iovec) + sizeof(mc_lqueue_element);

    // used for g_quotanofify_ratio, quota ratio for dynamic quota changing
    gs_set_quota(g_instance.attr.attr_network.comm_quota_size, 5);
}

static void gs_receivers_struct_set(int ctrl_port, int data_port)
{
    g_instance.comm_cxt.g_receivers->server_ctrl_tcp_port = ctrl_port;
    g_instance.comm_cxt.g_receivers->server_listen_conn.port = data_port;
    g_instance.comm_cxt.g_receivers->server_listen_conn.ss_len = 0;
    g_instance.comm_cxt.g_receivers->server_listen_conn.socket = -1;
    g_instance.comm_cxt.g_receivers->server_listen_conn.socket_id = -1;
    g_instance.comm_cxt.g_receivers->server_listen_conn.assoc_id = 0;

    for (int i = 0; i < MAX_CN_DN_NODE_NUM; i++) {
        LIBCOMM_PTHREAD_RWLOCK_INIT(&g_instance.comm_cxt.g_receivers->receiver_conn[i].rwlock, NULL);
        g_instance.comm_cxt.g_receivers->receiver_conn[i].port = 0;
        g_instance.comm_cxt.g_receivers->receiver_conn[i].ss_len = 0;
        g_instance.comm_cxt.g_receivers->receiver_conn[i].socket = -1;
        g_instance.comm_cxt.g_receivers->receiver_conn[i].socket_id = -1;
        g_instance.comm_cxt.g_receivers->receiver_conn[i].assoc_id = 0;
        g_instance.comm_cxt.g_receivers->receiver_conn[i].comm_bytes = 0;
        g_instance.comm_cxt.g_receivers->receiver_conn[i].comm_count = 0;

        g_instance.comm_cxt.g_r_node_sock[i].init();
    }

    return;
}

/*
 * function name    : gs_init_receivers
 * description      : init g_receivers
 * arguments        :
 *                __in ctrl_tcp_port:
 *                __in base_sctp_port:
 */
void gs_receivers_struct_init(int ctrl_port, int data_port)
{
    LIBCOMM_MALLOC(g_instance.comm_cxt.g_r_node_sock, (MAX_CN_DN_NODE_NUM * sizeof(struct node_sock)), node_sock);
    if (g_instance.comm_cxt.g_r_node_sock == NULL) {
        ereport(FATAL, (errmsg("(r|receivers init)\tFailed to malloc g_r_node_sock[%d].", MAX_CN_DN_NODE_NUM)));
    }

    LIBCOMM_MALLOC(g_instance.comm_cxt.g_receivers->receiver_conn,
        (MAX_CN_DN_NODE_NUM * sizeof(struct node_connection)),
        node_connection);
    if (g_instance.comm_cxt.g_receivers->receiver_conn == NULL) {
        ereport(FATAL, (errmsg("(r|receivers init)\tFailed to malloc g_receivers[%d].", MAX_CN_DN_NODE_NUM)));
    }

    /* initialize g_receivers */
    gs_receivers_struct_set(ctrl_port, data_port);

    /* initialize cmailbox */
    gs_cmailbox_init();

    return;
}

static void gs_senders_struct_init()
{
    LIBCOMM_MALLOC(g_instance.comm_cxt.g_s_node_sock, (MAX_CN_DN_NODE_NUM * sizeof(struct node_sock)), node_sock);
    if (g_instance.comm_cxt.g_s_node_sock == NULL) {
        ereport(FATAL, (errmsg("(s|sender init)\tFailed to malloc g_s_node_sock[%d].", MAX_CN_DN_NODE_NUM)));
    }
    LIBCOMM_MALLOC(
        g_instance.comm_cxt.g_delay_info, (MAX_CN_DN_NODE_NUM * sizeof(struct libcomm_delay_info)), libcomm_delay_info);
    if (g_instance.comm_cxt.g_delay_info == NULL) {
        ereport(FATAL, (errmsg("(s|sender init)\tFailed to malloc g_delay_info[%d].", MAX_CN_DN_NODE_NUM)));
    }
    LIBCOMM_MALLOC(g_instance.comm_cxt.g_senders->sender_conn,
        (MAX_CN_DN_NODE_NUM * sizeof(struct node_connection)),
        node_connection);
    if (g_instance.comm_cxt.g_senders->sender_conn == NULL) {
        ereport(FATAL, (errmsg("(s|sender init)\tFailed to malloc g_senders[%d].", MAX_CN_DN_NODE_NUM)));
    }

    // init g_s_node_sock and g_senders
    gs_senders_struct_set();

    // init pmailbox
    gs_pmailbox_init();

    return;
}  // gs_init_senders

/*
 * @Description : get kerberos keyfile path from GUC parameter pg_krb_server_keyfile.
 * @out : gs_krb_keyfile: save kerberos keyfile path.
 */
void gs_set_kerberos_keyfile()
{
    int path_len = 0;

    if (u_sess->attr.attr_security.pg_krb_server_keyfile == NULL) {
        return;
    }

    path_len = strlen(u_sess->attr.attr_security.pg_krb_server_keyfile);
    if (path_len <= 0 || path_len >= (INT_MAX - 1)) {
        return;
    }

    LIBCOMM_MALLOC(g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile, (unsigned)(path_len + 1), char);
    if (g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile != NULL) {
        errno_t ss_rc = strcpy_s(g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile,
            path_len + 1,
            u_sess->attr.attr_security.pg_krb_server_keyfile);
        securec_check(ss_rc, "\0", "\0");
    }
}

void gs_set_comm_session()
{
    g_comm_session.status = KNL_SESS_FAKE;
    g_comm_session.debug_query_id = 0;
    g_comm_session.session_id = 0;

    return;
}

/*
 * function name    : gs_check_stream_key
 * description    : Check mailbox version, nonequal meas mailbox has been changed by other thread
 * notice        : we must get the mailbox lock before!
 * arguments        :
 *                   _in_ version1: the version1 in mailbox.
 *                   _in_ version2: the version2 application used.
 * return value    :
 *                   true: version1 is equal to version2.
 *                   false: version1 is not equal to version2.
 */
bool gs_check_mailbox(uint16 version1, uint16 version2)
{
    return (version1 == version2);
}  // gs_check_mailbox

/*
 * function name: gs_clean_cmailbox
 * description:        clean cmailbox for pooler reuse
 * arguments:         gs_sock: logic conn addr.
 */
void gs_clean_cmailbox(const gsocket gs_sock)
{
    if (gs_sock.type == GSOCK_INVALID) {
        return;
    }

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    struct c_mailbox* cmailbox = &C_MAILBOX(gs_sock.idx, gs_sock.sid);

    bool TempImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = false;

    LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);
    if (cmailbox->buff_q->is_empty != 1) {
        // clear the data buffer list
        cmailbox->buff_q = mc_lqueue_clear(cmailbox->buff_q);
        cmailbox->query_id = 0;
    }
    if (cmailbox->session_info_ptr != NULL) {
        ereport(DEBUG5, (errmsg("Trace: (r|cmailbox clean). detail idx[%d], streamid[%d]",
            gs_sock.idx, gs_sock.sid)));
    }
    cmailbox->session_info_ptr = NULL;
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
    t_thrd.int_cxt.ImmediateInterruptOK = TempImmediateInterruptOK;

    return;
}  // gs_clean_cmailbox

void gs_r_reset_cmailbox(struct c_mailbox* cmailbox, int close_reason)
{
    if (cmailbox == NULL || cmailbox->state == MAIL_CLOSED) {
        return;
    }

    int node_idx = cmailbox->idx;
    int streamid = cmailbox->streamid;
    errno_t ss_rc;

    printf_cmailbox_statistic(cmailbox, g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename);

    cmailbox->local_version++;
    if (cmailbox->local_version >= MAX_MAILBOX_VERSION) {
        cmailbox->local_version = 0;
    }

    cmailbox->state = MAIL_CLOSED;
    cmailbox->close_reason = close_reason;
    cmailbox->ctrl_tcp_sock = -1;
    cmailbox->is_producer = 0;
    cmailbox->bufCAP = 0;
    // clear the data buffer list
    cmailbox->buff_q = mc_lqueue_clear(cmailbox->buff_q);
    cmailbox->query_id = 0;
    cmailbox->local_thread_id = 0;
    cmailbox->peer_thread_id = 0;
    cmailbox->remote_version = 0;
    cmailbox->semaphore = NULL;
    if (cmailbox->statistic != NULL) {
        ss_rc = memset_s(cmailbox->statistic, sizeof(cmailbox_statistic), 0, sizeof(cmailbox_statistic));
        securec_check(ss_rc, "\0", "\0");
        if (!g_instance.comm_cxt.commutil_cxt.g_stat_mode) {
            LIBCOMM_FREE(cmailbox->statistic, sizeof(struct cmailbox_statistic));
        }
    }
    if (cmailbox->session_info_ptr != NULL) {
        ereport(DEBUG5, (errmsg("Trace: (r|cmailbox reset). detail idx[%d], streamid[%d]",
            node_idx, streamid)));
    }
    cmailbox->session_info_ptr = NULL;
#ifdef SCTP_BUFFER_DEBUG
    cmailbox->buff_q_tmp = mc_lqueue_clear(cmailbox->buff_q_tmp);
#endif
}  // gs_r_reset_cmailbox

void gs_s_reset_pmailbox(struct p_mailbox* pmailbox, int close_reason)
{
    if (pmailbox == NULL || pmailbox->state == MAIL_CLOSED) {
        return;
    }

    int node_idx = pmailbox->idx;
    int streamid = pmailbox->streamid;
    errno_t ss_rc;

    printf_pmailbox_statistic(pmailbox, g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename);

    pmailbox->local_version++;
    if (pmailbox->local_version >= MAX_MAILBOX_VERSION) {
        pmailbox->local_version = 0;
    }

    pmailbox->state = MAIL_CLOSED;
    pmailbox->close_reason = close_reason;
    pmailbox->ctrl_tcp_sock = -1;
    pmailbox->is_producer = 1;
    pmailbox->bufCAP = 0;
    pmailbox->query_id = 0;
    pmailbox->local_thread_id = 0;
    pmailbox->peer_thread_id = 0;
    pmailbox->remote_version = 0;
    pmailbox->semaphore = NULL;
    if (pmailbox->statistic != NULL) {
        ss_rc = memset_s(pmailbox->statistic, sizeof(pmailbox_statistic), 0, sizeof(pmailbox_statistic));
        securec_check(ss_rc, "\0", "\0");
        if (!g_instance.comm_cxt.commutil_cxt.g_stat_mode) {
            LIBCOMM_FREE(pmailbox->statistic, sizeof(struct pmailbox_statistic));
        }
    }
    // push the stream index into the avaliable stream index queue
    // stream 0 is reserved
    if (streamid > 0) {
        (void)g_instance.comm_cxt.g_usable_streamid[node_idx].push(
            g_instance.comm_cxt.g_usable_streamid + node_idx, streamid);
    }
}  // gs_s_reset_pmailbox

// initialize cmailbox for Consumer working threads
//
static void gs_cmailbox_init()
{
    LIBCOMM_MALLOC(g_instance.comm_cxt.g_c_mailbox, MAX_CN_DN_NODE_NUM * sizeof(struct c_mailbox*), c_mailbox*);
    if (g_instance.comm_cxt.g_c_mailbox == NULL) {
        ereport(FATAL, (errmsg("(r|cmailbox init)\tFailed to init cmailbox.")));
    }

    return;
}  // gs_r_cmailbox_init

void gs_mailbox_destory(int idx)
{
    p_mailbox* pmailbox = NULL;
    c_mailbox* cmailbox = NULL;
    int sid = -1;

    mc_queue_destroy(g_instance.comm_cxt.g_usable_streamid + idx);

    if (g_instance.comm_cxt.g_p_mailbox[idx] != NULL) {
        pmailbox = &P_MAILBOX(idx, sid);
        LIBCOMM_PTHREAD_MUTEX_DESTORY(&(pmailbox->sinfo_lock));
        LIBCOMM_FREE(g_instance.comm_cxt.g_p_mailbox[idx],
            g_instance.comm_cxt.counters_cxt.g_max_stream_num * sizeof(struct p_mailbox));
    }

    if (g_instance.comm_cxt.g_c_mailbox[idx] != NULL) {
        for (sid = 0; sid < g_instance.comm_cxt.counters_cxt.g_max_stream_num; sid++) {
            cmailbox = &C_MAILBOX(idx, sid);
            if (cmailbox->session_info_ptr != NULL) {
                if (ENABLE_THREAD_POOL_DN_LOGICCONN) {
                    NotifyListener(cmailbox, true, __FUNCTION__);
                }
                LIBCOMM_ELOG(DEBUG5, "Trace: (cmailbox destroy). detail idx[%d], streamid[%d]", idx, sid);
            }
            cmailbox->session_info_ptr = NULL;
            mc_lqueue_clear(cmailbox->buff_q);
            LIBCOMM_FREE(cmailbox->buff_q, sizeof(struct mc_lqueue));
            LIBCOMM_PTHREAD_MUTEX_DESTORY(&(cmailbox->sinfo_lock));
        }
        LIBCOMM_FREE(g_instance.comm_cxt.g_c_mailbox[idx],
            g_instance.comm_cxt.counters_cxt.g_max_stream_num * sizeof(struct c_mailbox));
    }
}

// initialize cmailbox[idx] and pmailbox[idx]
//
bool gs_mailbox_build(int idx)
{
    int sid = -1;

    /* initialize the queue for this connection */
    if (mc_queue_init(g_instance.comm_cxt.g_usable_streamid + idx, g_instance.comm_cxt.counters_cxt.g_max_stream_num) ==
        -1) {
        LIBCOMM_ELOG(WARNING, "(mailbox build)\tFailed to initialize g_usable_streamid[%d].", idx);
        goto cleanup_mailbox;
    }

    /* malloc pmailbox array */
    LIBCOMM_MALLOC((*(g_instance.comm_cxt.g_p_mailbox + idx)),
        g_instance.comm_cxt.counters_cxt.g_max_stream_num * sizeof(struct p_mailbox),
        struct p_mailbox);
    if ((*(g_instance.comm_cxt.g_p_mailbox + idx)) == NULL) {
        LIBCOMM_ELOG(WARNING, "(mailbox build)\tFailed to malloc pmailbox[%d].", idx);
        goto cleanup_mailbox;
    }

    /* malloc cmailbox array */
    LIBCOMM_MALLOC((*(g_instance.comm_cxt.g_c_mailbox + idx)),
        g_instance.comm_cxt.counters_cxt.g_max_stream_num * sizeof(struct c_mailbox),
        struct c_mailbox);
    if ((*(g_instance.comm_cxt.g_c_mailbox + idx)) == NULL) {
        LIBCOMM_ELOG(WARNING, "(mailbox build)\tFailed to malloc cmailbox[%d].", idx);
        goto cleanup_mailbox;
    }

    /* do initialization */
    for (sid = 0; sid < g_instance.comm_cxt.counters_cxt.g_max_stream_num; sid++) {
        /* initialize pmailbox */
        P_MAILBOX(idx, sid).idx = idx;
        P_MAILBOX(idx, sid).streamid = sid;
        /* reset pmailbox and push the stream id to g_usable_streamid */
        gs_s_reset_pmailbox(&P_MAILBOX(idx, sid), 0);
        LIBCOMM_PTHREAD_MUTEX_INIT(&(P_MAILBOX(idx, sid).sinfo_lock), 0);

        /* initialize cmailbox */
        C_MAILBOX(idx, sid).idx = idx;
        C_MAILBOX(idx, sid).streamid = sid;
        gs_r_reset_cmailbox(&C_MAILBOX(idx, sid), 0);
        LIBCOMM_PTHREAD_MUTEX_INIT(&(C_MAILBOX(idx, sid).sinfo_lock), 0);

        /* initialize buffer queue, it is just a pointer, not space */
        C_MAILBOX(idx, sid).buff_q = mc_lqueue_init(g_instance.comm_cxt.quota_cxt.g_quota);
        if (C_MAILBOX(idx, sid).buff_q == NULL) {
            LIBCOMM_ELOG(WARNING, "(mailbox build)\tFailed to malloc buff_q from cmailbox[%d].", idx);
            goto cleanup_mailbox;
        }
    }
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_DYNAMIC_CAPACITY_FAILED)) {
        errno = ECOMMTCPMEMALLOC;
        LIBCOMM_ELOG(WARNING, "(mailbox build)\t[FAULT INJECTION]Failed to build p&cmailbox[%d].", idx);
        goto cleanup_mailbox;
    }
#endif

    COMM_DEBUG_LOG("(mailbox build)\tSuccess to build p&cmailbox[%d].", idx);
    return true;

cleanup_mailbox:
    gs_mailbox_destory(idx);
    return false;
}

// initialize pmailbox for Producer working threads
//
static void gs_pmailbox_init()
{
    LIBCOMM_MALLOC(g_instance.comm_cxt.g_p_mailbox, MAX_CN_DN_NODE_NUM * sizeof(struct p_mailbox*), p_mailbox*);
    if (g_instance.comm_cxt.g_p_mailbox == NULL) {
        ereport(FATAL, (errmsg("(s|pmailbox init)\tFailed to init pmailbox.")));
    }

    // malloc memory for stream id queues
    LIBCOMM_MALLOC(g_instance.comm_cxt.g_usable_streamid, MAX_CN_DN_NODE_NUM * sizeof(struct mc_queue), mc_queue);
    if (g_instance.comm_cxt.g_usable_streamid == NULL) {
        ereport(FATAL, (errmsg("(s|pmailbox init)\tFailed to init g_usable_streamid.")));
    }

    return;
}  // gs_s_pmailbox_init

static void gs_flow_thread_time(char *thread, uint64 *start_time, uint64 end_time,
                                uint64 *last_check_time, uint64 *work_all_time, int flag)
{
    uint64 work_time = 0;
    uint64 all_time = 0;
    uint64 curr_time = 0;

    if (thread == NULL || start_time == NULL || last_check_time  == NULL  || work_all_time  == NULL) {
        LIBCOMM_ELOG(WARNING, "Invalid args of gs_flow_thread_time.");
        return;
    }

    curr_time = mc_timers_us();
    all_time = ABS_SUB(curr_time, *start_time);
    *start_time = curr_time;
    work_time = ABS_SUB(*start_time, end_time);

    if (g_instance.comm_cxt.localinfo_cxt.g_libcomm_used_rate != NULL && all_time > 0) {
        g_instance.comm_cxt.localinfo_cxt.g_libcomm_used_rate[flag] = work_time * 100 / all_time;
    }

    if (work_time > THREAD_FREE_TIME_10S) {
        LIBCOMM_ELOG(WARNING, "%s thread block time exceeds 10s:%lus.", thread, work_time / THREAD_FREE_TIME_10S);
    }

    if (ABS_SUB(*start_time, *last_check_time) >= THREAD_INTSERVAL_60S) {
        if (*work_all_time * 1.0 / THREAD_INTSERVAL_60S > THREAD_WORK_PERCENT) {
            LIBCOMM_ELOG(WARNING, "%s thread account for more than 80%% of working time.", thread);
        }
        *last_check_time = *start_time;
        *work_all_time = 0;
    }
    *work_all_time += work_time;
    return;
}

/*
 * function name    : gs_reload_hba
 * description      : send signal to postmaster thread to reload hba
 * arguments        : fd: recevive/flower listen socket
 *                    ctrl_client: ctrl client's sockaddress.
 * return value     : retry_count: 0: client is internal IP; 1: client is no internal IP, signal pm to refresh pg_hba
 */
static int gs_reload_hba(int fd, const sockaddr ctrl_client)
{
    if (is_cluster_internal_IP(ctrl_client)) {
        return 0;
    }

    (void)gs_signal_send(PostmasterPid, SIGHUP);
    errno = ECOMMTCPNOTINTERNALIP;
    LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tNot cluster internal IP, listen socket[%d]:%s.", fd, mc_strerror(errno));

    return 1;
}

// process requests, there are just cancel requtest and close poll request now
//
static void gs_check_requested()
{
    // when executor send a cancel query request,
    // we should notify all working threads(producer & consumer) to exit from wating in gs_wait_poll()
    //
    if (g_instance.comm_cxt.reqcheck_cxt.g_cancel_requested) { // cancel query request
        LIBCOMM_ELOG(WARNING,
            "(r|chk requested)\tCancel is received for thread:%ld.",
            (long)g_instance.comm_cxt.reqcheck_cxt.g_cancel_requested);

        // signal all the Consumers or they may be waiting for data,
        // the threads will check if it should quit
        gs_broadcast_poll();

        g_instance.comm_cxt.reqcheck_cxt.g_cancel_requested = 0;
        DEBUG_QUERY_ID = 0;
    }
}  // gs_check_requested

/*
 * function name    : gs_online_change_capacity
 * description      : For database expansion or shrinkage.
 * notice           : !!Curently, we only support datanode expansion.
 * arguments        :
 *                __in expected_node_num: Nodes number(CN+DN) that user wants to change to.
 */
void gs_online_change_capacity()
{
    int cur_node_idx = -1;
    int cur_num = g_instance.comm_cxt.counters_cxt.g_cur_node_num;
    int expect_num = g_instance.comm_cxt.counters_cxt.g_expect_node_num;

    if (expect_num == cur_num) {
        return;
    }

    if (expect_num < cur_num) {
        LIBCOMM_ELOG(WARNING, "(auxiliary)\tNot support change %d datanodes reduce to %d.", cur_num, expect_num);

        atomic_set(&g_instance.comm_cxt.counters_cxt.g_expect_node_num, cur_num);
        return;
    }

    for (cur_node_idx = cur_num; cur_node_idx < expect_num; cur_node_idx++) {
        if (false == gs_mailbox_build(cur_node_idx)) {
            goto cleanup_capacity;
        }
    }

    LIBCOMM_ELOG(NOTICE, "(auxiliary)\tChange %d datanodes capacity to %d successfully.", cur_num, expect_num);
    atomic_set(&g_instance.comm_cxt.counters_cxt.g_cur_node_num, expect_num);
    return;

cleanup_capacity:
    /* if failed, we need to release the memory, from cur_node_idx-1 to cur_num */
    for (cur_node_idx -= 1; cur_node_idx >= cur_num; cur_node_idx--) {
        gs_mailbox_destory(cur_node_idx);
    }

    LIBCOMM_ELOG(NOTICE, "(auxiliary)\tFailed to change %d datanodes capacity to %d.", cur_num, expect_num);
    return;
}

/*
 * function name    : gs_get_libcomm_reply_socket
 * description      : get reply socket from g_s_node_sock.
 * arguments        : _in_ recv_idx: the node index.
 * return value     :
 *                   -1: failed.
 *                   >=0: reply socket.
 */
static void gs_get_libcomm_reply_socket(int recv_idx, struct sock_id *fd_id)
{
    fd_id->fd = g_instance.comm_cxt.g_r_node_sock[recv_idx].libcomm_reply_sock;
    fd_id->id = g_instance.comm_cxt.g_r_node_sock[recv_idx].libcomm_reply_sock_id;

    if (fd_id->fd >= 0) {
        return;
    }

    for (int send_idx = 0; send_idx < g_instance.comm_cxt.counters_cxt.g_cur_node_num; send_idx++) {
        if (strcmp(g_instance.comm_cxt.g_r_node_sock[recv_idx].remote_nodename,
                   g_instance.comm_cxt.g_s_node_sock[send_idx].remote_nodename) == 0) {
            fd_id->fd = g_instance.comm_cxt.g_senders->sender_conn[send_idx].socket;
            fd_id->id = g_instance.comm_cxt.g_senders->sender_conn[send_idx].socket_id;

            // save data reply socket in g_r_node_sock
            g_instance.comm_cxt.g_r_node_sock[recv_idx].lock();
            g_instance.comm_cxt.g_r_node_sock[recv_idx].libcomm_reply_sock = fd_id->fd;
            g_instance.comm_cxt.g_r_node_sock[recv_idx].libcomm_reply_sock_id = fd_id->id;
            g_instance.comm_cxt.g_r_node_sock[recv_idx].unlock();
            break;
        }
    }

    return;
}

/*
 * function name    : gs_delay_analysis
 * description      : analysis libcomm delay message from mailbox[node_idx][0].
 */
void gs_delay_analysis()
{
    struct c_mailbox* cmailbox = NULL;
    int node_idx = 0;
    struct iovec* iov = NULL;
    struct mc_lqueue_item* q_item = NULL;

    struct libcomm_delay_package* msg = NULL;

    while (node_idx < g_instance.comm_cxt.counters_cxt.g_cur_node_num) {
        cmailbox = &C_MAILBOX(node_idx, 0);
        LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);

        // receive next consumer mailbox
        if (cmailbox->buff_q->is_empty == 1) {
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
            node_idx++;
            continue;
        }

        // get libcomm delay message from cmailbox
        q_item = mc_lqueue_remove(cmailbox->buff_q, q_item);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);

        // receive next message
        if (q_item == NULL) {
            continue;
        }

        iov = q_item->element.data;
        // receive next message
        if (iov == NULL || iov->iov_len == 0) {
            libcomm_free_iov_item(&q_item, IOV_DATA_SIZE);
            continue;
        }
        Assert(iov->iov_len == sizeof(struct libcomm_delay_package));

        msg = (struct libcomm_delay_package*)iov->iov_base;
        uint32 delay = 0;
        int delay_array_idx = -1;
        struct sock_id fd_id = {-1, -1};
        switch (msg->type) {
            // set reply_time and reply libcomm delay message
            case LIBCOMM_PKG_TYPE_DELAY_REQUEST:
                // we get reply socket from g_s_node_sock
                gs_get_libcomm_reply_socket(node_idx, &fd_id);
                if (fd_id.fd < 0) {
                    break;
                }

                msg->type = LIBCOMM_PKG_TYPE_DELAY_REPLY;
                msg->reply_time = (uint32)mc_timers_us();

                LibcommSendInfo send_info;
                send_info.socket = fd_id.fd;
                send_info.socket_id = fd_id.id;
                send_info.node_idx = node_idx;
                send_info.streamid = 0;
                send_info.version = 0;
                send_info.msg = (char*)msg;
                send_info.msg_len = sizeof(struct libcomm_delay_package);

                (void)g_libcomm_adapt.block_send(&send_info);
                break;

            case LIBCOMM_PKG_TYPE_DELAY_REPLY:
                delay = (msg->finish_time - msg->start_time) - (msg->reply_time - msg->recv_time);

                delay_array_idx = g_instance.comm_cxt.g_delay_info[node_idx].current_array_idx;
                g_instance.comm_cxt.g_delay_info[node_idx].delay[delay_array_idx] = delay;

                COMM_DEBUG_LOG("[DELAY_INFO]remote_name=%s, delay[%d]=%uus",
                    REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx),
                    delay_array_idx,
                    delay);

                delay_array_idx++;
                if (delay_array_idx >= MAX_DELAY_ARRAY_INDEX) {
                    delay_array_idx = 0;
                }

                g_instance.comm_cxt.g_delay_info[node_idx].current_array_idx = delay_array_idx;
                break;

            default:
                break;
        }

        libcomm_free_iov_item(&q_item, IOV_DATA_SIZE);
    }
}

// set basic infomation for communication layer
//
int gs_set_basic_info(const char* local_host,  // ip of local host
    const char* local_node_name,               // local node name of the datanode, like PGXCNodeName
    int cur_node_num,                          // number of node
    char* sock_path)                           // unix domain path
{
    LIBCOMM_ELOG(LOG,
        "Initialize Communication Layer : node[%d] stream[%d], "
        "receiver[%d], quota[%dKB], total memory[%dKB], "
        "control port[%d], data port[%d], local_host[%s], local_node_name[%s], "
        "sock_path[%s], cn_dn_conn_type[%d].",
        cur_node_num,
        g_instance.attr.attr_network.comm_max_stream,
        g_instance.attr.attr_network.comm_max_receiver,
        g_instance.attr.attr_network.comm_quota_size,
        g_instance.attr.attr_network.comm_usable_memory,
        g_instance.attr.attr_network.comm_control_port,
        g_instance.attr.attr_network.comm_sctp_port,
        local_host,
        local_node_name,
        sock_path,
        g_instance.attr.attr_storage.comm_cn_dn_logic_conn);

    errno_t ss_rc;
    uint32 cpylen;

    init_comm_buffer_size();

    g_instance.comm_cxt.g_receivers = (struct local_receivers*)palloc0(sizeof(struct local_receivers));
    g_instance.comm_cxt.g_senders = (struct local_senders*)palloc0(sizeof(struct local_senders));
    g_instance.comm_cxt.quota_cxt.g_quota_changing = (struct binary_semaphore*)palloc0(sizeof(struct binary_semaphore));
    g_instance.comm_cxt.localinfo_cxt.g_local_host = (char*)palloc0(HOST_ADDRSTRLEN * sizeof(char));
    g_instance.comm_cxt.localinfo_cxt.g_self_nodename = (char*)palloc0(NAMEDATALEN * sizeof(char));
    g_instance.comm_cxt.g_unix_path = (char*)palloc0(MAXPGPATH * sizeof(char));
    g_instance.comm_cxt.pollers_cxt.g_libcomm_receiver_poller_list =
        (mc_poller_hndl_list*)palloc0(MAX_RECV_NUM * sizeof(mc_poller_hndl_list));
    g_instance.comm_cxt.pollers_cxt.g_r_libcomm_poller_list_lock = (pthread_mutex_t*)palloc0(sizeof(pthread_mutex_t));
    g_instance.comm_cxt.pollers_cxt.g_r_poller_list = (mc_poller_hndl_list*)palloc0(sizeof(mc_poller_hndl_list));
    g_instance.comm_cxt.pollers_cxt.g_r_poller_list_lock = (pthread_mutex_t*)palloc0(sizeof(pthread_mutex_t));
    g_instance.comm_cxt.pollers_cxt.g_s_poller_list = (mc_poller_hndl_list*)palloc0(sizeof(mc_poller_hndl_list));
    g_instance.comm_cxt.pollers_cxt.g_s_poller_list_lock = (pthread_mutex_t*)palloc0(sizeof(pthread_mutex_t));

    gs_set_usable_memory((long)g_instance.attr.attr_network.comm_usable_memory);
    gs_set_memory_pool_size((long)g_instance.attr.attr_network.comm_memory_pool);
    while (gs_memory_pool_queue_initial_success(
               (uint32)(g_instance.comm_cxt.commutil_cxt.g_memory_pool_size / IOV_ITEM_SIZE)) != 0) {
        LIBCOMM_ELOG(LOG, "g_memory_pool_queue initialization failed.");
    }

    g_instance.comm_cxt.libcomm_log_timezone = log_timezone;
    g_instance.comm_cxt.counters_cxt.g_comm_send_timeout = u_sess->attr.attr_network.PoolerTimeout;
    g_instance.comm_cxt.reqcheck_cxt.g_shutdown_requested = false;
    // set the number of recv loop thread
    g_instance.comm_cxt.counters_cxt.g_recv_num = g_instance.attr.attr_network.comm_max_receiver;
    // set the number of stream number
    g_instance.comm_cxt.counters_cxt.g_max_stream_num = g_instance.attr.attr_network.comm_max_stream;

    // set expect and current node num
    g_instance.comm_cxt.counters_cxt.g_expect_node_num = cur_node_num;
    g_instance.comm_cxt.counters_cxt.g_cur_node_num = cur_node_num;

    init_libcomm_cpu_rate();

    // save the node name
    cpylen = comm_get_cpylen(local_node_name, NAMEDATALEN);
    ss_rc = memset_s(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN, local_node_name, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    g_instance.comm_cxt.localinfo_cxt.g_self_nodename[cpylen] = '\0';

    // set the global variable
    gs_set_local_host(local_host);

    cpylen = comm_get_cpylen(sock_path, strlen(sock_path) + 1);
    ss_rc = memset_s(g_instance.comm_cxt.g_unix_path, MAXPGPATH, 0x0, strlen(sock_path) + 1);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(g_instance.comm_cxt.g_unix_path, MAXPGPATH, sock_path, strlen(sock_path) + 1);
    securec_check(ss_rc, "\0", "\0");
    g_instance.comm_cxt.g_unix_path[cpylen] = '\0';

    mc_tcp_set_timeout_param(u_sess->attr.attr_network.PoolerConnectTimeout, u_sess->attr.attr_network.PoolerTimeout);

    // set keepalive parameter
    mc_tcp_set_keepalive_param(u_sess->attr.attr_common.tcp_keepalives_idle,
        u_sess->attr.attr_common.tcp_keepalives_interval,
        u_sess->attr.attr_common.tcp_keepalives_count);

    mc_tcp_set_user_timeout(u_sess->attr.attr_common.tcp_user_timeout);

    // set kerberos parameter
    gs_set_kerberos_keyfile();

    gs_init_adapt_layer();

#ifdef USE_SSL
    // initialize SSL parameter
    if (g_instance.attr.attr_network.comm_enable_SSL) {
        LIBCOMM_ELOG(LOG, "comm_enable_SSL is open, call comm_initialize_SSL");
        comm_initialize_SSL();
        if (g_instance.attr.attr_network.SSL_server_context != NULL) {
             LIBCOMM_ELOG(LOG, "comm_initialize_SSL success");
        } else {
             LIBCOMM_ELOG(LOG, "comm_initialize_SSL failed");
        }
    } else {
        LIBCOMM_ELOG(LOG, "comm_enable_SSL is close");
    }
#endif

    int ret = LibCommInitChannelConn(cur_node_num);
    if (ret < 0) {
        ereport(FATAL, (errmsg("Initialize libcomm control or data channel conn failed!")));
    }

    // receiver structure setting
    gs_receivers_struct_init(
        g_instance.attr.attr_network.comm_control_port, g_instance.attr.attr_network.comm_sctp_port);

    // sender structure setting
    gs_senders_struct_init();

    // init p-, c-mailbox and g_usable_streamid one by one, if failed, return FATAL.
    for (int i = 0; i < g_instance.comm_cxt.counters_cxt.g_expect_node_num; i++) {
        if (false == gs_mailbox_build(i)) {
            ereport(FATAL, (errmsg("Failed to build mailbox[%d].", i)));
        }
    }
    LIBCOMM_ELOG(LOG,
        "(mailbox build)\tSuccess to build p&cmailbox from [0] to [%d].",
        g_instance.comm_cxt.counters_cxt.g_expect_node_num - 1);

    gs_set_comm_session();

    g_instance.pid_cxt.CommSenderFlowPID = startCommSenderFlow();
    g_instance.pid_cxt.CommReceiverFlowPID = startCommReceiverFlow();
    g_instance.pid_cxt.CommAuxiliaryPID = startCommAuxiliary();
    g_instance.pid_cxt.CommReceiverPIDS =
        (ThreadId*)palloc0(g_instance.attr.attr_network.comm_max_receiver * sizeof(ThreadId));
    if (g_instance.pid_cxt.CommReceiverPIDS == NULL) {
        ereport(FATAL, (errmsg("communicator palloc CommReceiverPIDS mempry failed")));
    }
    startCommReceiverWorker(g_instance.pid_cxt.CommReceiverPIDS);

    return 0;
}  // gs_set_basic_info

int comm_sender_flow_init()
{
    int error = 0;

    /* initialize epoll list */
    error = g_instance.comm_cxt.pollers_cxt.g_s_poller_list->init();

    if (error < 0) {
        ereport(FATAL, (errmsg("(s|flow ctrl init)\tFailed to init poller list:%s.", strerror(errno))));
    }
    LIBCOMM_PTHREAD_MUTEX_INIT(g_instance.comm_cxt.pollers_cxt.g_s_poller_list_lock, 0);

    return error;
}

int comm_receiver_flow_init(int ctrl_tcp_port)
{
    socklen_t addr_len;
    int error = 0;

    /* tcp listen */
    g_instance.comm_cxt.localinfo_cxt.g_local_ctrl_tcp_sock =
        mc_tcp_listen(g_instance.comm_cxt.localinfo_cxt.g_local_host, ctrl_tcp_port, &addr_len);
    if (g_instance.comm_cxt.localinfo_cxt.g_local_ctrl_tcp_sock < 0) {
        ereport(FATAL, (errmsg("(r|flow ctrl init)\tFailed to do listen:%s.", strerror(errno))));
    }

    /* poll list initialization */
    error = g_instance.comm_cxt.pollers_cxt.g_r_poller_list->init();
    if (error < 0) {
        ereport(FATAL, (errmsg("(r|flow ctrl init)\tFailed to init poller list:%s.", strerror(errno))));
    }
    LIBCOMM_PTHREAD_MUTEX_INIT(g_instance.comm_cxt.pollers_cxt.g_r_poller_list_lock, 0);

    return error;
}

void commResolveMessage(struct FCMSG_T fcmsgr, sock_id_entry* entry_id, bool found, struct sock_id f_fd_id)
{
    int idx;
    /* resolve the message */
    /* src is from the network, maybe do not have '\0' */
    fcmsgr.nodename[NAMEDATALEN - 1] = '\0';
    DEBUG_QUERY_ID = fcmsgr.query_id;

    int retry_count = 10;
    do {
        /* node_idx is uint16, so if gs_get_node_idx() returns -1, node_idx=65535 */
        if ((fcmsgr.node_idx = gs_get_node_idx(fcmsgr.nodename)) >=
            g_instance.comm_cxt.counters_cxt.g_cur_node_num) {
            usleep(10000);
            LIBCOMM_ELOG(WARNING,
                "(s|flow ctrl)\tReveive fault message with socket[%d] "
                "for[%s], type[%d], node index[%d], stream id[%d], get node_idx again.",
                f_fd_id.fd,
                fcmsgr.nodename,
                fcmsgr.type,
                fcmsgr.node_idx,
                fcmsgr.streamid);
        } else {
            break;
        }
    } while (retry_count--);

    COMM_DEBUG_CALL(printfcmsg("s|flow ctrl", &fcmsgr));
    /* step9: check the type of the message, and do different processing */
    if (fcmsgr.node_idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num || fcmsgr.streamid == 0 ||
        fcmsgr.streamid >= g_instance.comm_cxt.counters_cxt.g_max_stream_num) {
        fcmsgr.nodename[NAMEDATALEN - 1] = '\0';
        LIBCOMM_ELOG(WARNING,
            "(s|flow ctrl)\tReveive fault message with socket[%d] "
            "for[%s], type[%d], node index[%d], stream id[%d].",
            f_fd_id.fd,
            fcmsgr.nodename,
            fcmsgr.type,
            fcmsgr.node_idx,
            fcmsgr.streamid);
        errno = ECOMMTCPNODEIDFD;
        printfcmsg("s|flow ctrl", &fcmsgr);

        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
        entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &f_fd_id, HASH_FIND, &found);
        if (found) {
            idx = entry_id->entry.val;
        } else {
            idx = -1;
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

        if (idx >= 0) {
            g_instance.comm_cxt.g_s_node_sock[idx].lock();
        }
        gs_s_close_bad_ctrl_tcp_sock(&f_fd_id, ECOMMTCPTCPDISCONNECT, true, idx);
        if (idx >= 0) {
            g_instance.comm_cxt.g_s_node_sock[idx].unlock();
        }

        return;
    }

    switch (fcmsgr.type) {
        case CTRL_PEER_TID:
            gs_senders_flow_handle_tid_request(&fcmsgr);
            break;

        case CTRL_CONN_REJECT:
            gs_senders_flow_handle_init_request(&fcmsgr);
            break;

        case CTRL_CONN_ACCEPT:
            gs_senders_flow_handle_ready_request(&fcmsgr);
            break;

        case CTRL_ADD_QUOTA:
            gs_senders_flow_handle_resume_request(&fcmsgr);
            break;

        case CTRL_CLOSED:
            gs_senders_flow_handle_close_request(&fcmsgr);
            break;

        case CTRL_ASSERT_FAIL:
            gs_senders_flow_handle_assert_fail_request(&fcmsgr);
            break;
        case CTRL_STOP_QUERY:
            gs_senders_flow_handle_stop_query_request(&fcmsgr);
            break;
        default:
            /* should not happen */
            LIBCOMM_ASSERT(false, fcmsgr.node_idx, fcmsgr.streamid, ROLE_PRODUCER);
            break;
    }

    DEBUG_QUERY_ID = 0;
}

void commResolveNoMessage(int length, sock_id_entry* entry_id, bool found, struct sock_id f_fd_id)
{
    int idx;

    if (length < 0) {
        /* failed to read message */
        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
        entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &f_fd_id, HASH_FIND, &found);
        if (found) {
            idx = entry_id->entry.val;
        } else {
            idx = -1;
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

        LIBCOMM_ELOG(WARNING,
            "(s|flow ctrl)\tTCP disconnect with socket[%d] for node[%d]:%s, detail:%s.",
            f_fd_id.fd,
            idx,
            REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, idx),
            gs_comm_strerror());
        if (idx >= 0) {
            g_instance.comm_cxt.g_s_node_sock[idx].lock();
        }
        gs_s_close_bad_ctrl_tcp_sock(&f_fd_id, ECOMMTCPTCPDISCONNECT, true, idx);
        if (idx >= 0) {
            g_instance.comm_cxt.g_s_node_sock[idx].unlock();
        }
    } else if (length == 0) {
        /* error can be resolved by retry, just report warning */
        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
        entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &f_fd_id, HASH_FIND, &found);
        if (found) {
            idx = entry_id->entry.val;
        } else {
            idx = -1;
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

        LIBCOMM_ELOG(WARNING,
            "(s|flow ctrl)\tTCP receive reply with socket[%d] for node[%d]:%s, detail:%s.",
            f_fd_id.fd,
            idx,
            REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, idx),
            gs_comm_strerror());
    }
}

void commSenderFlowerLoop(uint64 *end_time)
{
    /* step1: define and intialize local variables */
    int i, rc, idx;
    bool found = false;
    sock_id_entry* entry_id = NULL;
    struct FCMSG_T fcmsgr = {0x0};

    /* step2:  waiting for network events */
    (void)mc_poller_wait(t_thrd.comm_cxt.g_libcomm_poller_list, EPOLL_TIMEOUT);
    *end_time = mc_timers_us();

    /* step3: get the epoll events */
    int nevents = t_thrd.comm_cxt.g_libcomm_poller_list->nevents;
    if (nevents < 0) {
        /*
         * EBADF  epfd is not a valid file descriptor.
         * EFAULT The memory area pointed to by events is not accessible with write permissions.
         * EINVAL epfd is not an epoll file descriptor, or maxevents is less than or equal to zero.
         */
        if (errno == EBADF || errno == EFAULT || errno == EINVAL) {
            ereport(PANIC,
                (errmsg("(s|flow ctrl)\tFailed to do epoll wait[%d] with errno[%d]:%s.",
                    t_thrd.comm_cxt.g_libcomm_poller_list->ep,
                    errno,
                    mc_strerror(errno))));
        }
        return;
    }
    /* step4: process each epoll event */
    for (i = 0; i < t_thrd.comm_cxt.g_libcomm_poller_list->nevents; i++) {
        /* step5: get the socket related to the current event */
        struct sock_id f_fd_id;
        f_fd_id.fd =
            (int)(((uint64)t_thrd.comm_cxt.g_libcomm_poller_list->events[i].data.u64) >> MC_POLLER_FD_ID_OFFSET);
        f_fd_id.id = (int)(((uint64)t_thrd.comm_cxt.g_libcomm_poller_list->events[i].data.u64) & MC_POLLER_FD_ID_MASK);
        /* step6: get the node index by the socket */
        /* errror happended to the connection */
        if ((t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events & EPOLLERR) ||
            (t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events & EPOLLHUP)) {
            LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
            entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &f_fd_id, HASH_FIND, &found);
            if (found) {
                idx = entry_id->entry.val;
            } else {
                idx = -1;
            }
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

            int error = 0;
            socklen_t errlen = sizeof(error);
            (void)getsockopt(f_fd_id.fd, SOL_SOCKET, SO_ERROR, (void*)&error, &errlen);
            LIBCOMM_ELOG(WARNING,
                "(s|flow ctrl)\tPoller receive error, "
                "close tcp socket[%d] to node[%d]:%s, events[%u], error[%d]:%s.",
                f_fd_id.fd,
                idx,
                REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, idx),
                t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events,
                error,
                mc_strerror(error));
            if (idx >= 0) {
                g_instance.comm_cxt.g_s_node_sock[idx].lock();
            }
            gs_s_close_bad_ctrl_tcp_sock(&f_fd_id, ECOMMTCPTCPDISCONNECT, true, idx);
            if (idx >= 0) {
                g_instance.comm_cxt.g_s_node_sock[idx].unlock();
            }
            continue;
        }

        /* step7: receive data from the connection */
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
        if (is_comm_fault_injection(LIBCOMM_FI_S_TCP_DISCONNECT)) {
            LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
            entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &f_fd_id, HASH_FIND, &found);
            if (found) {
                idx = entry_id->entry.val;
            } else {
                idx = -1;
            }
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

            LIBCOMM_ELOG(WARNING,
                "(s|flow ctrl)\t[FAULT INJECTION]TCP disconnect with socket[%d] for node[%d]:%s, detail:%s.",
                f_fd_id.fd,
                idx,
                REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, idx),
                gs_comm_strerror());
            if (idx >= 0) {
                g_instance.comm_cxt.g_s_node_sock[idx].lock();
            }
            gs_s_close_bad_ctrl_tcp_sock(&f_fd_id, ECOMMTCPTCPDISCONNECT, true, idx);
            if (idx >= 0) {
                g_instance.comm_cxt.g_s_node_sock[idx].unlock();
            }
            continue;
        }
#endif
        /* do receiving data */
        rc = mc_tcp_read_block(f_fd_id.fd, &fcmsgr, sizeof(fcmsgr), 0);
        if (rc > 0) {
            commResolveMessage(fcmsgr, entry_id, found, f_fd_id);
        } else {
            commResolveNoMessage(rc, entry_id, found, f_fd_id);
        }
    }
}

static int CommReceiverFlowerDealEvents(int nevents)
{
    if (nevents < 0) {
        /*
         * EBADF  epfd is not a valid file descriptor.
         * EFAULT The memory area pointed to by events is not accessible with write permissions.
         * EINVAL epfd is not an epoll file descriptor, or maxevents is less than or equal to zero.
         */
        if (errno == EBADF || errno == EFAULT || errno == EINVAL) {
            ereport(PANIC,
                (errmsg("(r|flow ctrl)\tFailed to do epoll wait[%d] with errno[%d]:%s.",
                    t_thrd.comm_cxt.g_libcomm_poller_list->ep,
                    errno,
                    mc_strerror(errno))));
        }
        return -1;
    }

    return 0;
}
static void CommReceiverFlowerAcceptNewConn(const struct sock_id* tFdId, int ltk)
{
    struct sockaddr ctrlClient;
    socklen_t sLen = sizeof(struct sockaddr);
    /* do accept */
    int ctk = mc_tcp_accept(ltk, (struct sockaddr*)&ctrlClient, (socklen_t*)&sLen);
    if (ctk < 0) {
        COMM_DEBUG_LOG("(r|flow ctrl)\tFailed to accept tcp connection on listen socket[%d]:%s",
            ltk,
            mc_strerror(errno));
        return;
    }

    sockaddr_in* pSin = (sockaddr_in*)&ctrlClient;
    char* ipstr = inet_ntoa(pSin->sin_addr);
    LIBCOMM_ELOG(LOG, "(r|flow ctrl)\tDetect incoming connection, socket[%d] from [%s].", ctk, ipstr);

    /* Server side gss kerberos authentication for tcp connection. */
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_GSS_TCP_FAILED)) {
        /* Server side gss kerberos authentication for tcp connection. */
        mc_tcp_close(ctk);
        errno = ECOMMTCPGSSAUTHFAIL;
        LIBCOMM_ELOG(WARNING,
            "(r|flow ctrl)\t[FAULT INJECTION]Control channel GSS authentication failed, listen "
            "socket[%d]:%s.",
            ltk,
            mc_strerror(errno));
        return;
    }
#endif

    /* Server side gss kerberos authentication for tcp connection.
     * if GSS authentication SUCC, no IP authentication is required.
     */
    if (g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile != NULL) {
#ifdef ENABLE_GSS
        if (GssServerAuth(ctk, g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile) < 0) {
            mc_tcp_close(ctk);
            errno = ECOMMTCPGSSAUTHFAIL;
            LIBCOMM_ELOG(WARNING,
                "(r|flow ctrl)\tControl channel GSS authentication failed, listen socket[%d]:%s.",
                ltk,
                mc_strerror(errno));
            return;
        }

        COMM_DEBUG_LOG("(r|flow ctrl)\tControl channel GSS authentication SUCC, listen socket[%d]:%s.",
            ltk,
            mc_strerror(errno));
#else
        mc_tcp_close(ctk);
        LIBCOMM_ELOG(WARNING,
            "(r|flow ctrl)\tControl channel GSS authentication is disable.");
        return;
#endif
    } else {
        /* send signal to postmaster thread to reload hba */
        if (gs_reload_hba(ltk, ctrlClient)) {
            mc_tcp_close(ctk);
            return;
        }
    }

#ifdef USE_SSL
    if (g_instance.attr.attr_network.comm_enable_SSL) {
         LIBCOMM_ELOG(LOG, "CommReceiverFlowerAcceptNewConn call comm_ssl_open_server, fd is %d, id is %d, sock is %d",
            tFdId->fd, tFdId->id, ctk);
         libcomm_sslinfo** libcomm_ctrl_port = comm_ssl_find_port(&g_instance.comm_cxt.libcomm_ctrl_port_list, ctk);
         if (*libcomm_ctrl_port == NULL) {
             *libcomm_ctrl_port = (libcomm_sslinfo*)palloc(sizeof(libcomm_sslinfo));
             if (*libcomm_ctrl_port == NULL) {
                 LIBCOMM_ELOG(ERROR, "(r|recv loop)\tSocket[%d] create new ctrl SSL connection failed.", ctk);
                 mc_tcp_close(ctk);
                 return;
             }
             (*libcomm_ctrl_port)->next = NULL;
             (*libcomm_ctrl_port)->node.ssl = NULL;
             (*libcomm_ctrl_port)->node.peer = NULL;
             (*libcomm_ctrl_port)->node.peer_cn = NULL;
             (*libcomm_ctrl_port)->node.count = 0;
             (*libcomm_ctrl_port)->node.sock = ctk;
             LIBCOMM_ELOG(LOG, "(r|recv loop)\tSocket[%d] create new ctrl SSL connection success.", ctk);
         } else {
             LIBCOMM_ELOG(WARNING, "(r|recv loop)\tSocket[%d] has already create ctrl SSL connection.", ctk);
             mc_tcp_close(ctk);
             return;
         }
         int error = comm_ssl_open_server(&(*libcomm_ctrl_port)->node, ctk);
         if (error == -1) {
             LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tFailed to open server ctrl port ssl.");
         } else if (error == -2) {
             LIBCOMM_ELOG(WARNING, "(r|recv loop)\tFailed to open server ctrl port ssl and remove from list.");
             libcomm_sslinfo* tmp = *libcomm_ctrl_port;
             comm_ssl_close(&(*libcomm_ctrl_port)->node);
             *libcomm_ctrl_port = (*libcomm_ctrl_port)->next;
             pfree(tmp);
         } else {
             LIBCOMM_ELOG(LOG, "(r|flow ctrl)\tSuccess to open server ctrl port ssl.");
         }
    }
#endif
    struct sock_id ctkFdId = {ctk, 0};
    /* update the new socket and its version */
    if (gs_update_fd_to_htab_socket_version(&ctkFdId) < 0) {
        LIBCOMM_ELOG(WARNING,
            "(r|flow ctrl)\tFailed to save socket[%d] and version[%d].",
            ctkFdId.fd,
            ctkFdId.id);
    }

    LIBCOMM_PTHREAD_MUTEX_LOCK(g_instance.comm_cxt.pollers_cxt.g_r_poller_list_lock);
    /* add the new socket to epoll list for monitoring data */
    if (g_instance.comm_cxt.pollers_cxt.g_r_poller_list->add_fd(&ctkFdId) < 0) {
        mc_tcp_close(ctk);
        LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tFailed to add socket[%d] to poller list.", ctk);
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(g_instance.comm_cxt.pollers_cxt.g_r_poller_list_lock);

    LIBCOMM_ELOG(LOG,
        "(r|flow ctrl)\tContoller tcp listener on socket[%d,%d] accepted socket[%d,%d].",
        tFdId->fd,
        tFdId->id,
        ctkFdId.fd,
        ctkFdId.id);
}

static void CommReceiverFlowerProcessMsg(struct sock_id* tFdId, struct FCMSG_T* fcmsgr)
{
    /* step13: check the type of the message, and do different processing */
    switch (fcmsgr->type) {
        case CTRL_PEER_TID:
            gs_receivers_flow_handle_tid_request(fcmsgr);
            break;

        case CTRL_CONN_DUAL:
        case CTRL_CONN_REQUEST:
            gs_receivers_flow_handle_ready_request(fcmsgr);
            break;

        case CTRL_CLOSED:
            gs_receivers_flow_handle_close_request(fcmsgr);
            break;

        case CTRL_ASSERT_FAIL:
            gs_receivers_flow_handle_assert_fail_request(fcmsgr);
            break;

        case CTRL_CONN_REGIST:
#ifdef USE_SPQ
        case CTRL_QE_BACKWARD:
        case CTRL_BACKWARD_REGIST:
#endif
        case CTRL_CONN_REGIST_CN:
            gs_accept_ctrl_conntion(tFdId, fcmsgr);
            break;

        case CTRL_PEER_CHANGED:
            gs_r_close_bad_ctrl_tcp_sock(tFdId, ECOMMTCPREMOETECLOSE);
            break;

        /* case CTRL_INIT, CTRL_RESUME, CTRL_HOLD */
        default:
            /* should not happen */
            LIBCOMM_ASSERT(false, fcmsgr->node_idx, fcmsgr->streamid, ROLE_CONSUMER);
            break;
    }

    return;
}

static void CommReceiverFlowerReceiveData(struct sock_id* tFdId)
{
    int rc;
    bool found = false;
    sock_id_entry* entryId = NULL;
    struct FCMSG_T fcmsgr = {0x0};
    int idx = -1;

    /* when query is started, other instances will send message to asking for a connection */
    if ((rc = mc_tcp_read_block(tFdId->fd, &fcmsgr, sizeof(fcmsgr), 0)) > 0) {
        /* step10: resolve the message  */
        /* src is from the network, maybe do not have '\0' */
        gs_update_recv_ready_time();
        fcmsgr.nodename[NAMEDATALEN - 1] = '\0';
        /* fault node checksum (to avoid port injection)
         * "g_htab_fd_id_node_idx.get_value() < 0" means first connection, so the message type must be REGIST
         * we need to check the type and extra_info.
         */
        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
        entryId = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, tFdId, HASH_FIND, &found);
        if (found) {
            idx = entryId->entry.val;
        } else {
            idx = -1;
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

        if ((idx < 0) && !((fcmsgr.type == CTRL_CONN_REGIST || fcmsgr.type == CTRL_CONN_REGIST_CN
#ifdef USE_SPQ
                            || fcmsgr.type == CTRL_QE_BACKWARD
#endif
                            ) && fcmsgr.extra_info == 0xEA)) {
            fcmsgr.nodename[NAMEDATALEN - 1] = '\0';
            LIBCOMM_ELOG(WARNING,
                "(r|flow ctrl)\tReveive fault message with socket[%d] for[%s], type[%d].",
                tFdId->fd,
                fcmsgr.nodename,
                fcmsgr.type);
            errno = ECOMMTCPNODEIDFD;
            printfcmsg("r|flow ctrl", &fcmsgr);
            gs_r_close_bad_ctrl_tcp_sock(tFdId, ECOMMTCPTCPDISCONNECT);

            return;
        }

        DEBUG_QUERY_ID = fcmsgr.query_id;

        int retryCount = 10;
        do {
            /* node_idx is uint16, so if gs_get_node_idx() returns -1, node_idx=65535 */
            if ((fcmsgr.node_idx = gs_get_node_idx(fcmsgr.nodename)) >=
                g_instance.comm_cxt.counters_cxt.g_cur_node_num) {
                LIBCOMM_ELOG(WARNING,
                    "(r|flow ctrl)\tReveive fault message with socket[%d] for[%s], type[%d], get node_idx "
                    "again.",
                    tFdId->fd,
                    fcmsgr.nodename,
                    fcmsgr.type);
                usleep(10000);
            } else {
                break;
            }
        } while (retryCount--);

        COMM_DEBUG_CALL(printfcmsg("r|flow ctrl", &fcmsgr));

        /* fault node index or fault stream id */
        /* node_idx is uint16, so if gs_get_node_idx() returns -1, node_idx=65535 */
        if (fcmsgr.node_idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num || fcmsgr.streamid == 0 ||
            fcmsgr.streamid >= g_instance.comm_cxt.counters_cxt.g_max_stream_num) {
            fcmsgr.nodename[NAMEDATALEN - 1] = '\0';
            LIBCOMM_ELOG(WARNING,
                "(r|flow ctrl)\tReveive fault message with socket[%d] for[%s], type[%d].",
                tFdId->fd,
                fcmsgr.nodename,
                fcmsgr.type);
            errno = ECOMMTCPNODEIDFD;
            printfcmsg("r|flow ctrl", &fcmsgr);
            gs_r_close_bad_ctrl_tcp_sock(tFdId, ECOMMTCPTCPDISCONNECT);

            return;
        }

        CommReceiverFlowerProcessMsg(tFdId, &fcmsgr);

        DEBUG_QUERY_ID = 0;
    } else if (rc < 0) {
        /* failed to read message */
        /* at receiver, if the tcp connection is broken, which caused the failure of receiving, we do cleanning
         * and report the error */
        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
        entryId = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, tFdId, HASH_FIND, &found);
        if (found) {
            idx = entryId->entry.val;
        } else {
            idx = -1;
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

        LIBCOMM_ELOG(WARNING,
            "(r|flow ctrl)\tTCP disconnect with socket[%d] for node[%d]:%s, detail:%s.",
            tFdId->fd,
            idx,
            REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx),
            gs_comm_strerror());
        gs_r_close_bad_ctrl_tcp_sock(tFdId, ECOMMTCPTCPDISCONNECT);
    } else if (rc == 0) {
        /* error can be resolved by retry, just report warning */
        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
        entryId = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, tFdId, HASH_FIND, &found);
        if (found) {
            idx = entryId->entry.val;
        } else {
            idx = -1;
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

        LIBCOMM_ELOG(WARNING,
            "(r|flow ctrl)\tTCP receive reply with socket[%d] for node[%d]:%s, detail:%s.",
            tFdId->fd,
            idx,
            REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx),
            gs_comm_strerror());
        return;
    }

    return;
}
static void CommReceiverFlowerReceiveMsg(struct sock_id* tFdId, int i)
{
    bool found = false;
    sock_id_entry* entryId = NULL;

    /* step8: if the socket is a normal conection socket, do receive message and other processes by the contents
     * of the message */
    int idx = -1;  // node index
#ifdef ENABLE_LLT
    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
    entryId = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, tFdId, HASH_FIND, &found);
    if (found) {
        idx = entryId->entry.val;
    } else {
        idx = -1;
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);
#endif

    /* errror happended to the connection */
    if ((t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events & EPOLLERR) ||
        (t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events & EPOLLHUP)) {
        /* if error happened, we get the node index by the socket, it maybe fail */
        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
        entryId = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, tFdId, HASH_FIND, &found);
        if (found) {
            idx = entryId->entry.val;
        } else {
            idx = -1;
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

        int error = 0;
        socklen_t errlen = sizeof(error);
        (void)getsockopt(tFdId->fd, SOL_SOCKET, SO_ERROR, (void*)&error, &errlen);
        LIBCOMM_ELOG(WARNING,
            "(r|flow ctrl)\tPoller receive error, "
            "close tcp socket[%d] to node[%d]:%s, events[%u], error[%d]:%s.",
            tFdId->fd,
            idx,
            REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx),
            t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events,
            error,
            mc_strerror(error));

        /* close the socket and clean the related information */
        gs_r_close_bad_ctrl_tcp_sock(tFdId, ECOMMTCPTCPDISCONNECT);

        return;
    }

    /* step9: receive data from the connection */
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_R_TCP_DISCONNECT)) {
        /* at receiver, if the tcp connection is broken, which caused the failure of receiving, we do cleanning
         * and report the error */
        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
        entryId = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, tFdId, HASH_FIND, &found);
        if (found) {
            idx = entryId->entry.val;
        } else {
            idx = -1;
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

        LIBCOMM_ELOG(WARNING,
            "(r|flow ctrl)\t[FAULT INJECTION]TCP disconnect with socket[%d] for node[%d]:%s, detail:%s.",
            tFdId->fd,
            idx,
            REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx),
            gs_comm_strerror());
        gs_r_close_bad_ctrl_tcp_sock(tFdId, ECOMMTCPTCPDISCONNECT);

        return;
    }
#endif

    CommReceiverFlowerReceiveData(tFdId);

    return;
}
void commReceiverFlowLoop(int ltk, uint64 *end_time)
{
    int i;

    /* step3:  waiting for network events */
    (void)mc_poller_wait(t_thrd.comm_cxt.g_libcomm_poller_list, EPOLL_TIMEOUT);
    *end_time = mc_timers_us();

    /* step4: get the epoll events */
    if (CommReceiverFlowerDealEvents(t_thrd.comm_cxt.g_libcomm_poller_list->nevents) < 0) {
        return;
    }

    /* step5: process each epoll event */
    for (i = 0; i < t_thrd.comm_cxt.g_libcomm_poller_list->nevents; i++) {
        /* step6: get the socket related to the current event */
        struct sock_id t_fd_id;
        t_fd_id.fd =
            (int)(((uint64)t_thrd.comm_cxt.g_libcomm_poller_list->events[i].data.u64) >> MC_POLLER_FD_ID_OFFSET);
        t_fd_id.id = (int)(((uint64)t_thrd.comm_cxt.g_libcomm_poller_list->events[i].data.u64) & MC_POLLER_FD_ID_MASK);
        if (t_fd_id.fd == ltk) {
            /* step7: if the socket is listening tcp control socket, do accept and update the new socket and its
             * version, then add it to epoll list for monitoring data */
            if (t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events & EPOLLIN) {
                CommReceiverFlowerAcceptNewConn(&t_fd_id, ltk);
            }
        } else {
            CommReceiverFlowerReceiveMsg(&t_fd_id, i);
        }
    }
}

void commAuxiliaryLoop(uint64* last_print_time)
{
    uint64 current_time;

    /* step1: wait signal */
    (void)g_instance.comm_cxt.quota_cxt.g_quota_changing->timed_wait(5);

    /* step3: check other request */
    gs_check_requested();

    /* step4: send delay survey messages */
    if (g_instance.comm_cxt.g_delay_survey_switch) {
        gs_delay_survey();
    }

    /* step5: receive delay survey messages and analysis whether switch is on */
    gs_delay_analysis();

#ifdef LIBCOMM_SPEED_TEST_ENABLE
    libcomm_performance_test();
#endif

    /* if g_cur_node_num==0, it means postmaster thread doesn't finish initilaization. */
    if ((g_instance.comm_cxt.counters_cxt.g_cur_node_num != 0) &&
        (g_instance.comm_cxt.counters_cxt.g_expect_node_num != g_instance.comm_cxt.counters_cxt.g_cur_node_num)) {
        gs_online_change_capacity();
    }

    current_time = mc_timers_ms();
    if ((g_instance.comm_cxt.g_delay_survey_switch) &&
        (g_instance.comm_cxt.g_delay_survey_start_time + g_print_interval_time < current_time)) {
        g_instance.comm_cxt.g_delay_survey_switch = false;
        LIBCOMM_ELOG(LOG, "delay survey switch is close");
    }

    if (g_instance.comm_cxt.commutil_cxt.g_debug_mode) {
        if (*last_print_time + g_print_interval_time < current_time) {
            print_stream_sock_info();
            *last_print_time = current_time;
        }
    }
}

static int CommReceiverAcceptNewConnect(const struct sock_id *fdId, int selfid)
{
    struct sockaddr ctrlClient;
    socklen_t sLen = sizeof(struct sockaddr);
    int maxLen, j;
    int currLen = 0;
    int dstIdx = 0;

    int clientSocket = g_libcomm_adapt.accept(g_instance.comm_cxt.g_receivers->server_listen_conn.socket,
        (struct sockaddr*)&ctrlClient,
        (socklen_t*)&sLen);
    if (clientSocket < 0) {
        LIBCOMM_ELOG(WARNING,
            "(r|recv loop)\tFailed to accept data connection on listen socket[%d]:%s.",
            g_instance.comm_cxt.g_receivers->server_listen_conn.socket,
            mc_strerror(errno));
        return 0;
    }

    sockaddr_in* pSin = (sockaddr_in*)&ctrlClient;
    char* ipstr = inet_ntoa(pSin->sin_addr);
    LIBCOMM_ELOG(
        LOG, "(r|recv loop)\tDetect incoming connection, socket[%d] from [%s].", clientSocket, ipstr);

#ifdef ENABLE_GSS
    /*
     * server side gss kerberos authentication for data connection.
     * authentication for tcp mode after accept.
     * if GSS authentication SUCC, no IP authentication is required.
     */
    if (g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile != NULL) {
        if (GssServerAuth(clientSocket, g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile) < 0) {
            mc_tcp_close(clientSocket);
            errno = ECOMMTCPGSSAUTHFAIL;
            LIBCOMM_ELOG(WARNING,
                "(r|recv loop)\tData channel GSS authentication failed, listen socket[%d]:%s.",
                fdId->fd,
                mc_strerror(errno));
            return 0;
        }

        LIBCOMM_ELOG(LOG,
            "(r|recv loop)\tData channel GSS authentication SUCC, listen socket[%d].",
            fdId->fd);
    }
#endif

#ifdef USE_SSL
    if (g_instance.attr.attr_network.comm_enable_SSL) {
        LIBCOMM_ELOG(LOG, "CommReceiverAcceptNewConnect call comm_ssl_open_server, fd is %d, id is %d, sock is %d", fdId->fd, fdId->id, clientSocket);
        libcomm_sslinfo** libcomm_data_port = comm_ssl_find_port(&g_instance.comm_cxt.libcomm_data_port_list, clientSocket);
        if (*libcomm_data_port == NULL) {
            *libcomm_data_port = (libcomm_sslinfo*)palloc(sizeof(libcomm_sslinfo));
            if (*libcomm_data_port == NULL) {
                LIBCOMM_ELOG(ERROR, "(r|recv loop)\tFailed to palloc data port ssl.");
                mc_tcp_close(clientSocket);
                return 0;
            }
            (*libcomm_data_port)->next = NULL;
            (*libcomm_data_port)->node.ssl = NULL;
            (*libcomm_data_port)->node.peer = NULL;
            (*libcomm_data_port)->node.peer_cn = NULL;
            (*libcomm_data_port)->node.count = 0;
            (*libcomm_data_port)->node.sock = clientSocket;
            LIBCOMM_ELOG(LOG, "(r|recv loop)\tSocket[%d] create new data SSL connection success.", clientSocket);
        } else {
            LIBCOMM_ELOG(WARNING, "(r|recv loop)\tSocket[%d] has already create data SSL connection.", clientSocket);
            mc_tcp_close(clientSocket);
            return 0;
        }
        int error = comm_ssl_open_server(&(*libcomm_data_port)->node, clientSocket);
        if (error == -1) {
            LIBCOMM_ELOG(WARNING, "(r|recv loop)\tFailed to open server data port ssl.");
        } else if (error == -2) {
            LIBCOMM_ELOG(WARNING, "(r|recv loop)\tFailed to open server data port ssl and remove from list.");
            libcomm_sslinfo* tmp = *libcomm_data_port;
            comm_ssl_close(&(*libcomm_data_port)->node);
            *libcomm_data_port = (*libcomm_data_port)->next;
            pfree(tmp);
        } else {
            LIBCOMM_ELOG(LOG, "(r|recv loop)\tSuccess to open server data port ssl.");
        }
    }
#endif

    struct sock_id ctkFdId = {clientSocket, 0};
    if (gs_update_fd_to_htab_socket_version(&ctkFdId) < 0) {
        mc_tcp_close(clientSocket);
        LIBCOMM_ELOG(WARNING,
            "(r|recv loop)\tFailed to save socket[%d] and version[%d]:%s.",
            ctkFdId.fd,
            ctkFdId.id,
            mc_strerror(errno));
        return 0;
    }
    /* add data socket to the poll list with minimal elements for waiting data */
    LIBCOMM_PTHREAD_MUTEX_LOCK(g_instance.comm_cxt.pollers_cxt.g_r_libcomm_poller_list_lock);
    maxLen = g_instance.comm_cxt.pollers_cxt.g_libcomm_receiver_poller_list[0].get_socket_count();
    currLen = 0;
    dstIdx = 0;
    for (j = 1; j < g_instance.comm_cxt.counters_cxt.g_recv_num; j++) {
        currLen = g_instance.comm_cxt.pollers_cxt.g_libcomm_receiver_poller_list[j].get_socket_count();
        if (maxLen > currLen) {
            dstIdx = j;
            break;
        }
    }
    if (g_instance.comm_cxt.pollers_cxt.g_libcomm_receiver_poller_list[dstIdx].add_fd(&ctkFdId) < 0) {
        mc_tcp_close(clientSocket);
        LIBCOMM_ELOG(
            WARNING, "(r|recv loop)\tFailed to add socket[%d] to poller list[%d].", clientSocket, selfid);
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(g_instance.comm_cxt.pollers_cxt.g_r_libcomm_poller_list_lock);
    LIBCOMM_ELOG(LOG,
        "(r|recv loop)\tReceiver data listener on socket[%d] accepted socket[%d,%d].",
        g_instance.comm_cxt.g_receivers->server_listen_conn.socket,
        ctkFdId.fd,
        ctkFdId.id);

    return 0;
}
static void commReceiverHandleDataMessage(const struct sock_id *fd_id, uint32 events)
{
    int idx = 0;
    int error = 0;
    int ret_recv = 0;
    bool found = false;
    socklen_t s_len = 0;
    sock_id_entry* entry_id = NULL;
    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
    entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, fd_id, HASH_FIND, &found);
    if (found) {
        idx = entry_id->entry.val;
    } else {
        idx = -1;
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

    /* Recv data message */
    if (events & EPOLLIN) {
        ret_recv = gs_internal_recv(*fd_id, idx, MSG_WAITALL);
        DEBUG_QUERY_ID = 0;

        if (ret_recv < 0) {
            LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
            entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, fd_id, HASH_FIND, &found);
            if (found) {
                idx = entry_id->entry.val;
            } else {
                idx = -1;
            }
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

            LIBCOMM_ELOG(WARNING,
                "(r|recv loop)\tFailed to recv message from node[%d]:%s on data socket[%d].",
                idx, REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx), fd_id->fd);
            if (ret_recv == RECV_MEM_ERROR) {
                /*
                    * If there is no usable memory, we traverse all the c_mailbox(es) to find the query who occupies
                    * maximun memory, and make it failure to release the memory. Otherwise, the communication layer
                    * maybe hang up.
                    */
                LIBCOMM_ELOG(WARNING, "(r|recv loop)\tFailed to malloc, begin release memory.");
                gs_r_release_comm_memory();
            } else if (ret_recv == RECV_NET_ERROR) {
                gs_r_close_bad_data_socket(idx, *fd_id, ECOMMTCPDISCONNECT, true);
            }
        }
    } else if ((events & EPOLLERR) || (events & EPOLLHUP)) {  /* ERROR handling */
        s_len = sizeof(error);
        (void)getsockopt(fd_id->fd, SOL_SOCKET, SO_ERROR, (void*)&error, &s_len);
        LIBCOMM_ELOG(WARNING,
            "(r|recv loop)\tBroken receive channel to node[%d]:%s "
            "on socket[%d], events[%u], error[%d]:%s.",
            idx, REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx), fd_id->fd, events, error, mc_strerror(error));
        gs_r_close_bad_data_socket(idx, *fd_id, ECOMMTCPDISCONNECT, true);
    }
}

int CommReceiverDealEvents(int nevents, int *epollTimeoutCount)
{
    if (nevents < 0) {
        /*
         * EBADF  epfd is not a valid file descriptor.
         * EFAULT The memory area pointed to by events is not accessible with write permissions.
         * EINVAL epfd is not an epoll file descriptor, or maxevents is less than or equal to zero.
         */
        if (errno == EBADF || errno == EFAULT || errno == EINVAL) {
            /* it should not happen, so we assert here */
            ereport(PANIC,
                (errmsg("(r|recv loop)\tFailed to do epoll wait[%d] with errno[%d]:%s.",
                    t_thrd.comm_cxt.g_libcomm_poller_list->ep,
                    errno,
                    mc_strerror(errno))));
        }
        return -1;
    }

    if (nevents == 0) {
        (*epollTimeoutCount)++;
        if (*epollTimeoutCount > MAX_EPOLL_TIMEOUT_COUNT) {
            *epollTimeoutCount = 0;
            COMM_DEBUG_LOG(
                "(r|recv loop)\tepoll[%d] wait timeout[%dms].",
                t_thrd.comm_cxt.g_libcomm_poller_list->ep, EPOLL_TIMEOUT);
        }
    } else {
        *epollTimeoutCount = 0;
    }

    return 0;
}
void commReceiverLoop(int selfid, uint64 *end_time, int *epoll_timeout_count)
{
    int i;
    struct sock_id fd_id;

    /* Wait poll */
    (void)mc_poller_wait(t_thrd.comm_cxt.g_libcomm_poller_list, EPOLL_TIMEOUT);
    *end_time = mc_timers_us();
    t_thrd.comm_cxt.g_receiver_loop_poll_up = COMM_STAT_TIME();

    if (CommReceiverDealEvents(t_thrd.comm_cxt.g_libcomm_poller_list->nevents, epoll_timeout_count) < 0) {
        return;
    }

    for (i = 0; i < t_thrd.comm_cxt.g_libcomm_poller_list->nevents; i++) {
        fd_id.fd = (int)(((uint64)t_thrd.comm_cxt.g_libcomm_poller_list->events[i].data.u64 >> MC_POLLER_FD_ID_OFFSET));
        fd_id.id = (int)(((uint64)t_thrd.comm_cxt.g_libcomm_poller_list->events[i].data.u64 & MC_POLLER_FD_ID_MASK));
        mc_assert(fd_id.fd > 0);
        /* Special case: handle with accepting connection */
        if (unlikely(fd_id.fd == g_instance.comm_cxt.g_receivers->server_listen_conn.socket)) {
            if (t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events & EPOLLIN) {
                CommReceiverAcceptNewConnect(&fd_id, selfid);
            }
            continue;
        }

        commReceiverHandleDataMessage(&fd_id, t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events);
    }
}

void commSenderFlowMain()
{
    uint64 thread_start_time = 0;
    uint64 thread_end_time = 0;
    uint64 thread_work_all_time = 0;
    uint64 thread_last_check_time = 0;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;

    /* reset MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    t_thrd.proc_cxt.MyProgName = "CommSenderFlowerWorker";

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* Identify myself via ps */
    init_ps_display("comm sender flower worker process", "", "", "");

    /* setup signal process hook */
    SetupCommSignalHook();

    /* initialize globals */
    log_timezone = g_instance.comm_cxt.libcomm_log_timezone;
    t_thrd.comm_cxt.LibcommThreadType = LIBCOMM_SEND_CTRL;

    (void)MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);

    t_thrd.comm_cxt.g_libcomm_poller_list =
        g_instance.comm_cxt.pollers_cxt.g_s_poller_list->get_poller();  // get poller for monitoring network events
    LIBCOMM_ELOG(LOG, "Sender flow control thread is initialized.");

    sigjmp_buf local_sigjmp_buf;
    int curTryCounter;
    int *oldTryCounter = NULL;

    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {

        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Forget any pending QueryCancel request */
        t_thrd.int_cxt.QueryCancelPending = false;
        disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false; /* again in case timeout occurred */

        /* Report the error to the client and/or server log */
        EmitErrorReport();

        (void)MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);

        FlushErrorState();

        LIBCOMM_ELOG(ERROR, "Sender flow control encounter ERROR level ereport which is unsupported.");
    }

    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    t_thrd.int_cxt.ImmediateInterruptOK = false;

    thread_start_time = thread_last_check_time = thread_end_time = mc_timers_us();
    for (;;) {
        /* if the datanode is to shutdown, close the listening socket and exit the thread */
        if (g_instance.comm_cxt.reqcheck_cxt.g_shutdown_requested) {
            break;
        }

        gs_flow_thread_time("(s|flow ctrl)\tgs_senders_flow_controller", &thread_start_time, thread_end_time,
            &thread_last_check_time, &thread_work_all_time, GS_SEND_flow);
        commSenderFlowerLoop(&thread_end_time);
    }
    /* All done, go away */
    proc_exit(0);
}

void commReceiverFlowMain()
{

    uint64 thread_start_time = 0;
    uint64 thread_end_time = 0;
    uint64 thread_work_all_time = 0;
    uint64 thread_last_check_time = 0;
    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;

    /* reset MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    t_thrd.proc_cxt.MyProgName = "CommReceiverFlowerWorker";

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* Identify myself via ps */
    init_ps_display("comm receiver flower worker process", "", "", "");

    /* setup signal process hook */
    SetupCommSignalHook();

    // initialize globals
    log_timezone = g_instance.comm_cxt.libcomm_log_timezone;
    t_thrd.comm_cxt.LibcommThreadType = LIBCOMM_RECV_CTRL;

    (void)MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);

    /* step1: define and intialize local variables */
    /* get the listening socket for tcp control message */
    int ltk = g_instance.comm_cxt.localinfo_cxt.g_local_ctrl_tcp_sock;
    mc_assert(ltk != INVALID_SOCK);
    /* get poller for monitoring network events */
    t_thrd.comm_cxt.g_libcomm_poller_list = g_instance.comm_cxt.pollers_cxt.g_r_poller_list->get_poller();
    struct sock_id ltk_fd_id = {ltk, 0};
    /* update the listening socket and socket version */
    while (gs_update_fd_to_htab_socket_version(&ltk_fd_id) < 0) {
        LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tFailed to save socket[%d] and version[%d].", ltk_fd_id.fd, ltk_fd_id.id);
        (void)sleep(1);
    }

    /* step2: add the listening socket to poller for monitoring connecting events from senders */
    LIBCOMM_PTHREAD_MUTEX_LOCK(g_instance.comm_cxt.pollers_cxt.g_r_poller_list_lock);
    while (g_instance.comm_cxt.pollers_cxt.g_r_poller_list->add_fd(&ltk_fd_id) < 0) {
        LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tFailed to add socket[%d] to poller list.", ltk);
        (void)sleep(1);
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(g_instance.comm_cxt.pollers_cxt.g_r_poller_list_lock);

    LIBCOMM_ELOG(LOG, "Receiver flow control thread is initialized, ready to accept on socket[%d].", ltk);

    sigjmp_buf local_sigjmp_buf;
    int curTryCounter;
    int *oldTryCounter = NULL;

    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {

        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Forget any pending QueryCancel request */
        t_thrd.int_cxt.QueryCancelPending = false;
        disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false; /* again in case timeout occurred */

        /* Report the error to the client and/or server log */
        EmitErrorReport();

        (void)MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);

        FlushErrorState();

        LIBCOMM_ELOG(ERROR, "Receiver flow control encounter ERROR level ereport which is unsupported.");
    }

    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    t_thrd.int_cxt.ImmediateInterruptOK = false;
    thread_start_time = thread_last_check_time = thread_end_time = mc_timers_us();
    for (;;) {
        /* if the datanode is to shutdown, close the listening socket and exit the thread */
        if (g_instance.comm_cxt.reqcheck_cxt.g_shutdown_requested) {
            LIBCOMM_PTHREAD_MUTEX_DESTORY(g_instance.comm_cxt.pollers_cxt.g_r_poller_list_lock);
            break;
        }

        gs_flow_thread_time("(r|flow ctrl)\tgs_receivers_flow_controller", &thread_start_time, thread_end_time,
            &thread_last_check_time, &thread_work_all_time, GS_RECV_FLOW);
        commReceiverFlowLoop(ltk, &thread_end_time);
    }

    /* All done, go away */
    proc_exit(0);
}

void commAuxiliaryMain()
{
    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;

    /* reset MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    t_thrd.proc_cxt.MyProgName = "CommAuxiliaryFlowerWorker";

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* Identify myself via ps */
    init_ps_display("comm auxiliary worker process", "", "", "");

    /* setup signal process hook */
    SetupCommSignalHook();

    /* initialize globals */
    log_timezone = g_instance.comm_cxt.libcomm_log_timezone;
    t_thrd.comm_cxt.LibcommThreadType = LIBCOMM_AUX;

    (void)MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);

    uint64 last_print_time;
    last_print_time = mc_timers_ms();

    LIBCOMM_ELOG(LOG, "Receiver flow control auxiliary thread is initialized.");

    sigjmp_buf local_sigjmp_buf;
    int curTryCounter;
    int *oldTryCounter = NULL;

    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {

        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;
    
        t_thrd.log_cxt.call_stack = NULL;
    

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Forget any pending QueryCancel request */
        t_thrd.int_cxt.QueryCancelPending = false;
        disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false; /* again in case timeout occurred */

        /* Report the error to the client and/or server log */
        EmitErrorReport();

        (void)MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);

        FlushErrorState();

        LIBCOMM_ELOG(ERROR, "Auxiliary encounter ERROR level ereport which is unsupported.");
    }

    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    t_thrd.int_cxt.ImmediateInterruptOK = false;

    for (;;) {
        /* step2: need shutdown */
        if (g_instance.comm_cxt.reqcheck_cxt.g_shutdown_requested) {
            break;
        }

        commAuxiliaryLoop(&last_print_time);
    }

    /* All done, go away */
    proc_exit(0);
}

void commReceiverMain(void* tid_callback)
{
    int epoll_timeout_count = 0;
    uint64 thread_start_time = 0;
    uint64 thread_end_time = 0;
    uint64 thread_work_all_time = 0;
    uint64 thread_last_check_time = 0;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;

    /* reset MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    t_thrd.proc_cxt.MyProgName = "CommReceiverWorker";

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* Identify myself via ps */
    init_ps_display("comm receiver worker process", "", "", "");

    /* setup signal process hook */
    SetupCommSignalHook();

    /* initialize globals */
    log_timezone = g_instance.comm_cxt.libcomm_log_timezone;
    t_thrd.comm_cxt.LibcommThreadType = LIBCOMM_RECV_LOOP;

    (void)MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);

    /* Initialize env */
    int selfid = *(int*)tid_callback;
    *(int*)tid_callback = -1;
    LIBCOMM_PTHREAD_MUTEX_LOCK(g_instance.comm_cxt.pollers_cxt.g_r_libcomm_poller_list_lock);
    t_thrd.comm_cxt.g_libcomm_poller_list =
        g_instance.comm_cxt.pollers_cxt.g_libcomm_receiver_poller_list[selfid].get_poller();
    t_thrd.comm_cxt.g_libcomm_recv_poller_hndl_list =
        &g_instance.comm_cxt.pollers_cxt.g_libcomm_receiver_poller_list[selfid];
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(g_instance.comm_cxt.pollers_cxt.g_r_libcomm_poller_list_lock);

    LIBCOMM_ELOG(LOG, "Receiver data receiving thread[%d] is initialized.", selfid + 1);

    sigjmp_buf local_sigjmp_buf;
    int curTryCounter;
    int *oldTryCounter = NULL;

    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {

        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Forget any pending QueryCancel request */
        t_thrd.int_cxt.QueryCancelPending = false;
        disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false; /* again in case timeout occurred */

        /* Report the error to the client and/or server log */
        EmitErrorReport();

        (void)MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);

        FlushErrorState();

        LIBCOMM_ELOG(ERROR, "Receiver Loop encounter ERROR level ereport which is unsupported.");
    }

    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    t_thrd.int_cxt.ImmediateInterruptOK = false;

    thread_start_time = thread_last_check_time = thread_end_time = mc_timers_us();
    /* loop forever on waiting for epoll events */
    for (;;) {
        /* if the datanode will shutdown, thread should exit */
        if (g_instance.comm_cxt.reqcheck_cxt.g_shutdown_requested) {
            mc_poller_term(t_thrd.comm_cxt.g_libcomm_poller_list);
            if (g_instance.comm_cxt.g_receivers->server_listen_conn.socket >= 0) {
                mc_tcp_close(g_instance.comm_cxt.g_receivers->server_listen_conn.socket);
            }
            break;
        }

        gs_flow_thread_time("(r|recv loop)\tgs_receivers_loop",
            &thread_start_time, thread_end_time, &thread_last_check_time, &thread_work_all_time, selfid + GS_RECV_LOOP);
        commReceiverLoop(selfid, &thread_end_time, &epoll_timeout_count);
    }

    /* All done, go away */
    proc_exit(0);
}

/*
 * Description: forkexec routine for the communicator process.
 *                    Format up the arglist, then fork and exec.
 *
 * Returns: ThreadId
 */
ThreadId commSenderFlowForkexec(void)
{
    return initialize_util_thread(COMM_SENDERFLOWER);
}

ThreadId commReceiverFlowForkexec(void)
{
    return initialize_util_thread(COMM_RECEIVERFLOWER);
}

ThreadId commAuxiliaryForkexec(void)
{
    return initialize_util_thread(COMM_AUXILIARY);
}

ThreadId commReceiverForkexec(void* tid)
{
    return initialize_util_thread(COMM_RECEIVER, tid);
}

void comm_receivers_comm_init()
{
    int error = 0;

    /* initialize libcomm address storage */
    error = mc_tcp_addr_init(g_instance.comm_cxt.localinfo_cxt.g_local_host,
        g_instance.comm_cxt.g_receivers->server_listen_conn.port,
        &(g_instance.comm_cxt.g_receivers->server_listen_conn.ss),
        &(g_instance.comm_cxt.g_receivers->server_listen_conn.ss_len));

    /* intialize libcomm receivers socket */
    if (error == 0) {
        g_instance.comm_cxt.g_receivers->server_listen_conn.socket = g_libcomm_adapt.listen();
    }
    if (error != 0 || g_instance.comm_cxt.g_receivers->server_listen_conn.socket < 0) {
        ereport(FATAL, (errmsg("(r|receiver init)\tFailed to init receiver listen socket:%s.", mc_strerror(errno))));
    }
    error = g_instance.comm_cxt.quota_cxt.g_quota_changing->init();  // initiaize quota notification semaphore
    if (error != 0) {
        ereport(FATAL, (errmsg("(r|receiver init)\tFailed to init receiver semaphore:%s.", mc_strerror(errno))));
    }
}

/*
 * Description: Main entry point for libcomm sender flow thread, to be called from the postmaster.
 *
 * Returns: ThreadId
 */
ThreadId startCommSenderFlow(void)
{
    ThreadId comm_sender_flower_pid;

    comm_sender_flow_init();

#ifdef EXEC_BACKEND
    switch ((comm_sender_flower_pid = commSenderFlowForkexec())) {
#else
    switch ((comm_sender_flower_pid = fork_process())) {
#endif
        case (ThreadId)-1:
            ereport(LOG, (errmsg("could not fork comm sender flower process: %m")));
            return 0;
#ifndef EXEC_BACKEND
        case 0:
            /* the code below is executing in postmaster child ... */
            /* Close the postmaster's sockets */
            ClosePostmasterPorts(false);

            /* Lose the postmaster's on-exit routines */
            on_exit_reset();

            commSenderFlowMain(0, NULL);
            return 0;
#endif
        default:
            return comm_sender_flower_pid;
    }
}

/*
 * Description: Main entry point for libcomm receiver flow thread, to be called from the postmaster.
 *
 * Returns: ThreadId
 */
ThreadId startCommReceiverFlow()
{
    ThreadId comm_receiver_flower_pid;

    comm_receivers_comm_init();
    comm_receiver_flow_init(g_instance.comm_cxt.g_receivers->server_ctrl_tcp_port);

#ifdef EXEC_BACKEND
    switch ((comm_receiver_flower_pid = commReceiverFlowForkexec())) {
#else
    switch ((comm_receiver_flower_pid = fork_process())) {
#endif
        case (ThreadId)-1:
            ereport(LOG, (errmsg("could not fork comm sender flower process: %m")));
            return 0;
#ifndef EXEC_BACKEND
        case 0:
            /* the code below is executing in postmaster child ... */
            /* Close the postmaster's sockets */
            ClosePostmasterPorts(false);

            /* Lose the postmaster's on-exit routines */
            on_exit_reset();

            commReceiverFlowMain();
            return 0;
#endif
        default:
            return comm_receiver_flower_pid;
    }
}

/*
 * Description: Main entry point for libcomm auxiliary thread, to be called from the postmaster.
 *
 * Returns: ThreadId
 */
ThreadId startCommAuxiliary()
{
    ThreadId comm_auxiliary_pid;

#ifdef EXEC_BACKEND
    switch ((comm_auxiliary_pid = commAuxiliaryForkexec())) {
#else
    switch ((comm_auxiliary_pid = fork_process())) {
#endif
        case (ThreadId)-1:
            ereport(LOG, (errmsg("could not fork comm auxiliary flower process: %m")));
            return 0;
#ifndef EXEC_BACKEND
        case 0:
            /* the code below is executing in postmaster child ... */
            /* Close the postmaster's sockets */
            ClosePostmasterPorts(false);

            /* Lose the postmaster's on-exit routines */
            on_exit_reset();
            commAuxiliaryMain();
            return 0;
#endif
        default:
            return comm_auxiliary_pid;
    }
}

/*
 * Description: Main entry point for libcomm receiver thread, to be called from the postmaster.
 *
 * Returns: ThreadId
 */
ThreadId startCommReceiver(int* tid)
{
    ThreadId comm_receiver_pid;

#ifdef EXEC_BACKEND
    switch ((comm_receiver_pid = commReceiverForkexec(tid)))
#else
    switch ((comm_receiver_pid = fork_process()))
#endif
    {
        case (ThreadId)-1:
            ereport(LOG, (errmsg("could not fork comm receiver process: %m")));
            return 0;
#ifndef EXEC_BACKEND
        case 0:
            /* the code below is executing in postmaster child ... */
            /* Close the postmaster's sockets */
            ClosePostmasterPorts(false);

            /* Lose the postmaster's on-exit routines */
            on_exit_reset();

            commReceiverMain(0, NULL);
            return 0;
#endif
        default:
            return comm_receiver_pid;
    }
}

void startCommReceiverWorker(ThreadId* threadid)
{
    LIBCOMM_PTHREAD_MUTEX_INIT(g_instance.comm_cxt.pollers_cxt.g_r_libcomm_poller_list_lock, 0);

    /* intialize broker(receiver) thread  to do receive */
    int* thd_receiver_id = NULL;
    int error = 0;
    const int max_retry = 10000;
    uint64 thd_receiver_id_size = (uint64)(g_instance.comm_cxt.counters_cxt.g_recv_num * sizeof(int));
    LIBCOMM_MALLOC(thd_receiver_id, thd_receiver_id_size, int);

    if (NULL == thd_receiver_id) {
        ereport(FATAL, (errmsg("(r|receiver init)\tFailed to init thd_receiver_id:%s.", strerror(errno))));
    }

    for (int i = 0; i < g_instance.comm_cxt.counters_cxt.g_recv_num; i++) {
        error = g_instance.comm_cxt.pollers_cxt.g_libcomm_receiver_poller_list[i].init();
        if (error < 0) {
            ereport(FATAL, (errmsg("(r|receiver init)\tFailed to init poller list:%s.", strerror(errno))));
        }

        thd_receiver_id[i] = i;

        /* if this is the first thread, it will listen and accept libcomm connection */
        if (i == 0) {
            struct sock_id ltk_fd_id = {g_instance.comm_cxt.g_receivers->server_listen_conn.socket, 0};

            /* save the libcomm socket & socket version to hash table */
            (void)gs_update_fd_to_htab_socket_version(&ltk_fd_id);

            /* add fd to poller cabinet */
            error = g_instance.comm_cxt.pollers_cxt.g_libcomm_receiver_poller_list[i].add_fd(&ltk_fd_id);
            if (error < 0) {
                ereport(FATAL,
                    (errmsg("(r|receiver init)\tFailed to add libcomm "
                            "listen socket[%d] and version[%d] to poller list.",
                        ltk_fd_id.fd,
                        ltk_fd_id.id)));
            }
        }
    }

    bool need_retry = false;
    int try_cnt = 0;

    /*
     * create receiver threads success with 2 conditions:
     * 1. get correct thread id, means receiver get thread resources
     * 2. set thd_receiver_id[i] = -1, means receiver thread[i] has be scheduled
     */
    for (;;) {
        need_retry = false;
        try_cnt++;

        /* first, create libcomm receiver threads parallel */
        for (int i = 0; i < g_instance.comm_cxt.counters_cxt.g_recv_num; i++) {
             if (threadid[i] == 0) {
                threadid[i] = startCommReceiver(thd_receiver_id + i);
            }
        }

        /* second, check create result and thread value */
        for (int i = 0; i < g_instance.comm_cxt.counters_cxt.g_recv_num; i++) {
            if (threadid[i] == 0 || thd_receiver_id[i] != -1) {
                need_retry = true;
                break;
            }
        }

        if (!need_retry) {
            break;
        }

        if (try_cnt > max_retry) {
            for (int i = 0; i < g_instance.comm_cxt.counters_cxt.g_recv_num; i++) {
                if (threadid[i] == 0) {
                    ereport(LOG, (errmsg("(r|receiver init)\tLibcomm receiver thread[%d] create fail.", i)));
                } else if (thd_receiver_id[i] != -1) {
                    ereport(LOG, (errmsg("(r|receiver init)\tLibcomm receiver thread[%d] create success, "
                        "but it's not scheduled for a long time, which may be caused by insufficient resources.", i)));
                }
            }
            ereport(FATAL, (errmsg("(r|receiver init)\tLibcomm init receiver threads fail, wait(10s) timeout, "
                "please check the machine resource usage, and try again later.")));
        }

        /* wait until 1000 * 10000 = 10s */
        (void)usleep(1000);
    }

    LIBCOMM_FREE(thd_receiver_id, thd_receiver_id_size);

    return;
}

