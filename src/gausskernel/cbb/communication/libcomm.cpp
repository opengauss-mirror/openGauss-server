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
 * libcomm.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/libcomm.cpp
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

#include "sctp_core/mc_sctp.h"
#include "sctp_core/mc_tcp.h"
#include "sctp_core/mc_poller.h"
#include "sctp_utils/sctp_platform.h"
#include "sctp_utils/sctp_thread.h"
#include "sctp_utils/sctp_lqueue.h"
#include "sctp_utils/sctp_queue.h"
#include "sctp_utils/sctp_lock_free_queue.h"
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

#ifdef ENABLE_UT
#define static
#endif

/*
 * we need to notify peer process when
 * assert failed so we can get the state
 * of both sender and receiver
 */
#define LIBCOMM_ASSERT(condition, nidx, sidx, node_role) gs_libcomm_handle_assert(condition, nidx, sidx, node_role)
#define IS_LOCAL_HOST(host) (strcmp(host, "*") == 0 || strcmp(host, "localhost") == 0 || strcmp(host, "0.0.0.0") == 0)

#define LIBCOMM_INTERFACE_END(wakeup_abnormal, immediate_interrupt)            \
    do {                                                                       \
        if (((wakeup_abnormal) || (immediate_interrupt)) &&                    \
            (t_thrd.postmaster_cxt.ProcessStartupPacketForLogicConn == false)) \
            CHECK_FOR_INTERRUPTS();                                            \
        t_thrd.int_cxt.ImmediateInterruptOK = (immediate_interrupt);           \
    } while (0)

#define IS_NOTIFY_REMOTE(reason) \
    ((reason) != ECOMMSCTPTCPDISCONNECT && (reason) != ECOMMSCTPREMOETECLOSE && (reason) != ECOMMSCTPREJECTSTREAM)

#define DEBUG_QUERY_ID (likely(u_sess == NULL) ? t_thrd.comm_cxt.debug_query_id : u_sess->debug_query_id)

static const int INVALID_SOCK = -1;
#define CHECKCONNSTATTIMEOUT 5
#define IPC_MSG_LEN 4

#ifndef MS_PER_S
#define MS_PER_S 1000
#endif

// the beginnig of global variable definition
static int g_print_interval_time = 60000;  // 60s
static int g_ackchk_time = 2000;           // default 2s

static knl_session_context g_comm_session;

// reload hba.conf
static const int RELOAD_HBA_RETRY_COUNT = 10;
static const int WAIT_SLEEP_200MS = 200000;

/* hash tables */
/* hash table to keep:  ip + port -> connection status */
static HTAB* g_htab_ip_state = NULL;
pthread_mutex_t g_htab_ip_state_lock;

/* hash table to keep: nodename -> node id */
static HTAB* g_htab_nodename_node_idx = NULL;
pthread_mutex_t g_htab_nodename_node_idx_lock;
static int g_nodename_count = 0;

/* hash table to keep: fd_id -> node id */
static HTAB* g_htab_fd_id_node_idx = NULL;
pthread_mutex_t g_htab_fd_id_node_idx_lock;

/* for wake up thread */
static HTAB* g_htab_tid_poll = NULL;
pthread_mutex_t g_htab_tid_poll_lock;

/* at receiver and sender: hash table to keep: socket -> socket version number, for socket management */
static HTAB* g_htab_socket_version = NULL;
pthread_mutex_t g_htab_socket_version_lock;

static ArrayLockFreeQueue<char> g_memory_pool_queue;

// some error definition for receiver
static const int RECV_MEM_ERROR = -2;
static const int RECV_NET_ERROR = -1;
#define RECV_NEED_RETRY 0

#define THREAD_FREE_TIME_10S 10000000
#define THREAD_INTSERVAL_60S 60000000 
#define THREAD_WORK_PERCENT 0.8

#define MSG_HEAD_MAGIC_NUM 0x9D
#define MSG_HEAD_MAGIC_NUM2 0x3E

#define MSG_HEAD_TEMP_CHECKSUM 0xCE3BA6CE

gsocket gs_invalid_gsock = {0, 0, 0, 0};

// if a big then b return a-b, else return 0
#define ABS_SUB(a, b) (((a) > (b)) ? ((a) - (b)) : 0)

#define STREAM_SCAN_FINISH 'F'
#define STREAM_SCAN_WAIT 'W'
#define STREAM_SCAN_DATA 'D'

// declarations of functions
static int gs_update_fd_to_htab_socket_version(struct sock_id* fd_id);
static void gs_s_close_bad_ctrl_tcp_sock(struct sock_id* fd_id, int close_reason, bool clean_epoll, int node_idx);
static void gs_r_close_bad_ctrl_tcp_sock(struct sock_id* fd_id, int close_reason);
static void gs_s_close_bad_data_socket(struct sock_id* fd_id, int close_reason, int node_idx);
static void gs_r_close_bad_data_socket(int node_idx, sock_id fd_id, bool is_lock);

static void gs_senders_struct_set();
static void gs_receivers_struct_set(int ctrl_port, int data_port);
static void gs_pmailbox_init();
static void gs_cmailbox_init();
static bool gs_mailbox_build(int idx);
static void gs_mailbox_destory(int idx);

static int gs_poll_create();
void gs_poll_close();
static int gs_poll(int time_out);
static void gs_poll_signal(binary_semaphore* semaphore);
static void gs_broadcast_poll();

static int gs_get_node_idx(char* node_name);
static void gs_s_close_logic_connection(struct p_mailbox* pmailbox, int close_reason, FCMSG_T* msg);
static void gs_r_close_logic_connection(struct c_mailbox* cmailbox, int close_reason, FCMSG_T* msg);
static int gs_s_close_stream(gsocket* gsock);
static int gs_r_close_stream(gsocket* gsock);
static void gs_libcomm_handle_assert(bool condition, int nidx, int sidx, int node_role);
static int gs_accept_data_conntion(struct iovec* iov, sock_id fd_id);
static void gs_accept_ctrl_conntion(struct sock_id* t_fd_id, struct FCMSG_T* fcmsgr);

static void gs_s_build_reply_conntion(libcommaddrinfo* addr_info, int remote_version);
static int gs_r_build_reply_conntion(FCMSG_T* fcmsgr, int local_version);

static int gs_s_build_tcp_ctrl_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx, bool is_reply);
static bool gs_s_check_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx, bool is_reply, int type);
extern bool executorEarlyStop();
static void gs_online_change_capacity();
static int gs_tcp_write_noblock(int node_idx, int sock, const char* msg, int msg_len, int *send_count);
static void gs_set_reply_sock(int node_idx);
static int gs_reload_hba(int fd, sockaddr ctrl_client);

extern GlobalNodeDefinition* global_node_definition;

extern knl_instance_context g_instance;

ThreadId startCommSenderFlow(void);
ThreadId startCommReceiverFlow();
ThreadId startCommAuxiliary();
ThreadId startCommReceiver(int* tid);
void startCommReceiverWorker(ThreadId* threadid);

// function implentations
//
void gs_set_debug_mode(bool mod)
{
    set_debug_mode(mod);
}  // gs_set_debug_mode

void gs_set_timer_mode(bool mod)
{
    set_timer_mode(mod);
}  // gs_set_timer_mode

void gs_set_stat_mode(bool mod)
{
    set_stat_mode(mod);
}  // gs_set_stat_mode

void gs_set_no_delay(bool mod)
{
    set_no_delay(mod);
}  // gs_set_no_delay

void gs_set_ackchk_time(int mod)
{
    g_ackchk_time = mod;
}

void gs_set_libcomm_used_rate(int rate)
{
    if (g_instance.comm_cxt.localinfo_cxt.g_libcomm_used_rate != NULL) {
        g_instance.comm_cxt.localinfo_cxt.g_libcomm_used_rate[POSTMASTER] = rate;
    }
}
void init_libcomm_cpu_rate()
{
    int i;
    int g_recv_num = g_instance.comm_cxt.counters_cxt.g_recv_num;
    
    if (g_instance.comm_cxt.localinfo_cxt.g_libcomm_used_rate == NULL) {
        LIBCOMM_MALLOC
            (g_instance.comm_cxt.localinfo_cxt.g_libcomm_used_rate, (sizeof(int)*(g_recv_num + GS_RECV_LOOP)), int);
        if (g_instance.comm_cxt.localinfo_cxt.g_libcomm_used_rate == NULL) {
            ereport(FATAL, (errmsg("(s|libcomm_cpu_rate init)\tFailed to malloc g_libcomm_used_rate.")));
        }
    }

    if (g_instance.comm_cxt.localinfo_cxt.g_libcomm_used_rate != NULL) {
        for (i = 0; i < g_recv_num + GS_RECV_LOOP; i++) {
            g_instance.comm_cxt.localinfo_cxt.g_libcomm_used_rate[i] = 0;
        }
    }
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

#ifdef LIBCOMM_SPEED_TEST_ENABLE
#define LIBCOMM_PERFORMANCE_PLAN_ID 1
#define LIBCOMM_PERFORMANCE_PN_ID 0XF000

void gs_set_test_thread_num(int newval)
{
    g_instance.comm_cxt.tests_cxt.libcomm_test_thread_num = newval;
}

void gs_set_test_msg_len(int newval)
{
    g_instance.comm_cxt.tests_cxt.libcomm_test_msg_len = newval;
}

void gs_set_test_send_sleep(int newval)
{
    g_instance.comm_cxt.tests_cxt.libcomm_test_send_sleep = newval;
}

void gs_set_test_send_once(int newval)
{
    g_instance.comm_cxt.tests_cxt.libcomm_test_send_once = newval;
}

void gs_set_test_recv_sleep(int newval)
{
    g_instance.comm_cxt.tests_cxt.libcomm_test_recv_sleep = newval;
}

void gs_set_test_recv_once(int newval)
{
    g_instance.comm_cxt.tests_cxt.libcomm_test_recv_once = newval;
}
#endif

#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
void gs_set_fault_injection(int newval)
{
    set_comm_fault_injection(newval);
}

static int g_comm_fault_injection = 0;
void set_comm_fault_injection(int type)
{
    if (type <= LIBCOMM_FI_MAX) {
        g_comm_fault_injection = type;
    } else {
        LIBCOMM_ELOG(WARNING, "[FAULT INJECTION] invalid fault injection type, %d.", type);
    }
}

/*
 * function name : is_comm_fault_injection
 * description   : Determine whether to perform fault injection
 * arguments     :  type: the type of fault injection.
 * return value  : true: FI succeed
 *                false:
 */
bool is_comm_fault_injection(LibcommFaultInjection type)
{
    // random Fault injection.
    // the value from -10 to -1.
    if (g_comm_fault_injection < 0) {
        int prob = g_comm_fault_injection * (-10);  // so the prob is [10, 20,30,,,100]
        int r = 0;

        // we use rand() to obtain random number.
        srand((unsigned)time(0));     // set random seed
        r = rand();                   // obtain random number
        r = (r >= 0) ? r : r * (-1);  // get positive number
        r = r % 100;                  // now, the range of r is (0,100)

        if (r <= prob) { // FI has (prob/100)% chance to happen.
            return true;
        } else {
            return false;
        }
    }

    if (g_comm_fault_injection == type) {
        return true;
    } else {
        return false;
    }
}
#endif

/*
 * function name    : gs_change_capacity
 * description      : If GUC parameter "comm_max_datanode" changed this function will be called.
 * notice           : Only for postmaster thread.
 * arguments        :
 *                __in newval: new value (sum of CN and DN).
 */
void gs_change_capacity(int new_node_num)
{
    /* Only postmaster thread can change the g_expect_node_num,
     * "g_cur_node_num==0" means postmaster doesn't finish initialization.
     */
    if ((t_thrd.proc_cxt.MyProcPid != PostmasterPid) ||
        (new_node_num == g_instance.comm_cxt.counters_cxt.g_cur_node_num) ||
        (g_instance.comm_cxt.counters_cxt.g_cur_node_num == 0)) {
        return;
    }

    /* range for node_num (2,4096) */
    if ((new_node_num > MAX_CN_DN_NODE_NUM) || (new_node_num < MIN_CN_DN_NODE_NUM)) {
        LIBCOMM_ELOG(WARNING, "(pm|change capacity)\tInvalidate node num: %d.", new_node_num);
        return;
    }

    g_instance.comm_cxt.counters_cxt.g_expect_node_num = new_node_num;
    g_instance.comm_cxt.quota_cxt.g_quota_changing->post();
    LIBCOMM_ELOG(LOG,
        "(pm|change capacity)\tg_cur_node_num [%d], g_expect_node_num [%d].",
        g_instance.comm_cxt.counters_cxt.g_cur_node_num,
        g_instance.comm_cxt.counters_cxt.g_expect_node_num);
}

/*
 * function name    : gs_online_change_capacity
 * description      : For database expansion or shrinkage.
 * notice           : !!Curently, we only support datanode expansion.
 * arguments        :
 *                __in expected_node_num: Nodes number(CN+DN) that user wants to change to.
 */
static void gs_online_change_capacity()
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

int gs_get_cur_node()
{
    return (g_instance.comm_cxt.counters_cxt.g_cur_node_num != 0)
               ? g_instance.comm_cxt.counters_cxt.g_cur_node_num
               : (g_instance.attr.attr_network.MaxCoords + u_sess->attr.attr_network.comm_max_datanode);
}

static void gs_set_local_host(const char* host)
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

uint64 gs_get_recv_ready_time()
{
    return g_instance.comm_cxt.localinfo_cxt.g_r_first_recv_time;
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

static void gs_receivers_struct_set(int ctrl_port, int data_port)
{
    g_instance.comm_cxt.g_receivers->server_ctrl_tcp_port = ctrl_port;
    g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.port = data_port;
    g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.ss_len = 0;
    g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.socket = -1;
    g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.socket_id = -1;
    g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.assoc_id = 0;

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
static void gs_receivers_struct_init(int ctrl_port, int data_port)
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
static void gs_set_kerberos_keyfile()
{
    int path_len = 0;

    if (u_sess->attr.attr_security.pg_krb_server_keyfile == NULL) {
        return;
    }

    path_len = strlen(u_sess->attr.attr_security.pg_krb_server_keyfile);
    if (path_len <= 0 || path_len >= (INT_MAX - 1)) {
        return;
    }

    LIBCOMM_MALLOC(g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile, (path_len + 1), char);
    if (g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile != NULL) {
        errno_t ss_rc = strcpy_s(g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile,
            path_len + 1,
            u_sess->attr.attr_security.pg_krb_server_keyfile);
        securec_check(ss_rc, "\0", "\0");
    }
}

#if ENABLE_MULTIPLE_NODES
/* sender initialize sctp socket for sending data */
static int gs_s_sender_sock_init(int node_idx, libcommaddrinfo* libcomm_addrinfo)
{
    struct sock_id fd_id = {-1, -1};

    // close old socket, if not, fd and port may be leaked
    if (g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket >= 0) {
        fd_id.fd = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket;
        fd_id.id = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id;
        gs_s_close_bad_data_socket(&fd_id, ECOMMSCTPSCTPDISCONNECT, node_idx);
        LIBCOMM_ELOG(WARNING,
            "(s|sender socket init)\tClose old socket[%d,%d].",
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket,
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id);
    }

    if (gs_s_sender_conn_init(node_idx, libcomm_addrinfo) < 0) {
        return -1;
    }

    fd_id.fd = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket;
    fd_id.id = 0;
    if (gs_update_fd_to_htab_socket_version(&fd_id) < 0) {
        gs_s_close_bad_data_socket(&fd_id, ECOMMSCTPSCTPDISCONNECT, node_idx);
        LIBCOMM_ELOG(WARNING, "(s|sender socket init)\tFailed to save socket[%d,%d], close it.", fd_id.fd, fd_id.id);
        return -1;
    }

    g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id = fd_id.id;

    if (gs_map_sock_id_to_node_idx(fd_id, node_idx) < 0) {
        LIBCOMM_ELOG(WARNING, "(s|sender socket init)\tFailed to save sock and sockid.");
        gs_s_close_bad_data_socket(&fd_id, ECOMMSCTPSCTPDISCONNECT, node_idx);
        return -1;
    }

    /* set reply socket for g_r_node_sock */
    gs_set_reply_sock(node_idx);

    return 0;
}

int libcomm_sctp_listen()
{
    return mc_sctp_server_init(&(g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.ss),
        g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.ss_len);
    return 0;
}

static int libcomm_sctp_send(LibcommSendInfo* send_info)
{
    int sock = send_info->socket;
    int sock_id = send_info->socket_id;
    int version = send_info->version;
    int streamid = send_info->streamid;
    int node_idx = send_info->node_idx;
    int msg_len = send_info->msg_len;
    char* msg = send_info->msg;
    int flag = MSG_NOSIGNAL;
    const int order = 1;
    int error = -1;

    LIBCOMM_PTHREAD_RWLOCK_RDLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);

    /* if socket saved before mismatch the socket in global variable
     * thsi socket may has been closed by other thread and reused
     * return -1 in this case to prvent send msg to wrong node
     */
    if ((sock != g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket) ||
        (sock_id != g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id)) {
        LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
        return -1;
    }

    for (;;) {
        error = mc_sctp_send(sock, streamid, order, 0, flag, NULL, msg, version, msg_len);
        if (error > 0) {
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].comm_bytes += error;
        }
        g_instance.comm_cxt.g_senders->sender_conn[node_idx].comm_count += 1;

        if (error == 0) {
            (void)usleep(100);
        } else {
            break;
        }
    }

    LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);

    return error;
}

static int libcomm_sctp_send_block_mode(LibcommSendInfo* send_info)
{
    int sock = send_info->socket;
    int node_idx = send_info->node_idx;
    int msg_len = send_info->msg_len;
    char* msg = send_info->msg;
    int error = -1;

    LIBCOMM_PTHREAD_RWLOCK_RDLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
    error = mc_sctp_send_block_mode(sock, msg, msg_len);
    LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);

    return error;
}

/*
 * function name    : libcomm_build_sctp_connection
 * description    : sender check and build sctp connection to receiver
 * notice        : we must get g_instance.comm_cxt.g_senders->sender_conn lock before!
 * arguments        :
 *                   _in_ sctp_addrinfo: remote infomation.
 *                   _in_ node_idx: remote node index.
 * return value :
 *                   -1: failed.
 *                   0: succeed.
 */
static int libcomm_build_sctp_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx)
{
    struct sock_id fd_id = {-1, -1};
    ip_key addr;
    int msg_len = 0;
    int error = -1;
    int try_times = REPEAT;
    errno_t ss_rc = 0;
    uint32 cpylen;

    LIBCOMM_PTHREAD_RWLOCK_WRLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
    // step1: check if the data connection is ok
    //
    // the connection is built already
    if (g_instance.comm_cxt.g_senders->sender_conn[node_idx].assoc_id != 0) {
        // check if the connection is ok
        // if not, close the bad socket, then continue to build a new connection
        // if ok, just return
        //
        if (mc_sctp_check_socket(g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket) != 0) {
            fd_id.fd = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket;
            fd_id.id = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id;
            LIBCOMM_ELOG(WARNING,
                "(s|build sctp connection)\tFailed to check sctp socket "
                "node[%d]:%s, errno[%d]:%s, close socket[%d,%d].",
                node_idx,
                g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
                errno,
                mc_strerror(errno),
                fd_id.fd,
                fd_id.id);
            gs_s_close_bad_data_socket(&fd_id, ECOMMSCTPSCTPDISCONNECT, node_idx);
        } else {
            COMM_DEBUG_LOG("(s|build sctp connection)\tAlready has sctp connection for node[%d]:%s.",
                node_idx,
                g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename);
            LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
            return 0;
        }
    }

    // step 2: initialize local variables
    //
    struct libcomm_connect_package connect_package;
    connect_package.type = SCTP_PKG_TYPE_CONNECT;
    connect_package.magic_num = MSG_HEAD_MAGIC_NUM2;
    ss_rc = strcpy_s(connect_package.node_name, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strcpy_s(connect_package.host, HOST_ADDRSTRLEN, g_instance.comm_cxt.localinfo_cxt.g_local_host);
    securec_check(ss_rc, "\0", "\0");
    msg_len = sizeof(struct libcomm_connect_package);

    // initialize remote sctp address
    int ss_len = 0;
    struct sockaddr_storage to_ss;
    ss_rc = memset_s(&to_ss, sizeof(sockaddr_storage), 0x0, sizeof(to_ss));
    securec_check(ss_rc, "\0", "\0");

    (void)mc_sctp_addr_init(libcomm_addrinfo->host, libcomm_addrinfo->sctp_port, &to_ss, &ss_len);

    do {
        // we init socket here
        if (gs_s_sender_sock_init(node_idx, libcomm_addrinfo) != 0) {
            errno = ECOMMSCTPSCTPADRINIT;
            LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
            return -1;
        }
        // step 3: connect to destination
        //
        error = mc_sctp_connect(
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket, &to_ss, sizeof(struct sockaddr));
        if (error != 0) {
            LIBCOMM_ELOG(WARNING,
                "(s|build sctp connection)\tFailed to build sctp connection "
                "to %s:%d for node[%d]:%s, error[%d:%d]:%s.",
                libcomm_addrinfo->host,
                libcomm_addrinfo->sctp_port,
                node_idx,
                g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
                error,
                errno,
                mc_strerror(errno));
            continue;
        }
        // step 4: send node name to remote
        //
        error = mc_sctp_send_block_mode(
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket, (char*)&connect_package, msg_len);
        if (error <= 0) {
            LIBCOMM_ELOG(WARNING,
                "(s|build sctp connection)\tFailed to send assoc id to %s:%d "
                "for node[%d]:%s on socket[%d].",
                libcomm_addrinfo->host,
                libcomm_addrinfo->sctp_port,
                node_idx,
                g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
                g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket);
            continue;
        }
        // step 5: receive the ack message
        //
        // block mode
        struct libcomm_accept_package ack_msg;
        error = mc_sctp_recv_block_mode(
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket, (char*)&ack_msg, sizeof(ack_msg));
        // if failed, we close the bad one and retry for 10 times
        if (error != 0 || ack_msg.result != 1 || ack_msg.type != SCTP_PKG_TYPE_ACCEPT) {
            LIBCOMM_ELOG(WARNING,
                "(s|build sctp connection)\tFailed to recv assoc id from %s:%d "
                "for node[%d]:%s on socket[%d].",
                libcomm_addrinfo->host,
                libcomm_addrinfo->sctp_port,
                node_idx,
                g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
                g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket);
            continue;
        }

        /* Client side gss kerberos authentication for sctp connection. */
        if (g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile != NULL &&
            GssClientAuth(g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket, libcomm_addrinfo->host) < 0) {
            LIBCOMM_ELOG(WARNING,
                "(s|connect)\tData channel GSS authentication failed, "
                "remote:%s[%s:%d]:%s.",
                g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
                libcomm_addrinfo->host,
                libcomm_addrinfo->sctp_port,
                mc_strerror(errno));
            errno = ECOMMSCTPGSSAUTHFAIL;
            break;
        } else {
            COMM_DEBUG_LOG("(s|connect)\tData channel GSS authentication SUCC, "
                           "remote:%s[%s:%d].",
                g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
                libcomm_addrinfo->host,
                libcomm_addrinfo->sctp_port);
        }

        // step 6: if everything is ok, we label the connection is built successfully
        //
        g_instance.comm_cxt.g_senders->sender_conn[node_idx].assoc_id = 1;
        LIBCOMM_ELOG(LOG,
            "(s|build sctp connection)\tSucceed to connect with socket[%d:%d] %s:%d for node[%d]:%s.",
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket,
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id,
            libcomm_addrinfo->host,
            libcomm_addrinfo->sctp_port,
            node_idx,
            REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));
        cpylen = comm_get_cpylen(libcomm_addrinfo->host, HOST_LEN_OF_HTAB);
        ss_rc = memset_s(addr.ip, HOST_LEN_OF_HTAB, 0x0, HOST_LEN_OF_HTAB);
        securec_check(ss_rc, "\0", "\0");
        ss_rc = strncpy_s(addr.ip, HOST_LEN_OF_HTAB, libcomm_addrinfo->host, cpylen + 1);
        securec_check(ss_rc, "\0", "\0");
        addr.ip[cpylen] = '\0';

        addr.port = libcomm_addrinfo->sctp_port;
        /* update connection state to succeed when connect succeed */
        gs_update_connection_state(addr, CONNSTATESUCCEED, true, node_idx);

        LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
        return 0;
    } while (try_times-- > 0);

    // Failed to build sctp connection
    fd_id.fd = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket;
    fd_id.id = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id;
    gs_s_close_bad_data_socket(&fd_id, ECOMMSCTPSCTPDISCONNECT, node_idx);
    LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
    return -1;
}  // gs_s_build_sctp_connection

static int mc_sctp_handle_notification(struct msghdr* inmessage)
{
    int re = 0;
    union comm_notification* sn = (union comm_notification*)inmessage->msg_iov[0].iov_base;
    struct iovec* iov = inmessage->msg_iov;
    int msg_len = iov->iov_len;

    switch (sn->sn_header.sn_type) {
        case COMM_SHUTDOWN_EVENT:
            mc_sctp_print_message(inmessage, msg_len);
            break;
        case COMM_ADDR_CHANGE:
            switch (sn->conn_change.conn_state) {
                case COMM_LOST:
                case COMM_SHUTDOWN:
                    mc_sctp_print_message(inmessage, msg_len);
                    re = -1;
                    break;
                case COMM_RESTART:
                    mc_sctp_print_message(inmessage, msg_len);
                    break;
                default:
                    break;
            }
            break;
        default:
            break;
    }
    COMM_DEBUG_CALL(mc_sctp_print_message(inmessage, msg_len));

    return re;
}

static int libcomm_sctp_recv(LibcommRecvInfo* recv_info)
{
    int sock = recv_info->socket;
    int flags = MSG_WAITALL;
    int error = -1;
    errno_t rc = 0;

    struct mc_lqueue_item* iov_item = NULL;
    struct iovec* iov = NULL;
    struct msghdr inmessage;
    char incmsg[CMSG_SPACE(sizeof(struct comm_buff_info))];
    rc = memset_s(&inmessage, sizeof(inmessage), 0x0, sizeof(struct msghdr));
    securec_check(rc, "\0", "\0");
    rc = memset_s(incmsg, CMSG_SPACE(sizeof(struct comm_buff_info)), 0x0, CMSG_SPACE(sizeof(struct comm_buff_info)));
    securec_check(rc, "\0", "\0");

    if (0 != libcomm_malloc_iov_item(&iov_item, IOV_DATA_SIZE)) {
        return RECV_MEM_ERROR;
    }
    iov = iov_item->element.data;

    iov->iov_len = IOV_DATA_SIZE;
    inmessage.msg_iov = iov;
    inmessage.msg_iovlen = 1;
    inmessage.msg_control = incmsg;
    inmessage.msg_controllen = sizeof(incmsg);

    error = mc_sctp_recv(sock, &inmessage, flags);  // do receive
    if (error > 0) {
        g_instance.comm_cxt.g_receivers->receiver_conn[recv_info->node_idx].comm_bytes += error;
    }
    g_instance.comm_cxt.g_receivers->receiver_conn[recv_info->node_idx].comm_count += 1;

    // not real network error, it can be resolved by trying again
    if ((error == -1) && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        libcomm_free_iov_item(&iov_item, IOV_DATA_SIZE);
        return RECV_NEED_RETRY;
    }

    // real network errors, we should report it
    // errno is not EAGAIN/EWOULDBLOCK/EINTR
    if (error <= 0) {
        libcomm_free_iov_item(&iov_item, IOV_DATA_SIZE);
        return RECV_NET_ERROR;
    }

    iov->iov_len = error;

    if (MSG_NOTIFICATION & inmessage.msg_flags) {
        error = mc_sctp_handle_notification(&inmessage);

        libcomm_free_iov_item(&iov_item, IOV_DATA_SIZE);

        if (error != 0) {
            return RECV_NET_ERROR;
        } else {
            return RECV_NEED_RETRY;
        }
    }

    recv_info->iov_item = iov_item;
    recv_info->streamid = mc_sctp_get_sid(&inmessage);
    recv_info->version = mc_sctp_get_ppid(&inmessage);

    // iov point must return and save to cmailbox, no need free
    return iov->iov_len;
}

/* sender initialize connection for sending data */
static int gs_s_sender_conn_init(int node_idx, libcommaddrinfo* libcomm_addrinfo)
{
    errno_t ss_rc;
    uint32 cpylen;

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

    g_instance.comm_cxt.g_senders->sender_conn[node_idx].port = libcomm_addrinfo->sctp_port;
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].assoc_id = 0;
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket =
        mc_sctp_client_init(&(g_instance.comm_cxt.g_senders->sender_conn[node_idx].ss),
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].ss_len);
    if (g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket < 0) {
        LIBCOMM_ELOG(WARNING,
            "(s|sender socket init)\tFailed to init socket for node[%d]:%s.",
            node_idx,
            g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename);
        errno = ECOMMSCTPSCTPFDINVAL;
        return -1;
    }
    return 0;
}

#endif

static int libcomm_tcp_listen()
{
    return mc_tcp_listen(g_instance.comm_cxt.localinfo_cxt.g_local_host,
        g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.port,
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
static int libcomm_malloc_iov_item(struct mc_lqueue_item** iov_item, int size)
{
    struct mc_lqueue_item* item = NULL;
    struct iovec* iov = NULL;

    /* get iov_item from memory pool  */
    *iov_item = (struct mc_lqueue_item*)g_memory_pool_queue.pop((char*)iov);
    if (*iov_item != NULL) {
        return 0;
    }

    /*
     * IF libcomm_used_memory + size is more than g_total_usable_memory
     * then errno = ECOMMSCTPMEMALLOC and return -1
     */
    /* if memory pool is empty, malloc iov_item */
    LIBCOMM_MALLOC(iov, sizeof(struct iovec), iovec);
    if (iov == NULL) {
        errno = ECOMMSCTPMEMALLOC;
        return -1;
    }

    LIBCOMM_MALLOC(iov->iov_base, size, void);
    if (iov->iov_base == NULL) {
        LIBCOMM_FREE(iov, sizeof(struct iovec));
        errno = ECOMMSCTPMEMALLOC;
        return -1;
    }
    iov->iov_len = 0;

    LIBCOMM_MALLOC(item, sizeof(struct mc_lqueue_item), mc_lqueue_item);
    if (item == NULL) {
        LIBCOMM_FREE(iov->iov_base, size);
        LIBCOMM_FREE(iov, sizeof(struct iovec));
        errno = ECOMMSCTPMEMALLOC;
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
    rc = g_memory_pool_queue.push((char*)item);
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
            errno = ECOMMSCTPSCTPDISCONNECT;
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
            errno = ECOMMSCTPPEERCHANGED;
            break;
        }

        time_now = mc_timers_ms();
        if (((time_now - time_enter) >
                ((uint64)g_instance.comm_cxt.counters_cxt.g_comm_send_timeout * SEC_TO_MICRO_SEC)) &&
            (time_now > time_enter)) {
            errno = ECOMMSCTPSENDTIMEOUT;
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
        gs_s_close_bad_data_socket(&fd_id, ECOMMSCTPSCTPDISCONNECT, node_idx);
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

static int libcomm_tcp_recv(LibcommRecvInfo* recv_info)
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

static int libcomm_build_tcp_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx);

static LibcommAdaptLayer g_libcomm_adapt;
static void gs_init_adapt_layer()
{
    if (g_instance.attr.attr_network.comm_tcp_mode) {
        g_libcomm_adapt.recv_data = libcomm_tcp_recv;
        g_libcomm_adapt.send_data = libcomm_tcp_send;
        g_libcomm_adapt.connect = libcomm_build_tcp_connection;
        g_libcomm_adapt.accept = mc_tcp_accept;
        g_libcomm_adapt.listen = libcomm_tcp_listen;
        g_libcomm_adapt.block_send = libcomm_tcp_send;
        g_libcomm_adapt.send_ack = mc_tcp_write_block;
        g_libcomm_adapt.check_socket = mc_tcp_check_socket;

        /*
         * Historical residual problem!
         * comm_control_port and comm_sctp_port is the same,
         * it is well on sctp mode, because we use two protocol.
         * and it is conflict when we only use tcp protocol on tcp mode.
         * so we use sctp_port+1 for data connection for tcp mode.
         */
    } else {
#ifdef ENABLE_MULTIPLE_NODES
        g_libcomm_adapt.recv_data = libcomm_sctp_recv;
        g_libcomm_adapt.send_data = libcomm_sctp_send;
        g_libcomm_adapt.connect = libcomm_build_sctp_connection;
        g_libcomm_adapt.accept = mc_sctp_accept;
        g_libcomm_adapt.listen = libcomm_sctp_listen;
        g_libcomm_adapt.block_send = libcomm_sctp_send_block_mode;
        g_libcomm_adapt.send_ack = mc_sctp_send_block_mode;
        g_libcomm_adapt.check_socket = mc_sctp_check_socket;
#endif
    }
}

static inline bool is_tcp_mode()
{
    return g_libcomm_adapt.recv_data == libcomm_tcp_recv;
}

static void gs_set_comm_session()
{
    g_comm_session.status = KNL_SESS_FAKE;
    g_comm_session.debug_query_id = 0;
    g_comm_session.session_id = 0;

    return;
}

void comm_fill_hash_ctl(HASHCTL* ctl, Size k_size, Size e_size)
{
    ctl->keysize = k_size;
    ctl->entrysize = e_size;
    ctl->hash = tag_hash;
    ctl->hcxt = g_instance.comm_cxt.comm_global_mem_cxt;
    return;
}

void gs_init_hash_table()
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);
    HASHCTL tid_ctl, sock_ver_ctl, sock_id_ctl, nodename_ctl, ipstat_ctl;
    int flags, rc;

    /* init g_htab_tid_poll */
    rc = memset_s(&tid_ctl, sizeof(tid_ctl), 0, sizeof(HASHCTL));
    securec_check(rc, "\0", "\0");

    comm_fill_hash_ctl(&tid_ctl, sizeof(int), sizeof(tid_entry));
    flags = HASH_FUNCTION | HASH_ELEM | HASH_SHRCTX;
    g_htab_tid_poll = hash_create("libcomm tid lookup hash", 65535, &tid_ctl, flags);
    LIBCOMM_PTHREAD_MUTEX_INIT(&g_htab_tid_poll_lock, 0);

    /* init g_htab_socket_version */
    rc = memset_s(&sock_ver_ctl, sizeof(sock_ver_ctl), 0, sizeof(HASHCTL));
    securec_check(rc, "\0", "\0");

    comm_fill_hash_ctl(&sock_ver_ctl, sizeof(int), sizeof(sock_ver_entry));
    flags = HASH_FUNCTION | HASH_ELEM | HASH_SHRCTX;
    g_htab_socket_version = hash_create("libcomm socket version lookup hash", 65535, &sock_ver_ctl, flags);
    LIBCOMM_PTHREAD_MUTEX_INIT(&g_htab_socket_version_lock, 0);

    /* init g_htab_socket_version */
    rc = memset_s(&sock_id_ctl, sizeof(sock_id_ctl), 0, sizeof(HASHCTL));
    securec_check(rc, "\0", "\0");

    comm_fill_hash_ctl(&sock_id_ctl, sizeof(sock_id), sizeof(sock_id_entry));
    flags = HASH_FUNCTION | HASH_ELEM | HASH_SHRCTX;
    g_htab_fd_id_node_idx = hash_create("libcomm socket & node_idx lookup hash", 65535, &sock_id_ctl, flags);
    LIBCOMM_PTHREAD_MUTEX_INIT(&g_htab_fd_id_node_idx_lock, 0);

    /* init g_htab_nodename_node_idx */
    rc = memset_s(&nodename_ctl, sizeof(nodename_ctl), 0, sizeof(HASHCTL));
    securec_check(rc, "\0", "\0");

    comm_fill_hash_ctl(&nodename_ctl, sizeof(char_key), sizeof(nodename_entry));
    flags = HASH_FUNCTION | HASH_ELEM | HASH_SHRCTX;
    g_htab_nodename_node_idx = hash_create("libcomm nodename & node_idx lookup hash", 65535, &nodename_ctl, flags);
    LIBCOMM_PTHREAD_MUTEX_INIT(&g_htab_nodename_node_idx_lock, 0);

    /* init g_htab_socket_version */
    rc = memset_s(&ipstat_ctl, sizeof(ipstat_ctl), 0, sizeof(HASHCTL));
    securec_check(rc, "\0", "\0");

    comm_fill_hash_ctl(&ipstat_ctl, sizeof(ip_key), sizeof(ip_state_entry));
    flags = HASH_FUNCTION | HASH_ELEM | HASH_SHRCTX;
    g_htab_ip_state = hash_create("libcomm ip & status lookup hash", 65535, &ipstat_ctl, flags);
    LIBCOMM_PTHREAD_MUTEX_INIT(&g_htab_ip_state_lock, 0);
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
        "control port[%d], data port[%d], local_host[%s], local_node_name[%s], is_tcp_mode[%d], "
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
        g_instance.attr.attr_network.comm_tcp_mode,
        sock_path,
        g_instance.attr.attr_storage.comm_cn_dn_logic_conn);

    errno_t ss_rc;
    uint32 cpylen;

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
    while (g_memory_pool_queue.initialize(
               (uint32)(g_instance.comm_cxt.commutil_cxt.g_memory_pool_size / IOV_ITEM_SIZE)) != 0) {
        LIBCOMM_ELOG(LOG, "g_memory_pool_queue initialization failed.");
    }

    g_instance.comm_cxt.libcomm_log_timezone = log_timezone;
    g_instance.comm_cxt.g_comm_tcp_mode = g_instance.attr.attr_network.comm_tcp_mode;
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

    // 5 used for g_quotanofify_ratio, quota ratio for dynamic quota changing
    gs_set_quota(g_instance.attr.attr_network.comm_quota_size, 5);

    mc_tcp_set_timeout_param(u_sess->attr.attr_network.PoolerConnectTimeout, u_sess->attr.attr_network.PoolerTimeout);

    // set keepalive parameter
    mc_tcp_set_keepalive_param(u_sess->attr.attr_common.tcp_keepalives_idle,
        u_sess->attr.attr_common.tcp_keepalives_interval,
        u_sess->attr.attr_common.tcp_keepalives_count);

    // set kerberos parameter
    gs_set_kerberos_keyfile();

    gs_init_adapt_layer();

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

void gs_set_hs_shm_data(HaShmemData* ha_shm_data)
{
    (void)atomic_set(&g_instance.comm_cxt.g_ha_shm_data, ha_shm_data);
}

int gs_get_stream_num(void)
{
    return g_instance.comm_cxt.counters_cxt.g_max_stream_num;
}  // gs_get_stream_num

/*
 * function name    : gs_map_sock_id_to_node_idx
 * description        : save the key of fd_id and value of node idx into g_htab_fd_id_node_idx
 * arguments        :
 *                   fd_id: struct of socket and socket id.
 *                   idx: node idx.
 * return value    :
 *                   0: succeed.
 *                   -1: save to htab failed.
 */
int gs_map_sock_id_to_node_idx(const sock_id fd_id, int idx)
{
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_SOCKID_NODEIDX_FAILED)) {
        errno = ECOMMSCTPMEMALLOC;
        LIBCOMM_ELOG(WARNING,
            "(sock_id_to_node_idx)\t[FAULT INJECTION]Failed to save socket[%d,%d] for node[%d].",
            fd_id.fd,
            fd_id.id,
            idx);
        return -1;
    }
#endif

    bool found = false;
    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);

    struct sock_id_entry* entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &fd_id, HASH_ENTER, &found);
    entry_id->entry.val = idx;

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

    return 0;
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
static inline bool gs_check_mailbox(uint16 version1, uint16 version2)
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
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
    t_thrd.int_cxt.ImmediateInterruptOK = TempImmediateInterruptOK;

    return;
}  // gs_clean_cmailbox

/*
 * function name    : gs_test_libcomm_conn
 * description    : we need to check the logic conn state when we reuse it in poller.
 *              moreover, reset the local tid here, cause logic conn from pooler
 *              may belong to other thread before.
 *              as pooler will only provide one logic conn to a specific thread.
 *              so lock is unnecessary here.
 * arguments:
 *                   gs_sock: logic conn addr.
 * return value    :
 *                   true: both pmailbox and cmailbox state is ok.
 *                   false: one of them is changed by other thread.
 */
bool gs_test_libcomm_conn(gsocket* gs_sock)
{
    struct c_mailbox* cmailbox = &C_MAILBOX(gs_sock->idx, gs_sock->sid);
    if (cmailbox->local_version != gs_sock->ver) {
        LIBCOMM_ELOG(LOG,
            "cmailbox version mismatch for node[nid:%d,sid:%d], mailbox_ver:%d gs_sock_ver:%d.",
            gs_sock->idx,
            gs_sock->sid,
            cmailbox->local_version,
            gs_sock->ver);
        return false;
    } else {
        cmailbox->local_thread_id = 0;
    }
    struct p_mailbox* pmailbox = &P_MAILBOX(gs_sock->idx, gs_sock->sid);
    if (pmailbox->local_version != gs_sock->ver) {
        LIBCOMM_ELOG(LOG,
            "pmailbox version mismatch for node[nid:%d,sid:%d], mailbox_ver:%d gs_sock_ver:%d.",
            gs_sock->idx,
            gs_sock->sid,
            pmailbox->local_version,
            gs_sock->ver);
        return false;
    } else {
        pmailbox->local_thread_id = 0;
    }

    if (cmailbox->buff_q->is_empty == 1) {
        return true;
    }

    LIBCOMM_ELOG(LOG,
        "unexpected data on connection to node [nid:%d,sid:%d,ver:%d].",
        gs_sock->idx,
        gs_sock->sid,
        cmailbox->local_version);

    return false;
}  // gs_test_libcomm_conn

static void gs_r_reset_cmailbox(struct c_mailbox* cmailbox, int close_reason)
{
    if (cmailbox == NULL || cmailbox->state == MAIL_CLOSED) {
        return;
    }

    int node_idx = cmailbox->idx;
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
#ifdef SCTP_BUFFER_DEBUG
    cmailbox->buff_q_tmp = mc_lqueue_clear(cmailbox->buff_q_tmp);
#endif
}  // gs_r_reset_cmailbox

static void gs_s_reset_pmailbox(struct p_mailbox* pmailbox, int close_reason)
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

static void gs_mailbox_destory(int idx)
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
static bool gs_mailbox_build(int idx)
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
        errno = ECOMMSCTPMEMALLOC;
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

/*
 * function name    : gs_set_reply_sock
 * description      : set reply socket for g_r_node_sock by compare remote node name
 * arguments          : node_idx, node we want to set reply sock
 */
static void gs_set_reply_sock(int node_idx)
{
    for (int recv_idx = 0; recv_idx < g_instance.comm_cxt.counters_cxt.g_cur_node_num; recv_idx++) {
        if (strcmp(g_instance.comm_cxt.g_r_node_sock[recv_idx].remote_nodename,
                   g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename) == 0) {
            // save reply socket in g_r_node_sock
            g_instance.comm_cxt.g_r_node_sock[recv_idx].lock();
            g_instance.comm_cxt.g_r_node_sock[recv_idx].sctp_reply_sock =
                g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket;
            g_instance.comm_cxt.g_r_node_sock[recv_idx].unlock();
            break;
        }
    }

    return;
}

/*
 * function name    : gs_get_stream_id
 * description      : initialize socket of local senders, for sending
 * notice           : retry 10 times inside if failure happens,
 * arguments      __in node_begin
 *                __in node_end
 */
static void gs_senders_struct_set()
{
#ifdef ENABLE_MULTIPLE_NODES
    int error = 0;
    int i;

    // initialize sender socket and address storage
    for (i = 0; i < MAX_CN_DN_NODE_NUM; i++) {
        // g_s_node_sock
        g_instance.comm_cxt.g_s_node_sock[i].init();

        // initialize sctp address storage
        // initialize socket in gs_connect
        error = mc_sctp_addr_init(g_instance.comm_cxt.localinfo_cxt.g_local_host,
            0,
            &(g_instance.comm_cxt.g_senders->sender_conn[i].ss),
            &(g_instance.comm_cxt.g_senders->sender_conn[i].ss_len));
        if (error != 0) {
            ereport(FATAL,
                (errmsg("(s|sender init)\tFailed to init sender[%d] for %s.",
                    i,
                    g_instance.comm_cxt.localinfo_cxt.g_local_host)));
        }

        // set g_instance.comm_cxt.g_senders->sender_conn AND g_sender_count
        LIBCOMM_PTHREAD_RWLOCK_INIT(&g_instance.comm_cxt.g_senders->sender_conn[i].rwlock, NULL);
        g_instance.comm_cxt.g_senders->sender_conn[i].socket = -1;
        g_instance.comm_cxt.g_senders->sender_conn[i].socket_id = -1;
        g_instance.comm_cxt.g_senders->sender_conn[i].comm_bytes = 0;
        g_instance.comm_cxt.g_senders->sender_conn[i].comm_count = 0;
    }

    return;
#endif
}

/*
 * function name    : gs_update_connection_state
 * description    : update the connections state of htab
 *                  signal all threads block on the connections
 * arguments        : addr: structure of IP & PORT
 *                  result: succeed or failed
 */
void gs_update_connection_state(ip_key addr, int result, bool is_signal, int node_idx)
{
    bool found = false;

    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_ip_state_lock);
    ip_state_entry* entry_poll = (ip_state_entry*)hash_search(g_htab_ip_state, &addr, HASH_FIND, &found);
    if (!found) {
        LIBCOMM_ELOG(WARNING, "(s|connect)\tFail to get connection state:port[%s:%d].", addr.ip, addr.port);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_ip_state_lock);
        Assert(found);
        return;
    }

    COMM_DEBUG_LOG(
        "(s|gs_update_connection_state)\tchange connection [%s:%d] state to %d.", addr.ip, addr.port, result);

    /* cannot update connection state when the node idx mismatch */
    if (entry_poll->entry.val.node_idx != node_idx) {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_ip_state_lock);
        return;
    }

    entry_poll->entry.val.conn_state = result;

    if (is_signal) {
        entry_poll->entry._signal_all();
    }

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_ip_state_lock);
    return;
}

/*
 * function name    : gs_s_get_connection_state
 * description      : gs_s_get_connection_state gives the
 *                    connection state of indicated IP & PORT
 * arguments        : addr: structure of IP & PORT
 * return value     :
 *    CONNSTATECONNECTING: need to build a new connection
 *    CONNSTATEFAIL: get connection state failed
 *    CONNSTATESUCCEED: exist a valid connection
 */
int gs_s_get_connection_state(ip_key addr, int node_idx, int type)
{
    int rc = -1;
    int old_slot_id = -1;
    int retry_count = 0;
    int state = CONNSTATEFAIL;
    bool found = false;

    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_ip_state_lock);
    ip_state_entry* entry_poll = (ip_state_entry*)hash_search(g_htab_ip_state, &addr, HASH_ENTER, &found);

    /* no connection exist before,  add in htab and return need to create */
    if (unlikely(!found)) {
        COMM_DEBUG_LOG("(s|gs_s_get_connection_state)\tno connection exist [%s:%d].", addr.ip, addr.port);

        entry_poll->entry.val.conn_state = CONNSTATECONNECTING;
        entry_poll->entry.val.node_idx = node_idx;
        entry_poll->entry._init();

        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_ip_state_lock);
        return CONNSTATECONNECTING;
    }

    switch (entry_poll->entry.val.conn_state) {
        case CONNSTATECONNECTING:
            /* someone is trying to make connetion, wait the result */
            COMM_DEBUG_LOG("(s|gs_s_get_connection_state)\twait for connection start [%s:%d].", addr.ip, addr.port);

            while (entry_poll->entry.val.conn_state == CONNSTATECONNECTING) {
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_ip_state_lock);
                rc = entry_poll->entry._timewait(CHECKCONNSTATTIMEOUT);
                retry_count++;
                LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_ip_state_lock);
                if (retry_count > (g_instance.comm_cxt.counters_cxt.g_comm_send_timeout / CHECKCONNSTATTIMEOUT)) {
                    errno = ECOMMSCTPCONNTIMEOUT;
                    break;
                }
            }
            COMM_DEBUG_LOG("(s|gs_s_get_connection_state)\twait for connection end [%s:%d]:%d.",
                addr.ip,
                addr.port,
                entry_poll->entry.val.conn_state);

            /* connect state update became succeed, return CONNSTATESUCCEED */
            if (entry_poll->entry.val.conn_state == CONNSTATESUCCEED) {
                state = CONNSTATESUCCEED;
            } else {  /* connect state is not succeed, return CONNSTATEFAIL */
                errno = (rc == ETIMEDOUT) ? ECOMMSCTPCONNTIMEOUT : ECOMMSCTPTCPCONNFAIL;
                state = CONNSTATEFAIL;
            }
            break;

        case CONNSTATEFAIL:
            COMM_DEBUG_LOG(
                "(s|gs_s_get_connection_state)\tconnection invalid, need to create[%s:%d].", addr.ip, addr.port);

            /* connection is failed before, update state to connecting and  return need to create */
            entry_poll->entry.val.conn_state = CONNSTATECONNECTING;
            entry_poll->entry.val.node_idx = node_idx;
            state = CONNSTATECONNECTING;
            break;

        case CONNSTATESUCCEED:
            /* when the node idx mismatch with the valid connection before
             * we assume it as a new connection, update node idx and close
             * the old connection
             */
            if (node_idx != entry_poll->entry.val.node_idx) {
                old_slot_id = entry_poll->entry.val.node_idx;
                entry_poll->entry.val.conn_state = CONNSTATECONNECTING;
                entry_poll->entry.val.node_idx = node_idx;
                state = CONNSTATECONNECTING;
            } else {
                /* a valid connection in htab, return connection succeed */
                state = CONNSTATESUCCEED;
            }
            break;

        default:
            /* unexpected cases */
            LIBCOMM_ELOG(WARNING,
                "(s|connect)\tUnexpected state in checking connection state:port[%s:%d], state:%d, node_idx:%d.",
                addr.ip,
                addr.port,
                entry_poll->entry.val.conn_state,
                entry_poll->entry.val.node_idx);
            state = CONNSTATEFAIL;
            break;
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_ip_state_lock);

    /* close old data connection */
    if (old_slot_id != -1) {
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tClose the old connections for node%d[%s]:port[%s:%d], type:%d.",
            old_slot_id,
            REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, old_slot_id),
            addr.ip,
            addr.port,
            type);
        if (type == DATA_CHANNEL) {
            g_instance.comm_cxt.g_senders->sender_conn[old_slot_id].ip_changed = true;
            LIBCOMM_PTHREAD_RWLOCK_WRLOCK(&g_instance.comm_cxt.g_senders->sender_conn[old_slot_id].rwlock);
            struct sock_id sctp_fd_id = {g_instance.comm_cxt.g_senders->sender_conn[old_slot_id].socket,
                g_instance.comm_cxt.g_senders->sender_conn[old_slot_id].socket_id};
            gs_s_close_bad_data_socket(&sctp_fd_id, ECOMMSCTPPEERCHANGED, node_idx);
            LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[old_slot_id].rwlock);
        } else {
            g_instance.comm_cxt.g_s_node_sock[node_idx].ip_changed = true;
            g_instance.comm_cxt.g_s_node_sock[node_idx].lock();
            struct sock_id ctrl_fd_id = {g_instance.comm_cxt.g_s_node_sock[old_slot_id].ctrl_tcp_sock,
                g_instance.comm_cxt.g_s_node_sock[old_slot_id].ctrl_tcp_sock_id};
            gs_s_close_bad_ctrl_tcp_sock(&ctrl_fd_id, ECOMMSCTPPEERCHANGED, false, node_idx);
            g_instance.comm_cxt.g_s_node_sock[node_idx].unlock();
        }
    }

    return state;
}

/*
 * add local thread id to g_htab_tid_poll
 * then thread will call gs_poll during connecting, send and recv
 */
static int gs_poll_create()
{
    struct tid_entry* entry_tid = NULL;
    bool found = false;

    if (t_thrd.comm_cxt.libcomm_semaphore != NULL) {
        return 0;
    }

#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_CREATE_POLL_FAILED)) {
        errno = ECOMMSCTPMEMALLOC;
        LIBCOMM_ELOG(WARNING, "(poll create)\t[FAULT INJECTION]Failed to add local tid to g_htab_tid_poll.");
        return -1;
    }
#endif

    if (t_thrd.comm_cxt.MyPid <= 0) {
        t_thrd.comm_cxt.MyPid = gettid();
    }

    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_tid_poll_lock);
    entry_tid = (tid_entry*)hash_search(g_htab_tid_poll, &t_thrd.comm_cxt.MyPid, HASH_ENTER, &found);
    if (!found) {
        entry_tid->entry.val = -1;
        entry_tid->entry._init();
    }

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_tid_poll_lock);
    t_thrd.comm_cxt.libcomm_semaphore = &(entry_tid->entry.sem);

    return 0;
}

// delete tid from tid_poll, usually called when thread exit or logic conn is closed.
// but when the thread needed to delete is calling gs_poll, just signal it instead of del it.
// because for CN, thread usually wait for multiple logic connection, so we cannot del it when
// some logic connection is close.
//
void gs_poll_close()
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);
    if (t_thrd.comm_cxt.libcomm_semaphore != NULL) {
        t_thrd.comm_cxt.libcomm_semaphore = NULL;

        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_tid_poll_lock);
        hash_search(g_htab_tid_poll, &t_thrd.comm_cxt.MyPid, HASH_REMOVE, NULL);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_tid_poll_lock);
    }

    return;
}

/*
 * thread which call gs_poll and block in here
 * until the expected event happened
 * or some error happened(timeout, logic conn is closed, interruption happened).
 */
static int gs_poll(int time_out)
{
    return t_thrd.comm_cxt.libcomm_semaphore->timed_wait(time_out);
}

/*
 * siganl thread when the expected event happened or some error happened
 */
static void gs_poll_signal(binary_semaphore* sem)
{
    if (sem != NULL) {
        sem->post();
    }
}

/*
 * when recv some interruption, gs_auxiliary will
 * signal all thread waitting in gs_poll
 * and threads will check interruption.
 */
static void gs_broadcast_poll()
{
    HASH_SEQ_STATUS hash_seq;
    tid_entry* element = NULL;

    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_tid_poll_lock);

    hash_seq_init(&hash_seq, g_htab_tid_poll);

    while ((element = (tid_entry*)hash_seq_search(&hash_seq)) != NULL) {
        element->entry._signal();
    }

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_tid_poll_lock);
}

/*
 * function name      : gs_get_node_idx
 * description        : gs_get_node_idx gives the node idx of backend.
 *                      the first node connected to current process is node idx 0.
 * arguments          :
 *                      node_name: name of backend
 *                      len: NAMEDATALEN
 * return value       : -1:failed
 *                      >0:node id
 */
static int gs_get_node_idx(char* node_name)
{
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_NO_NODEIDX)) {
        errno = ECOMMSCTPINVALNODEID;
        LIBCOMM_ELOG(WARNING, "(s|get nodeid)\t[FAULT INJECTION]Failed to obtain node id for node %s.", node_name);
        return -1;
    }
#endif
    // get node index
    struct nodename_entry* entry_name = NULL;
    struct char_key ckey;
    bool found = false;
    errno_t ss_rc;
    int ret = -1;
    uint32 cpylen = comm_get_cpylen(node_name, NAMEDATALEN);
    ss_rc = memset_s(ckey.name, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(ckey.name, NAMEDATALEN, node_name, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    ckey.name[cpylen] = '\0';

    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_nodename_node_idx_lock);
    entry_name = (nodename_entry*)hash_search(g_htab_nodename_node_idx, &ckey, HASH_ENTER, &found);

    if (found) {
        ret = entry_name->entry.val;
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_nodename_node_idx_lock);
        return ret;
    }

    // if the node is not registed, get a node index and save node name -> node index to hash table
    int node_idx = g_nodename_count + 1;
    if (node_idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num) {
        hash_search(g_htab_nodename_node_idx, &ckey, HASH_REMOVE, NULL);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_nodename_node_idx_lock);
        errno = ECOMMSCTPINVALNODEID;
        return -1;
    }

    g_nodename_count++;
    entry_name->entry.val = node_idx;
    ret = entry_name->entry.val;
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_nodename_node_idx_lock);
    LIBCOMM_ELOG(LOG, "(s|get idx)\tGenerate node idx [%d] for node:%s.", ret, node_name);

    return ret;
}

/*
 * function name    : gs_get_stream_id
 * description      : producer get usable stream index for current query, which is designed by StreamKey.
 *                    if the key is already in the hash table, return it!
 * arguments        :
 *                    _in_ key_ns: sctp stream key with node index.
 * return value     : -1:failed
 *                    >0:stream id
 */
static int gs_get_stream_id(int node_idx)
{
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_NO_STREAMID)) {
        errno = ECOMMSCTPSTREAMIDX;
        LIBCOMM_ELOG(WARNING,
            "(s|get sid)\t[FAULT INJECTION]Failed to obtain sctp stream for node[%d]:%s.",
            node_idx,
            g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename);
        return -1;
    }
#endif

    int streamid = -1;

    // have no usable stream id
    if (g_instance.comm_cxt.g_usable_streamid[node_idx].pop(
            g_instance.comm_cxt.g_usable_streamid + node_idx, &streamid) <= 0) {
        errno = ECOMMSCTPSTREAMIDX;
        LIBCOMM_ELOG(WARNING,
            "(s|get sid)\tFailed to obtain sctp stream for node[%d]:%s, usable:%d/%d.",
            node_idx,
            g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
            g_instance.comm_cxt.g_usable_streamid[node_idx].count,
            g_instance.comm_cxt.counters_cxt.g_max_stream_num);
        return -1;
    }

    // succeed to return the entry in g_s_htab_nodeid_skey_to_stream
    COMM_DEBUG_LOG("(s|get sid)\tObtain sctp stream[%d] for node[%d]:%s.",
        streamid,
        node_idx,
        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename);

    return streamid;
}  // gs_r_get_usable_streamid

static int gs_update_fd_to_htab_socket_version(struct sock_id* fd_id)
{
    struct sock_ver_entry* entry_ver = NULL;
    bool found = false;
    int fd = fd_id->fd;
    int id = fd_id->id;

    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_socket_version_lock);

    entry_ver = (sock_ver_entry*)hash_search(g_htab_socket_version, &fd, HASH_ENTER, &found);
    if (!found) {
        entry_ver->entry.val = id;
    } else {  // if there is an entry already, we update the version(id) of the socket(fd)
        entry_ver->entry.val = (entry_ver->entry.val == MAX_FD_ID) ? 0 : (entry_ver->entry.val + 1);
        fd_id->id = entry_ver->entry.val;  // set the new id into fd_id !!!
        COMM_DEBUG_LOG("(add fd & version)\tSucceed to update socket[%d] version[%d].", fd, fd_id->id);
    }

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_socket_version_lock);

    return 0;
}  // gs_update_fd_to_htab_socket_version

// send control message to remoter without_lock
// the parameter sock and the fd_id are result parameters
//
static int gs_send_ctrl_msg_without_lock(struct node_sock* ns, FCMSG_T* msg, int node_idx, int role)
{
    int ctrl_sock = -1;
    int ctrl_sock_id = -1;
    int rc = -1;
    int send_bytes = 0;
    uint64 time_enter, time_now;
    time_enter = mc_timers_ms();

    ctrl_sock = ns->get_nl(CTRL_TCP_SOCK, &ctrl_sock_id);
    if (ctrl_sock >= 0) {
        for (;;) {
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
                errno = ECOMMSCTPPEERCHANGED;
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
                    (((uint64)g_instance.comm_cxt.counters_cxt.g_comm_send_timeout) * SEC_TO_MICRO_SEC)) &&
                (time_now > time_enter)) {
                errno = ECOMMSCTPSENDTIMEOUT;
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
static int gs_send_ctrl_msg(struct node_sock* ns, FCMSG_T* msg, int role)
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
static int gs_send_ctrl_msg_by_socket(int ctrl_sock, FCMSG_T* msg)
{
    int rc = mc_tcp_write_block(ctrl_sock, (void*)msg, sizeof(FCMSG_T));  // do send message
    // if tcp send failed, close tcp connction
    if (rc <= 0) {
        LIBCOMM_ELOG(WARNING,
            "(SendCtrlMsg)\tFailed to send type[%s] message to node[%d]:%s with socket[%d]:%s.",
            ctrl_msg_string(msg->type),
            msg->node_idx,
            msg->nodename,
            ctrl_sock,
            mc_strerror(errno));
    }

    COMM_DEBUG_CALL(printfcmsg("SendCtrlMsg", msg));

    return rc;
}  // gs_r_send_ctrl_msg

// receiver close all streams of a node which is designed by control tcp socket
// we call this function because of the broken control tcp connection or sctp connection,
// if it is sctp connection, we should send the close info to remote
// step 1: get node index (node_idx)
// step 2: traverse g_c_mailbox[node_idx][*]
// step 3: do notify and reset all cmailbox
//
static void gs_r_close_all_streams_by_fd_idx(int fd, int node_idx, int close_reason)
{
    struct c_mailbox* cmailbox = NULL;
    struct FCMSG_T fcmsgs = {0x0};
    // Note: we should have locked at the caller, so we need not lock here again
    //
    LIBCOMM_ELOG(WARNING,
        "(r|close all streams)\tTo reset all streams "
        "by socket[%d] for node[%d]:%s, detail:%s.",
        fd,
        node_idx,
        g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename,
        mc_strerror(close_reason));

    for (int j = 1; j < g_instance.comm_cxt.counters_cxt.g_max_stream_num; j++) {
        cmailbox = &C_MAILBOX(node_idx, j);
        LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);

        if ((fd == -1 || cmailbox->ctrl_tcp_sock == fd) && (cmailbox->state != MAIL_CLOSED)) {
            gs_r_close_logic_connection(cmailbox, close_reason, &fcmsgs);
            // reset local stream logic connection info
            COMM_DEBUG_LOG("(r|close all streams)\tTo close stream[%d], "
                           "node[%d]:%s, query[%lu], socket[%d].",
                j,
                node_idx,
                g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename,
                cmailbox->query_id,
                cmailbox->ctrl_tcp_sock);
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
            // Send close ctrl msg to remote without cmailbox lock
            if (IS_NOTIFY_REMOTE(close_reason)) {
                (void)gs_send_ctrl_msg(&g_instance.comm_cxt.g_r_node_sock[node_idx], &fcmsgs, ROLE_CONSUMER);
            }
        } else {
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
        }
    }
}  // gs_r_reset_all_streams_by_fd_idx

// sender close all streams of a node which is designed by control tcp socket
// we call this function because of the broken control tcp connection, so we did not need to send the status to remote
// step 1: get node index (node_idx)
// step 2: traverse g_p_mailbox[node_idx][*]
// step 3: do notification and reset all pmailbox
//
static void gs_s_close_all_streams_by_fd_idx(int fd, int node_idx, int close_reason, bool with_ctrl_lock)
{
    struct p_mailbox* pmailbox = NULL;
    struct FCMSG_T fcmsgs = {0x0};
    // Note: we should have locked at the caller, so we need not lock here again
    //
    LIBCOMM_ELOG(WARNING,
        "(s|close all streams)\tTo reset all streams "
        "by socket[%d] for node[%d]:%s, detail:%s.",
        fd,
        node_idx,
        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
        mc_strerror(close_reason));

    for (int j = 1; j < g_instance.comm_cxt.counters_cxt.g_max_stream_num; j++) {
        pmailbox = &P_MAILBOX(node_idx, j);
        LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);

        if (((pmailbox->ctrl_tcp_sock == -1) || (fd == -1) || (pmailbox->ctrl_tcp_sock == fd)) &&
            (pmailbox->state != MAIL_CLOSED)) {
            COMM_DEBUG_LOG("(s|close all streams)\tTo close stream[%d], "
                           "node[%d]:%s, query[%lu], socket[%d].",
                j,
                node_idx,
                g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
                pmailbox->query_id,
                pmailbox->ctrl_tcp_sock);

            gs_s_close_logic_connection(pmailbox, close_reason, &fcmsgs);
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
            // Send close ctrl msg to remote without cmailbox lock
            if (IS_NOTIFY_REMOTE(close_reason)) {
                if (with_ctrl_lock) {
                    (void)gs_send_ctrl_msg_without_lock(
                        &g_instance.comm_cxt.g_s_node_sock[node_idx], &fcmsgs, node_idx, ROLE_PRODUCER);
                } else {
                    (void)gs_send_ctrl_msg(&g_instance.comm_cxt.g_s_node_sock[node_idx], &fcmsgs, ROLE_PRODUCER);
                }
            }
        } else {
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
        }
    }
}  // gs_s_close_all_streams_by_ctrl_tcp_sock

/* To remove the closed fd in the pooler list.
 * for example, if we have many events in the poll_list [poll_1, poll_2, poll_3,...], the poller at the rear may be
 * closed by the front one. So we need to check and delete the closed one in the poll_list.
 * Notice: if the g_libcomm_poller_list doesn't belong to the Caller, it just returns.
 */
static void gs_clean_events(struct sock_id* old_fd_id)
{
    int i = 0;
    int fd = -1;
    int id = -1;

    if (t_thrd.comm_cxt.g_libcomm_poller_list == NULL) {
        return;
    }
    int nevents = t_thrd.comm_cxt.g_libcomm_poller_list->nevents;
    for (i = 0; i < nevents; i++) {
        fd = (int)(((uint64)t_thrd.comm_cxt.g_libcomm_poller_list->events[i].data.u64 >> MC_POLLER_FD_ID_OFFSET));
        id = (int)(((uint64)t_thrd.comm_cxt.g_libcomm_poller_list->events[i].data.u64 & MC_POLLER_FD_ID_MASK));
        if ((old_fd_id->fd == fd) && (old_fd_id->id == id)) {
            COMM_DEBUG_LOG("(clean events)\tClean socket[%d,%d] in the poller list.", fd, id);

            /* if the old_fd_id in the poller list, we need to remove it.
             * To simplify, we just move the last one to this position.
             * if "i" is the last one, i == nevents-1, it doesn't matter.
             * if i<nevents-1, move the last one to i-th position.
             */
            t_thrd.comm_cxt.g_libcomm_poller_list->events[i] =
                t_thrd.comm_cxt.g_libcomm_poller_list->events[nevents - 1];
            t_thrd.comm_cxt.g_libcomm_poller_list->nevents--;
            break;
        }
    }

    return;
}

// receiver close and clear bad tcp control socket, and related information
//
static void gs_r_close_bad_ctrl_tcp_sock(struct sock_id* fd_id, int close_reason)
{
    int fd = fd_id->fd;
    int id = fd_id->id;
    bool found = false;

    if (fd < 0 || id < 0) {
        return;
    }

    LIBCOMM_PTHREAD_MUTEX_LOCK(g_instance.comm_cxt.pollers_cxt.g_r_poller_list_lock);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_socket_version_lock);

    // step1: remove the fd from the poller cabinet
    //
    if (g_instance.comm_cxt.pollers_cxt.g_r_poller_list->del_fd(fd_id) != 0) {
        LIBCOMM_ELOG(WARNING,
            " (r|close tcp socket)\tFailed to delete socket[%d,%d] from poll list:%s.",
            fd,
            id,
            mc_strerror(errno));
    }

    // step2: get node index by fd
    //
    int node_idx = -1;
    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
    sock_id_entry* entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &(*fd_id), HASH_FIND, &found);
    if (found) {
        node_idx = entry_id->entry.val;
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);
    // step3: make sure the fd and the version are matched, or it has been closed already
    //
    struct sock_ver_entry* entry_ver = (sock_ver_entry*)hash_search(g_htab_socket_version, &fd, HASH_FIND, &found);

    if ((!found) || (entry_ver->entry.val != fd_id->id)) {
        LIBCOMM_ELOG(WARNING,
            "(r|close tcp socket)\tFailed to close socket[%d,%d], maybe already reused[%d,%d].",
            fd,
            id,
            (found) ? fd : -1,
            (found) ? entry_ver->entry.val : -1);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_socket_version_lock);
        ;
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(g_instance.comm_cxt.pollers_cxt.g_r_poller_list_lock);
        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
        hash_search(g_htab_fd_id_node_idx, &(*fd_id), HASH_REMOVE, NULL);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);
        return;
    }

    LIBCOMM_ELOG(LOG, "(r|close bad tcp ctrl fds)\tClose bad socket with socket entry[%d,%d].", fd, id);

    if (node_idx >= 0) {
        // step4: close all mailbox at receiver
        //
        gs_r_close_all_streams_by_fd_idx(fd_id->fd, node_idx, close_reason);
        // step5: close the socket and reset the socket infomation structure(g_r_node_sock[node_idx])
        //
        g_instance.comm_cxt.g_r_node_sock[node_idx].lock();
        if (g_instance.comm_cxt.g_r_node_sock[node_idx].ctrl_tcp_sock == fd_id->fd &&
            g_instance.comm_cxt.g_r_node_sock[node_idx].ctrl_tcp_sock_id == fd_id->id) {
            g_instance.comm_cxt.g_r_node_sock[node_idx].close_socket_nl(CTRL_TCP_SOCK);

            LIBCOMM_ELOG(WARNING,
                "(r|close tcp socket)\tTCP disconnect with socket[%d,%d] "
                "to host:%s, node[%d]:[%s].",
                fd,
                id,
                g_instance.comm_cxt.g_r_node_sock[node_idx].remote_host,
                node_idx,
                g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename);
        }
        g_instance.comm_cxt.g_r_node_sock[node_idx].unlock();

        gs_clean_events(fd_id);
    } else {
        mc_tcp_close(fd_id->fd);
    }
    // step6: if the fd is closed, we update the fd version
    //
    entry_ver->entry.val = (entry_ver->entry.val == MAX_FD_ID) ? 0 : (entry_ver->entry.val + 1);
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_socket_version_lock);
    ;
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(g_instance.comm_cxt.pollers_cxt.g_r_poller_list_lock);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
    hash_search(g_htab_fd_id_node_idx, &(*fd_id), HASH_REMOVE, NULL);
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

    return;
}  // gs_r_close_bad_ctrl_tcp_sock

// sender close and clear bad tcp control socket, and related information
// clean_epoll is true when this function is called by sender flow ctrl thread,
// we delete fd from epoll list and close fd.
// clean_epoll is false when this function is called by producer thread,
// in this case, we cannot close fd and delete from epoll list,
// cause other thread may use this fd after close,
// while sender flow control thread still use this fd to recv.
// NOTE: fd can be closed and deleted from epoll list only under the sender flow ctrl.
static void gs_s_close_bad_ctrl_tcp_sock(struct sock_id* fd_id, int close_reason, bool clean_epoll, int node_idx)
{
    int fd = fd_id->fd;
    int id = fd_id->id;
    ip_key addr;
    bool is_addr = false;
    errno_t ss_rc;
    uint32 cpylen;
    bool found = false;

    if (fd < 0 || id < 0) {
        return;
    }

    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_socket_version_lock);
    // step1: remove the fd from the poller cabinet
    //
    if (clean_epoll) {
        gs_clean_events(fd_id);
        if (g_instance.comm_cxt.pollers_cxt.g_s_poller_list->del_fd(fd_id) != 0) {
            COMM_DEBUG_LOG("(s|cls bad tcp socket)\tFailed to remove bad socket with socket entry[%d,%d]:%s.",
                fd,
                id,
                mc_strerror(errno));
        }
    }

    // step2: make sure the fd and the version are matched, or it has been closed already
    //
    struct sock_ver_entry* entry_ver = (sock_ver_entry*)hash_search(g_htab_socket_version, &fd, HASH_FIND, &found);
    if (!found || entry_ver->entry.val != fd_id->id) {
        LIBCOMM_ELOG(WARNING,
            "(s|cls bad tcp socket)\tFailed to close bad socket[%d,%d], socket entry[%d,%d].",
            fd,
            id,
            (found) ? fd : -1,
            (found) ? entry_ver->entry.val : -1);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_socket_version_lock);
        return;
    }

    // step3: close all mailbox at receiver
    //
    if (node_idx >= 0) {
        gs_s_close_all_streams_by_fd_idx(fd_id->fd, node_idx, close_reason, true);
    }

    // step4: close the socket and reset the socket infomation structure(g_s_node_sock[node_idx])
    //
    LIBCOMM_ELOG(LOG,
        "(s|close bad tcp ctrl fds)\tClose bad socket with socket entry[%d,%d] : %s.",
        fd,
        id,
        mc_strerror(close_reason));
    if (node_idx >= 0) {
        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
        hash_search(g_htab_fd_id_node_idx, &(*fd_id), HASH_REMOVE, NULL);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

        if (g_instance.comm_cxt.g_s_node_sock[node_idx].ctrl_tcp_sock == fd_id->fd &&
            g_instance.comm_cxt.g_s_node_sock[node_idx].ctrl_tcp_sock_id == fd_id->id) {
            cpylen = comm_get_cpylen(g_instance.comm_cxt.g_s_node_sock[node_idx].remote_host, HOST_LEN_OF_HTAB);
            ss_rc = memset_s(addr.ip, HOST_LEN_OF_HTAB, 0x0, HOST_LEN_OF_HTAB);
            securec_check(ss_rc, "\0", "\0");
            ss_rc = strncpy_s(
                addr.ip, HOST_LEN_OF_HTAB, g_instance.comm_cxt.g_s_node_sock[node_idx].remote_host, cpylen + 1);
            securec_check(ss_rc, "\0", "\0");
            addr.ip[cpylen] = '\0';

            addr.port = g_instance.comm_cxt.g_s_node_sock[node_idx].ctrl_tcp_port;
            is_addr = true;
            // producer thread detect destination ip is changed
            // then notify the origination backend to close connection
            if (close_reason == ECOMMSCTPPEERCHANGED) {
                struct FCMSG_T fcmsgs = {0x0};
                fcmsgs.type = CTRL_PEER_CHANGED;
                fcmsgs.node_idx = node_idx;
                fcmsgs.streamid = 1;

                cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
                ss_rc = memset_s(fcmsgs.nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
                securec_check(ss_rc, "\0", "\0");
                ss_rc = strncpy_s(
                    fcmsgs.nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
                securec_check(ss_rc, "\0", "\0");
                fcmsgs.nodename[cpylen] = '\0';

                (void)gs_send_ctrl_msg_without_lock(
                    &g_instance.comm_cxt.g_s_node_sock[node_idx], &fcmsgs, node_idx, ROLE_PRODUCER);
            }
            g_instance.comm_cxt.g_s_node_sock[node_idx].set_nl(-1, CTRL_TCP_SOCK);
            g_instance.comm_cxt.g_s_node_sock[node_idx].set_nl(-1, CTRL_TCP_SOCK_ID);
            LIBCOMM_ELOG(WARNING,
                "(s|cls bad tcp socket)\tClose bad socket[%d,%d] "
                "for host:%s, node[%d]:%s.",
                fd,
                id,
                g_instance.comm_cxt.g_s_node_sock[node_idx].remote_host,
                node_idx,
                g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename);
        }
    }

    // clean_epoll is true only under sender flow control thread
    if (clean_epoll) {
        mc_tcp_close(fd_id->fd);
        // step5: if the fd is closed, we update the fd version
        //
        entry_ver->entry.val = (entry_ver->entry.val == MAX_FD_ID) ? 0 : (entry_ver->entry.val + 1);
    }
    // step6: update connection state in htab
    //
    if (is_addr) {
        gs_update_connection_state(addr, CONNSTATEFAIL, false, node_idx);
    }

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_socket_version_lock);
}  // gs_s_close_bad_ctrl_tcp_sock

// Calculation how many quota size need to add in mailbox
static long gs_add_quota_size(c_mailbox* cmailbox)
{
#define COMM_HIGH_MEM_USED (used_memory >= (g_instance.comm_cxt.commutil_cxt.g_total_usable_memory * 0.8))
#define COMM_LOW_MEM_USED (used_memory <= (g_instance.comm_cxt.commutil_cxt.g_total_usable_memory * 0.5))

    long used_memory = gs_get_comm_used_memory();
    long add_quota = 0;
    long max_buff = 0;                          // must be equal [DEFULTMSGLEN, comm_quota_size]
    long buff_used = cmailbox->buff_q->u_size;  // the used buffer size in this mailbox
    long old_quota = cmailbox->bufCAP;          // the quota size in this mailbox

    used_memory -= g_memory_pool_queue.size() * IOV_ITEM_SIZE;

    // Calculate the maximum buffer size for this mailbox
    if (COMM_HIGH_MEM_USED) {
        max_buff = DEFULTMSGLEN;
    } else if (COMM_LOW_MEM_USED) {
        max_buff = (g_instance.comm_cxt.quota_cxt.g_quota > DEFULTMSGLEN) ? g_instance.comm_cxt.quota_cxt.g_quota
                                                                          : DEFULTMSGLEN;
    } else {
        max_buff = (g_instance.comm_cxt.quota_cxt.g_quota / 8 > DEFULTMSGLEN)
                       ? g_instance.comm_cxt.quota_cxt.g_quota / 8
                       : DEFULTMSGLEN;
    }

    // because:    max_buff = buff_used + old_quota + add_quota
    // so:        add_quota = max_buff - buff_used - old_quota
    add_quota = max_buff - buff_used - old_quota;

    /*
     * buff_used+old_quota is total data size that can be received when no send quota.
     * if (buff_used+old_quota < g_quota/2), need send quota.
     * if (buff_used+old_quota < DEFULTMSGLEN), need send quota.
     */
    if ((buff_used + old_quota < (long)(g_instance.comm_cxt.quota_cxt.g_quota >> 1)) ||
        (buff_used + old_quota < DEFULTMSGLEN)) {
        return add_quota < 0 ? 0 : add_quota;
    } else {
        return 0;
    }
}

// auxiliary thread use it to change the stream state and send control message to remote point (sender)
//
static bool gs_r_quota_notify(c_mailbox* cmailbox, FCMSG_T* msg)
{
    errno_t ss_rc;
    uint32 cpylen;
    int node_idx = cmailbox->idx;
    int streamid = cmailbox->streamid;
    unsigned long add_quota = gs_add_quota_size(cmailbox);

    if (add_quota > 0) {
        // change local stream state and quota first
        cmailbox->bufCAP += add_quota;
        cmailbox->state = MAIL_RUN;

        COMM_DEBUG_LOG("(r|quota notify)\tSend quota to node[%d]:%s on stream[%d].",
            node_idx,
            g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename,
            streamid);

        // send resume message to change remote stream state and quota
        msg->type = CTRL_ADD_QUOTA;
        msg->node_idx = cmailbox->idx;
        msg->streamid = cmailbox->streamid;
        msg->streamcap = add_quota;
        msg->version = cmailbox->remote_version;
        msg->query_id = cmailbox->query_id;

        cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
        ss_rc = memset_s(msg->nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
        securec_check(ss_rc, "\0", "\0");
        ss_rc = strncpy_s(msg->nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
        securec_check(ss_rc, "\0", "\0");
        msg->nodename[cpylen] = '\0';

        return true;
    }

    return false;
}  // gs_r_quota_notify

// traverse all the c_mailbox(es) to find the first query who used memory,
// and make it failure to release the memory. Otherwise, the communication layer maybe hang up.
//
static void gs_r_release_comm_memory()
{
    uint64 release_query_id = 0;
    int nid = 0;
    int sid = 1;
    struct c_mailbox* cmailbox = NULL;
    unsigned long buff_size = 0;
    unsigned long total_buff_size = 0;
    struct FCMSG_T fcmsgs = {0x0};

    for (nid = 0; nid < g_instance.comm_cxt.counters_cxt.g_cur_node_num; nid++) {
        for (sid = 1; sid < g_instance.comm_cxt.counters_cxt.g_max_stream_num; sid++) {
            cmailbox = &C_MAILBOX(nid, sid);
            LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);

            if (cmailbox->buff_q->u_size <= 0) {
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
                continue;
            }

            // find the first query to release memory, save query id
            if (release_query_id == 0) {
                release_query_id = cmailbox->query_id;
            }

            if (cmailbox->query_id == release_query_id) {
                buff_size = cmailbox->buff_q->u_size;
                total_buff_size += buff_size;
                COMM_DEBUG_LOG("(r|release memory)\tReset stream[%d] on node[%d]:%s "
                               "for query[%lu] to release memory[%lu Byte].",
                    sid,
                    nid,
                    REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, nid),
                    release_query_id,
                    buff_size);

                gs_r_close_logic_connection(cmailbox, ECOMMSCTPRELEASEMEM, &fcmsgs);
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
                (void)gs_send_ctrl_msg(&g_instance.comm_cxt.g_r_node_sock[nid], &fcmsgs, ROLE_CONSUMER);
            } else {
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
            }
        }
    }

    LIBCOMM_ELOG(WARNING,
        "(r|release memory)\tReset query[%lu] "
        "to release memory[%lu Byte].",
        release_query_id,
        total_buff_size);
}  // gs_r_release_comm_memory

// if we failed to receive message from a sctp listen socket, we should do following things
// step1: reset the streams of the related node
// step2: delete it from epoll cabinet
// step3: update the socket version
// step4: delete the socket from hash table socke -> node index (g_r_htab_data_socket_node_idx)
// step5: close the old sctp socket
//
static void gs_r_close_bad_data_socket(int node_idx, sock_id fd_id, bool is_lock)
{
    if (node_idx >= 0) {
        gs_r_close_all_streams_by_fd_idx(-1, node_idx, ECOMMSCTPSCTPDISCONNECT);
        if (is_lock) {
            LIBCOMM_PTHREAD_RWLOCK_WRLOCK(&g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].rwlock);
        }
        g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].socket = -1;
        if (is_lock) {
            LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].rwlock);
        }
    }

    bool found = false;
    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_socket_version_lock);
    struct sock_ver_entry* entry_ver =
        (sock_ver_entry*)hash_search(g_htab_socket_version, &fd_id.fd, HASH_FIND, &found);
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_socket_version_lock);

    if (!found || entry_ver->entry.val != fd_id.id) {
        LIBCOMM_ELOG(WARNING,
            "(r|close bad data socket)\tFailed to close bad socket[%d,%d], socket entry[%d,%d].",
            fd_id.fd,
            fd_id.id,
            (found) ? fd_id.fd : -1,
            (found) ? entry_ver->entry.val : -1);
        return;
    }

    bool is_delete = false;

    LIBCOMM_PTHREAD_MUTEX_LOCK(g_instance.comm_cxt.pollers_cxt.g_r_libcomm_poller_list_lock);

    /* try to delete old_fd in g_libcomm_receiver_poller_list.
     * because we have several recv thread, if the old_fd_id belongs to this thread, it can delete it successfully,
     * otherwise, it returns false.
     */
    if (t_thrd.comm_cxt.g_libcomm_recv_poller_hndl_list != NULL) {
        is_delete = (t_thrd.comm_cxt.g_libcomm_recv_poller_hndl_list->del_fd(&fd_id) == 0) ? true : false;
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(g_instance.comm_cxt.pollers_cxt.g_r_libcomm_poller_list_lock);

    /* del fd_id in the htab,
     * next time, -1 = g_htab_fd_id_node_idx.get_value(fd_id), So we needn't to gs_r_close_all_streams again.
     */
    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
    hash_search(g_htab_fd_id_node_idx, &fd_id, HASH_REMOVE, NULL);
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

    if (is_delete) {
        // if the old_fd_id belongs to this recv thread, we need to clean it in the poll list, and then close(fd).
        gs_clean_events(&fd_id);
        if (gs_update_fd_to_htab_socket_version(&fd_id) < 0) {
            LIBCOMM_ELOG(
                WARNING, "(r|close bad data socket)\tFailed to update bad data socket[%d,%d].", fd_id.fd, fd_id.id);
        }
        mc_tcp_close(fd_id.fd);
    } else {
        /* if the old_fd_id belongs to other recv thread, it means, the old_fd_id isn't in this poll_list,
         * So we needn't to gs_clean_events(). we just use shutdown to send notification signal.
         */
        shutdown(fd_id.fd, SHUT_RDWR);
        COMM_DEBUG_LOG("(r|close bad data socket)\tSend shutdown signal for [%d,%d].", fd_id.fd, fd_id.id);
    }
}

// if we failed to send message to the destination, we should do following things
//
static void gs_s_close_bad_data_socket(struct sock_id* fd_id, int close_reason, int node_idx)
{
    errno_t ss_rc;
    uint32 cpylen;
    int fd = fd_id->fd;
    int id = fd_id->id;
    ip_key addr;
    bool is_addr = false;
    bool found = false;

    if ((fd_id->fd < 0) || (fd_id->id < 0)) {
        return;
    }

    // step1: make sure the fd and the version are matched, or it has been closed already
    //
    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_socket_version_lock);
    struct sock_ver_entry* entry_ver = (sock_ver_entry*)hash_search(g_htab_socket_version, &fd, HASH_FIND, &found);

    if (!found) {
        mc_tcp_close(fd_id->fd);
    }

    if (!found || entry_ver->entry.val != fd_id->id) {
        LIBCOMM_ELOG(WARNING,
            "(s|cls bad data socket)\tFailed to close bad socket[%d,%d], socket entry[%d,%d].",
            fd,
            id,
            (found) ? fd : -1,
            (found) ? entry_ver->entry.val : -1);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_socket_version_lock);
        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
        hash_search(g_htab_fd_id_node_idx, &(*fd_id), HASH_REMOVE, NULL);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

        return;
    }

    // step2: close the bad socket
    //
    LIBCOMM_ELOG(LOG, "(s|cls bad data socket)\tClose bad socket with socket entry[%d,%d].", fd, id);

    if (node_idx >= 0) {
        /* reset the socket for sender, unexpected case if this condition mismatch */
        if (g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket == fd_id->fd &&
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id == fd_id->id) {

            cpylen =
                comm_get_cpylen(g_instance.comm_cxt.g_senders->sender_conn[node_idx].remote_host, HOST_LEN_OF_HTAB);
            ss_rc = memset_s(addr.ip, HOST_LEN_OF_HTAB, 0x0, HOST_LEN_OF_HTAB);
            securec_check(ss_rc, "\0", "\0");
            ss_rc = strncpy_s(addr.ip,
                HOST_LEN_OF_HTAB,
                g_instance.comm_cxt.g_senders->sender_conn[node_idx].remote_host,
                cpylen + 1);
            securec_check(ss_rc, "\0", "\0");
            addr.ip[cpylen] = '\0';

            addr.port = g_instance.comm_cxt.g_senders->sender_conn[node_idx].port;
            is_addr = true;

            mc_tcp_close(g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket);
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].port = -1;
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket = -1;
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id = -1;
            g_instance.comm_cxt.g_senders->sender_conn[node_idx].assoc_id = 0;
            LIBCOMM_ELOG(WARNING,
                "(s|cls bad data socket)\tClose bad data socket with socket entry[%d,%d] "
                "to host:%s, node[%d], node name[%s]:%s.",
                fd,
                id,
                g_instance.comm_cxt.g_s_node_sock[node_idx].remote_host,
                node_idx,
                g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
                mc_strerror(errno));
        }
    } else {
        mc_tcp_close(fd_id->fd);
    }
    // step3: update connection state in htab
    //
    if (is_addr) {
        gs_update_connection_state(addr, CONNSTATEFAIL, false, node_idx);
    }

    // step4: if the bad socket is closed, we update the fd version
    //
    entry_ver->entry.val = (entry_ver->entry.val == MAX_FD_ID) ? 0 : (entry_ver->entry.val + 1);
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_socket_version_lock);

    /*
     * close all p_mailbox[node_idx][*]
     * without g_htab_socket_version lock
     * with g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock
     */
    if (node_idx >= 0) {
        gs_s_close_all_streams_by_fd_idx(-1, node_idx, close_reason, false);
    }

    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
    hash_search(g_htab_fd_id_node_idx, &(*fd_id), HASH_REMOVE, NULL);
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

    return;
}  // gs_s_close_bad_data_socket

/*
 * @Description:    push the data package to cmailbox buffer.
 * @IN cmailbox:    point of cmailbox.
 * @IN iov:         data package.
 * @Return:         -1:    push data failed.
 *                    0:     push data succsessed.
 * @See also:
 */
static int gs_push_cmailbox_buffer(c_mailbox* cmailbox, struct mc_lqueue_item* q_item, int version)
{
    struct iovec* iov = q_item->element.data;
    COMM_TIMER_INIT();

    int sid = cmailbox->streamid;
    int idx = cmailbox->idx;
    uint64 signal_start = 0;
    uint64 signal_end = 0;
    uint64 time_now = 0;

    LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);

    // if the stream is closed or ready to close, the data should be dropped.
    if (false == gs_check_mailbox(cmailbox->local_version, version)) {
        COMM_DEBUG_LOG("(r|inner recv)\tStream[%d] is closed for node[%d]:%s, drop reveived message[%d].",
            sid,
            idx,
            g_instance.comm_cxt.g_r_node_sock[idx].remote_nodename,
            (int)iov->iov_len);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
        errno = cmailbox->close_reason;
        return -1;
    }

    DEBUG_QUERY_ID = cmailbox->query_id;

    // there is buffer/quota to process the received data
    if (cmailbox->bufCAP >= (unsigned long)(iov->iov_len)) {
        COMM_DEBUG_LOG("(r|inner recv)\tNode[%d]:%s stream[%d] recv %zu msg:%c, "
                       "bufCAP[%lu] and buff_q->u_size[%lu].",
            idx,
            g_instance.comm_cxt.g_r_node_sock[idx].remote_nodename,
            sid,
            iov->iov_len,
            ((char*)iov->iov_base)[0],
            cmailbox->bufCAP,
            cmailbox->buff_q->u_size);

        // put the message to the buffer in the c_mailbox
        (void)mc_lqueue_add(cmailbox->buff_q, q_item);

        if (g_instance.comm_cxt.quota_cxt.g_having_quota) {
            cmailbox->bufCAP -= iov->iov_len;
        }

        signal_start = COMM_STAT_TIME();
        // wake up the Consumer thread of executor, to notify Consumer of arriving new message
        gs_poll_signal(cmailbox->semaphore);

        COMM_TIMER_LOG("(r|inner recv)\tCache data from node[%d]:%s stream[%d].",
            idx,
            g_instance.comm_cxt.g_r_node_sock[idx].remote_nodename,
            sid);
    } else {  // there is no buffer/quota to process the received data, it should not happen
        LIBCOMM_ELOG(WARNING,
            "(r|inner recv)\tNode[%d] stream[%d], node name[%s] has bufCAP[%lu] and got[%d].",
            idx,
            sid,
            g_instance.comm_cxt.g_r_node_sock[idx].remote_nodename,
            cmailbox->bufCAP,
            (int)iov->iov_len);
        LIBCOMM_ASSERT(false, idx, sid, ROLE_CONSUMER);
    }

    /* update the statistic information of the mailbox */
    if (cmailbox->statistic != NULL) {
        time_now = COMM_STAT_TIME();
        if (cmailbox->statistic->first_recv_time == 0) {
            cmailbox->statistic->first_recv_time = time_now;
        }
        signal_end = time_now;
        cmailbox->statistic->total_signal_time += ABS_SUB(signal_end, signal_start);
        cmailbox->statistic->last_recv_time = time_now;
        cmailbox->statistic->recv_bytes += iov->iov_len;
        cmailbox->statistic->recv_loop_time += ABS_SUB(time_now, t_thrd.comm_cxt.g_receiver_loop_poll_up);
        cmailbox->statistic->recv_loop_count++;
    }

    if (cmailbox->bufCAP < DEFULTMSGLEN) {
        cmailbox->state = MAIL_HOLD;
    }

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);

    return 0;
}

/*
 * function name    : gs_reload_hba
 * description      : send signal to postmaster thread to reload hba
 * arguments        : fd: recevive/flower listen socket
 *                    ctrl_client: ctrl client's sockaddress.
 * return value     : retry_count: if bigger than RELOAD_HBA_RETRY_COUNT means failed
 */
static int gs_reload_hba(int fd, const sockaddr ctrl_client)
{
    errno = ECOMMSCTPNOTINTERNALIP;
    int retry_count = 0;
    while (retry_count < RELOAD_HBA_RETRY_COUNT) {
        if (is_cluster_internal_IP(ctrl_client)) {
            break;
        }

        LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tNot cluster internal IP, listen socket[%d]:%s.", fd, mc_strerror(errno));
        (void)gs_signal_send(PostmasterPid, SIGHUP);
        pg_usleep(WAIT_SLEEP_200MS);
        retry_count++;
    }

    if (retry_count < RELOAD_HBA_RETRY_COUNT) {
        COMM_DEBUG_LOG("(r|flow ctrl)\tRelod hba successfully, listen socket[%d]:%s.", fd, mc_strerror(errno));
    } else {
        LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tRelod hba fail, listen socket[%d]:%s.", fd, mc_strerror(errno));
    }

    return retry_count;
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
static int gs_accept_data_conntion(struct iovec* iov, const sock_id fd_id)
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
        gs_r_close_bad_data_socket(node_idx, old_fd_id, false);
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
    ack_msg.type = SCTP_PKG_TYPE_ACCEPT;
    ack_msg.result = 1;
    if (g_libcomm_adapt.send_ack(fd_id.fd, (char*)&ack_msg, sizeof(ack_msg)) < 0) {
        gs_r_close_bad_data_socket(node_idx, fd_id, true);
        return RECV_NET_ERROR;
    }

#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_GSS_SCTP_FAILED)) {
        errno = ECOMMSCTPGSSAUTHFAIL;
        LIBCOMM_ELOG(WARNING,
            "(r|recv loop)\t[FAULT INJECTION]Data channel GSS authentication failed, listen socket[%d]:%s.",
            fd_id.fd,
            mc_strerror(errno));
        return RECV_NET_ERROR;
    }
#endif

    /* server side gss kerberos authentication for data connection.
     * authentication for sctp mode after connection package replay.
     * if GSS authentication SUCC, no IP authentication is required.
     */
    if (g_instance.comm_cxt.g_comm_tcp_mode == false) {
        struct sockaddr ctrl_client;
        socklen_t len = sizeof(struct sockaddr);
        int ret = getpeername(fd_id.fd, (struct sockaddr*)&ctrl_client, &len);
        if (ret < 0) {
            LIBCOMM_ELOG(WARNING, "getpeername failed, sockfd[%d], node[%d].\n", fd_id.fd, node_idx);
            return RECV_NET_ERROR;
        }

        if (g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile != NULL) {
            if (GssServerAuth(fd_id.fd, g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile) < 0) {
                LIBCOMM_ELOG(WARNING,
                    "(r|recv loop)\tData channel GSS authentication failed, listen socket[%d]:%s.",
                    fd_id.fd,
                    mc_strerror(errno));
                errno = ECOMMSCTPGSSAUTHFAIL;
                return RECV_NET_ERROR;
            } else {
                LIBCOMM_ELOG(LOG, "(r|recv loop)\tData channel GSS authentication SUCC, listen socket[%d].", fd_id.fd);
            }
        } else {
            /* send signal to postmaster thread to reload hba */
            int retry_count = gs_reload_hba(fd_id.fd, ctrl_client);
            if (retry_count >= RELOAD_HBA_RETRY_COUNT) {
                return RECV_NET_ERROR;
            }
        }
    }

    LIBCOMM_ELOG(LOG,
        "(r|recv loop)\tAccept data connection for "
        "node[%d]:%s with socket[%d,%d].",
        node_idx,
        g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename,
        fd_id.fd,
        fd_id.id);

    return 0;
}

int gs_handle_data_delay_message(int idx, struct mc_lqueue_item* q_item, uint16 msg_type)
{
    struct c_mailbox* cmailbox = NULL;
    struct libcomm_delay_package* delay_msg = NULL;
    struct iovec* iov = q_item->element.data;

    if (idx < 0) {
        return -1;
    }

    delay_msg = (struct libcomm_delay_package*)iov->iov_base;

    if (msg_type == SCTP_PKG_TYPE_DELAY_REQUEST) {
        delay_msg->recv_time = (uint32)mc_timers_us();
    } else if (msg_type == SCTP_PKG_TYPE_DELAY_REPLY) {
        delay_msg->finish_time = (uint32)mc_timers_us();
    }

    // put the message to the buffer in the c_mailbox
    cmailbox = &C_MAILBOX(idx, 0);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);
    (void)mc_lqueue_add(cmailbox->buff_q, q_item);

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);

    return 0;
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
static int gs_internal_recv(const sock_id fd_id, int node_idx)
{
    int recvsk = fd_id.fd;
    int idx = node_idx;
    int error = 0;
    struct c_mailbox* cmailbox = NULL;
    // Initialize inmessage with enough space for DATA, and control message.
    COMM_TIMER_INIT();

    for (;;) {
        LibcommRecvInfo recv_info;
        COMM_TIMER_LOG("(r|inner recv)\tInternal receive start.");

        int64 curr_rcv_time;
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
                send(recvsk, "ACK", sizeof("ACK"), 0);
            }
        }

        /* Speicail message for connection request */
        if (streamid == 0) {
            uint16 msg_type = *(uint16*)iov->iov_base;
            error = RECV_NEED_RETRY;

            switch (msg_type) {
                case SCTP_PKG_TYPE_CONNECT:
                    error = gs_accept_data_conntion(iov, fd_id);
                    break;

                case SCTP_PKG_TYPE_DELAY_REQUEST:
                case SCTP_PKG_TYPE_DELAY_REPLY:
                    error = gs_handle_data_delay_message(idx, iov_item, msg_type);
                    if (error == 0) {
                        /* iov save to cmailbox[idx][0], no need free */
                        return RECV_NEED_RETRY;
                    }
                    break;

                default:
                    struct libcomm_delay_package* delay_msg = (struct libcomm_delay_package*)iov->iov_base;
                    COMM_DEBUG_LOG("[DELAY_INFO]recv invalid type[%d] sn=%d\n", msg_type, delay_msg->sn);
                    error = RECV_NET_ERROR;
                    break;
            }

            libcomm_free_iov_item(&iov_item, IOV_DATA_SIZE);
            return error;
        } else if (streamid > 0 && idx >= 0) { // if the sid is not 0, we should receive data
            cmailbox = &C_MAILBOX(idx, streamid);
            if (gs_push_cmailbox_buffer(cmailbox, iov_item, version) < 0) {
                libcomm_free_iov_item(&iov_item, IOV_DATA_SIZE);
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
        errno = ECOMMSCTPARGSINVAL;
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
        errno = ECOMMSCTPARGSINVAL;
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
        errno = ECOMMSCTPNODATA;
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
        errno = ECOMMSCTPNODATA;
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

    return ret;
}  // gs_recv

/*
 * function name    : gs_s_close_logic_connection
 * description      : producer close logic connetion and reset mailbox,
 *                    if producer call this, we only set state is CTRL_TO_CLOSE,
 *                    then really close when consumer send MAIL_CLOSED message.
 * notice           : we must get mailbox lock before
 * arguments        : _in_ cmailbox: libcomm logic conntion info.
 *                    _in_ close_reason: close reason.
 */
static void gs_s_close_logic_connection(struct p_mailbox* pmailbox, int close_reason, FCMSG_T* msg)
{
    errno_t ss_rc;
    uint32 cpylen;

    if (pmailbox->state == MAIL_CLOSED) {
        return;
    }

    // when sender closes pmailbox, if close reason != remote close, and the state of pmailbox is MAIL_READY,
    // we set pmailbox to MAIL_TO_CLOSE, then wait for top consumer(gs_connect()) to close it.
    if ((pmailbox->state == MAIL_READY) && (close_reason != ECOMMSCTPREMOETECLOSE)) {
        pmailbox->state = MAIL_TO_CLOSE;
        // wake up the producer who is waiting
        gs_poll_signal(pmailbox->semaphore);
        pmailbox->semaphore = NULL;
        return;
    }

    // 1, if tcp disconnect, we can not send control message on tcp channel,
    //    remote can receive disconnect event when flow control thread call epoll_wait.
    // 2, close reason is ECOMMSCTPREMOETECLOSE means remote send MAIL_CLOSED,
    //    we could not reply MAIL_CLOSED message.
    if (IS_NOTIFY_REMOTE(close_reason) && msg) {
        msg->type = CTRL_CLOSED;
        msg->node_idx = pmailbox->idx;
        msg->streamid = pmailbox->streamid;
        msg->streamcap = 0;
        msg->version = pmailbox->remote_version;
        msg->query_id = pmailbox->query_id;

        cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
        ss_rc = memset_s(msg->nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
        securec_check(ss_rc, "\0", "\0");
        ss_rc = strncpy_s(msg->nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
        securec_check(ss_rc, "\0", "\0");
        msg->nodename[cpylen] = '\0';
    }

    // wake up the producer who is waiting
    gs_poll_signal(pmailbox->semaphore);

    // At last, reset mailbox and clean hash table
    gs_s_reset_pmailbox(pmailbox, close_reason);

    return;
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
static void gs_r_close_logic_connection(struct c_mailbox* cmailbox, int close_reason, FCMSG_T* msg)
{
    errno_t ss_rc;
    uint32 cpylen;

    if (cmailbox->state == MAIL_CLOSED) {
        return;
    }

    // 1, if tcp disconnect, we can not send control message on tcp channel,
    //    remote can receive disconnect event when flow control thread call epoll_wait.
    // 2, close reason is ECOMMSCTPREMOETECLOSE means remote send MAIL_CLOSED,
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

//    send assert fail msg via ctrl connection if debug mode enable and assert failed
//
static void gs_libcomm_handle_assert(bool condition, int nidx, int sidx, int node_role)
{
    errno_t ss_rc;
    uint32 cpylen;

    if (mc_unlikely(!condition)) {
        struct FCMSG_T fcmsgs = {0x0};
        // notify peer assertion failed
        fcmsgs.type = CTRL_ASSERT_FAIL;
        fcmsgs.node_idx = nidx;
        fcmsgs.streamid = sidx;

        cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
        ss_rc = memset_s(fcmsgs.nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
        securec_check(ss_rc, "\0", "\0");
        ss_rc = strncpy_s(fcmsgs.nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
        securec_check(ss_rc, "\0", "\0");
        fcmsgs.nodename[cpylen] = '\0';

        if (node_role == ROLE_PRODUCER) {
            (void)gs_send_ctrl_msg(&g_instance.comm_cxt.g_s_node_sock[nidx], &fcmsgs, node_role);
            struct p_mailbox* pmailbox = NULL;
            pmailbox = &(P_MAILBOX(nidx, sidx));
            LIBCOMM_ELOG(WARNING,
                "(s|handle assert)\tNode[%d] stream[%d] assert fail, node name[%s] with state[%d] has bufCAP[%lu].",
                nidx,
                sidx,
                g_instance.comm_cxt.g_s_node_sock[nidx].remote_nodename,
                pmailbox->state,
                pmailbox->bufCAP);
            MAILBOX_ELOG(pmailbox, WARNING, "(s|handle assert)\tMailbox Info which assert fail.");
        } else {
            (void)gs_send_ctrl_msg(&g_instance.comm_cxt.g_r_node_sock[nidx], &fcmsgs, node_role);
            struct c_mailbox* cmailbox = NULL;
            cmailbox = &(C_MAILBOX(nidx, sidx));
            LIBCOMM_ELOG(WARNING,
                "(r|handle assert)\tNode[%d] stream[%d] assert fail, node name[%s] with state[%d] has bufCAP[%lu] and "
                "buff_q->u_size[%lu].",
                nidx,
                sidx,
                g_instance.comm_cxt.g_r_node_sock[nidx].remote_nodename,
                cmailbox->state,
                cmailbox->bufCAP,
                cmailbox->buff_q->u_size);
            MAILBOX_ELOG(cmailbox, WARNING, "(r|handle assert)\tMailbox Info which assert fail.");
        }
        Assert(condition);
    }
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
 * function name    : gs_get_libcomm_reply_socket
 * description      : get reply socket from g_s_node_sock.
 * arguments        : _in_ recv_idx: the node index.
 * return value     :
 *                   -1: failed.
 *                   >=0: reply socket.
 */
int gs_get_libcomm_reply_socket(int recv_idx)
{
    int socket = g_instance.comm_cxt.g_r_node_sock[recv_idx].sctp_reply_sock;

    if (socket >= 0) {
        return socket;
    }

    for (int send_idx = 0; send_idx < g_instance.comm_cxt.counters_cxt.g_cur_node_num; send_idx++) {
        if (strcmp(g_instance.comm_cxt.g_r_node_sock[recv_idx].remote_nodename,
                   g_instance.comm_cxt.g_s_node_sock[send_idx].remote_nodename) == 0) {
            socket = g_instance.comm_cxt.g_senders->sender_conn[send_idx].socket;

            // save data reply socket in g_r_node_sock
            g_instance.comm_cxt.g_r_node_sock[recv_idx].lock();
            g_instance.comm_cxt.g_r_node_sock[recv_idx].sctp_reply_sock = socket;
            g_instance.comm_cxt.g_r_node_sock[recv_idx].unlock();
            break;
        }
    }

    return socket;
}

/*
 * function name    : gs_delay_analysis
 * description      : analysis libcomm delay message from mailbox[node_idx][0].
 */
void gs_delay_analysis()
{
    struct c_mailbox* cmailbox = NULL;
    int node_idx = 0;
    int socket = -1;
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
        switch (msg->type) {
            // set reply_time and reply sctp delay message
            case SCTP_PKG_TYPE_DELAY_REQUEST:
                // we get reply socket from g_s_node_sock
                socket = gs_get_libcomm_reply_socket(node_idx);
                if (socket < 0) {
                    break;
                }

                msg->type = SCTP_PKG_TYPE_DELAY_REPLY;
                msg->reply_time = (uint32)mc_timers_us();

                LibcommSendInfo send_info;
                send_info.socket = socket;
                send_info.node_idx = node_idx;
                send_info.streamid = 0;
                send_info.version = 0;
                send_info.msg = (char*)msg;
                send_info.msg_len = sizeof(struct libcomm_delay_package);

                (void)g_libcomm_adapt.block_send(&send_info);
                break;

            case SCTP_PKG_TYPE_DELAY_REPLY:
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

// libcomm delay message number
static int g_libcomm_delay_no = 0;

/*
 * function name    : gs_delay_survey
 * description      : send libcomm delay message to all connection.
 */
void gs_delay_survey()
{
    int node_idx = -1;
    int socket = -1;

    errno_t ss_rc = 0;
    struct libcomm_delay_package msg;

    ss_rc = memset_s(&msg, sizeof(msg), 0, sizeof(struct libcomm_delay_package));
    securec_check(ss_rc, "\0", "\0");

    msg.type = SCTP_PKG_TYPE_DELAY_REQUEST;
    for (node_idx = 0; node_idx < g_instance.comm_cxt.counters_cxt.g_cur_node_num; node_idx++) {
        // if the connection is not ready, continue
        if (g_instance.comm_cxt.g_senders->sender_conn[node_idx].assoc_id == 0) {
            continue;
        }

        socket = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket;
        msg.sn = g_libcomm_delay_no;
        msg.start_time = (uint32)mc_timers_us();

        LibcommSendInfo send_info;
        send_info.socket = socket;
        send_info.node_idx = node_idx;
        send_info.streamid = 0;
        send_info.version = 0;
        send_info.msg = (char*)&msg;
        send_info.msg_len = sizeof(struct libcomm_delay_package);

        (void)g_libcomm_adapt.block_send(&send_info);
    }
    g_libcomm_delay_no++;
}

#ifdef ENABLE_MULTIPLE_NODES

void* libcommProducerThread(void* arg)
{
    int producer_sn = *(int*)arg;
    const unsigned int plan_id = LIBCOMM_PERFORMANCE_PLAN_ID;
    const unsigned int plan_node_id = LIBCOMM_PERFORMANCE_PN_ID + producer_sn;
    sctpaddrinfo** consumerAddr = NULL;
    int consumerNum = 0;
    NodeDefinition* nodesDef = NULL;
    int i;
    errno_t rc = EOK;
    int error = 0;
    char* msg_buf = NULL;
    int msg_len = g_instance.comm_cxt.tests_cxt.libcomm_test_msg_len;
    int once_send = g_instance.comm_cxt.tests_cxt.libcomm_test_send_once;
    int sleep_time = g_instance.comm_cxt.tests_cxt.libcomm_test_send_sleep;
    int current_send = 0;

    (void)mc_thread_block_signal();
    gs_memprot_thread_init();
    // initialize globals
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    DEBUG_QUERY_ID = plan_node_id;
    log_timezone = g_instance.comm_cxt.libcomm_log_timezone;
    t_thrd.comm_cxt.LibcommThreadType = LIBCOMM_AUX;

    consumerNum = global_node_definition->num_nodes;
    consumerAddr = (sctpaddrinfo**)calloc(consumerNum, sizeof(sctpaddrinfo*));
    if (consumerAddr == NULL) {
        goto clean_return;
    }

    for (i = 0; i < consumerNum; i++) {
        nodesDef = &(global_node_definition->nodesDefinition[i]);

        consumerAddr[i] = (sctpaddrinfo*)calloc(1, sizeof(sctpaddrinfo));
        if (consumerAddr[i] == NULL) {
            goto clean_return;
        }

        consumerAddr[i]->host = nodesDef->nodehost.data;
        consumerAddr[i]->ctrl_port = nodesDef->nodectlport;
        consumerAddr[i]->sctp_port = nodesDef->nodesctpport;
        consumerAddr[i]->nodeIdx = nodesDef->nodeid;

        rc = strncpy_s(
            consumerAddr[i]->nodename, NAMEDATALEN, nodesDef->nodename.data, strlen(nodesDef->nodename.data) + 1);
        securec_check(rc, "\0", "\0");

        consumerAddr[i]->sctpKey.queryId = plan_id;
        consumerAddr[i]->sctpKey.planNodeId = plan_node_id;
    }

    error = gs_connect(consumerAddr, consumerNum, -1);
    if (error != 0) {
        goto clean_return;
    }

    if (msg_len < 0 || msg_len > PG_INT32_MAX) {
        goto clean_return;
    }

    msg_buf = (char*)malloc(msg_len);
    if (msg_buf == NULL) {
        goto clean_return;
    }
    for (;;) {
        for (i = 0; i < consumerNum; i++) {
            error = gs_send(&(consumerAddr[i]->gs_sock), msg_buf, msg_len, -1, true);
            if (error < 0 || g_instance.comm_cxt.tests_cxt.libcomm_stop_flag == true) {
                goto clean_return;
            }
        }

        current_send += error;
        if (current_send >= once_send && sleep_time > 0) {
            usleep(sleep_time * 1000);
            current_send = 0;
        }
    }

clean_return:
    if (consumerAddr != NULL) {
        for (i = 0; i < consumerNum; i++) {
            if (consumerAddr[i] != NULL) {
                gs_close_gsocket(&(consumerAddr[i]->gs_sock));
                free(consumerAddr[i]);
                consumerAddr[i] = NULL;
            }
        }
        free(consumerAddr);
        consumerAddr = NULL;
    }

    if (msg_buf != NULL) {
        free(msg_buf);
        msg_buf = NULL;
    }

    atomic_sub(&g_instance.comm_cxt.tests_cxt.libcomm_test_current_thread, 1);

    return NULL;
}  // producerThread;

void* libcommConsumerThread(void* arg)
{
    int consumer_sn = *(int*)arg;
    const unsigned int plan_id = LIBCOMM_PERFORMANCE_PLAN_ID;
    const unsigned int plan_node_id = LIBCOMM_PERFORMANCE_PN_ID + consumer_sn;
    int idx, sid;
    int ready_conn = 0;
    char* msg_buf = NULL;
    int* datamarks = NULL;
    int msg_len = g_instance.comm_cxt.tests_cxt.libcomm_test_msg_len;
    int once_recv = g_instance.comm_cxt.tests_cxt.libcomm_test_recv_once;
    int sleep_time = g_instance.comm_cxt.tests_cxt.libcomm_test_recv_sleep;
    int current_recv = 0;
    gsocket* gsockAddr = NULL;
    int producerNum = 0;
    c_mailbox* cmailbox = NULL;
    int error = 0;
    int i;

    (void)mc_thread_block_signal();
    gs_memprot_thread_init();
    // initialize globals
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    DEBUG_QUERY_ID = plan_node_id;
    log_timezone = g_instance.comm_cxt.libcomm_log_timezone;
    t_thrd.comm_cxt.LibcommThreadType = LIBCOMM_AUX;

    producerNum = global_node_definition->num_nodes;
    gsockAddr = (gsocket*)calloc(producerNum, sizeof(gsocket));
    if (gsockAddr == NULL) {
        goto clean_return;
    }

    for (;;) {
        for (idx = 0; idx < g_instance.comm_cxt.counters_cxt.g_cur_node_num; idx++) {  // node index
            for (sid = 1; sid < g_instance.comm_cxt.counters_cxt.g_max_stream_num; sid++) {  // stream index
                cmailbox = &C_MAILBOX(idx, sid);
                if (cmailbox->state != MAIL_CLOSED && cmailbox->stream_key.queryId == plan_id &&
                    cmailbox->stream_key.planNodeId == plan_node_id) {
                    gsockAddr[ready_conn].idx = idx;
                    gsockAddr[ready_conn].sid = sid;
                    gsockAddr[ready_conn].ver = cmailbox->local_version;
                    gsockAddr[ready_conn].type = GSOCK_CONSUMER;

                    ready_conn++;
                }
            }
        }

        if (g_instance.comm_cxt.tests_cxt.libcomm_stop_flag == true) {
            goto clean_return;
        }

        if (ready_conn == producerNum) {
            break;
        } else {
            ready_conn = 0;
            sleep(1);
        }
    }

    msg_buf = (char*)malloc(msg_len);
    datamarks = (int*)calloc(producerNum, sizeof(int));
    if (msg_buf == NULL || datamarks == NULL) {
        goto clean_return;
    }

    for (;;) {
        error = gs_wait_poll(gsockAddr, producerNum, datamarks, -1, false);
        if (error < 0 || g_instance.comm_cxt.tests_cxt.libcomm_stop_flag == true) {
            goto clean_return;
        }

        for (i = 0; i < producerNum; i++) {
            if (datamarks[i] > 0) {
                error = gs_recv(&gsockAddr[i], msg_buf, msg_len);
                if (error < 0 && errno == ECOMMSCTPNODATA) {
                    continue;
                }

                if (error < 0 || g_instance.comm_cxt.tests_cxt.libcomm_stop_flag == true) {
                    goto clean_return;
                }
            }
        }

        current_recv += msg_len;
        if (current_recv >= once_recv && sleep_time > 0) {
            usleep(sleep_time * 1000);
            current_recv = 0;
        }
    }

clean_return:
    if (gsockAddr != NULL) {
        for (i = 0; i < producerNum; i++) {
            gs_close_gsocket(&(gsockAddr[i]));
        }
        free(gsockAddr);
        gsockAddr = NULL;
    }

    if (msg_buf != NULL) {
        free(msg_buf);
        msg_buf = NULL;
    }

    if (datamarks != NULL) {
        free(datamarks);
        datamarks = NULL;
    }

    atomic_sub(&g_instance.comm_cxt.tests_cxt.libcomm_test_current_thread, 1);

    return NULL;
}  // consumerThread

void libcomm_performance_test()
{
    /* connections is not build complete. */
    if (global_node_definition == NULL || global_node_definition->num_nodes == 0) {
        return;
    }

    /* all work thread is running. */
    if (g_instance.comm_cxt.tests_cxt.libcomm_test_current_thread ==
        g_instance.comm_cxt.tests_cxt.libcomm_test_thread_num * 2) {
        return;
    }

    /* set stop flag, wait all work thread exit. */
    g_instance.comm_cxt.tests_cxt.libcomm_stop_flag = true;
    while (g_instance.comm_cxt.tests_cxt.libcomm_test_current_thread) {
        gs_broadcast_poll();
        sleep(1);
    }
    g_instance.comm_cxt.tests_cxt.libcomm_stop_flag = false;

    if (g_instance.comm_cxt.tests_cxt.libcomm_test_thread_arg != NULL) {
        free(g_instance.comm_cxt.tests_cxt.libcomm_test_thread_arg);
        g_instance.comm_cxt.tests_cxt.libcomm_test_thread_arg = NULL;
    }
    g_instance.comm_cxt.tests_cxt.libcomm_test_thread_arg =
        (int*)calloc(g_instance.comm_cxt.tests_cxt.libcomm_test_thread_num, sizeof(int));
    if (g_instance.comm_cxt.tests_cxt.libcomm_test_thread_arg == NULL) {
        return;
    }

    pthread_t t_id;
    for (int i = 0; i < g_instance.comm_cxt.tests_cxt.libcomm_test_thread_num; i++) {
        g_instance.comm_cxt.tests_cxt.libcomm_test_thread_arg[i] = i;
        if (0 == pthread_create(
                &t_id, NULL, &libcommProducerThread, &g_instance.comm_cxt.tests_cxt.libcomm_test_thread_arg[i])) {
            atomic_add(&g_instance.comm_cxt.tests_cxt.libcomm_test_current_thread, 1);
        }
        if (0 == pthread_create(
                &t_id, NULL, &libcommConsumerThread, &g_instance.comm_cxt.tests_cxt.libcomm_test_thread_arg[i])) {
            atomic_add(&g_instance.comm_cxt.tests_cxt.libcomm_test_current_thread, 1);
        }
    }

    return;
}
#endif

/*
 * function name    : gs_accept_ctrl_conntion
 * description      : check connection, if connection is new, close old one.
 * arguments        : _in_ t_fd_id: the fd and fd id for this connection.
 *                    _in_ fcmsgr: the message we received.
 */
static void gs_accept_ctrl_conntion(struct sock_id* t_fd_id, struct FCMSG_T* fcmsgr)
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
            gs_r_close_bad_ctrl_tcp_sock(&old_fd_id, ECOMMSCTPPEERCHANGED);
            LIBCOMM_ELOG(WARNING,
                "(r|flow ctrl)\tOld connection exist, maybe the primary is changed, old address of "
                "node[%d] is:%s, the connection will be reset.",
                idx,
                g_instance.comm_cxt.g_r_node_sock[idx].remote_host);
        }

        /* regist new sock and sock id to node idx */
        if (gs_map_sock_id_to_node_idx(*t_fd_id, idx) < 0) {
            LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tFailed to save sock and sockid.");
            gs_r_close_bad_ctrl_tcp_sock(t_fd_id, ECOMMSCTPTCPDISCONNECT);
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
            gs_r_close_bad_ctrl_tcp_sock(t_fd_id, ECOMMSCTPTCPDISCONNECT);

            return;
        }
    } else if (fcmsgr->type == CTRL_CONN_REGIST_CN) {
        if (g_instance.comm_cxt.g_ha_shm_data) {
            current_mode = g_instance.comm_cxt.g_ha_shm_data->current_mode;
        } else {
            current_mode = UNKNOWN_MODE;
            LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tCannot get current mode, postmaster exit.");
        }

        if (current_mode != STANDBY_MODE && current_mode != PENDING_MODE && current_mode != UNKNOWN_MODE) {
            ack = 'o';
        } else {
            ack = 'r';
        }

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

            gs_r_close_bad_ctrl_tcp_sock(t_fd_id, ECOMMSCTPTCPDISCONNECT);

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
 * function name    : gs_receivers_flow_handle_tid_request
 * description    : save peer thread id when received CTRL_PEER_TID message.
 * arguments        : _in_ fcmsgr: the message that receivers_flow thread received.
 * return value    : void
 */
static void gs_receivers_flow_handle_tid_request(FCMSG_T* fcmsgr)
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
static int gs_send_msg_by_unix_domain(const void* msg, int msg_len)
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

/*
 * function name    : gs_r_build_reply_conntion
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
static int gs_r_build_reply_conntion(FCMSG_T* fcmsgr, int local_version)
{
    errno_t ss_rc;
    uint32 cpylen;

    int node_idx = fcmsgr->node_idx;
    int streamid = fcmsgr->streamid;
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
    libcomm_addrinfo.sctp_port = remote_data_port;
    cpylen = comm_get_cpylen(remote_nodename, NAMEDATALEN);
    ss_rc = memset_s(libcomm_addrinfo.nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(libcomm_addrinfo.nodename, NAMEDATALEN, remote_nodename, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    libcomm_addrinfo.nodename[cpylen] = '\0';

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
            libcomm_addrinfo.sctp_port,
            node_idx,
            libcomm_addrinfo.nodename,
            mc_strerror(errno));
        errno = ECOMMSCTPSCTPCONNFAIL;
        return -1;
    }

    COMM_DEBUG_LOG("(r|build reply conn)\tBuild data logical connection for node[%d]:%s.",
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    struct p_mailbox* pmailbox = &P_MAILBOX(node_idx, streamid);

    LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
    if (pmailbox->state != MAIL_CLOSED) {
        MAILBOX_ELOG(pmailbox,
            WARNING,
            "(r|build reply conn)\tFailed to get mailbox for node[%d,%d]:%s.",
            node_idx,
            streamid,
            REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));
        gs_s_close_logic_connection(pmailbox, ECOMMSCTPREMOETECLOSE, NULL);
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
    if (g_instance.comm_cxt.commutil_cxt.g_stat_mode && (pmailbox->statistic == NULL)) {
        LIBCOMM_MALLOC(pmailbox->statistic, sizeof(struct pmailbox_statistic), pmailbox_statistic);
        if (pmailbox->statistic == NULL) {
            errno = ECOMMSCTPRELEASEMEM;
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
            return -1;
        }
    }

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);

    return 0;
}

/*
 * function name    : gs_s_build_reply_conntion
 * description      : as the connection between cn & dn is duplex.
 *                    cn need to inital cmailbox to recv msgs from dn
 * arguments        : fcmsgr: provides gs_sock
 */
static void gs_s_build_reply_conntion(libcommaddrinfo* addr_info, int remote_version)
{
    int node_idx = addr_info->gs_sock.idx;
    int streamid = addr_info->gs_sock.sid;
    int local_version = addr_info->gs_sock.ver;

    // initialize consumer cmailbox
    struct c_mailbox* cmailbox = &C_MAILBOX(node_idx, streamid);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);

    if (gs_check_mailbox(cmailbox->local_version, local_version) == true) {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
        return;
    }

    if (cmailbox->state != MAIL_CLOSED) {
        MAILBOX_ELOG(cmailbox,
            WARNING,
            "(s|build reply conn)\tFailed to get mailbox for node[%d,%d]:%s.",
            node_idx,
            streamid,
            REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, node_idx));
        gs_r_close_logic_connection(cmailbox, ECOMMSCTPREMOETECLOSE, NULL);
    }

    cmailbox->local_version = local_version;
    cmailbox->remote_version = remote_version;
    cmailbox->ctrl_tcp_sock = g_instance.comm_cxt.g_r_node_sock[node_idx].ctrl_tcp_sock;
    cmailbox->state = MAIL_RUN;
    cmailbox->bufCAP = DEFULTMSGLEN;
    cmailbox->stream_key = addr_info->sctpKey;
    cmailbox->query_id = DEBUG_QUERY_ID;
    cmailbox->local_thread_id = 0;
    cmailbox->peer_thread_id = 0;
    cmailbox->close_reason = 0;
    if (g_instance.comm_cxt.commutil_cxt.g_stat_mode && (cmailbox->statistic == NULL)) {
        LIBCOMM_MALLOC(cmailbox->statistic, sizeof(struct cmailbox_statistic), cmailbox_statistic);
        if (cmailbox->statistic == NULL) {
            errno = ECOMMSCTPRELEASEMEM;
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
            return;
        }
    }
    COMM_STAT_CALL(cmailbox, cmailbox->statistic->start_time = (uint32)mc_timers_ms());
    COMM_DEBUG_LOG("(s|build reply conn)\tNode[%d] stream[%d], node name[%s] is in state[%s].",
        node_idx,
        streamid,
        g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename,
        stream_stat_string(cmailbox->state));
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);

    return;
}

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
static void gs_receivers_flow_handle_ready_request(FCMSG_T* fcmsgr)
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
        errno = ECOMMSCTPAPPCLOSE;
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
        gs_r_close_logic_connection(cmailbox, ECOMMSCTPREMOETECLOSE, NULL);
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
            errno = ECOMMSCTPRELEASEMEM;
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

    // for dual connection, we need to build an inverse logic conn with same gs_sock
    if (fcmsgr->type == CTRL_CONN_DUAL) {
        u_sess->pgxc_cxt.NumDataNodes = (int)(fcmsgr->extra_info);
        // build pmailbox with the same version and remote_verion as cmailbox,
        // when this connection is duplex.
        if (gs_r_build_reply_conntion(fcmsgr, local_version) != 0) {
            LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);
            gs_r_close_logic_connection(cmailbox, ECOMMSCTPTCPDISCONNECT, NULL);
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
            goto accept_failed;
        }
    }

    gs_sock.idx = node_idx;
    gs_sock.sid = streamid;
    gs_sock.ver = local_version;

    if (fcmsgr->type == CTRL_CONN_DUAL) {
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
        errno = ECOMMSCTPTCPDISCONNECT;
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
    if ((uint64)mc_tcp_get_connect_timeout() < deal_time) {
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
        connInfo.port.sctpLayer.gsock = gs_sock;

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
        if ((uint64)mc_tcp_get_connect_timeout() < deal_time) {
            LIBCOMM_ELOG(WARNING,
                         "(r|flow ctrl)\tWake up consumer timeout for node[%d]:%s, it takes %lus, "
                          "because the lock wait timeout, query id:%lu :%s.",
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
static void gs_receivers_flow_handle_close_request(FCMSG_T* fcmsgr)
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

    gs_r_close_logic_connection(cmailbox, ECOMMSCTPREMOETECLOSE, NULL);
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
static void gs_receivers_flow_handle_assert_fail_request(FCMSG_T* fcmsgr)
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
static void gs_senders_flow_handle_tid_request(FCMSG_T* fcmsgr)
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
static void gs_senders_flow_handle_init_request(FCMSG_T* fcmsgr)
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
    gs_s_close_logic_connection(pmailbox, ECOMMSCTPREJECTSTREAM, NULL);
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
static void gs_senders_flow_handle_ready_request(FCMSG_T* fcmsgr)
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
static void gs_senders_flow_handle_resume_request(FCMSG_T* fcmsgr)
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
static void gs_senders_flow_handle_close_request(FCMSG_T* fcmsgr)
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
    gs_s_close_logic_connection(pmailbox, ECOMMSCTPREMOETECLOSE, NULL);
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
static void gs_senders_flow_handle_assert_fail_request(FCMSG_T* fcmsgr)
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
static void gs_senders_flow_handle_stop_query_request(FCMSG_T* fcmsgr)
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
    int sock = mc_tcp_connect(libcomm_addrinfo->host, libcomm_addrinfo->sctp_port);
    if (sock < 0) {
        LIBCOMM_ELOG(WARNING,
            "(s|build tcp connection)\tFailed to build data connection "
            "to %s:%d for node[%d]:%s, error[%d:%d]:%s.",
            libcomm_addrinfo->host,
            libcomm_addrinfo->sctp_port,
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
            libcomm_addrinfo->sctp_port,
            mc_strerror(errno));
        errno = ECOMMSCTPGSSAUTHFAIL;
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
    connect_package.type = SCTP_PKG_TYPE_CONNECT;
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
            libcomm_addrinfo->sctp_port,
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
    if (error < 0 || ack_msg.result != 1 || ack_msg.type != SCTP_PKG_TYPE_ACCEPT) {
        LIBCOMM_ELOG(WARNING,
            "(s|build tcp connection)\tFailed to recv assoc id from %s:%d "
            "for node[%d]:%s on socket[%d].",
            libcomm_addrinfo->host,
            libcomm_addrinfo->sctp_port,
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
    struct sock_id sctp_fd_id = {g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket,
        g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id};
    gs_s_close_bad_data_socket(&sctp_fd_id, ECOMMSCTPPEERCHANGED, node_idx);

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

    g_instance.comm_cxt.g_senders->sender_conn[node_idx].port = libcomm_addrinfo->sctp_port;
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

    addr.port = libcomm_addrinfo->sctp_port;
    /* update connection state to succeed when connect succeed */
    gs_update_connection_state(addr, CONNSTATESUCCEED, true, node_idx);

    LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
    LIBCOMM_ELOG(LOG,
        "(s|build tcp connection)\tSucceed to connect %s:%d with socket[%d:%d] for node[%d]:%s.",
        libcomm_addrinfo->host,
        libcomm_addrinfo->sctp_port,
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
static int gs_s_build_tcp_ctrl_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx, bool is_reply)
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
        errno = ECOMMSCTPTCPCONNFAIL;
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
        errno = ECOMMSCTPGSSAUTHFAIL;
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

    error = gs_send_ctrl_msg_by_socket(tcp_sock, &fcmsgs);
    if (error < 0) {
        mc_tcp_close(tcp_sock);
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tSend ctrl msg failed remote[%s] with addr[%s:%d].",
            remote_nodename,
            remote_host,
            remote_tcp_port);
        errno = ECOMMSCTPTCPCONNFAIL;
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
            errno = ECOMMSCTPTCPCONNFAIL;
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
            errno = ECOMMSCTPTCPCONNFAIL;
            return -1;
        }
    }

    struct sock_id fd_id = {tcp_sock, 0};
    // if we successfully to connect, we should record the socket(fd) and the version(id)
    if (gs_update_fd_to_htab_socket_version(&fd_id) < 0) {
        mc_tcp_close(tcp_sock);
        LIBCOMM_ELOG(WARNING, "(s|connect)\tFailed to malloc for socket.");
        errno = ECOMMSCTPMEMALLOC;
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
    struct sock_id sctp_fd_id = {g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket,
        g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id};
    gs_s_close_bad_data_socket(&sctp_fd_id, ECOMMSCTPPEERCHANGED, node_idx);
    LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);

    /* close old ctrl connection */
    g_instance.comm_cxt.g_s_node_sock[node_idx].ip_changed = true;
    g_instance.comm_cxt.g_s_node_sock[node_idx].lock();
    ctrl_sock = g_instance.comm_cxt.g_s_node_sock[node_idx].get_nl(CTRL_TCP_SOCK, &ctrl_sock_id);
    struct sock_id ctrl_fd_id = {ctrl_sock, ctrl_sock_id};
    gs_s_close_bad_ctrl_tcp_sock(&ctrl_fd_id, ECOMMSCTPPEERCHANGED, false, node_idx);

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
        errno = ECOMMSCTPMEMALLOC;
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

/*
 * function name   : gs_s_check_connection
 * description     : sender check that is receiver changed.
 *                   if the destination ip is changed,
 *                   which means the primary and the standby are reverted,
 *                   then we close tcp/sctp connection and rebuild later.
 * arguments       :
 *                   _in_ sctp_addrinfo: remote infomation.
 *                   _in_ node_idx: remote node index.
 * return value    :
 *                   false: failed.
 *                   true : succeed.
 */
static bool gs_s_check_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx, bool is_reply, int type)
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
        addr.port = libcomm_addrinfo->sctp_port;
    }

retry:

    int rc = gs_s_get_connection_state(addr, node_idx, type);

    if (likely(rc == CONNSTATESUCCEED)) {
        /* data channel need to check socket even if the connection state in htap is succeed */
        if (type == DATA_CHANNEL) {
            LIBCOMM_PTHREAD_RWLOCK_WRLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
            if (g_libcomm_adapt.check_socket(g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket) != 0) {
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
                gs_s_close_bad_data_socket(&fd_id, ECOMMSCTPSCTPDISCONNECT, node_idx);
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
        errno = ECOMMSCTPARGSINVAL;
        return -1;
    }

    if (unlikely((libcomm_addrinfo->host == NULL) || (libcomm_addrinfo->ctrl_port <= 0) ||
                 (libcomm_addrinfo->sctp_port <= 0))) {
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tInvalid argument: %s"
            "tcp port:%d, sctp port:%d.",
            libcomm_addrinfo->host == NULL ? "host is NULL, " : "",
            libcomm_addrinfo->ctrl_port,
            libcomm_addrinfo->sctp_port);
        errno = ECOMMSCTPARGSINVAL;
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
        errno = ECOMMSCTPSCTPCONNFAIL;
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
    pmailbox->stream_key = libcomm_addrinfo->sctpKey;
    pmailbox->query_id = DEBUG_QUERY_ID;
    pmailbox->local_thread_id = 0;
    pmailbox->peer_thread_id = 0;
    pmailbox->close_reason = 0;
    if (g_instance.comm_cxt.commutil_cxt.g_stat_mode && (pmailbox->statistic == NULL)) {
        LIBCOMM_MALLOC(pmailbox->statistic, sizeof(struct pmailbox_statistic), pmailbox_statistic);
        if (pmailbox->statistic == NULL) {
            errno = ECOMMSCTPRELEASEMEM;
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
    if (IS_PGXC_COORDINATOR) {
        fcmsgs.type = CTRL_CONN_DUAL;
        fcmsgs.extra_info = (u_sess != NULL) ? (unsigned long)u_sess->pgxc_cxt.NumDataNodes : 0;
    } else {
        fcmsgs.type = CTRL_CONN_REQUEST;
        fcmsgs.extra_info = 0;
    }

    fcmsgs.node_idx = node_idx;
    fcmsgs.streamid = streamid;
    // send pmailbox version to cmailbox,
    // and cmailbox will save as cmailbox->remote_version.
    fcmsgs.version = version;
    fcmsgs.stream_key = libcomm_addrinfo->sctpKey;
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
    fcmsgs.streamcap = ((unsigned long)g_instance.attr.attr_network.comm_control_port << 32) +
                       (long)g_instance.attr.attr_network.comm_sctp_port;

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&(pmailbox->sinfo_lock));
    rc = gs_send_ctrl_msg(&g_instance.comm_cxt.g_s_node_sock[node_idx], &fcmsgs, ROLE_PRODUCER);
    if (rc <= 0) {
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tFailed to send ready msg to node[%d]:%s, detail:%s.",
            node_idx,
            REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx),
            mc_strerror(errno));

        errno = ECOMMSCTPTCPDISCONNECT;
        return -1;
    }

    /*
     * Step 8: set the node index and stream index for caller, and return the stream index
     */
    libcomm_addrinfo->gs_sock.idx = node_idx;
    libcomm_addrinfo->gs_sock.sid = streamid;
    libcomm_addrinfo->gs_sock.ver = version;
    if (IS_PGXC_COORDINATOR) {
        libcomm_addrinfo->gs_sock.type = GSOCK_DAUL_CHANNEL;
    } else {
        libcomm_addrinfo->gs_sock.type = GSOCK_PRODUCER;
    }

    COMM_DEBUG_LOG("(s|send connect)\tConnect finish for node[%d]:%s.",
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    return streamid;
}  // gs_connect

// Registed a Consumer callback function to wake up the Consumer (thread at executor) when a Producer (thread at
// executor) connected successfully
//
void gs_connect_regist_callback(wakeup_hook_type wakeup_callback)
{
    g_instance.comm_cxt.gs_wakeup_consumer = wakeup_callback;
}  // gs_connect_regist_callback

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
    int wait_index = -1;
    int i;
    int node_idx = -1;
    int streamid = -1;
    int version = -1;
    if (timeout == -1) {
        timeout = CONNECT_TIMEOUT;
    }
    libcommaddrinfo* addr_info = NULL;
    struct p_mailbox* pmailbox = NULL;
    int remote_version = -1;
    int error_index = -1;
    bool build_reply_conn = false;

    if (libcomm_addrinfo == NULL || addr_num == 0) {
        LIBCOMM_ELOG(WARNING,
            "(s|connect)\tInvalid argument: %saddress number is %d.",
            libcomm_addrinfo == NULL ? "libcomm addr info is NULL, " : "",
            addr_num);
        errno = ECOMMSCTPARGSINVAL;
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
                goto clean_connection;
            }
        }
    }

    // check all mailboxs state is MAIL_READY
    // if the state of mailbox is MAIL_TO_CLOSE, top consumer will close the logic connection.
    // if mailbox state is CTRL_CLOSE(gs_check_mailbox return false),
    // means consumer do not need the data from this datanode, not report error
    for (;;) {
        wait_index = -1;
        for (i = 0; i < addr_num; i++) {
            addr_info = libcomm_addrinfo[i];
            node_idx = addr_info->gs_sock.idx;
            streamid = addr_info->gs_sock.sid;
            version = addr_info->gs_sock.ver;
            build_reply_conn = false;

            pmailbox = &P_MAILBOX(node_idx, streamid);
            LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
            // if gs_check_mailbox return false, means consumer close it, not need report error
            if (gs_check_mailbox(pmailbox->local_version, version) == true) {
                if (pmailbox->state == MAIL_READY) {
                    pmailbox->semaphore = t_thrd.comm_cxt.libcomm_semaphore;
                    COMM_DEBUG_LOG(
                        "(s|parallel connect)\tWait node[%d] stream[%d] state[%s], node name[%s], bufCAP[%lu].",
                        node_idx,
                        streamid,
                        stream_stat_string(pmailbox->state),
                        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
                        pmailbox->bufCAP);
                    wait_index = i;
                } else if (pmailbox->state == MAIL_TO_CLOSE) {  // mail would close later, sender closes pmailbox,
                    LIBCOMM_ELOG(WARNING,
                        "(s|parallel connect)\tMAIL_TO_CLOSE node[%d] stream[%d] state[%s], node name[%s].",
                        node_idx,
                        streamid,
                        stream_stat_string(pmailbox->state),
                        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename);
                    gs_s_close_logic_connection(pmailbox, ECOMMSCTPREMOETECLOSE, NULL);

                    // before continue or goto clean_connection, we must release the sinfo_lock.
                    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
                    if (IS_PGXC_COORDINATOR) {
                        addr_info->gs_sock = GS_INVALID_GSOCK;
                        continue;
                    } else {
                        errno = ECOMMSCTPSCTPCONNFAIL;
                        error_index = i;
                        goto clean_connection;
                    }
                } else {
                    pmailbox->semaphore = NULL;
                    // for cn initial cmailbox as well as the connection is duplex
                    if (IS_PGXC_COORDINATOR) {
                        remote_version = pmailbox->remote_version;
                        build_reply_conn = true;
                    }
                }
            } else {
                // if gs_check_mailbox return false, means consumer close it, we need to reset gsockt
                addr_info->gs_sock = GS_INVALID_GSOCK;
            }

            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);

            // for cn initial cmailbox as well as the connection is duplex
            if (build_reply_conn) {
                // build cmailbox with the same version and remote_verion as pmailbox,
                // when this connection is duplex.
                gs_s_build_reply_conntion(addr_info, remote_version);
            }
        }

        // we wait on the last mailbox that state is not MAIL_READY
        if (wait_index >= 0) {
            pgstat_report_waitstatus_comm(STATE_STREAM_WAIT_CONNECT_NODES,
                libcomm_addrinfo[wait_index]->nodeIdx,
                wait_index + 1,
                -1,
                global_node_definition ? global_node_definition->num_nodes : -1);

            re = gs_poll(timeout);
            if (re == ETIMEDOUT) {
                if (IS_PGXC_COORDINATOR) {
                    /* close all timeout connections */
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
                            gs_s_close_logic_connection(pmailbox, ECOMMSCTPEPOLLTIMEOUT, NULL);
                        }
                        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
                    }
                } else {
                    errno = ETIMEDOUT;
                    error_index = wait_index;
                    goto clean_connection;
                }
            }
        } else {
            break;
        }
    }
    LIBCOMM_INTERFACE_END(false, TempImmediateInterruptOK);

    // if gs_check_mailbox return false, means consumer close it, not need report error
    return 0;
// clean all connecions, and return failed.
clean_connection:
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
 * function name   : gs_s_send_start_ctrl_msg
 * description     : producer send local thread id to consumer,
 *                   it is means producer start to send data.
 * notice          : we must get mailbox lock before.
 * arguments       :
 *                   _in_ pmailbox: logic conntion info.
 * return value    :
 *                   false: failed.
 *                   true : succeed.
 */
static bool gs_s_form_start_ctrl_msg(p_mailbox* pmailbox, FCMSG_T* msg)
{
    pid_t local_tid = t_thrd.comm_cxt.MyPid;
    errno_t ss_rc;
    uint32 cpylen;

    if (pmailbox->query_id != DEBUG_QUERY_ID) {
        pmailbox->query_id = DEBUG_QUERY_ID;
    }

    // send local thread id to remote
    if (local_tid != pmailbox->local_thread_id) {
        int node_idx = pmailbox->idx;
        int streamid = pmailbox->streamid;

        // change local stream state and quota first
        pmailbox->local_thread_id = local_tid;

        // change local stream state and quota first
        msg->type = CTRL_PEER_TID;
        msg->node_idx = node_idx;
        msg->streamid = streamid;
        msg->streamcap = 0;
        msg->version = pmailbox->remote_version;
        msg->extra_info = pmailbox->local_thread_id;
        msg->query_id = pmailbox->query_id;

        cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
        ss_rc = memset_s(msg->nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
        securec_check(ss_rc, "\0", "\0");
        ss_rc = strncpy_s(msg->nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
        securec_check(ss_rc, "\0", "\0");
        msg->nodename[cpylen] = '\0';

        return true;
    }

    return false;
}  // gs_s_send_start_ctrl_msg

/*
 * @Description:    push the data package to local cmailbox buffer.
 * @IN streamid:    the producer and consumer have the same stream id.
 * @IN message:        data message.
 * @IN m_len:        message len.
 * @Return:            -1:        push data failed.
 *                     m_len:     push data succsessed.
 * @See also:        local producer can use memcpy to push data package,
 *                    no need push to data stack
 */
static int gs_push_local_buffer(int streamid, const char* message, int m_len, int cmailbox_version)
{
    int cmailbox_idx = -1;
    struct char_key ckey;
    c_mailbox* cmailbox = NULL;
    errno_t ss_rc;
    bool found = false;
    uint32 cpylen;

    t_thrd.comm_cxt.g_receiver_loop_poll_up = COMM_STAT_TIME();

    cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
    ss_rc = memset_s(ckey.name, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(ckey.name, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    ckey.name[cpylen] = '\0';

    LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_nodename_node_idx_lock);
    nodename_entry* entry_name = (nodename_entry*)hash_search(g_htab_nodename_node_idx, &ckey, HASH_FIND, &found);
    if (found) {
        cmailbox_idx = entry_name->entry.val;
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_nodename_node_idx_lock);

    if (cmailbox_idx < 0) {
        errno = ECOMMSCTPREMOETECLOSE;
        return -1;
    }
    cmailbox = &C_MAILBOX(cmailbox_idx, streamid);

    struct iovec* iov = NULL;
    struct mc_lqueue_item* iov_item = NULL;
    // use share memory malloc for buffer received data message
    if (libcomm_malloc_iov_item(&iov_item, IOV_DATA_SIZE) != 0) {
        return -1;
    }
    iov = iov_item->element.data;

    // copy the datat to the buffer of executor
    ss_rc = memcpy_s(iov->iov_base, IOV_DATA_SIZE, message, m_len);
    securec_check(ss_rc, "\0", "\0");
    iov->iov_len = m_len;

    if (gs_push_cmailbox_buffer(cmailbox, iov_item, cmailbox_version) < 0) {
        libcomm_free_iov_item(&iov_item, IOV_DATA_SIZE);
        return -1;
    }

    return m_len;
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
    if (gs_sock == NULL) {
        LIBCOMM_ELOG(WARNING, "(s|send)\tInvalid argument: gs_sock is NULL");
        errno = ECOMMSCTPARGSINVAL;
        return -1;
    }

    // step 1: get node index, stream index, and connection information
    //
    int node_idx = gs_sock->idx;
    int streamid = gs_sock->sid;
    int local_version = gs_sock->ver;
    int remote_version = -1;

    if ((message == NULL) || (m_len <= 0) || (node_idx < 0) ||
        (node_idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num) || (streamid <= 0) ||
        (streamid >= g_instance.comm_cxt.counters_cxt.g_max_stream_num)) {
        LIBCOMM_ELOG(WARNING,
            "(s|send)\tInvalid argument: %s"
            "len=%d, "
            "node idx=%d, stream id=%d.",
            message == NULL ? "message is NULL, " : "",
            m_len,
            node_idx,
            streamid);

        errno = ECOMMSCTPARGSINVAL;
        return -1;
    }

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);
    bool TempImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = false;
    errno = 0;

    COMM_TIMER_INIT();

    int ret = 0;
    int sent_size = 0;
    bool send_msg = false;
    uint64 time_enter = COMM_STAT_TIME();
    uint64 time_now = time_enter;
    uint64 wait_quota_start = 0;
    uint64 wait_quota_end = 0;
    unsigned long need_send_len = 0;
    struct FCMSG_T fcmsgs = {0x0};
    struct sock_id fd_id = {0, 0};
    bool notify_remote = false;
    struct p_mailbox* pmailbox = NULL;

    // set default time out to 600 seconds(10 minutes)
    if (time_out == -1) {
        time_out = WAITQUOTA_TIMEOUT;
    }
    int total_wait_time = 0;
    int single_timeout = SINGLE_WAITQUOTA;

    char* node_name = NULL;
    uint32 send_start, send_end;
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

    // check the stream state and quota size, if the state is not RESUME, we should wait
    //
    COMM_TIMER_LOG("(s|send)\tWait quota start for node[%d,%d]:%s.",
        node_idx,
        streamid,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    if (m_len > DEFULTMSGLEN) {
        need_send_len = DEFULTMSGLEN;
    } else {
        need_send_len = (unsigned long)m_len;
    }

    fd_id.fd = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket;
    fd_id.id = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id;

    for (;;) {
        // stream state is right and quota size is enough
        if (pmailbox->bufCAP >= need_send_len) {
            pmailbox->state = MAIL_RUN;
            break;
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
        if (block_mode == FALSE) {
            ret = BROADCAST_WAIT_QUOTA;
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
            goto return_result;
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);

        pgstat_report_waitstatus_phase(PHASE_WAIT_QUOTA);

        /* wait quota start */
        StreamTimeWaitQuotaStart(t_thrd.pgxc_cxt.GlobalNetInstr);
        wait_quota_start = mc_timers_ms();
        // wait quota 3 seconds, then wake up and check mailbox
        ret = gs_poll(single_timeout);
        wait_quota_end = mc_timers_ms();
        StreamTimeWaitQuotaEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
        /* wait quota end */
        LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);
        // the mailbox is closed or reused by other query, we should return error
        if (gs_check_mailbox(pmailbox->local_version, local_version) == false) {
            COMM_DEBUG_LOG("(s|send)\tStream has already closed by remote:%s, detail:%s.",
                REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx),
                mc_strerror(pmailbox->close_reason));

            errno = pmailbox->close_reason;
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
            ret = -1;
            goto return_result;
        }

        pmailbox->semaphore = NULL;

        // stream state is right and quota size is enough, can send now
        if (pmailbox->bufCAP >= need_send_len) {
            pmailbox->state = MAIL_RUN;
            break;
        }

        // if it is waked up normal, and quota size is not enough,
        // there must be interruption, we should return and CHECK_FOR_INTERRUPT
        if (ret == 0) {
            ret = 0;
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
            goto return_result;
        }

        // wait quota timedout, if total_wait_time > time_out,
        // we should return and CHECK_FOR_INTERRUPT
        if (ret == ETIMEDOUT) {
            total_wait_time += single_timeout;
            if (total_wait_time >= time_out) {
                errno = ECOMMSCTPEPOLLTIMEOUT;
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
                ret = 0;
                goto return_result;
            }
        }

        // wait quota error (not timedout)
        if (ret != 0 && ret != ETIMEDOUT) {
            MAILBOX_ELOG(pmailbox, WARNING, "(s|send)\tStream waked up by error[%d]:%s.", ret, mc_strerror(errno));

            gs_s_close_logic_connection(pmailbox, ECOMMSCTPWAITQUOTAFAIL, &fcmsgs);
            notify_remote = true;
            errno = ECOMMSCTPWAITQUOTAFAIL;
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
            ret = -1;
            goto return_result;
        }
    }

    COMM_TIMER_LOG("(s|send)\tWait quota end for node[%d,%d]:%s.",
        node_idx,
        streamid,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    send_start = COMM_STAT_TIME();
    remote_version = pmailbox->remote_version;

    send_msg = gs_s_form_start_ctrl_msg(pmailbox, &fcmsgs);

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
    // send local thread id to remote without cmailbox lock
    if (send_msg && (gs_send_ctrl_msg(&g_instance.comm_cxt.g_s_node_sock[node_idx], &fcmsgs, ROLE_PRODUCER) <= 0)) {
        errno = ECOMMSCTPTCPDISCONNECT;
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

        do {
            ret = g_libcomm_adapt.send_data(&send_info);
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
                g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket,
                errno,
                mc_strerror(errno));

            errno = ECOMMSCTPSCTPSND;
            ret = -1;
            goto return_result;
        }

        if (is_tcp_mode() && g_ackchk_time > 0) {
            recv_ackchk_msg(send_info.socket);
        }
    }

    // step 4: if we send successfully, change the state and quota of the stream
    //
    sent_size = ret;
    send_end = COMM_STAT_TIME();
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
    LIBCOMM_ASSERT((bool)(pmailbox->bufCAP >= (unsigned long)sent_size), node_idx, streamid, ROLE_PRODUCER);

    // update quota
    if (g_instance.comm_cxt.quota_cxt.g_having_quota) {
        pmailbox->bufCAP -= sent_size;
    }

    if (pmailbox->state != MAIL_RUN) {
        LIBCOMM_ELOG(WARNING, "(s|send)\tsend state is wrong[%d].", pmailbox->state);
    }

    /* update the statistic information of the mailbox */
    if (pmailbox->statistic != NULL) {
        if (pmailbox->statistic->first_send_time == 0) {
            pmailbox->statistic->first_send_time = send_start;
            /* first time, calculate time difference between connect finish time and first send time  */
            pmailbox->statistic->producer_elapsed_time += (uint32)ABS_SUB(time_enter, pmailbox->statistic->start_time);
        } else {
            /* other time, calculate time difference between last send time and this send time  */
            pmailbox->statistic->producer_elapsed_time +=
                (uint32)ABS_SUB(time_enter, t_thrd.comm_cxt.g_producer_process_duration);
        }
        pmailbox->statistic->last_send_time = send_start;
        pmailbox->statistic->wait_quota_overhead += (uint32)ABS_SUB(wait_quota_end, wait_quota_start);
        pmailbox->statistic->os_send_overhead += ABS_SUB(send_end, send_start);

        time_now = COMM_STAT_TIME();
        pmailbox->statistic->total_send_time += ABS_SUB(time_now, time_enter);
        pmailbox->statistic->send_bytes += (uint64)sent_size;
        pmailbox->statistic->call_send_count++;
    }
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);

    COMM_DEBUG_LOG("(s|send)\tSend to node[%d]:%s on stream[%d] with msg:%c, m_len[%d] @ bufCAP[%lu].",
        node_idx,
        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
        streamid,
        message[0],
        sent_size,
        pmailbox->bufCAP);
    // step 5: send successfully, reture the sent size
    //
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
    t_thrd.comm_cxt.g_producer_process_duration = time_now;
    pgstat_report_waitstatus_phase(oldPhase);

    return ret;
}  // gs_send

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
    if ((libcomm_addr_head == NULL) || (message == NULL) || (m_len <= 0)) {
        LIBCOMM_ELOG(WARNING,
            "(s|send)\tInvalid argument: %s%s, len=%d.",
            libcomm_addr_head == NULL ? "addr list is NULL, " : "",
            message == NULL ? "message is NULL, " : "",
            m_len);
        errno = ECOMMSCTPARGSINVAL;
        return -1;
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
        errno = ECOMMSCTPARGSINVAL;
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
                addr_info->sctp_port = 0;
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
    addr_info = libcomm_addr_head;
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
    return 0;
}

/*
 * function name    : gs_r_send_start_ctrl_msg
 * description    : consumer send local thread id to producer,
 *                   it is means consumer start to receive data,
 *                   so we also send quota to producer.
 * notice        : we must get mailbox lock before.
 * arguments        :
 *                   _in_ cmailbox:  logic conntion info.
 * return value    :
 *                   false: failed.
 *                   true : succeed.
 */
static bool gs_r_form_start_ctrl_msg(c_mailbox* cmailbox, FCMSG_T* msg)
{
    pid_t local_tid = t_thrd.comm_cxt.MyPid;
    errno_t ss_rc;
    uint32 cpylen;

    if (cmailbox->query_id != DEBUG_QUERY_ID) {
        cmailbox->query_id = DEBUG_QUERY_ID;
    }

    // send local thread id and quota to remote
    if (local_tid != cmailbox->local_thread_id) {
        int node_idx = cmailbox->idx;
        int streamid = cmailbox->streamid;
        long add_quota = 0;

        if (g_instance.comm_cxt.quota_cxt.g_having_quota) {
            add_quota = gs_add_quota_size(cmailbox);
        }

        // change local stream state and quota first
        cmailbox->local_thread_id = local_tid;
        cmailbox->bufCAP += add_quota;
        cmailbox->state = MAIL_RUN;

        // then change remote stream state and quota
        msg->type = CTRL_PEER_TID;
        msg->node_idx = node_idx;
        msg->streamid = streamid;
        msg->streamcap = add_quota;
        msg->version = cmailbox->remote_version;
        msg->extra_info = cmailbox->local_thread_id;
        msg->query_id = cmailbox->query_id;

        cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
        ss_rc = memset_s(msg->nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
        securec_check(ss_rc, "\0", "\0");
        ss_rc = strncpy_s(msg->nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
        securec_check(ss_rc, "\0", "\0");
        msg->nodename[cpylen] = '\0';

        return true;
    }

    return false;
}  // gs_r_send_start_ctrl_msg

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
        errno = ECOMMSCTPARGSINVAL;
        return -1;
    }

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    if (0 != gs_poll_create()) {
        LIBCOMM_ELOG(WARNING, "(r|wait poll)\tPoll create failed! Detail:%s.", mc_strerror(errno));
        return -1;
    }

    // step 1: initialize local variables
    //
    int i = 0;
    int n_got_data = 0;
    int first_cycle = 1;
    int ret = 0;
    int idx = -1;
    int streamid = -1;
    int version = -1;
    // if waked by other threads, the flag is set to 1
    int poll_error_flag = 0;
    struct c_mailbox* cmailbox = NULL;
    uint64 wait_lock_start = 0;
    uint64 wait_data_time = 0;
    uint64 wait_data_start = 0;
    uint64 wait_data_end = 0;
    uint64 time_enter = COMM_STAT_TIME();
    uint64 time_now = time_enter;
    bool send_msg = false;
    bool TempImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = false;
    errno = 0;
    struct FCMSG_T fcmsgs = {0x0};

    COMM_TIMER_INIT();
    COMM_TIMER_LOG("(r|wait poll)\tStart timer log.");

    for (;;) {
        wait_lock_start = COMM_STAT_TIME();

        // step 2: check if there is data in the c_mailbox of the given node index and stream index already
        //
        for (i = 0; i < nproducer; i++) {
            idx = gs_sock_array[i].idx;
            streamid = gs_sock_array[i].sid;
            version = gs_sock_array[i].ver;
            Assert(idx >= 0 && idx < g_instance.comm_cxt.counters_cxt.g_cur_node_num && streamid > 0 &&
                   streamid < g_instance.comm_cxt.counters_cxt.g_max_stream_num);

            // get the cmailbox then lock it
            cmailbox = &C_MAILBOX(idx, streamid);
            LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);

            // check the state of the mailbox is correct
            // ret -2 means close by remote
            if (false == gs_check_mailbox(cmailbox->local_version, version)) {
                if (!close_expected) {
                    MAILBOX_ELOG(cmailbox,
                        WARNING,
                        "(r|wait poll)\tStream has already closed, detail:%s.",
                        mc_strerror(cmailbox->close_reason));
                }
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
                errno = cmailbox->close_reason;
                // set the error flag for Consumer thread
                producer[i] = WAIT_POLL_FLAG_ERROR;
                ret = -2;
                goto return_result;
            }

            // there is data in the mailbox already
            if (cmailbox->buff_q->count > 0) {
                n_got_data++;
                // set the having label for Consumer thread
                producer[i] = WAIT_POLL_FLAG_GOT;
            }

            /* update the statistic information of the mailbox */
            if (cmailbox->statistic != NULL) {
                wait_data_time = ABS_SUB(wait_data_end, wait_data_start);
                cmailbox->statistic->wait_data_time += wait_data_time;

                time_now = COMM_STAT_TIME();
                cmailbox->statistic->wait_lock_time += ABS_SUB(time_now, wait_lock_start);

                if (cmailbox->statistic != NULL && cmailbox->statistic->first_poll_time == 0) {
                    cmailbox->statistic->first_poll_time = time_enter;
                    cmailbox->statistic->consumer_elapsed_time += ABS_SUB(time_enter, cmailbox->statistic->start_time);
                } else {
                    cmailbox->statistic->consumer_elapsed_time +=
                        ABS_SUB(time_enter, t_thrd.comm_cxt.g_consumer_process_duration);
                }

                if (first_cycle) {
                    cmailbox->statistic->call_poll_count++;
                    cmailbox->statistic->last_poll_time = time_enter;
                }

                if (n_got_data > 0 || poll_error_flag == 1) {
                    cmailbox->statistic->total_poll_time += ABS_SUB(time_now, time_enter);
                }
            }

            // need return
            if (n_got_data > 0 || poll_error_flag == 1) {
                /* gs_wait_poll will return, clean semaphore */
                cmailbox->semaphore = NULL;
                if (producer[i] == WAIT_POLL_FLAG_WAIT) {
                    producer[i] = WAIT_POLL_FLAG_IDLE;
                }
            } else {
                /* gs_wait_poll will enter gs_poll, regist semaphore */
                cmailbox->semaphore = t_thrd.comm_cxt.libcomm_semaphore;
                producer[i] = WAIT_POLL_FLAG_WAIT;
            }

            // send local thread id and quota to remote
            send_msg = gs_r_form_start_ctrl_msg(cmailbox, &fcmsgs);
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);

            // send local thread id and quota to remote without cmailbox lock
            if (send_msg && (gs_send_ctrl_msg(&g_instance.comm_cxt.g_r_node_sock[idx], &fcmsgs, ROLE_CONSUMER) <= 0)) {
                errno = ECOMMSCTPTCPDISCONNECT;
                ret = -1;
                goto return_result;
            }
        }
        first_cycle = 0;
        // step 3: if data is found, return the number of mailboxes which have data
        // or, if it is waked up here but no data found, there must be interruption, we should return and
        // CHECK_FOR_INTERRUPT
        //
        if (n_got_data > 0) {
            COMM_TIMER_LOG("(r|wait poll)\tGet data for node[%d,%d]:%s.",
                idx,
                streamid,
                REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx));
            ret = n_got_data;
            goto return_result;
        } else if (poll_error_flag == 1) {
            COMM_DEBUG_LOG("(r|wait poll)\tWaked up but no data.");

            errno = ECOMMSCTPWAITPOLLERROR;
            ret = -1;
            goto return_result;
        }
        // step 4: if no data found and waked up flag is not set, we should wait on CV
        //
        int pool_re = 0;
        int time_out = -1;
        if (timeout > 0) {
            time_out = timeout;
        }

        wait_data_start = COMM_STAT_TIME();
        COMM_TIMER_LOG("(r|wait poll)\tWait poll start.");
        pool_re = gs_poll(time_out);
        COMM_TIMER_LOG("(r|wait poll)\tWait poll end.");
        wait_data_end = COMM_STAT_TIME();

        // step 5: if it is waked up normal, return 0, then receive again
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
            poll_error_flag = 1;
            continue;
        }

        // timed out, in fact, we do not set the timeout now !!!
        if (pool_re == ETIMEDOUT) {
            LIBCOMM_ELOG(WARNING, "(r|wait poll)\tFailed to wait for notify, timeout:%ds.", time_out);
            errno = ECOMMSCTPEPOLLTIMEOUT;
        } else { // waked up abnormal, unknown error
            LIBCOMM_ELOG(WARNING, "(r|wait poll)\tFailed to wait poll, detail: %s.", mc_strerror(errno));
            errno = ECOMMSCTPWAITPOLLERROR;
        }
        ret = -1;
        goto return_result;
    }

return_result:
    for (i = 0; i < nproducer; i++) {
        if (producer[i] == WAIT_POLL_FLAG_WAIT) {
            producer[i] = WAIT_POLL_FLAG_IDLE;
            cmailbox = &C_MAILBOX(gs_sock_array[i].idx, gs_sock_array[i].sid);
            LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);
            if (true == gs_check_mailbox(cmailbox->local_version, gs_sock_array[i].ver)) {
                cmailbox->semaphore = NULL;
            }
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
        }
    }

    // ret 0 means poll is waked unexpected, so we check interruption
    LIBCOMM_INTERFACE_END((ret == 0), TempImmediateInterruptOK);
    t_thrd.comm_cxt.g_consumer_process_duration = COMM_STAT_TIME();

    return ret;
}  // gs_wait_poll

/* Handle the same message and print message receiving and sending log. */
void gs_comm_ipc_print(MessageIpcLog *ipc_log, char *remotenode, gsocket *gs_sock, CommMsgOper msg_oper)
{
    /* if the same number of messages is greater than 1, print this message. */
    if (ipc_log->last_msg_count > 1) {
        ereport(LOG, 
                (errmsg("(%s) comm_ipc_log, msgtype:%c, total_len:%d, msg_count:%d, node:%s, last msg time:%s.",
                        msg_oper_string(msg_oper), ipc_log->last_msg_type, ipc_log->last_msg_len, 
                        ipc_log->last_msg_count, remotenode, ipc_log->last_msg_time)));
    }

    /* print current message */
    if ((gs_sock != NULL && gs_sock->idx == 0 && gs_sock->sid == 0) || gs_sock == NULL) {
        ereport(LOG, (errmsg("(%s) comm_ipc_log, msgtype:%c, len:%d, node:%s.",
                             msg_oper_string(msg_oper), ipc_log->type, ipc_log->msg_len, remotenode)));
    } else {
        ereport(LOG, (errmsg("(%s) comm_ipc_log, msgtype:%c, len:%d, node:%s[nid:%d,sid:%d].",
                             msg_oper_string(msg_oper), ipc_log->type, ipc_log->msg_len,
                             remotenode, gs_sock->idx, gs_sock->sid)));
    }
}

void handle_message(MessageIpcLog *ipc_log, void *ptr, int n, char *remotenode,
                    gsocket *gs_sock, CommMsgOper msg_oper)
{
    int offset;
    int rc;
    int i = n;
    char *tmp = NULL;

    while (i > 0) {
        offset = 0;
        /* step1.parse type */
        if (ipc_log->type == 0) {
            ipc_log->type = ((char*)ptr)[0];
            offset = 1;
        } else if (ipc_log->len_cursor < IPC_MSG_LEN) {
            /* step2.parse length complete length */
            if (i >= IPC_MSG_LEN - ipc_log->len_cursor) {
                offset = IPC_MSG_LEN - ipc_log->len_cursor;
            } else {
                /* partial length(that is less than 4 bytes) */
                offset = i;
            }

            rc = memcpy_s((char*)(&(ipc_log->len_cache)) + ipc_log->len_cursor, IPC_MSG_LEN, (char*)ptr, offset);
            securec_check(rc, "\0", "\0");

            ipc_log->len_cursor += offset;
            /* parse length */
            if (ipc_log->len_cursor == IPC_MSG_LEN) {
                ipc_log->msg_len = (int) ntohl(ipc_log->len_cache) - IPC_MSG_LEN;
            }
        } else if (ipc_log->msg_cursor <= ipc_log->msg_len) {
            /* step3.parse msg compose a complete message */
            if (i >= ipc_log->msg_len - ipc_log->msg_cursor) {
                offset = ipc_log->msg_len - ipc_log->msg_cursor;

                /* save the last message information and do not print the log */
                if (ipc_log->last_msg_type == ipc_log->type) {
                    ipc_log->last_msg_count++;
                    ipc_log->last_msg_len += ipc_log->msg_len;
                    comm_ipc_log_get_time(ipc_log->last_msg_time, MSG_TIME_LEN);
                } else {
                    /* new message, print the current message and the previous message log */
                    gs_comm_ipc_print(ipc_log, remotenode, gs_sock, msg_oper);
                    ipc_log->last_msg_count = 1;
                    ipc_log->last_msg_type = ipc_log->type;
                    ipc_log->last_msg_len = ipc_log->msg_len;
                }

                /* restored data structure */
                ipc_log->type = 0;
                ipc_log->msg_cursor = 0;
                ipc_log->msg_len = 0;

                ipc_log->len_cursor = 0;
                ipc_log->len_cache = 0;
            } else {
                offset = i;
                ipc_log->msg_cursor += offset;
            }
        }

        i -= offset;
        tmp = (char *)ptr;
        tmp += offset;
        ptr = (void*)tmp;
    }
    return;
}

/* Send and receive data between nodes, for performance problem location.
 * A complete message consists of three parts, 1 byte type + 4 bytes length + contents.
 * 'ptr' may contain several messages, parse the 'ptr' and print the log.
 */
MessageCommLog* gs_comm_ipc_performance(MessageCommLog *msgLog, void *ptr, int n,
                                        char *remotenode, gsocket *gs_sock, CommMsgOper msg_oper)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->top_mem_cxt);
    MessageIpcLog *ipc_log = NULL;

    if (msgLog == NULL) {
        msgLog = (MessageCommLog*) palloc0(sizeof(MessageCommLog));
        if (msgLog == NULL) {
            MemoryContextSwitchTo(oldcontext);
            return NULL;
        }
    }

    /* initializing variable */
    if (msg_oper == SEND_SOME || msg_oper == SECURE_WRITE) {
        ipc_log = &(msgLog->send_ipc_log);
    } else if (msg_oper == SECURE_READ || msg_oper == READ_DATA || msg_oper == READ_DATA_FROM_LOGIC) {
        ipc_log = &(msgLog->recv_ipc_log);
    }

    handle_message(ipc_log, ptr, n, remotenode, gs_sock, msg_oper);

    MemoryContextSwitchTo(oldcontext);
    return msgLog;
}


// shutdown the communication layer
//
void gs_shutdown_comm()
{
    if (g_instance.comm_cxt.reqcheck_cxt.g_shutdown_requested == true) {
        return;
    }

    LIBCOMM_ELOG(LOG, "Communication layer will be shutdown.");

    g_instance.comm_cxt.reqcheck_cxt.g_shutdown_requested = true;
}

// cancel request for receiver (called by die() or StatementCancelHandler() in postgresMain )
//
void gs_r_cancel()
{
    // use g_cancel_requested save DEBUG_QUERY_ID as a flag
    g_instance.comm_cxt.reqcheck_cxt.g_cancel_requested =
        (t_thrd.proc_cxt.MyProcPid != 0) ? t_thrd.proc_cxt.MyProcPid : 1;
}

// receiver close logic stream, call by Consumer thread
// when no data need or all data are received, or error happed
//
static int gs_r_close_stream(gsocket* gsock)
{
    int node_idx = gsock->idx;
    int stream_idx = gsock->sid;
    int version = gsock->ver;
    int type = gsock->type;
    struct FCMSG_T fcmsgs = {0x0};

    if ((node_idx < 0) || (node_idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num) || (stream_idx <= 0) ||
        (stream_idx >= g_instance.comm_cxt.counters_cxt.g_max_stream_num) || (type == GSOCK_PRODUCER)) {
        COMM_DEBUG_LOG("(r|cls stream)\tInvalid argument: "
                       "node idx[%d], stream id[%d], type[%d].",
            node_idx,
            stream_idx,
            type);
        errno = ECOMMSCTPARGSINVAL;
        return -1;
    }

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    // step 1: get the mailbox and check the state of the cmailbox
    // if it is closed, delete the entry in the hash table (g_r_htab_nodeid_skey_to_stream)
    //
    struct c_mailbox* cmailbox = &(C_MAILBOX(node_idx, stream_idx));
    if (cmailbox->state == MAIL_CLOSED) {
        return 0;
    }

    LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);

    // if it was closed or reused, we need do nothing here,
    // but we will do close poll and delete the entry for sure,
    // there is no side effect
    if (gs_check_mailbox(cmailbox->local_version, version) == false) {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
        return 0;
    }

    // step 2: reset the cmailbox, close poll and delete the entry in hash table (g_r_htab_nodeid_skey_to_stream)
    //
    COMM_DEBUG_LOG("(r|cls stream)\tTo close stream[%d] for node[%d]:%s.",
        stream_idx,
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, node_idx));

    gs_r_close_logic_connection(cmailbox, ECOMMSCTPAPPCLOSE, &fcmsgs);

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);

    // Send close ctrl msg to remote without cmailbox lock
    (void)gs_send_ctrl_msg(&g_instance.comm_cxt.g_r_node_sock[node_idx], &fcmsgs, ROLE_CONSUMER);

    return 0;
}  // gs_r_close_stream

// sender close logic stream, call by Producer thread when it failed to send data
//
static int gs_s_close_stream(gsocket* gsock)
{
    int node_idx = gsock->idx;
    int stream_idx = gsock->sid;
    int version = gsock->ver;
    int type = gsock->type;
    struct FCMSG_T fcmsgs = {0x0};

    if ((node_idx < 0) || (node_idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num) || (stream_idx <= 0) ||
        (stream_idx >= g_instance.comm_cxt.counters_cxt.g_max_stream_num) || (type == GSOCK_CONSUMER)) {
        COMM_DEBUG_LOG("(s|cls stream)\tInvalid argument: "
                       "node idx[%d], stream id[%d], type[%d].",
            node_idx,
            stream_idx,
            type);
        errno = ECOMMSCTPARGSINVAL;
        return -1;
    }

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    // step 1: get the mailbox and check the state of the cmailbox,
    // if the keys of the pmailbox is not matched, return error
    //
    struct p_mailbox* pmailbox = &P_MAILBOX(node_idx, stream_idx);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);

    if (gs_check_mailbox(pmailbox->local_version, version) == false) {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
        return 0;
    }

    // step 2: reset the state of the pmailbox
    //
    COMM_DEBUG_LOG("(s|cls stream)\tTo close stream[%d] for node[%d]:%s.",
        stream_idx,
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));

    gs_s_close_logic_connection(pmailbox, ECOMMSCTPAPPCLOSE, &fcmsgs);
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
    // Send close ctrl msg to remote without pmailbox lock
    (void)gs_send_ctrl_msg(&g_instance.comm_cxt.g_s_node_sock[node_idx], &fcmsgs, ROLE_PRODUCER);

    return 0;
}

// close logic socket, it will return when the sock type is invalid.
//
void gs_close_gsocket(gsocket* gsock)
{
    int type = gsock->type;

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    bool TempImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = false;

    if (type == GSOCK_INVALID) {
        LIBCOMM_INTERFACE_END(false, TempImmediateInterruptOK);
        return;
    }

    if (type == GSOCK_DAUL_CHANNEL || type == GSOCK_PRODUCER) {
        (void)gs_s_close_stream(gsock);
    }

    if (type == GSOCK_DAUL_CHANNEL || type == GSOCK_CONSUMER) {
        (void)gs_r_close_stream(gsock);
    }

    *gsock = GS_INVALID_GSOCK;

    LIBCOMM_INTERFACE_END(false, TempImmediateInterruptOK);
    return;
}

bool gs_stop_query(gsocket* gsock, uint32 remote_pid)
{
    struct FCMSG_T fcmsgs = {0x0};
    int rc;
    errno_t ss_rc;
    uint32 cpylen;

    fcmsgs.type = CTRL_STOP_QUERY;
    fcmsgs.node_idx = gsock->idx;
    fcmsgs.streamid = gsock->sid;
    fcmsgs.version = gsock->ver;
    fcmsgs.query_id = DEBUG_QUERY_ID;
    fcmsgs.extra_info = remote_pid;
    cpylen = comm_get_cpylen(g_instance.comm_cxt.localinfo_cxt.g_self_nodename, NAMEDATALEN);
    ss_rc = memset_s(fcmsgs.nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(fcmsgs.nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename, cpylen + 1);
    securec_check(ss_rc, "\0", "\0");
    fcmsgs.nodename[cpylen] = '\0';

    rc = gs_send_ctrl_msg(&g_instance.comm_cxt.g_r_node_sock[gsock->idx], &fcmsgs, ROLE_CONSUMER);

    return (rc > 0);
}

/*
 * check if the OS is RELIABLE for SCTP protocol
 * we do not support SUSE linux who's kernel version under 2.6.32.22
 */
int gs_check_SLESSP2_version()
{
    return mc_check_SLESSP2_version();
}

/* check if the OS support SCTP */
int gs_check_sctp_support()
{
    return mc_check_sctp_support();
}

/* get the error information of communication layer */
const char* gs_comm_strerror()
{
    return mc_strerror(errno);
}

/* get communication layer stream status at receiver end as a tuple for pg_comm_stream_status */
bool get_next_recv_stream_status(CommRecvStreamStatus* stream_status)
{
    int idx, sid;
    uint32 time_now = (uint32)mc_timers_ms();
    uint64 run_time = 0;
    struct c_mailbox* cmailbox = NULL;

    /* if node index is invalid or stream index is invalid, return false */
    if (stream_status->idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num ||
        stream_status->stream_id >= g_instance.comm_cxt.counters_cxt.g_max_stream_num) {
        return false;
    }

    /* traves all mailbox and get the stream status in C_MAILBOX. */
    for (idx = stream_status->idx; idx < g_instance.comm_cxt.counters_cxt.g_cur_node_num; idx++) {
        for (sid = stream_status->stream_id + 1; sid < g_instance.comm_cxt.counters_cxt.g_max_stream_num; sid++) {
            /* do not need return the closed stream status. */
            cmailbox = &C_MAILBOX(idx, sid);
            if (cmailbox->state != MAIL_CLOSED) {
                stream_status->idx = cmailbox->idx;
                stream_status->stream_id = cmailbox->streamid;
                stream_status->stream_state = stream_stat_string(cmailbox->state);
                stream_status->quota_size = cmailbox->bufCAP;
                stream_status->query_id = cmailbox->query_id;
                stream_status->stream_key = cmailbox->stream_key;
                stream_status->buff_usize = cmailbox->buff_q->u_size;
                stream_status->bytes = cmailbox->statistic ? cmailbox->statistic->recv_bytes : 0;
                stream_status->local_thread_id = cmailbox->local_thread_id;
                stream_status->peer_thread_id = cmailbox->peer_thread_id;
                stream_status->time = cmailbox->statistic ? (uint64)(time_now - cmailbox->statistic->start_time) : 0;

                run_time = (stream_status->time > 0) ? stream_status->time : 1;
                stream_status->speed = stream_status->bytes * 1000 / run_time;

                stream_status->tcp_sock = g_instance.comm_cxt.g_r_node_sock[idx].ctrl_tcp_sock;
                errno_t ss_rc;
                ss_rc = strcpy_s(
                    stream_status->remote_host, HOST_ADDRSTRLEN, g_instance.comm_cxt.g_r_node_sock[idx].remote_host);
                securec_check(ss_rc, "\0", "\0");
                ss_rc = strcpy_s(
                    stream_status->remote_node, NAMEDATALEN, g_instance.comm_cxt.g_r_node_sock[idx].remote_nodename);
                securec_check(ss_rc, "\0", "\0");

                return true;
            }
        }
        /* set stream index to -1 for next node */
        stream_status->stream_id = -1;
    }

    return false;
}

/* get communication layer stream status at sender end as a tuple for pg_comm_send_stream */
bool get_next_send_stream_status(CommSendStreamStatus* stream_status)
{
    int idx, sid;
    uint32 time_now = (uint32)mc_timers_ms();
    uint64 run_time = 0;
    struct p_mailbox* pmailbox = NULL;

    /* if node index is invalid or stream index is invalid, return false */
    if (stream_status->idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num ||
        stream_status->stream_id >= g_instance.comm_cxt.counters_cxt.g_max_stream_num) {
        return false;
    }

    /* traves all mailbox and get the stream status in P_MAILBOX. */
    for (idx = stream_status->idx; idx < g_instance.comm_cxt.counters_cxt.g_cur_node_num; idx++) {
        for (sid = stream_status->stream_id + 1; sid < g_instance.comm_cxt.counters_cxt.g_max_stream_num; sid++) {
            /* no need return the closed stream status */
            pmailbox = &P_MAILBOX(idx, sid);
            if (pmailbox->state != MAIL_CLOSED) {
                stream_status->idx = pmailbox->idx;
                stream_status->stream_id = pmailbox->streamid;
                stream_status->stream_state = stream_stat_string(pmailbox->state);
                stream_status->quota_size = pmailbox->bufCAP;
                stream_status->query_id = pmailbox->query_id;
                stream_status->stream_key = pmailbox->stream_key;
                stream_status->bytes = pmailbox->statistic ? pmailbox->statistic->send_bytes : 0;
                stream_status->wait_quota = pmailbox->statistic ? (uint64)pmailbox->statistic->wait_quota_overhead : 0;
                stream_status->local_thread_id = pmailbox->local_thread_id;
                stream_status->peer_thread_id = pmailbox->peer_thread_id;
                stream_status->time = pmailbox->statistic ? (uint64)(time_now - pmailbox->statistic->start_time) : 0;
                stream_status->tcp_sock = g_instance.comm_cxt.g_s_node_sock[idx].ctrl_tcp_sock;

                run_time = (stream_status->time > 0) ? stream_status->time : 1;
                stream_status->speed = stream_status->bytes * 1000 / run_time;

                errno_t ss_rc;
                ss_rc = strcpy_s(
                    stream_status->remote_host, HOST_ADDRSTRLEN, g_instance.comm_cxt.g_s_node_sock[idx].remote_host);
                securec_check(ss_rc, "\0", "\0");
                ss_rc = strcpy_s(
                    stream_status->remote_node, NAMEDATALEN, g_instance.comm_cxt.g_s_node_sock[idx].remote_nodename);
                securec_check(ss_rc, "\0", "\0");

                return true;
            }
        }
        /* set stream index to -1 for next node */
        stream_status->stream_id = -1;
    }

    return false;
}

/*
 * function name    : get_next_comm_delay_info
 * description    : get libcomm delay info with delay_info->idx .
 * arguments        : _in_ delay_info->idx: the node index.
 *                  _out_ delay_info: return delay info
 * return value    :
 *                   true: return delay info.
 *                   false: no delay info.
 */
bool get_next_comm_delay_info(CommDelayInfo* delay_info)
{
    int node_idx = delay_info->idx;
    int array_idx = -1;
    uint32 delay = 0;
    uint32 delay_min = 0;
    uint32 delay_max = 0;
    uint32 delay_sum = 0;

    g_instance.comm_cxt.g_delay_survey_switch = true;

    /* if node index is invalid, return false */
    if (node_idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num) {
        return false;
    }

    for (;;) {
        if (g_instance.comm_cxt.g_senders->sender_conn[node_idx].assoc_id == 0) {
            node_idx++;
        } else {
            break;
        }

        if (node_idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num) {
            return false;
        }
    }

    /* calculate delay info */
    for (array_idx = 0; array_idx < MAX_DELAY_ARRAY_INDEX; array_idx++) {
        delay = g_instance.comm_cxt.g_delay_info[node_idx].delay[array_idx];
        if (delay < delay_min || delay_min == 0) {
            delay_min = delay;
        }
        if (delay > delay_max) {
            delay_max = delay;
        }
        delay_sum += delay;
    }

    /* save delay info in delay_info */
    errno_t ss_rc;
    ss_rc = strcpy_s(delay_info->remote_host, HOST_ADDRSTRLEN, g_instance.comm_cxt.g_s_node_sock[node_idx].remote_host);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strcpy_s(delay_info->remote_node, NAMEDATALEN, g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename);
    securec_check(ss_rc, "\0", "\0");
    delay_info->stream_num =
        g_instance.comm_cxt.counters_cxt.g_max_stream_num - g_instance.comm_cxt.g_usable_streamid[node_idx].count - 1;
    delay_info->min_delay = delay_min;
    delay_info->dev_delay = delay_sum / MAX_DELAY_ARRAY_INDEX;
    delay_info->max_delay = delay_max;

    /* move to next node index */
    delay_info->idx = node_idx + 1;

    return true;
}

/* get communication layer status as a tuple for pg_comm_status */
bool gs_get_comm_stat(CommStat* comm_stat)
{
    if (comm_stat == NULL || g_instance.comm_cxt.counters_cxt.g_cur_node_num == 0) {
        return false;
    }

    if (g_instance.comm_cxt.localinfo_cxt.g_libcomm_used_rate != NULL) {
        comm_stat->postmaster = g_instance.comm_cxt.localinfo_cxt.g_libcomm_used_rate[POSTMASTER];
    }

    if (g_instance.attr.attr_storage.comm_cn_dn_logic_conn == false && IS_PGXC_COORDINATOR) {
        return true;
    }        
    
    int idx, sid, i;
    int used_stream = 0;
    struct c_mailbox* cmailbox = NULL;
    struct p_mailbox* pmailbox = NULL;

    const int G_CUR_NODE_NUM = g_instance.comm_cxt.counters_cxt.g_cur_node_num;
    int *libcomm_used_rate = g_instance.comm_cxt.localinfo_cxt.g_libcomm_used_rate;
    long recv_bytes[G_CUR_NODE_NUM] = {0};
    int recv_count[G_CUR_NODE_NUM] = {0};
    int recv_count_speed = 0;
    long recv_speed = 0;

    long send_speed = 0;
    long send_bytes[G_CUR_NODE_NUM] = {0};
    int send_count[G_CUR_NODE_NUM] = {0};
    int send_count_speed = 0;

    if (libcomm_used_rate != NULL) {
        comm_stat->postmaster = libcomm_used_rate[POSTMASTER];
        comm_stat->gs_sender_flow = libcomm_used_rate[GS_SEND_flow];
        comm_stat->gs_receiver_flow = libcomm_used_rate[GS_RECV_FLOW];
        comm_stat->gs_receiver_loop = libcomm_used_rate[GS_RECV_LOOP];
        for (i = GS_RECV_LOOP + 1; i < g_instance.comm_cxt.counters_cxt.g_recv_num + GS_RECV_LOOP; i++) {
            if (comm_stat->gs_receiver_loop < libcomm_used_rate[i]) {
                comm_stat->gs_receiver_loop = libcomm_used_rate[i];
            }
        }
    }

    /* sum of recv_speed/send_speed in all stream */
    i = 0;
    while (i < 2) {
        /* idx: node index */
        for (idx = 0; idx < G_CUR_NODE_NUM; idx++) {
            if (i == 0) {
                recv_bytes[idx] = g_instance.comm_cxt.g_receivers->receiver_conn[idx].comm_bytes;
                recv_count[idx] = g_instance.comm_cxt.g_receivers->receiver_conn[idx].comm_count;
                send_bytes[idx] = g_instance.comm_cxt.g_senders->sender_conn[idx].comm_bytes;
                send_count[idx] = g_instance.comm_cxt.g_senders->sender_conn[idx].comm_count;
                /* sid: stream index */
                for (sid = 1; sid < g_instance.comm_cxt.counters_cxt.g_max_stream_num; sid++) {
                    cmailbox = &C_MAILBOX(idx, sid);
                    if (cmailbox->state != MAIL_CLOSED) {
                        comm_stat->buffer += cmailbox->buff_q->u_size;
                    }

                    pmailbox = &P_MAILBOX(idx, sid);
                    if (pmailbox->state != MAIL_CLOSED) {
                        used_stream++;
                    }
                }
            } else if (i == 1) {
                recv_speed +=  g_instance.comm_cxt.g_receivers->receiver_conn[idx].comm_bytes - recv_bytes[idx];
                recv_count_speed += g_instance.comm_cxt.g_receivers->receiver_conn[idx].comm_count - recv_count[idx];
                send_speed += g_instance.comm_cxt.g_senders->sender_conn[idx].comm_bytes - send_bytes[idx];
                send_count_speed += g_instance.comm_cxt.g_senders->sender_conn[idx].comm_count - send_count[idx];
            }    
        }
        i++;
        usleep(100000);
    }

    comm_stat->recv_speed = recv_speed * 10 / 1024;
    comm_stat->recv_count_speed = recv_count_speed * 10;
    comm_stat->send_speed = send_speed * 10 / 1024;
    comm_stat->send_count_speed = send_count_speed * 10;
    comm_stat->mem_libcomm = libcomm_used_memory;
    comm_stat->mem_libpq = libpq_used_memory;
    comm_stat->stream_conn_num = used_stream;
    return true;
}

/* Output the contents of structure into log file */
void gs_log_comm_status()
{
    LIBCOMM_ELOG(LOG, "[LOG STATUS]Comm Layer Status: Do nothing now, please ignore it.");
}

/* release memory of communication layer, just for LLT */
int gs_release_comm_memory()
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);
    gs_r_release_comm_memory();
    return 0;
}

int gs_close_all_stream_by_debug_id(uint64 query_id)
{
    int idx, sid;
    struct c_mailbox* cmailbox = NULL;
    struct p_mailbox* pmailbox = NULL;
    int cmailbox_count = 0;
    int pmailbox_count = 0;
    struct FCMSG_T fcmsgs = {0x0};

    if (query_id == 0) {
        LIBCOMM_ELOG(WARNING, "(cls all stream)\tInvalid argument: query id is 0!");
        return -1;
    }

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    for (idx = 0; idx < g_instance.comm_cxt.counters_cxt.g_cur_node_num; idx++) {
        for (sid = 1; sid < g_instance.comm_cxt.counters_cxt.g_max_stream_num; sid++) {
            cmailbox = &C_MAILBOX(idx, sid);
            if (cmailbox->query_id == query_id && cmailbox->stream_key.planNodeId != 0) {
                LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);

                if (cmailbox->query_id == query_id && cmailbox->stream_key.planNodeId != 0 &&
                    cmailbox->state != MAIL_CLOSED) {
                    cmailbox_count++;
                    gs_r_close_logic_connection(cmailbox, ECOMMSCTPAPPCLOSE, &fcmsgs);
                    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
                    /* Send close ctrl msg to remote without pmailbox lock */
                    (void)gs_send_ctrl_msg(&g_instance.comm_cxt.g_r_node_sock[idx], &fcmsgs, ROLE_CONSUMER);
                } else {
                    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
                }
            }

            pmailbox = &P_MAILBOX(idx, sid);
            if (pmailbox->query_id == query_id && pmailbox->stream_key.planNodeId != 0) {
                LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);

                if (pmailbox->query_id == query_id && pmailbox->stream_key.planNodeId != 0 &&
                    pmailbox->state != MAIL_CLOSED) {
                    pmailbox_count++;
                    gs_s_close_logic_connection(pmailbox, ECOMMSCTPAPPCLOSE, &fcmsgs);
                    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
                    /* Send close ctrl msg to remote without pmailbox lock */
                    (void)gs_send_ctrl_msg(&g_instance.comm_cxt.g_s_node_sock[idx], &fcmsgs, ROLE_PRODUCER);
                } else {
                    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
                }
            }
        }
    }

    if (cmailbox_count != 0 || pmailbox_count != 0) {
        LIBCOMM_ELOG(LOG,
            "(cls all stream)\tClose all stream by debug id[%lu], "
            "close %d cmailbox and %d pmailbox.",
            query_id,
            cmailbox_count,
            pmailbox_count);
    }
    return 0;
}

/*
 * @Description: Add stream key to g_r_htab_nodeid_skey_to_memory_poll
 *
 * @param[IN] key_s: stream key
 */
void gs_memory_init_entry(StreamSharedContext* sharedContext, int consumerNum, int producerNum)
{
    struct hash_entry* entry = NULL;
    struct hash_entry** poll_entrys = NULL;
    struct hash_entry*** quota_entrys = NULL;

    poll_entrys = (struct hash_entry**)palloc(sizeof(struct hash_entry*) * consumerNum);
    quota_entrys = (struct hash_entry***)palloc(sizeof(struct hash_entry**) * consumerNum);

    for (int i = 0; i < consumerNum; i++) {
        entry = (struct hash_entry*)palloc(sizeof(struct hash_entry));
        (void)entry->_init();
        poll_entrys[i] = entry;
        quota_entrys[i] = (struct hash_entry**)palloc(sizeof(struct hash_entry*) * producerNum);
        for (int j = 0; j < producerNum; j++) {
            entry = (struct hash_entry*)palloc(sizeof(struct hash_entry));
            (void)entry->_init();
            quota_entrys[i][j] = entry;
        }
    }

    sharedContext->poll_entrys = poll_entrys;
    sharedContext->quota_entrys = quota_entrys;
}

/*
 * @Description: Send Error/Notice through memory
 *
 * @param[IN] buf: Error/Notice string info
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] nthChannel: destination consumer
 */
void gs_message_by_memory(StringInfo buf, StreamSharedContext* sharedContext, int nthChannel)
{
    StringInfo buf_dst = NULL;
    struct hash_entry* entry = NULL;

    /* Copy Error/Notice messages to shared context. */
    buf_dst = sharedContext->messages[nthChannel][u_sess->stream_cxt.smp_id];

    /*
     * If producer is waked up and shared buffer has been consumed while waiting,
     * it can continue to append data to its messages of sharedContext.
     */
    entry = sharedContext->quota_entrys[nthChannel][u_sess->stream_cxt.smp_id];
    while (buf_dst->len > 0) {
        (void)entry->_timewait(SINGLE_WAITQUOTA);
    }
    appendBinaryStringInfo(buf_dst, buf->data, buf->len);
    buf_dst->cursor = buf->cursor;

    /* Send signal to dest consumer. */
    entry = sharedContext->poll_entrys[nthChannel];
    entry->_signal();

    pfree(buf->data);
    buf->data = NULL;
}

void gs_memory_disconnect(StreamSharedContext* sharedContext, int nthChannel)
{
    struct hash_entry* entry = NULL;
    sharedContext->dataStatus[nthChannel][u_sess->stream_cxt.smp_id] = CONN_ERR;
    entry = sharedContext->poll_entrys[nthChannel];
    entry->_signal();
}

#ifdef __aarch64__
/*
 * @Description: Judge whether the databuff is empty
 *
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] nthChannel: destination consumer
 */
bool gs_is_databuff_empty(StreamSharedContext* sharedContext, int nthChannel)
{
    if (sharedContext->vectorized) {
        VectorBatch* batch = sharedContext->sharedBatches[nthChannel][u_sess->stream_cxt.smp_id];
        if (batch->m_rows == 0) {
            return true;
        }
    } else {
        TupleVector* tupleVec = sharedContext->sharedTuples[nthChannel][u_sess->stream_cxt.smp_id];
        if (tupleVec->tuplePointer == 0) {
            return true;
        }
    }
    return false;
}
#endif

/*
 * @Description: Send data to local consumer through shared memory
 *
 * @param[IN] tuple: tuple to be sent
 * @param[IN] batchsrc: batch to be send
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] nthChannel: destination consumer
 * @param[IN] nthRow: the Nth row to be sent in batch
 */
void gs_memory_send(
    TupleTableSlot* tuple, VectorBatch* batchsrc, StreamSharedContext* sharedContext, int nthChannel, int nthRow)
{
    VectorBatch* batch = NULL;
    TupleVector* tupleVec = NULL;
    bool ready_to_send = false;
    DataStatus dataStatus;
    struct hash_entry* entry = NULL;

    WaitState oldStatus = pgstat_report_waitstatus_comm(STATE_WAIT_FLUSH_DATA,
        u_sess->pgxc_cxt.PGXCNodeId,
        -1,
        u_sess->stream_cxt.producer_obj->getParentPlanNodeId(),
        global_node_definition ? global_node_definition->num_nodes : -1);

    StreamTimeSendStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    entry = sharedContext->quota_entrys[nthChannel][u_sess->stream_cxt.smp_id];
    for (;;) {
        /* Check for interrupt at the beginning of the loop. */
        CHECK_FOR_INTERRUPTS();

        /* Check if we should early stop. */
        /* Quit if the connection close, especially in a early close case. */
        if (executorEarlyStop() || sharedContext->is_connect_end[nthChannel][u_sess->stream_cxt.smp_id]) {
            (void)pgstat_report_waitstatus(oldStatus);
            return;
        }

        dataStatus = sharedContext->dataStatus[nthChannel][u_sess->stream_cxt.smp_id];
        /* Break the loop if we find quota. */
        if ((dataStatus == DATA_EMPTY
#ifdef __aarch64__
             && gs_is_databuff_empty(sharedContext, nthChannel)
#endif
             ) ||
            dataStatus == DATA_PREPARE) {
            break;
        }

        StreamTimeWaitQuotaStart(t_thrd.pgxc_cxt.GlobalNetInstr);
        (void)entry->_timewait(SINGLE_WAITQUOTA);
        StreamTimeWaitQuotaEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
    }

    StreamTimeCopyStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    /* Copy data to shared context. */
    if (sharedContext->vectorized) {
        Assert(sharedContext->sharedBatches != NULL);
        batch = sharedContext->sharedBatches[nthChannel][u_sess->stream_cxt.smp_id];
        /* data copy */
        if (-1 == nthRow) {
            /* Do deep copy of all rows, for local roundrobin & local broadcast. */
            Assert(batch->m_rows == 0);
            batch->Copy<true, false>(batchsrc);
            ready_to_send = true;
        } else {
            batch->CopyNth(batchsrc, nthRow);
            if (BatchMaxSize == batch->m_rows) {
                ready_to_send = true;
            }
        }
    } else {
        Assert(sharedContext->sharedTuples != NULL);
        tupleVec = sharedContext->sharedTuples[nthChannel][u_sess->stream_cxt.smp_id];
        int n = tupleVec->tuplePointer;
        ExecCopySlot(tupleVec->tupleVector[n], tuple);
        tupleVec->tuplePointer++;
        if (TupleVectorMaxSize == tupleVec->tuplePointer) {
            ready_to_send = true;
        }
    }
    StreamTimeCopyEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

    /* send the signal if copy finished */
    if (ready_to_send) {
#ifdef __aarch64__
        pg_memory_barrier();
#endif
        /* set flag */
        sharedContext->dataStatus[nthChannel][u_sess->stream_cxt.smp_id] = DATA_READY;
        /* send signal */
        entry = sharedContext->poll_entrys[nthChannel];
        entry->_signal();
    } else {
        sharedContext->dataStatus[nthChannel][u_sess->stream_cxt.smp_id] = DATA_PREPARE;
    }
    StreamTimeSendEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

    (void)pgstat_report_waitstatus(oldStatus);
}

/*
 * @Description: catch a tuple from stream's buffer.
 *
 * @param[IN] node: stream state
 * @return bool: true -- found data
 */
FORCE_INLINE
bool gs_return_tuple(StreamState* node)
{
    TupleVector* tupleVec = node->tempTupleVec;

    if (tupleVec->tuplePointer == 0) {
        return false;
    }

    tupleVec->tuplePointer--;
    int n = tupleVec->tuplePointer;
    node->ss.ps.ps_ResultTupleSlot = tupleVec->tupleVector[n];

    return true;
}

/*
 * @Description: Consume the data in shared memory from local producers.
 *
 * @param[IN] node: stream state
 * @param[IN] loc: data location
 * @return bool: true -- found data
 */
bool gs_consume_memory_data(StreamState* node, int loc)
{
    StreamSharedContext* sharedContext = node->sharedContext;

    NetWorkTimeCopyStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    /* Take data from the shared context. */
    if (sharedContext->vectorized) {
        VectorBatch* batchsrc = sharedContext->sharedBatches[u_sess->stream_cxt.smp_id][loc];
        VectorBatch* batchdst = ((VecStreamState*)node)->m_CurrentBatch;

        if (batchsrc->m_rows == 0) {
            return false;
        }

        batchdst->Copy<true, false>(batchsrc);

        batchsrc->Reset();
    } else {
        TupleVector* tuplesrc = sharedContext->sharedTuples[u_sess->stream_cxt.smp_id][loc];
        TupleVector* tupledst = node->tempTupleVec;

        if (tuplesrc->tuplePointer == 0) {
            return false;
        }

        for (int i = 0; i < tuplesrc->tuplePointer; i++) {
            (void)ExecCopySlot(tupledst->tupleVector[i], tuplesrc->tupleVector[i]);
        }

        tupledst->tuplePointer = tuplesrc->tuplePointer;
        tuplesrc->tuplePointer = 0;
        (void)gs_return_tuple(node);
    }
    NetWorkTimeCopyEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

    struct hash_entry* entry = NULL;
    entry = sharedContext->quota_entrys[u_sess->stream_cxt.smp_id][loc];

#ifdef __aarch64__
    pg_memory_barrier();
#endif
    /* Reset flag */
    sharedContext->dataStatus[u_sess->stream_cxt.smp_id][loc] = DATA_EMPTY;

    /* send signal */
    entry->_signal();

    node->sharedContext->scanLoc[u_sess->stream_cxt.smp_id] = loc;
    return true;
}

/*
 * @Description: Scan the producer status to find the data.
 *
 * @param[IN] node: stream state
 * @return char: STREAM_SCAN_DATA -- successfully find data from producer.
 *                  STREAM_SCAN_WAIT -- still need to poll to wait for data.
 *                 STREAM_SCAN_FINISH -- stream scan finished.
 */
char gs_find_memory_data(StreamState* node, int* waitnode_count)
{
    DataStatus dataStatus;
    StringInfo buf = NULL;
    int scanLoc = node->sharedContext->scanLoc[u_sess->stream_cxt.smp_id];
    int i = scanLoc;
    bool finished = true;
    bool is_conn_end = false;
    int waitnodeCount = 0;
    struct hash_entry* entry = NULL;

    /* Check if there is available data, and scan from last time location. */
    do {
        i++;
        if (i == node->conn_count) {
            i = 0;
        }

        /* Update scan location. */
        node->sharedContext->scanLoc[u_sess->stream_cxt.smp_id] = i;
        dataStatus = node->sharedContext->dataStatus[u_sess->stream_cxt.smp_id][i];
        is_conn_end = node->sharedContext->is_connect_end[u_sess->stream_cxt.smp_id][i];

        if (!is_conn_end) {
            finished = false;
            waitnodeCount++;
        }

        /*
         * Firstly, we handle error or notice messages.
         * If an error occured, we should stop scan now.
         * If an notice occured, we can still receive data.
         */
        buf = node->sharedContext->messages[u_sess->stream_cxt.smp_id][i];
        if (buf->len > 0) {
            if (buf->cursor == 'E') {
                HandleStreamError(node, buf->data, buf->len);
                return STREAM_SCAN_FINISH;
            } else if (buf->cursor == 'N') {
                HandleStreamNotice(node, buf->data, buf->len);
                resetStringInfo(buf);

                /* After one notice message has handled, send signal and wake up the dest producer. */
                entry = node->sharedContext->quota_entrys[u_sess->stream_cxt.smp_id][i];
                entry->_signal();

                return STREAM_SCAN_WAIT;
            }
        }

        switch (dataStatus) {
            case DATA_EMPTY:
                break;

            case DATA_PREPARE:
                /* Take the rest data away when the connection is end. */
                if (is_conn_end) {
                    /* Return data if any. */
                    if (gs_consume_memory_data(node, i)) {
                        return STREAM_SCAN_DATA;
                    }
                }
                break;

            case DATA_READY:
                if (gs_consume_memory_data(node, i)) {
                    return STREAM_SCAN_DATA;
                } else {
                    break;
                }

            case CONN_ERR:
                ereport(ERROR,
                    (errcode(ERRCODE_STREAM_REMOTE_CLOSE_SOCKET),
                        errmsg("Failed to read response from Local Stream Node,"
                               " Detail: Node %s, Plan Node ID %u, SMP ID %d",
                            g_instance.attr.attr_common.PGXCNodeName,
                            node->sharedContext->key_s.planNodeId,
                            i)));
                break;
            // dataStatus is enum,
            default:
                break;
        }
    } while (i != scanLoc);

    *waitnode_count = waitnodeCount;

    if (finished) {
        return STREAM_SCAN_FINISH;
    } else {
        return STREAM_SCAN_WAIT;
    }
}

/*
 * @Description: Receive data from shared memory for local stream.
 *
 * @param[IN] node: stream state
 * @return bool: true -- successed to find data and need more data.
 *                  false -- all connection finished or recerive error.
 */
bool gs_memory_recv(StreamState* node)
{
    char result;
    struct hash_entry* entry = NULL;
    entry = node->sharedContext->poll_entrys[u_sess->stream_cxt.smp_id];
    bool re = true;
    int waitnode_count = 0;

    /* If there is already tuple in buffer, return the data at once. */
    if (!node->sharedContext->vectorized && gs_return_tuple(node)) {
        return true;
    }

    for (;;) {
        /* Check for interrupt at the beginning of the loop. */
        CHECK_FOR_INTERRUPTS();

        /* Check if we can early stop now. */
        if (executorEarlyStop()) {
            re = false;
            break;
        }

        /* Search all producers to find data. */
        result = gs_find_memory_data(node, &waitnode_count);
        if (result == STREAM_SCAN_DATA) {
            re = true;
            break;
        } else if (result == STREAM_SCAN_FINISH) {
            re = false;
            break;
        }

        WaitStatePhase oldPhase = pgstat_report_waitstatus_phase(PHASE_NONE, true);
        WaitState oldStatus = pgstat_report_waitstatus_comm(STATE_WAIT_NODE,
            u_sess->pgxc_cxt.PGXCNodeId,
            waitnode_count,
            node->sharedContext->key_s.planNodeId,
            global_node_definition ? global_node_definition->num_nodes : -1);

        /* Poll to wait data from producers. */
        NetWorkTimePollStart(t_thrd.pgxc_cxt.GlobalNetInstr);
        (void)entry->_timewait(SINGLE_WAITQUOTA);
        NetWorkTimePollEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

        pgstat_reset_waitStatePhase(oldStatus, oldPhase);
    }

    return re;
}

/*
 * @Description: Inform all related consuemrs that there is no more data to send.
 *
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] connNum: producer connection number
 */
void gs_memory_send_finish(StreamSharedContext* sharedContext, int connNum)
{
    struct hash_entry* entry = NULL;

    for (int i = 0; i < connNum; i++) {
        /* Set flags. */
        sharedContext->is_connect_end[i][u_sess->stream_cxt.smp_id] = true;

        /* send signal */
        entry = sharedContext->poll_entrys[i];
        entry->_signal();
    }
}

/*
 * @Description: Set all connections with this producer to close.
 *
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] connNum: producer connection number
 * @param[IN] smpId: producer smp id
 */
void gs_memory_close_conn(StreamSharedContext* sharedContext, int connNum, int consumerId)
{
    struct hash_entry* entry = NULL;

    for (int i = 0; i < connNum; i++) {
        /* Set flags. */
        sharedContext->is_connect_end[consumerId][i] = true;

        /*
         * Send signal to the producers which may be still waiting quota,
         * in a query like "limit XXX", when consumer don't need data anymore,
         * but the producers haven't send all data yet.
         */
        entry = sharedContext->quota_entrys[consumerId][i];
        entry->_signal();
    }
}

void SetupCommSignalHook()
{
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);

    (void)gspqsignal(SIGTERM, SIG_IGN);
    (void)gspqsignal(SIGQUIT, SIG_IGN);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_IGN);
    /* when support guc online change, we can accept sighup, but now we don't handle it */
    (void)gspqsignal(SIGHUP, SIG_IGN);
}

/* SIGTERM: set flag to exit normally */
static void PoolCleanerShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.poolcleaner_cxt.shutdown_requested = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

void SetupPoolerCleanSignalHook()
{
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGQUIT, SIG_IGN);
    (void)gspqsignal(SIGTERM, PoolCleanerShutdownHandler);
    (void)gspqsignal(SIGINT, PoolCleanerShutdownHandler);                      /* cancel current query */
    (void)gspqsignal(SIGALRM, SIG_IGN);            /* timeout conditions */
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE, FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
}

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

void comm_receivers_comm_init()
{
#ifdef ENABLE_MULTIPLE_NODES
    int error = 0;

    /* initialize sctp address storage */
    error = mc_sctp_addr_init(g_instance.comm_cxt.localinfo_cxt.g_local_host,
        g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.port,
        &(g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.ss),
        &(g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.ss_len));

    /* intialize sctp receivers socket */
    if (error == 0) {
        g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.socket = g_libcomm_adapt.listen();
    }
    if (error != 0 || g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.socket < 0) {
        ereport(FATAL, (errmsg("(r|receiver init)\tFailed to init receiver listen socket:%s.", mc_strerror(errno))));
    }
    error = g_instance.comm_cxt.quota_cxt.g_quota_changing->init();  // initiaize quota notification semaphore
    if (error != 0) {
        ereport(FATAL, (errmsg("(r|receiver init)\tFailed to init receiver semaphore:%s.", mc_strerror(errno))));
    }
#endif
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
            gs_s_close_bad_ctrl_tcp_sock(&f_fd_id, ECOMMSCTPTCPDISCONNECT, true, idx);
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
            gs_s_close_bad_ctrl_tcp_sock(&f_fd_id, ECOMMSCTPTCPDISCONNECT, true, idx);
            if (idx >= 0) {
                g_instance.comm_cxt.g_s_node_sock[idx].unlock();
            }
            continue;
        }
#endif

        /* do receiving data */
        if ((rc = mc_tcp_read_block(f_fd_id.fd, &fcmsgr, sizeof(fcmsgr), 0)) > 0) {
            /* step8: resolve the message */
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
                errno = ECOMMSCTPNODEIDXSCTPFD;
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
                gs_s_close_bad_ctrl_tcp_sock(&f_fd_id, ECOMMSCTPTCPDISCONNECT, true, idx);
                if (idx >= 0) {
                    g_instance.comm_cxt.g_s_node_sock[idx].unlock();
                }
                continue;
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
        } else if (rc < 0) {
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
            gs_s_close_bad_ctrl_tcp_sock(&f_fd_id, ECOMMSCTPTCPDISCONNECT, true, idx);
            if (idx >= 0) {
                g_instance.comm_cxt.g_s_node_sock[idx].unlock();
            }
        } else if (rc == 0) {
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
            continue;
        }
    }
}

void commReceiverFlowLoop(int ltk, uint64 *end_time)
{
    int rc, i;
    bool found = false;
    sock_id_entry* entry_id = NULL;
    struct FCMSG_T fcmsgr = {0x0};

    /* step3:  waiting for network events */
    (void)mc_poller_wait(t_thrd.comm_cxt.g_libcomm_poller_list, EPOLL_TIMEOUT);
    *end_time = mc_timers_us();

    /* step4: get the epoll events */
    int nevents = t_thrd.comm_cxt.g_libcomm_poller_list->nevents;
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
        return;
        ;
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
                struct sockaddr ctrl_client;
                socklen_t s_len = sizeof(struct sockaddr);
                /* do accept */
                int ctk = mc_tcp_accept(ltk, (struct sockaddr*)&ctrl_client, (socklen_t*)&s_len);
                if (ctk < 0) {
                    COMM_DEBUG_LOG("(r|flow ctrl)\tFailed to accept tcp connection on listen socket[%d]:%s",
                        ltk,
                        mc_strerror(errno));
                    continue;
                }

                sockaddr_in* pSin = (sockaddr_in*)&ctrl_client;
                char* ipstr = inet_ntoa(pSin->sin_addr);
                LIBCOMM_ELOG(LOG, "(r|flow ctrl)\tDetect incoming connection, socket[%d] from [%s].", ctk, ipstr);

                /* Server side gss kerberos authentication for tcp connection. */
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
                if (is_comm_fault_injection(LIBCOMM_FI_GSS_TCP_FAILED)) {
                    /* Server side gss kerberos authentication for tcp connection. */
                    mc_tcp_close(ctk);
                    errno = ECOMMSCTPGSSAUTHFAIL;
                    LIBCOMM_ELOG(WARNING,
                        "(r|flow ctrl)\t[FAULT INJECTION]Control channel GSS authentication failed, listen "
                        "socket[%d]:%s.",
                        ltk,
                        mc_strerror(errno));
                    continue;
                }
#endif
                /* Server side gss kerberos authentication for tcp connection.
                 * if GSS authentication SUCC, no IP authentication is required.
                 */
                if (g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile != NULL) {
                    if (GssServerAuth(ctk, g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile) < 0) {
                        mc_tcp_close(ctk);
                        errno = ECOMMSCTPGSSAUTHFAIL;
                        LIBCOMM_ELOG(WARNING,
                            "(r|flow ctrl)\tControl channel GSS authentication failed, listen socket[%d]:%s.",
                            ltk,
                            mc_strerror(errno));
                        continue;
                    } else {
                        COMM_DEBUG_LOG("(r|flow ctrl)\tControl channel GSS authentication SUCC, listen socket[%d]:%s.",
                            ltk,
                            mc_strerror(errno));
                    }
                } else {
                    /* send signal to postmaster thread to reload hba */
                    int retry_count = gs_reload_hba(ltk, ctrl_client);
                    if (retry_count >= RELOAD_HBA_RETRY_COUNT) {
                        mc_tcp_close(ctk);
                        continue;
                    }
                }

                struct sock_id ctk_fd_id = {ctk, 0};
                /* update the new socket and its version */
                if (gs_update_fd_to_htab_socket_version(&ctk_fd_id) < 0) {
                    LIBCOMM_ELOG(WARNING,
                        "(r|flow ctrl)\tFailed to save socket[%d] and version[%d].",
                        ctk_fd_id.fd,
                        ctk_fd_id.id);
                }

                LIBCOMM_PTHREAD_MUTEX_LOCK(g_instance.comm_cxt.pollers_cxt.g_r_poller_list_lock);
                /* add the new socket to epoll list for monitoring data */
                if (g_instance.comm_cxt.pollers_cxt.g_r_poller_list->add_fd(&ctk_fd_id) < 0) {
                    mc_tcp_close(ctk);
                    LIBCOMM_ELOG(WARNING, "(r|flow ctrl)\tFailed to add socket[%d] to poller list.", ctk);
                }
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(g_instance.comm_cxt.pollers_cxt.g_r_poller_list_lock);

                LIBCOMM_ELOG(LOG,
                    "(r|flow ctrl)\tContoller tcp listener on socket[%d,%d] accepted socket[%d,%d].",
                    t_fd_id.fd,
                    t_fd_id.id,
                    ctk_fd_id.fd,
                    ctk_fd_id.id);
            }
        } else {
            /* step8: if the socket is a normal conection socket, do receive message and other processes by the contents
             * of the message */
            int idx = -1;  // node index
#ifdef ENABLE_LLT
            LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
            entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &t_fd_id, HASH_FIND, &found);
            if (found) {
                idx = entry_id->entry.val;
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
                entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &t_fd_id, HASH_FIND, &found);
                if (found) {
                    idx = entry_id->entry.val;
                } else {
                    idx = -1;
                }
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

                int error = 0;
                socklen_t errlen = sizeof(error);
                (void)getsockopt(t_fd_id.fd, SOL_SOCKET, SO_ERROR, (void*)&error, &errlen);
                LIBCOMM_ELOG(WARNING,
                    "(r|flow ctrl)\tPoller receive error, "
                    "close tcp socket[%d] to node[%d]:%s, events[%u], error[%d]:%s.",
                    t_fd_id.fd,
                    idx,
                    REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx),
                    t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events,
                    error,
                    mc_strerror(error));

                /* close the socket and clean the related information */
                gs_r_close_bad_ctrl_tcp_sock(&t_fd_id, ECOMMSCTPTCPDISCONNECT);
                continue;
            }
            /* step9: receive data from the connection */
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
            if (is_comm_fault_injection(LIBCOMM_FI_R_TCP_DISCONNECT)) {
                /* at receiver, if the tcp connection is broken, which caused the failure of receiving, we do cleanning
                 * and report the error */
                LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
                entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &t_fd_id, HASH_FIND, &found);
                if (found) {
                    idx = entry_id->entry.val;
                } else {
                    idx = -1;
                }
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

                LIBCOMM_ELOG(WARNING,
                    "(r|flow ctrl)\t[FAULT INJECTION]TCP disconnect with socket[%d] for node[%d]:%s, detail:%s.",
                    t_fd_id.fd,
                    idx,
                    REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx),
                    gs_comm_strerror());
                gs_r_close_bad_ctrl_tcp_sock(&t_fd_id, ECOMMSCTPTCPDISCONNECT);
                continue;
            }
#endif

            /* when query is started, other instances will send message to asking for a connection */
            if ((rc = mc_tcp_read_block(t_fd_id.fd, &fcmsgr, sizeof(fcmsgr), 0)) > 0) {
                /* step10: resolve the message  */
                /* src is from the network, maybe do not have '\0' */
                gs_update_recv_ready_time();
                fcmsgr.nodename[NAMEDATALEN - 1] = '\0';
                /* fault node checksum (to avoid port injection)
                 * "g_htab_fd_id_node_idx.get_value() < 0" means first connection, so the message type must be REGIST
                 * we need to check the type and extra_info.
                 */
                LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
                entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &t_fd_id, HASH_FIND, &found);
                if (found) {
                    idx = entry_id->entry.val;
                } else {
                    idx = -1;
                }
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

                if ((idx < 0) && !((fcmsgr.type == CTRL_CONN_REGIST || fcmsgr.type == CTRL_CONN_REGIST_CN) &&
                                     fcmsgr.extra_info == 0xEA)) {
                    fcmsgr.nodename[NAMEDATALEN - 1] = '\0';
                    LIBCOMM_ELOG(WARNING,
                        "(r|flow ctrl)\tReveive fault message with socket[%d] for[%s], type[%d].",
                        t_fd_id.fd,
                        fcmsgr.nodename,
                        fcmsgr.type);
                    errno = ECOMMSCTPNODEIDXSCTPFD;
                    printfcmsg("r|flow ctrl", &fcmsgr);
                    gs_r_close_bad_ctrl_tcp_sock(&t_fd_id, ECOMMSCTPTCPDISCONNECT);
                    continue;
                }

                DEBUG_QUERY_ID = fcmsgr.query_id;

                int retry_count = 10;
                do {
                    /* node_idx is uint16, so if gs_get_node_idx() returns -1, node_idx=65535 */
                    if ((fcmsgr.node_idx = gs_get_node_idx(fcmsgr.nodename)) >=
                        g_instance.comm_cxt.counters_cxt.g_cur_node_num) {
                        LIBCOMM_ELOG(WARNING,
                            "(r|flow ctrl)\tReveive fault message with socket[%d] for[%s], type[%d], get node_idx "
                            "again.",
                            t_fd_id.fd,
                            fcmsgr.nodename,
                            fcmsgr.type);
                        usleep(10000);
                    } else {
                        break;
                    }
                } while (retry_count--);

                COMM_DEBUG_CALL(printfcmsg("r|flow ctrl", &fcmsgr));

                /* fault node index or fault stream id */
                /* node_idx is uint16, so if gs_get_node_idx() returns -1, node_idx=65535 */
                if (fcmsgr.node_idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num || fcmsgr.streamid == 0 ||
                    fcmsgr.streamid >= g_instance.comm_cxt.counters_cxt.g_max_stream_num) {
                    fcmsgr.nodename[NAMEDATALEN - 1] = '\0';
                    LIBCOMM_ELOG(WARNING,
                        "(r|flow ctrl)\tReveive fault message with socket[%d] for[%s], type[%d].",
                        t_fd_id.fd,
                        fcmsgr.nodename,
                        fcmsgr.type);
                    errno = ECOMMSCTPNODEIDXSCTPFD;
                    printfcmsg("r|flow ctrl", &fcmsgr);
                    gs_r_close_bad_ctrl_tcp_sock(&t_fd_id, ECOMMSCTPTCPDISCONNECT);
                    continue;
                }
                /* step13: check the type of the message, and do different processing */
                switch (fcmsgr.type) {
                    case CTRL_PEER_TID:
                        gs_receivers_flow_handle_tid_request(&fcmsgr);
                        break;

                    case CTRL_CONN_DUAL:
                    case CTRL_CONN_REQUEST:
                        gs_receivers_flow_handle_ready_request(&fcmsgr);
                        break;

                    case CTRL_CLOSED:
                        gs_receivers_flow_handle_close_request(&fcmsgr);
                        break;

                    case CTRL_ASSERT_FAIL:
                        gs_receivers_flow_handle_assert_fail_request(&fcmsgr);
                        break;

                    case CTRL_CONN_REGIST:
                    case CTRL_CONN_REGIST_CN:
                        gs_accept_ctrl_conntion(&t_fd_id, &fcmsgr);
                        break;

                    case CTRL_PEER_CHANGED:
                        gs_r_close_bad_ctrl_tcp_sock(&t_fd_id, ECOMMSCTPREMOETECLOSE);
                        break;

                    /* case CTRL_INIT, CTRL_RESUME, CTRL_HOLD */
                    default:
                        /* should not happen */
                        LIBCOMM_ASSERT(false, fcmsgr.node_idx, fcmsgr.streamid, ROLE_CONSUMER);
                        break;
                }
                DEBUG_QUERY_ID = 0;
            } else if (rc < 0) {
                /* failed to read message */
                /* at receiver, if the tcp connection is broken, which caused the failure of receiving, we do cleanning
                 * and report the error */
                LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
                entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &t_fd_id, HASH_FIND, &found);
                if (found) {
                    idx = entry_id->entry.val;
                } else {
                    idx = -1;
                }
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

                LIBCOMM_ELOG(WARNING,
                    "(r|flow ctrl)\tTCP disconnect with socket[%d] for node[%d]:%s, detail:%s.",
                    t_fd_id.fd,
                    idx,
                    REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx),
                    gs_comm_strerror());
                gs_r_close_bad_ctrl_tcp_sock(&t_fd_id, ECOMMSCTPTCPDISCONNECT);
            } else if (rc == 0) {
                /* error can be resolved by retry, just report warning */
                LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
                entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &t_fd_id, HASH_FIND, &found);
                if (found) {
                    idx = entry_id->entry.val;
                } else {
                    idx = -1;
                }
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

                LIBCOMM_ELOG(WARNING,
                    "(r|flow ctrl)\tTCP receive reply with socket[%d] for node[%d]:%s, detail:%s.",
                    t_fd_id.fd,
                    idx,
                    REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx),
                    gs_comm_strerror());
                continue;
            }
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

    /* step4: send/receive delay survey messages and analysis */
    if (g_instance.comm_cxt.g_delay_survey_switch) {
        gs_delay_survey();
        gs_delay_analysis();
    }

#ifdef ENABLE_MULTIPLE_NODES
    libcomm_performance_test();
#endif

    /* if g_cur_node_num==0, it means postmaster thread doesn't finish initilaization. */
    if ((g_instance.comm_cxt.counters_cxt.g_cur_node_num != 0) &&
        (g_instance.comm_cxt.counters_cxt.g_expect_node_num != g_instance.comm_cxt.counters_cxt.g_cur_node_num)) {
        gs_online_change_capacity();
    }

    if (g_instance.comm_cxt.commutil_cxt.g_debug_mode) {
        current_time = mc_timers_ms();
        if (*last_print_time + g_print_interval_time < current_time) {
            print_stream_sock_info();
            *last_print_time = current_time;
        }
    }
}

void commReceiverLoop(int selfid, uint64 *end_time, int *epoll_timeout_count)
{
    int nevents, idx, ret_recv, client_socket, retry_count, max_len, error, i, j;
    bool found = false;
    sock_id_entry* entry_id = NULL;
    struct sock_id fd_id;
    struct sockaddr ctrl_client;
    socklen_t s_len = 0;
    int curr_len = 0;
    int dst_idx = 0;
    
    /* Wait poll */
    (void)mc_poller_wait(t_thrd.comm_cxt.g_libcomm_poller_list, EPOLL_TIMEOUT);
    *end_time = mc_timers_us();
    t_thrd.comm_cxt.g_receiver_loop_poll_up = COMM_STAT_TIME();

    nevents = t_thrd.comm_cxt.g_libcomm_poller_list->nevents;
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
        return;
    }

    if (nevents == 0) {
        (*epoll_timeout_count)++;
        if (*epoll_timeout_count > MAX_EPOLL_TIMEOUT_COUNT) {
            *epoll_timeout_count = 0;
            COMM_DEBUG_LOG(
                "(r|recv loop)\tepoll[%d] wait timeout[%dms].",
                t_thrd.comm_cxt.g_libcomm_poller_list->ep, EPOLL_TIMEOUT);
        }
    } else {
        *epoll_timeout_count = 0;
    }

    for (i = 0; i < t_thrd.comm_cxt.g_libcomm_poller_list->nevents; i++) {
        ret_recv = 0;
        s_len = sizeof(struct sockaddr);

        fd_id.fd = (int)(((uint64)t_thrd.comm_cxt.g_libcomm_poller_list->events[i].data.u64 >> MC_POLLER_FD_ID_OFFSET));
        fd_id.id = (int)(((uint64)t_thrd.comm_cxt.g_libcomm_poller_list->events[i].data.u64 & MC_POLLER_FD_ID_MASK));
        mc_assert(fd_id.fd > 0);
        /* Special case: handle with accepting connection */
        if (unlikely(fd_id.fd == g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.socket)) {
            if (t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events & EPOLLIN) {
                client_socket = g_libcomm_adapt.accept(g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.socket,
                    (struct sockaddr*)&ctrl_client,
                    (socklen_t*)&s_len);
                if (client_socket < 0) {
                    LIBCOMM_ELOG(WARNING,
                        "(r|recv loop)\tFailed to accept data connection on listen socket[%d]:%s.",
                        g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.socket,
                        mc_strerror(errno));
                    continue;
                }

                sockaddr_in* pSin = (sockaddr_in*)&ctrl_client;
                char* ipstr = inet_ntoa(pSin->sin_addr);
                LIBCOMM_ELOG(
                    LOG, "(r|recv loop)\tDetect incoming connection, socket[%d] from [%s].", client_socket, ipstr);

                /*
                 * server side gss kerberos authentication for data connection.
                 * authentication for tcp mode after accept.
                 * if GSS authentication SUCC, no IP authentication is required.
                 */
                if (g_instance.comm_cxt.g_comm_tcp_mode == true) {
                    if (g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile != NULL) {
                        if (GssServerAuth(client_socket, g_instance.comm_cxt.localinfo_cxt.gs_krb_keyfile) < 0) {
                            mc_tcp_close(client_socket);
                            errno = ECOMMSCTPGSSAUTHFAIL;
                            LIBCOMM_ELOG(WARNING,
                                "(r|recv loop)\tData channel GSS authentication failed, listen socket[%d]:%s.",
                                fd_id.fd,
                                mc_strerror(errno));
                            continue;
                        } else {
                            LIBCOMM_ELOG(LOG,
                                "(r|recv loop)\tData channel GSS authentication SUCC, listen socket[%d].",
                                fd_id.fd);
                        }
                    } else {
                        /* send signal to postmaster thread to reload hba */
                        retry_count = gs_reload_hba(fd_id.fd, ctrl_client);
                        if (retry_count >= RELOAD_HBA_RETRY_COUNT) {
                            mc_tcp_close(client_socket);
                            continue;
                        }
                    }
                }

                struct sock_id ctk_fd_id = {client_socket, 0};
                if (gs_update_fd_to_htab_socket_version(&ctk_fd_id) < 0) {
                    mc_tcp_close(client_socket);
                    LIBCOMM_ELOG(WARNING,
                        "(r|recv loop)\tFailed to save socket[%d] and version[%d]:%s.",
                        ctk_fd_id.fd,
                        ctk_fd_id.id,
                        mc_strerror(errno));
                    continue;
                }
                /* add data socket to the poll list with minimal elements for waiting data */
                LIBCOMM_PTHREAD_MUTEX_LOCK(g_instance.comm_cxt.pollers_cxt.g_r_libcomm_poller_list_lock);
                max_len = g_instance.comm_cxt.pollers_cxt.g_libcomm_receiver_poller_list[0].get_socket_count();
                curr_len = 0;
                dst_idx = 0;
                for (j = 1; j < g_instance.comm_cxt.counters_cxt.g_recv_num; j++) {
                    curr_len = g_instance.comm_cxt.pollers_cxt.g_libcomm_receiver_poller_list[j].get_socket_count();
                    if (max_len > curr_len) {
                        dst_idx = j;
                        break;
                    }
                }
                if (g_instance.comm_cxt.pollers_cxt.g_libcomm_receiver_poller_list[dst_idx].add_fd(&ctk_fd_id) < 0) {
                    mc_tcp_close(client_socket);
                    LIBCOMM_ELOG(
                        WARNING, "(r|recv loop)\tFailed to add socket[%d] to poller list[%d].", client_socket, selfid);
                }
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(g_instance.comm_cxt.pollers_cxt.g_r_libcomm_poller_list_lock);
                LIBCOMM_ELOG(LOG,
                    "(r|recv loop)\tReceiver data listener on socket[%d] accepted socket[%d,%d].",
                    g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.socket,
                    ctk_fd_id.fd,
                    ctk_fd_id.id);
            }
            continue;
        }

        LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
        entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &fd_id, HASH_FIND, &found);
        if (found) {
            idx = entry_id->entry.val;
        } else {
            idx = -1;
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

        /* Recv data message */
        if (t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events & EPOLLIN) {
            ret_recv = gs_internal_recv(fd_id, idx);
            DEBUG_QUERY_ID = 0;

            if (ret_recv < 0) {
                LIBCOMM_PTHREAD_MUTEX_LOCK(&g_htab_fd_id_node_idx_lock);
                entry_id = (sock_id_entry*)hash_search(g_htab_fd_id_node_idx, &fd_id, HASH_FIND, &found);
                if (found) {
                    idx = entry_id->entry.val;
                } else {
                    idx = -1;
                }
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_fd_id_node_idx_lock);

                LIBCOMM_ELOG(WARNING,
                    "(r|recv loop)\tFailed to recv message from node[%d]:%s on data socket[%d].",
                    idx,
                    REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx),
                    fd_id.fd);
                if (ret_recv == RECV_MEM_ERROR) {
                    /*
                     * If there is no usable memory, we traverse all the c_mailbox(es) to find the query who occupies
                     * maximun memory, and make it failure to release the memory. Otherwise, the communication layer
                     * maybe hang up.
                     */
                    LIBCOMM_ELOG(WARNING, "(r|recv loop)\tFailed to malloc, begin release memory.");
                    gs_r_release_comm_memory();
                } else if (ret_recv == RECV_NET_ERROR) {
                    gs_r_close_bad_data_socket(idx, fd_id, true);
                }
            }
        } else if ((t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events & EPOLLERR) ||
                   (t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events & EPOLLHUP)) {  /* ERROR handling */
            error = 0;
            s_len = sizeof(error);
            (void)getsockopt(fd_id.fd, SOL_SOCKET, SO_ERROR, (void*)&error, &s_len);
            LIBCOMM_ELOG(WARNING,
                "(r|recv loop)\tBroken receive channel to node[%d]:%s "
                "on socket[%d], events[%u], error[%d]:%s.",
                idx,
                REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx),
                fd_id.fd,
                t_thrd.comm_cxt.g_libcomm_poller_list->events[i].events,
                error,
                mc_strerror(error));
            gs_r_close_bad_data_socket(idx, fd_id, true);
        }
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

    knl_thread_set_name("CommSendStream");

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* Identify myself via ps */
    init_ps_display("CommSendStream worker process", "", "", "");

    /* setup signal process hook */
    SetupCommSignalHook();

    /* initialize globals */
    log_timezone = g_instance.comm_cxt.libcomm_log_timezone;
    t_thrd.comm_cxt.LibcommThreadType = LIBCOMM_SEND_CTRL;

    (void)MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);

    t_thrd.comm_cxt.g_libcomm_poller_list =
        g_instance.comm_cxt.pollers_cxt.g_s_poller_list->get_poller();  // get poller for monitoring network events
    LIBCOMM_ELOG(LOG, "CommSendStream thread is initialized.");

    sigjmp_buf local_sigjmp_buf;
    int curTryCounter;
    int *oldTryCounter = NULL;

    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {

        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

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

    knl_thread_set_name("CommRcvStream");

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* Identify myself via ps */
    init_ps_display("CommRcvStream worker process", "", "", "");

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

    LIBCOMM_ELOG(LOG, "CommRcvStream thread is initialized, ready to accept on socket[%d].", ltk);

    sigjmp_buf local_sigjmp_buf;
    int curTryCounter;
    int *oldTryCounter = NULL;

    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {

        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

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

    knl_thread_set_name("CommAuxStream");

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* Identify myself via ps */
    init_ps_display("CommAuxStream worker process", "", "", "");

    /* setup signal process hook */
    SetupCommSignalHook();

    /* initialize globals */
    log_timezone = g_instance.comm_cxt.libcomm_log_timezone;
    t_thrd.comm_cxt.LibcommThreadType = LIBCOMM_AUX;

    (void)MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);

    uint64 last_print_time;
    last_print_time = mc_timers_ms();

    LIBCOMM_ELOG(LOG, "CommAuxStream thread is initialized.");

    sigjmp_buf local_sigjmp_buf;
    int curTryCounter;
    int *oldTryCounter = NULL;

    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {

        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

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

#ifdef ENABLE_MULTIPLE_NODES
void init_clean_pooler_idle_connections()
{
    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = COMM_POOLER_CLEAN;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    knl_thread_set_name("CommPoolCleaner");

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* Identify myself via ps */
    init_ps_display("CommPoolCleaner process", "", "", "");

    /* set processing mode */
    SetProcessingMode(InitProcessing);

    /* setup signal process hook */
    SetupPoolerCleanSignalHook();

    /* We allow SIGQUIT (quickdie) at all times */
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /* Early initialization */
    BaseInit();

#ifndef EXEC_BACKEND
    InitProcess();
#endif

    u_sess->proc_cxt.MyProcPort->SessionStartTime = GetCurrentTimestamp();
    return;
}

extern void clean_pooler_idle_connections(void);
void commPoolCleanerMain()
{
    sigjmp_buf local_sigjmp_buf;
    uint64 current_time, last_start_time, poolerMaxIdleTime, nodeNameHashVal;
    MemoryContext poolCleaner_context;
    char *curNodeName = g_instance.attr.attr_common.PGXCNodeName;
    const Size nodeNameHashMaxVal = 60000;

    ereport(LOG, (errmsg("commPoolCleanerMain started")));
    init_clean_pooler_idle_connections();

    /*
     * Create the memory context we will use in the main loop.
     *
     * t_thrd.mem_cxt.msg_mem_cxt is reset once per iteration of the main loop, ie, upon
     * completion of processing of each command message from the client.
     */
    poolCleaner_context = AllocSetContextCreate(t_thrd.top_mem_cxt, "Pool Cleaner", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    (void)MemoryContextSwitchTo(poolCleaner_context);

    /* If an exception is encountered, processing resumes here. */
    int curTryCounter;
    int* oldTryCounter = NULL;

    /* Normal exit */
    if (t_thrd.poolcleaner_cxt.shutdown_requested) {
        g_instance.pid_cxt.CommPoolerCleanPID = 0;
        proc_exit(0); /* done */
    }
    
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        (void)MemoryContextSwitchTo(poolCleaner_context);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(poolCleaner_context);

        /*
         * process exit. Note that because we called InitProcess, a
         * callback was registered to do ProcKill, which will clean up
         * necessary state.
         */
        proc_exit(0);
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    /* report this backend in the PgBackendStatus array */
    pgstat_report_appname("PoolCleaner");

    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Pool cleaner");
    SetProcessingMode(NormalProcessing);

    /* To ensure the minimum pool concept, do not clean the idle connections of each CN at the same time.
     * Time difference: within 60s
     */
    if (curNodeName != NULL) {
        nodeNameHashVal = (uint64)(string_hash((void*)curNodeName, (strlen(curNodeName) + 1))) % nodeNameHashMaxVal;
    }
    
    last_start_time = mc_timers_ms() + nodeNameHashVal;
    for (;;) {
        if (t_thrd.poolcleaner_cxt.shutdown_requested == true || g_instance.status > NoShutdown) {
            break;
        }
        
        sleep(5);

        current_time = mc_timers_ms();
        poolerMaxIdleTime = (g_instance.attr.attr_network.PoolerMaxIdleTime) * SECS_PER_MINUTE * MS_PER_S;
        if (IS_PGXC_COORDINATOR && (current_time - last_start_time) >= poolerMaxIdleTime) {
            clean_pooler_idle_connections();
            last_start_time = current_time;
        }
    }

    /* All done, go away */
    g_instance.pid_cxt.CommPoolerCleanPID = 0;
    proc_exit(0);
}
#endif

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

    knl_thread_set_name("CommRcvWorker");

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* Identify myself via ps */
    init_ps_display("CommRcvWorker process", "", "", "");

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

    LIBCOMM_ELOG(LOG, "CommRcvWorker thread[%d] is initialized.", selfid + 1);

    sigjmp_buf local_sigjmp_buf;
    int curTryCounter;
    int *oldTryCounter = NULL;

    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {

        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

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
            if (g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.socket >= 0) {
                mc_tcp_close(g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.socket);
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
        case -1:
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
        case -1:
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
        case -1:
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
        case -1:
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

        /* if this is the first thread, it will listen and accept sctp connection */
        if (i == 0) {
            struct sock_id ltk_fd_id = {g_instance.comm_cxt.g_receivers->server_listen_sctp_conn.socket, 0};

            /* save the sctp socket & socket version to hash table */
            error = gs_update_fd_to_htab_socket_version(&ltk_fd_id);
            if (error < 0) {
                ereport(FATAL,
                    (errmsg("(r|receiver init)\tFailed to save sctp"
                            " listen socket[%d] and version[%d].",
                        ltk_fd_id.fd,
                        ltk_fd_id.id)));
            }

            /* add fd to poller cabinet */
            error = g_instance.comm_cxt.pollers_cxt.g_libcomm_receiver_poller_list[i].add_fd(&ltk_fd_id);
            if (error < 0) {
                ereport(FATAL,
                    (errmsg("(r|receiver init)\tFailed to add sctp "
                            "listen socket[%d] and version[%d] to poller list.",
                        ltk_fd_id.fd,
                        ltk_fd_id.id)));
            }
        }
    }

    for (int i = 0; i < g_instance.comm_cxt.counters_cxt.g_recv_num; i++) {
        /* start broker thread */
        threadid[i] = startCommReceiver(thd_receiver_id + i);
        if (threadid[i] == 0) {
            ereport(FATAL, (errmsg("(r|receiver init)\tFailed to init broker thread.")));
        } else {
            while (thd_receiver_id[i] != -1) {
                (void)usleep(100);
            }
        }
    }

    LIBCOMM_FREE(thd_receiver_id, thd_receiver_id_size);

    return;
}

