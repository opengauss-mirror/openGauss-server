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

#include "libcomm_core/mc_tcp.h"
#include "libcomm_core/mc_poller.h"
#include "libcomm_utils/libcomm_thread.h"
#include "libcomm_utils/libcomm_lqueue.h"
#include "libcomm_utils/libcomm_queue.h"
#include "libcomm_utils/libcomm_lock_free_queue.h"
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
#include "libcomm_common.h"

#ifdef ENABLE_UT
#define static
#endif

#define CHECKCONNSTATTIMEOUT 5

#ifndef MS_PER_S
#define MS_PER_S 1000
#endif


/* hash tables */
/* hash table to keep:  ip + port -> connection status */
static HTAB* g_htab_ip_state = NULL;
pthread_mutex_t g_htab_ip_state_lock;

/* hash table to keep: nodename -> node id */
static HTAB* g_htab_nodename_node_idx = NULL;
pthread_mutex_t g_htab_nodename_node_idx_lock;
static int nodename_count = 0;

/* hash table to keep: fd_id -> node id */
HTAB* g_htab_fd_id_node_idx = NULL;
pthread_mutex_t g_htab_fd_id_node_idx_lock;

/* for wake up thread */
static HTAB* g_htab_tid_poll = NULL;
pthread_mutex_t g_htab_tid_poll_lock;

/* at receiver and sender: hash table to keep: socket -> socket version number, for socket management */
static HTAB* g_htab_socket_version = NULL;
pthread_mutex_t g_htab_socket_version_lock;

static ArrayLockFreeQueue<char> g_memory_pool_queue;

unsigned long IOV_DATA_SIZE = 1024 * 8;
unsigned long IOV_ITEM_SIZE = IOV_DATA_SIZE + sizeof(struct iovec) + sizeof(mc_lqueue_element);
unsigned long DEFULTMSGLEN = 1024 * 8;
unsigned long LIBCOMM_BUFFER_SIZE  = 1024 * 8;

gsocket gs_invalid_gsock = {0, 0, 0, 0};

static void gs_s_build_reply_conntion(libcommaddrinfo* addr_info, int remote_version);

extern GlobalNodeDefinition* global_node_definition;

extern knl_instance_context g_instance;

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
        errno = ECOMMTCPMEMALLOC;
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
 * function name    : gs_set_reply_sock
 * description      : set reply socket for g_r_node_sock by compare remote node name
 * arguments          : node_idx, node we want to set reply sock
 */
void gs_set_reply_sock(int node_idx)
{
    for (int recv_idx = 0; recv_idx < g_instance.comm_cxt.counters_cxt.g_cur_node_num; recv_idx++) {
        if (strcmp(g_instance.comm_cxt.g_r_node_sock[recv_idx].remote_nodename,
                   g_instance.comm_cxt.g_r_node_sock[node_idx].remote_nodename) == 0) {
            // save reply socket in g_r_node_sock
            g_instance.comm_cxt.g_r_node_sock[recv_idx].lock();
            g_instance.comm_cxt.g_r_node_sock[recv_idx].libcomm_reply_sock =
                g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket;
            g_instance.comm_cxt.g_r_node_sock[recv_idx].libcomm_reply_sock_id =
                g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id;
            g_instance.comm_cxt.g_r_node_sock[recv_idx].unlock();
            break;
        }
    }

    return;
}

/*
 * function name    : gs_senders_struct_set
 * description      : initialize socket of local senders, for sending
 * notice           : retry 10 times inside if failure happens,
 * arguments      __in node_begin
 *                __in node_end
 */
void gs_senders_struct_set()
{
    int error = 0;
    int i;

    // initialize sender socket and address storage
    for (i = 0; i < MAX_CN_DN_NODE_NUM; i++) {
        // g_s_node_sock
        g_instance.comm_cxt.g_s_node_sock[i].init();

        // initialize address storage
        // initialize socket in gs_connect
        error = mc_tcp_addr_init(g_instance.comm_cxt.localinfo_cxt.g_local_host,
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
                    errno = ECOMMTCPCONNTIMEOUT;
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
                errno = (rc == ETIMEDOUT) ? ECOMMTCPCONNTIMEOUT : ECOMMTCPCONNFAIL;
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
            struct sock_id libcomm_fd_id = {g_instance.comm_cxt.g_senders->sender_conn[old_slot_id].socket,
                g_instance.comm_cxt.g_senders->sender_conn[old_slot_id].socket_id};
            gs_s_close_bad_data_socket(&libcomm_fd_id, ECOMMTCPPEERCHANGED, node_idx);
            LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[old_slot_id].rwlock);
        } else {
            g_instance.comm_cxt.g_s_node_sock[node_idx].ip_changed = true;
            g_instance.comm_cxt.g_s_node_sock[node_idx].lock();
            struct sock_id ctrl_fd_id = {g_instance.comm_cxt.g_s_node_sock[old_slot_id].ctrl_tcp_sock,
                g_instance.comm_cxt.g_s_node_sock[old_slot_id].ctrl_tcp_sock_id};
            gs_s_close_bad_ctrl_tcp_sock(&ctrl_fd_id, ECOMMTCPPEERCHANGED, false, node_idx);
            g_instance.comm_cxt.g_s_node_sock[node_idx].unlock();
        }
    }

    return state;
}

/*
 * add local thread id to g_htab_tid_poll
 * then thread will call gs_poll during connecting, send and recv
 */
int gs_poll_create()
{
    struct tid_entry* entry_tid = NULL;
    bool found = false;

    if (t_thrd.comm_cxt.libcomm_semaphore != NULL) {
        return 0;
    }

    if (g_htab_tid_poll == NULL) {
        errno = ECOMMTCPCVINIT;
        LIBCOMM_ELOG(WARNING, "(libcomm tid lookup hash)\tg_htab_tid_poll is NULL.");
        return -1;
    }

#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_CREATE_POLL_FAILED)) {
        errno = ECOMMTCPMEMALLOC;
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
int gs_poll(int time_out)
{
    return t_thrd.comm_cxt.libcomm_semaphore->timed_wait(time_out);
}

/*
 * siganl thread when the expected event happened or some error happened
 */
void gs_poll_signal(binary_semaphore* sem)
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
void gs_broadcast_poll()
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
int gs_get_node_idx(char* node_name)
{
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_NO_NODEIDX)) {
        errno = ECOMMTCPINVALNODEID;
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
    int node_idx = nodename_count + 1;
    if (node_idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num) {
        hash_search(g_htab_nodename_node_idx, &ckey, HASH_REMOVE, NULL);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&g_htab_nodename_node_idx_lock);
        errno = ECOMMTCPINVALNODEID;
        return -1;
    }

    nodename_count++;
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
 *                    _in_ key_ns: libcomm stream key with node index.
 * return value     : -1:failed
 *                    >0:stream id
 */
int gs_get_stream_id(int node_idx)
{
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_NO_STREAMID)) {
        errno = ECOMMTCPSTREAMIDX;
        LIBCOMM_ELOG(WARNING,
            "(s|get sid)\t[FAULT INJECTION]Failed to obtain stream for node[%d]:%s.",
            node_idx,
            g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename);
        return -1;
    }
#endif

    int streamid = -1;

    // have no usable stream id
    if (g_instance.comm_cxt.g_usable_streamid[node_idx].pop(
            g_instance.comm_cxt.g_usable_streamid + node_idx, &streamid) <= 0) {
        errno = ECOMMTCPSTREAMIDX;
        LIBCOMM_ELOG(WARNING,
            "(s|get sid)\tFailed to obtain stream for node[%d]:%s, usable:%d/%d.",
            node_idx,
            g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
            g_instance.comm_cxt.g_usable_streamid[node_idx].count,
            g_instance.comm_cxt.counters_cxt.g_max_stream_num);
        return -1;
    }

    // succeed to return the entry in g_s_htab_nodeid_skey_to_stream
    COMM_DEBUG_LOG("(s|get sid)\tObtain stream[%d] for node[%d]:%s.",
        streamid,
        node_idx,
        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename);

    return streamid;
}  // gs_r_get_usable_streamid

int gs_update_fd_to_htab_socket_version(struct sock_id* fd_id)
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

// receiver close all streams of a node which is designed by control tcp socket
// we call this function because of the broken control tcp connection or tcp connection,
// if it is tcp connection, we should send the close info to remote
// step 1: get node index (node_idx)
// step 2: traverse g_c_mailbox[node_idx][*]
// step 3: do notify and reset all cmailbox
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
            COMM_DEBUG_LOG("(r|close all streams)\tTo close stream[%d], node[%d]:%s, query[%lu], socket[%d].",
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
static void gs_s_close_all_streams_by_fd_idx(int fd, int node_idx, int close_reason, bool with_ctrl_lock)
{
    struct p_mailbox* pmailbox = NULL;
    struct FCMSG_T fcmsgs = {0x0};
    // Note: we should have locked at the caller, so we need not lock here again
    //
    LIBCOMM_ELOG(WARNING,
        "(s|close all streams)\tTo reset all streams by socket[%d] for node[%d]:%s, detail:%s.",
        fd,
        node_idx,
        g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename,
        mc_strerror(close_reason));

    for (int j = 1; j < g_instance.comm_cxt.counters_cxt.g_max_stream_num; j++) {
        pmailbox = &P_MAILBOX(node_idx, j);
        LIBCOMM_PTHREAD_MUTEX_LOCK(&pmailbox->sinfo_lock);

        if (((pmailbox->ctrl_tcp_sock == -1) || (fd == -1) || (pmailbox->ctrl_tcp_sock == fd)) &&
            (pmailbox->state != MAIL_CLOSED)) {
            COMM_DEBUG_LOG("(s|close all streams)\tTo close stream[%d], node[%d]:%s, query[%lu], socket[%d].",
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
    int fd = -1;
    int id = -1;

    if (t_thrd.comm_cxt.g_libcomm_poller_list == NULL) {
        return;
    }
    int nevents = t_thrd.comm_cxt.g_libcomm_poller_list->nevents;
    for (int i = 0; i < nevents; i++) {
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
void gs_r_close_bad_ctrl_tcp_sock(struct sock_id* fd_id, int close_reason)
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
                "(r|close tcp socket)\tTCP disconnect with socket[%d,%d] to host:%s, node[%d]:[%s].",
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
void gs_s_close_bad_ctrl_tcp_sock(struct sock_id* fd_id, int close_reason, bool clean_epoll, int node_idx)
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
            if (close_reason == ECOMMTCPPEERCHANGED) {
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
                "(s|cls bad tcp socket)\tClose bad socket[%d,%d] for host:%s, node[%d]:%s.",
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

int gs_memory_pool_queue_initial_success(uint32 index)
{
    return g_memory_pool_queue.initialize(index);
}

struct mc_lqueue_item* gs_memory_pool_queue_pop(char* iov)
{
    return (struct mc_lqueue_item*)g_memory_pool_queue.pop(iov);
}

bool gs_memory_pool_queue_push(char* item)
{
    return g_memory_pool_queue.push(item);
}

// Calculation how many quota size need to add in mailbox
static long gs_add_quota_size(c_mailbox* cmailbox)
{
#define COMM_HIGH_MEM_USED (used_memory >= (g_instance.comm_cxt.commutil_cxt.g_total_usable_memory * 0.8))
#define COMM_MIDDLE_MEM_USED                                                         \
    (used_memory > (g_instance.comm_cxt.commutil_cxt.g_total_usable_memory * 0.5) && \
        used_memory < (g_instance.comm_cxt.commutil_cxt.g_total_usable_memory * 0.8))
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
    } else { // COMM_MIDDLE_MEM_USED
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
        ((unsigned long)(buff_used + old_quota) < DEFULTMSGLEN)) {
        return add_quota < 0 ? 0 : add_quota;
    } else {
        return 0;
    }
}

// auxiliary thread use it to change the stream state and send control message to remote point (sender)
bool gs_r_quota_notify(c_mailbox* cmailbox, FCMSG_T* msg)
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
void gs_r_release_comm_memory()
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

                gs_r_close_logic_connection(cmailbox, ECOMMTCPRELEASEMEM, &fcmsgs);
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
                (void)gs_send_ctrl_msg(&g_instance.comm_cxt.g_r_node_sock[nid], &fcmsgs, ROLE_CONSUMER);
            } else {
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
            }
        }
    }

    LIBCOMM_ELOG(WARNING,
        "(r|release memory)\tReset query[%lu] to release memory[%lu Byte].",
        release_query_id,
        total_buff_size);
}  // gs_r_release_comm_memory

// if we failed to receive message from a tcp listen socket, we should do following things
// step1: reset the streams of the related node
// step2: delete it from epoll cabinet
// step3: update the socket version
// step4: delete the socket from hash table socke -> node index (g_r_htab_data_socket_node_idx)
// step5: close the old tcp socket
void gs_r_close_bad_data_socket(int node_idx, sock_id fd_id, int close_reason, bool is_lock)
{
    if (node_idx >= 0) {
        gs_r_close_all_streams_by_fd_idx(-1, node_idx, ECOMMTCPDISCONNECT);
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
void gs_s_close_bad_data_socket(struct sock_id* fd_id, int close_reason, int node_idx)
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
int gs_push_cmailbox_buffer(c_mailbox* cmailbox, struct mc_lqueue_item* q_item, int version)
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
        COMM_DEBUG_LOG("(r|inner recv)\tNode[%d]:%s stream[%d] recv %zu msg:%c, bufCAP[%lu] and buff_q->u_size[%lu].",
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

int gs_handle_data_delay_message(int idx, struct mc_lqueue_item* q_item, uint16 msg_type)
{
    struct c_mailbox* cmailbox = NULL;
    struct libcomm_delay_package* delay_msg = NULL;
    struct iovec* iov = q_item->element.data;

    if (idx < 0) {
        return -1;
    }

    delay_msg = (struct libcomm_delay_package*)iov->iov_base;

    if (msg_type == LIBCOMM_PKG_TYPE_DELAY_REQUEST) {
        delay_msg->recv_time = (uint32)mc_timers_us();
    } else if (msg_type == LIBCOMM_PKG_TYPE_DELAY_REPLY) {
        delay_msg->finish_time = (uint32)mc_timers_us();
    }

    // put the message to the buffer in the c_mailbox
    cmailbox = &C_MAILBOX(idx, 0);
    LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);
    (void)mc_lqueue_add(cmailbox->buff_q, q_item);

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);

    return 0;
}

/*
 * function name    : gs_s_close_logic_connection
 * description      : producer close logic connetion and reset mailbox,
 *                    if producer call this, we only set state is CTRL_TO_CLOSE,
 *                    then really close when consumer send MAIL_CLOSED message.
 * notice           : we must get mailbox lock before
 * arguments        : _in_ cmailbox: libcomm logic conntion info.
 *                    _in_ close_reason: close reason.
 */
void gs_s_close_logic_connection(struct p_mailbox* pmailbox, int close_reason, FCMSG_T* msg)
{
    errno_t ss_rc;
    uint32 cpylen;

    if (pmailbox->state == MAIL_CLOSED) {
        return;
    }

    // when sender closes pmailbox, if close reason != remote close, and the state of pmailbox is MAIL_READY,
    // we set pmailbox to MAIL_TO_CLOSE, then wait for top consumer(gs_connect()) to close it.
    if ((pmailbox->state == MAIL_READY) && (close_reason != ECOMMTCPREMOETECLOSE)) {
        pmailbox->state = MAIL_TO_CLOSE;
        // wake up the producer who is waiting
        gs_poll_signal(pmailbox->semaphore);
        pmailbox->semaphore = NULL;
        return;
    }

    // 1, if tcp disconnect, we can not send control message on tcp channel,
    //    remote can receive disconnect event when flow control thread call epoll_wait.
    // 2, close reason is ECOMMTCPREMOETECLOSE means remote send MAIL_CLOSED,
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

//    send assert fail msg via ctrl connection if debug mode enable and assert failed
//
void gs_libcomm_handle_assert(bool condition, int nidx, int sidx, int node_role)
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
        gs_r_close_logic_connection(cmailbox, ECOMMTCPREMOETECLOSE, NULL);
    }

    cmailbox->local_version = local_version;
    cmailbox->remote_version = remote_version;
    cmailbox->ctrl_tcp_sock = g_instance.comm_cxt.g_r_node_sock[node_idx].ctrl_tcp_sock;
    cmailbox->state = MAIL_RUN;
    cmailbox->bufCAP = DEFULTMSGLEN;
    cmailbox->stream_key = addr_info->streamKey;
    cmailbox->query_id = DEBUG_QUERY_ID;
    cmailbox->local_thread_id = 0;
    cmailbox->peer_thread_id = 0;
    cmailbox->close_reason = 0;
    if (g_instance.comm_cxt.commutil_cxt.g_stat_mode && (cmailbox->statistic == NULL)) {
        LIBCOMM_MALLOC(cmailbox->statistic, sizeof(struct cmailbox_statistic), cmailbox_statistic);
        if (NULL == cmailbox->statistic) {
            errno = ECOMMTCPRELEASEMEM;
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

/*
 * check all mailboxs state is MAIL_READY
 * if the state of mailbox is MAIL_TO_CLOSE, top consumer will close the logic connection.
 * if mailbox state is CTRL_CLOSE(gs_check_mailbox return false),
 * means consumer do not need the data from this datanode, not report error
 */
int gs_check_all_mailbox(libcommaddrinfo** libcomm_addrinfo, int addr_num, int re,
                              bool TempImmediateInterruptOK, int timeout)
{
    int wait_index;
    int i;
    int node_idx = -1;
    int streamid = -1;
    int version = -1;
    int remote_version = -1;
    int error_index = -1;
    bool build_reply_conn = false;
    libcommaddrinfo* addr_info = NULL;
    struct p_mailbox* pmailbox = NULL;

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
                    gs_s_close_logic_connection(pmailbox, ECOMMTCPREMOETECLOSE, NULL);

                    // before continue or goto clean_connection, we must release the sinfo_lock.
                    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
                    if (IS_PGXC_COORDINATOR) {
                        addr_info->gs_sock = GS_INVALID_GSOCK;
                        continue;
                    } else {
                        errno = ECOMMTCPCONNFAIL;
                        error_index = i;
                        return gs_clean_connection(libcomm_addrinfo, addr_num, error_index,
                                                   re, TempImmediateInterruptOK);
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
                    gs_close_timeout_connections(libcomm_addrinfo, addr_num, node_idx, streamid);
                } else {
                    errno = ETIMEDOUT;
                    error_index = wait_index;
                    return gs_clean_connection(libcomm_addrinfo, addr_num, error_index,
                                               re, TempImmediateInterruptOK);
                }
            }
        } else {
            break;
        }
    }

    return 0;
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
bool gs_s_form_start_ctrl_msg(p_mailbox* pmailbox, FCMSG_T* msg)
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
int gs_push_local_buffer(int streamid, const char* message, int m_len, int cmailbox_version)
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
        errno = ECOMMTCPREMOETECLOSE;
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

    /*
     * This process is invoked when a DN sends a message to itself.
     * When the libcomm sends data to the local node, the data is directly inserted into the memory
     * and needs to be proactively notified to the listener.
     */
    if (ENABLE_THREAD_POOL_DN_LOGICCONN) {
        NotifyListener(cmailbox, false, __FUNCTION__);
    }
    return m_len;
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

static void update_cmailbox_statistic(struct c_mailbox* cmailbox, check_cmailbox_option opt, int n_got_data)
{
    libcomm_time_record* time_record = opt.time_record;
    if (cmailbox->statistic != NULL) {
        time_record->wait_data_time = ABS_SUB(time_record->wait_data_end, time_record->wait_data_start);
        cmailbox->statistic->wait_data_time += time_record->wait_data_time;

        time_record->time_now = COMM_STAT_TIME();
        cmailbox->statistic->wait_lock_time += ABS_SUB(time_record->time_now, time_record->wait_lock_start);

        if (cmailbox->statistic->first_poll_time == 0) {
            cmailbox->statistic->first_poll_time = time_record->time_enter;
            cmailbox->statistic->consumer_elapsed_time +=
                ABS_SUB(time_record->time_enter, cmailbox->statistic->start_time);
        } else {
            cmailbox->statistic->consumer_elapsed_time +=
                ABS_SUB(time_record->time_enter, t_thrd.comm_cxt.g_consumer_process_duration);
        }

        if (opt.first_cycle) {
            cmailbox->statistic->call_poll_count++;
            cmailbox->statistic->last_poll_time = time_record->time_enter;
        }

        if (n_got_data > 0 || opt.poll_error_flag == 1) {
            cmailbox->statistic->total_poll_time += ABS_SUB(time_record->time_now, time_record->time_enter);
        }
    }
}

static void gs_update_producer(struct c_mailbox* cmailbox, int* producer, int* n_got_data, int poll_error_flag)
{
    // there is data in the mailbox already
    if (cmailbox->buff_q->count > 0) {
        (*n_got_data)++;
        // set the having label for Consumer thread
        *producer = WAIT_POLL_FLAG_GOT;
    }
    // need return
    if (*n_got_data > 0 || poll_error_flag == 1) {
        /* gs_wait_poll will return, clean semaphore */
        cmailbox->semaphore = NULL;
        if (*producer == WAIT_POLL_FLAG_WAIT) {
            *producer = WAIT_POLL_FLAG_IDLE;
        }
    } else {
        /* gs_wait_poll will enter gs_poll, regist semaphore */
        cmailbox->semaphore = t_thrd.comm_cxt.libcomm_semaphore;
        *producer = WAIT_POLL_FLAG_WAIT;
    }
}

// check if there is data in the c_mailbox of the given node index and stream index already
int gs_check_cmailbox_data(const gsocket* gs_sock_array, // array of producers node index
    int nproducer,                                              // number of producers
    int* producer,                                              // producers number triggers poll
    bool close_expected,                                        // is logic connection closed by remote is an expected result
    check_cmailbox_option opt)
{
    int n_got_data = 0;
    int idx = -1;
    int streamid = -1;
    int version = -1;
    libcomm_time_record* time_record = opt.time_record;
    COMM_TIMER_COPY_INIT(time_record->t_begin);
    struct c_mailbox* cmailbox = NULL;
    struct FCMSG_T fcmsgs = {0x0};
    for (int i = 0; i < nproducer; i++) {
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
                MAILBOX_ELOG(cmailbox, WARNING, "(r|wait poll)\tStream has already closed, detail:%s.",
                    mc_strerror(cmailbox->close_reason));
            }
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
            errno = cmailbox->close_reason;
            // set the error flag for Consumer thread
            producer[i] = WAIT_POLL_FLAG_ERROR;
            return -2;
        }

        gs_update_producer(cmailbox, &producer[i], &n_got_data, opt.poll_error_flag);

        /* update the statistic information of the mailbox */
        update_cmailbox_statistic(cmailbox, opt, n_got_data);

        // send local thread id and quota to remote
        bool send_msg = gs_r_form_start_ctrl_msg(cmailbox, &fcmsgs);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);

        // send local thread id and quota to remote without cmailbox lock
        if (send_msg && (gs_send_ctrl_msg(&g_instance.comm_cxt.g_r_node_sock[idx], &fcmsgs, ROLE_CONSUMER) <= 0)) {
            errno = ECOMMTCPTCPDISCONNECT;
            return -1;
        }
    }
    // if data is found, return the number of mailboxes which have data
    // or, if it is waked up here but no data found, there must be interruption, we should return and
    // CHECK_FOR_INTERRUPT
    //
    if (n_got_data > 0) {
        COMM_TIMER_LOG("(r|wait poll)\tGet data for node[%d,%d]:%s.", idx, streamid,
            REMOTE_NAME(g_instance.comm_cxt.g_r_node_sock, idx));
        return n_got_data;
    } else if (opt.poll_error_flag == 1) {
        COMM_DEBUG_LOG("(r|wait poll)\tWaked up but no data.");
        errno = ECOMMTCPWAITPOLLERROR;
        return -1;
    }
    return 0;
}

void gs_check_all_producers_mailbox(const gsocket* gs_sock_array, int nproducer, int* producer)
{
    struct c_mailbox* cmailbox = NULL;
    for (int i = 0; i < nproducer; i++) {
        if (producer[i] == WAIT_POLL_FLAG_WAIT) {
            producer[i] = WAIT_POLL_FLAG_IDLE;
            cmailbox = &C_MAILBOX(gs_sock_array[i].idx, gs_sock_array[i].sid);
            LIBCOMM_PTHREAD_MUTEX_LOCK(&cmailbox->sinfo_lock);
            if (gs_check_mailbox(cmailbox->local_version, gs_sock_array[i].ver) == true) {
                cmailbox->semaphore = NULL;
            }
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);
        }
    }
}

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

// cancel request for receiver (called by die() or StatementCancelHandler() in postgresMain )
void gs_r_cancel()
{
    // use g_cancel_requested save DEBUG_QUERY_ID as a flag
    g_instance.comm_cxt.reqcheck_cxt.g_cancel_requested =
        (t_thrd.proc_cxt.MyProcPid != 0) ? t_thrd.proc_cxt.MyProcPid : 1;
}

// receiver close logic stream, call by Consumer thread
// when no data need or all data are received, or error happed
int gs_r_close_stream(gsocket* gsock)
{
    int node_idx = gsock->idx;
    int stream_idx = gsock->sid;
    int version = gsock->ver;
    int type = gsock->type;
    struct FCMSG_T fcmsgs = {0x0};

    if ((node_idx < 0) || (node_idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num) || (stream_idx <= 0) ||
        (stream_idx >= g_instance.comm_cxt.counters_cxt.g_max_stream_num) || (type == GSOCK_PRODUCER)) {
        COMM_DEBUG_LOG("(r|cls stream)\tInvalid argument: node idx[%d], stream id[%d], type[%d].",
            node_idx,
            stream_idx,
            type);
        errno = ECOMMTCPARGSINVAL;
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

    gs_r_close_logic_connection(cmailbox, ECOMMTCPAPPCLOSE, &fcmsgs);

    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&cmailbox->sinfo_lock);

    // Send close ctrl msg to remote without cmailbox lock
    (void)gs_send_ctrl_msg(&g_instance.comm_cxt.g_r_node_sock[node_idx], &fcmsgs, ROLE_CONSUMER);

    return 0;
}  // gs_r_close_stream

// sender close logic stream, call by Producer thread when it failed to send data
int gs_s_close_stream(gsocket* gsock)
{
    int node_idx = gsock->idx;
    int stream_idx = gsock->sid;
    int version = gsock->ver;
    int type = gsock->type;
    struct FCMSG_T fcmsgs = {0x0};

    if ((node_idx < 0) || (node_idx >= g_instance.comm_cxt.counters_cxt.g_cur_node_num) || (stream_idx <= 0) ||
        (stream_idx >= g_instance.comm_cxt.counters_cxt.g_max_stream_num) || (type == GSOCK_CONSUMER)) {
        COMM_DEBUG_LOG("(s|cls stream)\tInvalid argument: node idx[%d], stream id[%d], type[%d].",
            node_idx,
            stream_idx,
            type);
        errno = ECOMMTCPARGSINVAL;
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

    gs_s_close_logic_connection(pmailbox, ECOMMTCPAPPCLOSE, &fcmsgs);
    LIBCOMM_PTHREAD_MUTEX_UNLOCK(&pmailbox->sinfo_lock);
    // Send close ctrl msg to remote without pmailbox lock
    (void)gs_send_ctrl_msg(&g_instance.comm_cxt.g_s_node_sock[node_idx], &fcmsgs, ROLE_PRODUCER);

    return 0;
}

// close logic socket, it will return when the sock type is invalid.
void gs_close_gsocket(gsocket* gsock)
{
    bool TempImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = false;

    if (gsock->type == GSOCK_INVALID) {
        LIBCOMM_INTERFACE_END(false, TempImmediateInterruptOK);
        return;
    }

    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    if (gsock->type == GSOCK_DAUL_CHANNEL || gsock->type == GSOCK_PRODUCER) {
        (void)gs_s_close_stream(gsock);
    }

    if (gsock->type == GSOCK_DAUL_CHANNEL || gsock->type == GSOCK_CONSUMER) {
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

/* get the error information of communication layer */
const char* gs_comm_strerror()
{
    bool savedVal = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = false;
    const char *errMsg = mc_strerror(errno);
    t_thrd.int_cxt.ImmediateInterruptOK = savedVal;
    return errMsg;
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

    if (g_instance.comm_cxt.g_delay_survey_switch == false) {
        g_instance.comm_cxt.g_delay_survey_start_time = mc_timers_ms();
        LIBCOMM_ELOG(LOG, "delay survey switch is open");
    }
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
    long recv_bytes[G_CUR_NODE_NUM];
    int recv_count[G_CUR_NODE_NUM];
    int recv_count_speed = 0;
    long recv_speed = 0;

    long send_speed = 0;
    long send_bytes[G_CUR_NODE_NUM];
    int send_count[G_CUR_NODE_NUM];
    int send_count_speed = 0;

    errno_t rc = memset_s(recv_bytes, sizeof(recv_bytes), 0, sizeof(recv_bytes));
    securec_check(rc, "\0", "\0");
    rc = memset_s(recv_count, sizeof(recv_count), 0, sizeof(recv_count));
    securec_check(rc, "\0", "\0");
    rc = memset_s(send_bytes, sizeof(send_bytes), 0, sizeof(send_bytes));
    securec_check(rc, "\0", "\0");
    rc = memset_s(send_count, sizeof(send_count), 0, sizeof(send_count));
    securec_check(rc, "\0", "\0");

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
        sleep(0.1);
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
                    gs_r_close_logic_connection(cmailbox, ECOMMTCPAPPCLOSE, &fcmsgs);
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
                    gs_s_close_logic_connection(pmailbox, ECOMMTCPAPPCLOSE, &fcmsgs);
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
            "(cls all stream)\tClose all stream by debug id[%lu], close %d cmailbox and %d pmailbox.",
            query_id,
            cmailbox_count,
            pmailbox_count);
    }
    return 0;
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

/* SIGHUP: re-read config file */
static void PoolCleanerSighupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.poolcleaner_cxt.got_SIGHUP = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

void SetupPoolerCleanSignalHook()
{
    (void)gspqsignal(SIGHUP, PoolCleanerSighupHandler);
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

#ifdef ENABLE_MULTIPLE_NODES
void init_clean_pooler_idle_connections()
{
    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = COMM_POOLER_CLEAN;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    t_thrd.proc_cxt.MyProgName = "commPoolerCleaner";

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* Identify myself via ps */
    init_ps_display("pooler cleaner process", "", "", "");

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

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

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

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /* report this backend in the PgBackendStatus array */
    pgstat_report_appname("PoolCleaner");

    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Pool cleaner",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));
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

        if (t_thrd.poolcleaner_cxt.got_SIGHUP) {
            t_thrd.poolcleaner_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        sleep(1);
        current_time = mc_timers_ms();
        poolerMaxIdleTime = (u_sess->attr.attr_network.PoolerMaxIdleTime) * MS_PER_S;

        /* If pooler_maximum_idle_time is zero, do not call clean connection procedure */
        if (poolerMaxIdleTime == 0) {
            continue;
        }

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

