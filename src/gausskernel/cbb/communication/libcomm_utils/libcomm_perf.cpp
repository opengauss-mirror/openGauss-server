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
 * libcomm_perf.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/libcomm_utils/libcomm_perf.cpp
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

extern GlobalNodeDefinition* global_node_definition;
extern knl_instance_context g_instance;

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

void gs_set_working_version_num(int num)
{
    if (!IsConnFromCoord()) {
        return;
    }
    if (u_sess && u_sess->proc_cxt.MyProcPort) {
        u_sess->proc_cxt.MyProcPort->SessionVersionNum = (uint32)num;
    }

    if (t_thrd.proc) {
        t_thrd.proc->workingVersionNum = (uint32)num;
    }
} // gs_set_working_version_num

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

int gs_get_cur_node()
{
    return (g_instance.comm_cxt.counters_cxt.g_cur_node_num != 0)
               ? g_instance.comm_cxt.counters_cxt.g_cur_node_num
               : (g_instance.attr.attr_network.MaxCoords + u_sess->attr.attr_network.comm_max_datanode);
}

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
    if ((gs_sock == NULL) || (gs_sock->type == GSOCK_INVALID)) {
        return false;
    }

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

#ifdef LIBCOMM_SPEED_TEST_ENABLE

void* libcommProducerThread(void* arg)
{
    int producer_sn = *(int*)arg;
    const unsigned int plan_id = LIBCOMM_PERFORMANCE_PLAN_ID;
    const unsigned int plan_node_id = LIBCOMM_PERFORMANCE_PN_ID + producer_sn;
    libcomm_addrinfo** consumerAddr = NULL;
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
    consumerAddr = (libcomm_addrinfo**)calloc(consumerNum, sizeof(libcomm_addrinfo*));
    if (consumerAddr == NULL) {
        goto clean_return;
    }

    for (i = 0; i < consumerNum; i++) {
        nodesDef = &(global_node_definition->nodesDefinition[i]);

        consumerAddr[i] = (libcomm_addrinfo*)calloc(1, sizeof(libcomm_addrinfo));
        if (consumerAddr[i] == NULL) {
            goto clean_return;
        }

        consumerAddr[i]->host = nodesDef->nodehost.data;
        consumerAddr[i]->ctrl_port = nodesDef->nodectlport;
        consumerAddr[i]->listen_port = nodesDef->nodesctpport;
        consumerAddr[i]->nodeIdx = nodesDef->nodeid;

        rc = strncpy_s(
            consumerAddr[i]->nodename, NAMEDATALEN, nodesDef->nodename.data, strlen(nodesDef->nodename.data) + 1);
        securec_check(rc, "\0", "\0");

        consumerAddr[i]->tcpKey.queryId = plan_id;
        consumerAddr[i]->tcpKey.planNodeId = plan_node_id;
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

void libcommRecv(gsocket* gsockAddr, int producerNum, int* datamarks, char* msg_buf, int msg_len)
{
    int once_recv = g_instance.comm_cxt.tests_cxt.libcomm_test_recv_once;
    int sleep_time = g_instance.comm_cxt.tests_cxt.libcomm_test_recv_sleep;
    int current_recv = 0;
    int i;

    for (;;) {
        error = gs_wait_poll(gsockAddr, producerNum, datamarks, -1, false);
        if (error < 0 || g_instance.comm_cxt.tests_cxt.libcomm_stop_flag == true) {
            return;
        }

        for (i = 0; i < producerNum; i++) {
            if (datamarks[i] > 0) {
                error = gs_recv(&gsockAddr[i], msg_buf, msg_len);

                if (error < 0 && errno == ECOMMTCPNODATA) {
                    continue;
                }

                if (error < 0 || g_instance.comm_cxt.tests_cxt.libcomm_stop_flag == true) {
                    return;
                }
            }
        }

        current_recv += msg_len;
        if (current_recv >= once_recv && sleep_time > 0) {
            usleep(sleep_time * 1000);
            current_recv = 0;
        }
    }

    return;
}

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

    libcommRecv(gsockAddr, producerNum, datamarks, msg_buf, msg_len);

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
    MemoryContext oldcontext =
        MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));
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
