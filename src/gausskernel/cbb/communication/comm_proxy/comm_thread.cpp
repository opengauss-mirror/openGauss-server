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
 * comm_thread.cpp
 *        TODO add contents
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_thread.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <signal.h>
#include "comm_core.h"
#include "comm_proxy.h"
#include "communication/commproxy_interface.h"
#include <sys/prctl.h>
#include "utils/builtins.h"
#ifdef __USE_NUMA
#include <numa.h>
#endif

/*
 * --------------------------------------------------------------------------------------
 * Function & Variables declerations
 * --------------------------------------------------------------------------------------
 */
static void CommProxyBaseInit();
static void* CommProxyMainFunc(void* ptr);
static void* CommProxyStatMainFunc(void* ptr);

/*
 * --------------------------------------------------------------------------------------
 * Export Functions
 * --------------------------------------------------------------------------------------
 */
void CommStartProxyer(ThreadId *thread_id, ThreadPoolCommunicator *comm)
{
#if defined(GAUSSDB_KERNEL__NO)
    *thread_id = startCommProxyer(comm);
#else
    int err = 0;
    sigset_t new_sigmask;
    sigset_t old_sigmask;
    (void)sigfillset(&new_sigmask);

    (void)pthread_sigmask(SIG_BLOCK, &new_sigmask, &old_sigmask);

    /* Start CommProx thread */
    if ((err = pthread_create(thread_id, NULL, CommProxyMainFunc, (void*)comm)) != 0) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("Failed to create a new thread: error %d.", err),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
        exit(-1);
    }

    (void)pthread_sigmask(SIG_SETMASK, &old_sigmask, NULL);
#endif

    return;
}

void CommStartProxyStatThread(CommController *controller)
{
    /* Start CommProx thread */
    ThreadId threadId;
    pthread_create(&threadId, NULL, CommProxyStatMainFunc, (void *)controller);
    return;
}

/*
 * --------------------------------------------------------------------------------------
 * Internal Functions
 * --------------------------------------------------------------------------------------
 */
void SetupCommProxySignalHook()
{
#if defined(GAUSSDB_KERNEL__NO)

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
#else
    sigset_t sigs;
	(void)sigemptyset(&sigs);
	(void)sigaddset(&sigs, SIGHUP);
	(void)sigaddset(&sigs, SIGINT);
	(void)sigaddset(&sigs, SIGTERM);
	(void)sigaddset(&sigs, SIGUSR1);
	(void)sigaddset(&sigs, SIGUSR2);
	(void)pthread_sigmask(SIG_BLOCK, &sigs, NULL);
#endif
}

#if defined(GAUSSDB_KERNEL__NO)

static ThreadId commProxyerForkexec(void* comm)
{
    return initialize_util_thread(COMM_PROXYER, comm);
}

static ThreadId startCommProxyer(ThreadPoolCommunicator *comm)
{
    ThreadId comm_proxy_pid = commProxyerForkexec((void*)comm);
    if (comm_proxy_pid == -1) {
        ereport(LOG, (errmsg("could not fork comm proxyer process: %m")));
        return 0;
    }

    return comm_proxy_pid;
}
#endif

static void CommProxyBaseInit()
{
    MemoryContextInit();
    knl_thread_init(COMM_PROXYER);
    t_thrd.fake_session = create_session_context(t_thrd.top_mem_cxt, 0);
    t_thrd.fake_session->status = KNL_SESS_FAKE;
    u_sess = t_thrd.fake_session;

    t_thrd.proc_cxt.MyProcPid = gs_thread_self();	/* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProgName = "CommProxy";

    t_thrd.proc_cxt.MyStartTime = time(NULL);

    pg_timezone_initialize();

    SetupCommProxySignalHook();

    return;
}

/*
 *****************************************************************************************
 * Thread functions
 *****************************************************************************************
 */
static void* CommProxyMainFunc(void* ptr)
{
    ThreadPoolCommunicator* comm = (ThreadPoolCommunicator*)ptr;
    gaussdb_numa_memory_bind(comm->m_group_id);

    int rc = memset_s(t_thrd.proxy_cxt.identifier, IDENTIFIER_LENGTH, '\0', IDENTIFIER_LENGTH);
    securec_check(rc, "\0", "\0");

    snprintf_s(t_thrd.proxy_cxt.identifier, IDENTIFIER_LENGTH, IDENTIFIER_LENGTH - 1, "commproxy[%d-%d]",
        comm->m_group_id, comm->m_comm_id);
    prctl(PR_SET_NAME, "commproxy");

    CommProxyBaseInit();

    ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("%s start! tid:%lu",
        t_thrd.proxy_cxt.identifier, comm->m_comm_proxy_thread_id)));

    /* Wait controller is properly set up */
    WaitCondPositive((g_comm_controller != NULL), 10);

    /* Set CPU affinity */
    /* comm proxy should select other cpu core except ltran bind */
    SetCPUAffinity(comm->m_bind_core);
    
    comm->CeateCommEpoll(comm->m_group_id);
    ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("%s create epoll fd: %d",
        t_thrd.proxy_cxt.identifier, comm->m_epoll_fd)));

    comm->NotifyStatus(CommProxyerReady);
    comm->m_thread_id = gs_thread_self();
#if COMM_SEND_QUEUE_WITH_DPDP_MPSC
    char sock_ring_name[50] = "communicator_ready_ring_0_0";
    int len = snprintf_s(sock_ring_name, sizeof(sock_ring_name), sizeof(sock_ring_name) - 1,
        "communicator_ready_ring_%d_%d", comm->m_group_id, comm->m_comm_id);
    securec_check_ss(len, "\0", "\0");
    sock_ring_name[len] = '\0';
    comm->m_ready_sock_ring = MpScRingQueueCreate(sock_ring_name, 10240, comm->m_group_id);
#endif

    /* ServerLoop of RX */
    for (;;) {
        if (g_comm_controller->m_proxy_recv_loop_cnt == 0 ||
            g_comm_controller->m_proxy_send_loop_cnt == 0) {
            break;
            }
        ProcessProxyWork(comm);
    }

    return NULL;
}

static void GetCommProxyThreadStatusStaticInfo(ThreadPoolCommunicator* comm)
{
    comm->m_thread_status.s_proxy_thread_id = comm->m_thread_id;
    int cpu_id = comm->m_bind_core;
    int numa_id = comm->m_group_id;
    int rc = sprintf_s(comm->m_thread_status.s_thread_affinity, 32, "%d-%d", numa_id, cpu_id);
    securec_check_ss(rc, "\0", "\0");
    ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("CommProxyThead: pid:%lu, numa affinity is %s",
        comm->m_thread_id, comm->m_thread_status.s_thread_affinity)));
    return;
}

static void* CommProxyStatMainFunc(void* ptr)
{
    /* Wait controller is properly set up */
    CommController *controller = (CommController *)ptr;
    int rc = memset_s(t_thrd.proxy_cxt.identifier, IDENTIFIER_LENGTH, '\0', IDENTIFIER_LENGTH);
    securec_check(rc, "\0", "\0");
    snprintf_s(t_thrd.proxy_cxt.identifier, IDENTIFIER_LENGTH, IDENTIFIER_LENGTH - 1, "commproxy stat");
    prctl(PR_SET_NAME, "commproxy");

    CommProxyBaseInit();

    int nums = controller->m_communicator_nums[0];
    const int one_second = 1000*1000;
    for (;;) {
        if (nums == 0) {
            break;
        }
        pg_usleep(one_second);
        for (int i = 0; i < nums; i++) {
            ThreadPoolCommunicator *comm = controller->m_communicators[0][i];

            GetCommProxyThreadStatusStaticInfo(comm);
            uint64 rxpps = comm->m_thread_status.s_recv_packet_num - comm->m_thread_status.s_previous_recv_packet_num;
            uint64 txpps = comm->m_thread_status.s_send_packet_num - comm->m_thread_status.s_previous_send_packet_num;
            ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("CommProxyThead[%d]: pid:%lu, rxpps: %ld txpps:%ld\n",
                i, comm->m_thread_id, rxpps, txpps)));

            /* update previous value */
            comm->m_thread_status.s_previous_recv_packet_num = comm_atomic_read_u64(&comm->m_thread_status.s_recv_packet_num);
            comm->m_thread_status.s_previous_send_packet_num = comm_atomic_read_u64(&comm->m_thread_status.s_send_packet_num);

            comm->m_thread_status.s_recv_pps = rxpps;
            comm->m_thread_status.s_send_pps = txpps;
        }
    }

    return NULL;
}