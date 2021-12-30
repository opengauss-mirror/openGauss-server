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
 * libcomm_util.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/libcomm_utils/libcomm_util.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/time.h>
#include "libcomm_util.h"
#include "pgtime.h"
#include "libpq/libpq-be.h"

#define INVALID_FD (-1)

extern long libpq_used_memory;
extern long libcomm_used_memory;
extern long comm_peak_used_memory;

void gs_set_usable_memory(long usable_memory)
{
    g_instance.comm_cxt.commutil_cxt.g_total_usable_memory = usable_memory * 1024;
}

void gs_set_memory_pool_size(long mem_pool)
{
    g_instance.comm_cxt.commutil_cxt.g_memory_pool_size = mem_pool * 1024;
}

long gs_get_comm_used_memory(void)
{
    return libcomm_used_memory + libpq_used_memory;
}

long gs_get_comm_peak_memory(void)
{
    return comm_peak_used_memory;
}

Size gs_get_comm_context_memory(void)
{
    if (g_instance.comm_cxt.comm_global_mem_cxt != NULL) {
        return (((AllocSet)g_instance.comm_cxt.comm_global_mem_cxt)->totalSpace);
    } else {
        return 0;
    }
}

int cmp_addr(sockaddr_storage_t* addr1, sockaddr_storage_t* addr2)
{
    if (addr1->sa.sa_family != addr2->sa.sa_family) {
        return -1;
    }
    switch (addr1->sa.sa_family) {
        case AF_INET6:
            if (addr1->v6.sin6_port != addr2->v6.sin6_port) {
                return -1;
            }
            return memcmp(&addr1->v6.sin6_addr, &addr2->v6.sin6_addr, sizeof(addr1->v6.sin6_addr));
        case AF_INET:
            if (addr1->v4.sin_port != addr2->v4.sin_port) {
                return -1;
            }
            return memcmp(&addr1->v4.sin_addr, &addr2->v4.sin_addr, sizeof(addr1->v4.sin_addr));
        default:
            return -1;
    }
}

void set_debug_mode(bool mod)
{
    g_instance.comm_cxt.commutil_cxt.g_debug_mode = mod;
}
void set_timer_mode(bool mod)
{
    g_instance.comm_cxt.commutil_cxt.g_timer_mode = mod;
}
void set_stat_mode(bool mod)
{
    g_instance.comm_cxt.commutil_cxt.g_stat_mode = mod;
}
void set_no_delay(bool mod)
{
    g_instance.comm_cxt.commutil_cxt.g_no_delay = mod;
}

// set FL or FD attribute of socket by parameter FL_OR_FD,
//  FL_OR_FD is 0 for setting FL (O_NONBLOCK)
//  FL_OR_FD is 1 for setting FD (FD_CLOEXEC)
//
int set_socketopt(int sock, int FL_OR_FD, long arg)
{
    int flg, rc;
    int get_cmd = 0;
    int set_cmd = 0;
    switch (FL_OR_FD) {
        case 0:  // FL
            get_cmd = F_GETFL;
            set_cmd = F_SETFL;
            break;
        case 1:  // FD
            get_cmd = F_GETFD;
            set_cmd = F_SETFD;
            break;
        default:
            get_cmd = -1;
            set_cmd = -1;
            break;
    }
    if (get_cmd == -1 || set_cmd == -1) {
        LIBCOMM_ELOG(WARNING, "(set socket opt)\tUnkown: command type[%d].", FL_OR_FD);
        return -1;
    }
    if ((flg = fcntl(sock, get_cmd, 0)) < 0) { // F_GETFL, F_GETFD
        LIBCOMM_ELOG(WARNING,
            "(set socket opt)\tFail to get fcntl[%d:%ld] of socket[%d]:fcntl failed,return[%d].",
            get_cmd,
            arg,
            sock,
            flg);
        return flg;
    }

    flg |= arg;                                // O_NONBLOCK, FD_CLOEXEC
    if ((rc = fcntl(sock, set_cmd, flg)) < 0) { // F_SETFL, F_SETFL
        LIBCOMM_ELOG(WARNING,
            "(set socket opt)\tFail to set fcntl[%d:%ld] of socket[%d]:fcntl failed,return[%d].",
            set_cmd,
            arg,
            sock,
            flg);
        return rc;
    }

    return rc;
}

uint32 comm_get_cpylen(const char* src, uint32 max_len)
{
    uint32 cpylen = 0;

    if (src == NULL || strlen(src) == 0) {
        return cpylen;
    }
    /*
     * src is from the network
     * maybe do not have '\0'
     * can not use strlen
     */
    while (cpylen < max_len - 1) {
        if (src[cpylen] != '\0') {
            cpylen++;
        } else {
            break;
        }
    }

    return cpylen;
}

static const char* MAILBOX_STAT[MAIL_MAX_TYPE] = {"UNKNOWN", "READY", "RUN", "HOLD", "CLOSED", "TO_CLOSED"};

const char* stream_stat_string(int type)
{
    if (type < 0 || type >= MAIL_MAX_TYPE) {
        type = 0;
    }

    return MAILBOX_STAT[type];
}

static const char* CTRL_MSG_STAT[CTRL_MAX_TYPE] = {"UNKNOWN",
    "REGIST",
    "REGIST_CN",
    "REJECT",
    "DUAL CONNECT",
    "CANCEL",
    "CONNECT",
    "ACCEPT",
    "QUOTA",
    "CLOSED",
    "TID",
    "ASSERT",
    "PEER_CHANGED",
    "STOP_QUERY"};

const char* ctrl_msg_string(int type)
{
    if (type < 0 || type >= CTRL_MAX_TYPE) {
        type = 0;
    }

    return CTRL_MSG_STAT[type];
}

static const char *MSG_OPER_STAT[READ_DATA_FROM_LOGIC + 1] = {
    "send_some",
    "secure_read",
    "secure_write",
    "read_data",
    "read_data_from_logic"
};

const char* msg_oper_string(CommMsgOper msg_oper)
{
    Assert(msg_oper >= SEND_SOME && msg_oper <= READ_DATA_FROM_LOGIC);
    return MSG_OPER_STAT[msg_oper];
}

void printfcmsg(const char* caller, struct FCMSG_T* pfcmsg)
{
    mc_elog(LOG,
        "(%s)\tctrl msg: type[%s],idx[%d] stream[%d] ver[%d], quota[%ld], query[%lu], node[%s].",
        caller,
        ctrl_msg_string(pfcmsg->type),
        pfcmsg->node_idx,
        pfcmsg->streamid,
        pfcmsg->version,
        pfcmsg->streamcap,
        pfcmsg->query_id,
        pfcmsg->nodename);
}

void printf_cmailbox_statistic(c_mailbox* cmailbox, char* nodename)
{
    if (NULL == cmailbox->statistic || MAIL_UNKNOWN == cmailbox->state) {
        return;
    }

    uint64 time_now = mc_timers_ms();
    uint64 run_time = time_now - cmailbox->statistic->start_time;
    uint64 recv_time = cmailbox->statistic->total_poll_time;
    uint64 speed = (recv_time > 0) ? (cmailbox->statistic->recv_bytes * 1000 / (uint64)recv_time)
                                 : (cmailbox->statistic->recv_bytes * 1000);

    uint64 first_poll_time = cmailbox->statistic->first_poll_time - cmailbox->statistic->start_time;
    uint64 last_poll_time = cmailbox->statistic->last_poll_time - cmailbox->statistic->start_time;

    uint64 first_recv_time = cmailbox->statistic->first_recv_time - cmailbox->statistic->start_time;
    uint64 last_recv_time = cmailbox->statistic->last_recv_time - cmailbox->statistic->start_time;

    int idx = cmailbox->idx;
    int sid = cmailbox->streamid;
    int ver = cmailbox->local_version;

    mc_elog(LOG,
        "[STAT](r reset)\t1.node idx|%d| 2.stream idx|%d| 3.version|%d| 4.query id|%lu| 5.plan node id|%u| 6.p smp "
        "id|%u| 7.c smp id|%u| "
        "8.remote|%s| 9.recv bytes|%lu| 10.duration|%lu|ms 11.speed|%lu|/s "
        "12.first poll|%lu|ms 13.last poll|%lu|ms 14.call times|%lu| 15.poll duration|%lu|ms 16.wait data|%lu|ms "
        "17.first recv|%lu|ms 18.last recv|%lu|ms 19.signal used|%lu|ms 20.gs recv|%lu|ms 21.consumer cost|%lu|ms "
        "22.wait lock|%lu|ms 23.recv loop time|%lu|ms 25.recv loop count|%u|.",
        idx,
        sid,
        ver,
        cmailbox->query_id,
        cmailbox->stream_key.planNodeId,
        cmailbox->stream_key.producerSmpId,
        cmailbox->stream_key.consumerSmpId,
        nodename,
        cmailbox->statistic->recv_bytes,
        run_time,
        speed,
        first_poll_time,
        last_poll_time,
        cmailbox->statistic->call_poll_count,
        cmailbox->statistic->total_poll_time,
        cmailbox->statistic->wait_data_time,
        first_recv_time,
        last_recv_time,
        cmailbox->statistic->total_signal_time,
        cmailbox->statistic->gs_recv_time,
        cmailbox->statistic->consumer_elapsed_time - cmailbox->statistic->gs_recv_time,
        cmailbox->statistic->wait_lock_time,
        cmailbox->statistic->recv_loop_time,
        cmailbox->statistic->recv_loop_count);
}

void printf_pmailbox_statistic(p_mailbox* pmailbox, char* nodename)
{
    if (NULL == pmailbox->statistic || MAIL_UNKNOWN == pmailbox->state) {
        return;
    }

    uint64 time_now = (uint32)mc_timers_ms();
    uint64 run_time = time_now - pmailbox->statistic->start_time;
    uint32 send_time = pmailbox->statistic->total_send_time;
    uint64 speed = (send_time > 0) ? (pmailbox->statistic->send_bytes * 1000 / (uint64)send_time)
                                 : (pmailbox->statistic->send_bytes * 1000);

    uint64 first_send_time = pmailbox->statistic->first_send_time - pmailbox->statistic->start_time;
    uint64 last_send_time = pmailbox->statistic->last_send_time - pmailbox->statistic->start_time;
    int idx = pmailbox->idx;
    int sid = pmailbox->streamid;
    int ver = pmailbox->local_version;

    mc_elog(LOG,
        "[STAT](s reset)\t1.node idx|%d| 2.stream idx|%d| 3.version|%d| 4.query id|%lu| 5.plan node id|%u| 6.p smp "
        "id|%u| 7.c smp id|%u| 8.remote|%s| "
        "9.send bytes|%lu| 10.duration|%lu|ms 11.speed|%lu|/s "
        "12.connect used|%lu|us 13.call times|%lu| 14.send duration|%lu|ms 15.first send|%lu|ms 16.last send|%lu|ms "
        "17.wait quota|%lu|ms 18.os send|%lu|ms 19.producer cost|%lu|ms.",
        idx,
        sid,
        ver,
        pmailbox->query_id,
        pmailbox->stream_key.planNodeId,
        pmailbox->stream_key.producerSmpId,
        pmailbox->stream_key.consumerSmpId,
        nodename,
        pmailbox->statistic->send_bytes,
        run_time,
        speed,
        pmailbox->statistic->connect_time,
        pmailbox->statistic->call_send_count,
        pmailbox->statistic->total_send_time,
        first_send_time,
        last_send_time,
        pmailbox->statistic->wait_quota_overhead,
        pmailbox->statistic->os_send_overhead,
        pmailbox->statistic->producer_elapsed_time);
}

void print_socket_info(int sock, struct tcp_info* info, bool sender)
{
    mc_elog(LOG,
        "[%s]\tsocket[%d],tcp_state[%u],ca_state[%u],total_retrans[%u],"
        "rto[%u], ato[%u], snd_mss[%u],rcv_mss[%u],rtt[%u], pmtu[%u], "
        "snd_cwnd[%u],rcv_rtt[%u],rcv_space[%u],last_data_sent[%u],"
        "last_data_recv[%u],last_ack_recv[%u].",
        sender ? "TCP SENDER STAT" : "TCP RECEIVER STAT",
        sock,
        (unsigned int)info->tcpi_state,
        (unsigned int)info->tcpi_ca_state,
        info->tcpi_total_retrans,
        info->tcpi_rto,
        info->tcpi_ato,
        info->tcpi_snd_mss,
        info->tcpi_rcv_mss,
        info->tcpi_rtt,
        info->tcpi_pmtu,
        info->tcpi_snd_cwnd,
        info->tcpi_rcv_rtt,
        info->tcpi_rcv_space,
        info->tcpi_last_data_sent,
        info->tcpi_last_data_recv,
        info->tcpi_last_ack_recv);
}

// error logging function
//
extern uint32 GetTopTransactionIdIfAny(void);

void mc_elog(int elevel, const char* format, ...)
{
#define MSLEN 4
#define MSOFFSET 19
#define TIMELEN 128
#define MSGLEN 8192

    if (g_instance.comm_cxt.commutil_cxt.ut_libcomm_test || 
        elevel < u_sess->attr.attr_common.log_min_messages) {
        return;
    }

    char msg[MSGLEN] = {'0'};
    va_list args;
    va_start(args, format);
    int ss_rc = vsprintf_s(msg, MSGLEN, format, args);
    securec_check_ss(ss_rc, "\0", "\0");
    va_end(args);

    // make time string start, like setup_formatted_log_time
    struct timeval tv = {0};
    struct pg_tm* localtime = NULL;
    pg_time_t stamp_time;
    char msbuf[MSLEN * 2];
    char timebuf[TIMELEN];

    (void)gettimeofday(&tv, NULL);
    stamp_time = (pg_time_t)tv.tv_sec;

    /* Session initialization status, which may be empty. */
    log_timezone = log_timezone ? log_timezone : g_instance.comm_cxt.libcomm_log_timezone;
    if (log_timezone == NULL) {
        return;
    }
    localtime = pg_localtime(&stamp_time, log_timezone);
    if (localtime != NULL) {
        (void)pg_strftime(timebuf,
            TIMELEN,
            // leave room for milliseconds...
            "%Y-%m-%d %H:%M:%S     %Z",
            localtime);
    }

    // 'paste' milliseconds into place...
    ss_rc = snprintf_s(msbuf, sizeof(msbuf), MSLEN, ".%03d", (int)(tv.tv_usec / 1000));
    securec_check_ss(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(timebuf + MSOFFSET, TIMELEN - MSOFFSET - 1, msbuf, MSLEN);
    securec_check(ss_rc, "\0", "\0");

    if (u_sess) {
        fprintf(stdout,
            "%s %s "  /* timestamp with milliseconds */
            "%ld.%d " /* session ID */
            "%s "     /* database name */
            "%lu "    /* process ID */
            "%s "     /* application name */
            "%u "     /* transaction ID (0 if none) */
            "%s "     /* DataNode name */
            "%s "     /* SQL state */
            "%lu "    /* u_sess->debug_query_id */
            "%s: "    /* log level */
            "%s\n",   /* error message */
            timebuf,
            timebuf + MSOFFSET + MSLEN + 1,
            (long)(t_thrd.proc_cxt.MyStartTime),
            t_thrd.myLogicTid,
            (u_sess->proc_cxt.MyProcPort && u_sess->proc_cxt.MyProcPort->database_name &&
                u_sess->proc_cxt.MyProcPort->database_name[0] != '\0')
                ? u_sess->proc_cxt.MyProcPort->database_name
                : "[unknown]",
            t_thrd.proc_cxt.MyProcPid,
            (u_sess->proc_cxt.MyProcPort && u_sess->attr.attr_common.application_name &&
                u_sess->attr.attr_common.application_name[0] != '\0')
                ? u_sess->attr.attr_common.application_name
                : "[unknown]",
            GetTopTransactionIdIfAny(),
            g_instance.attr.attr_common.PGXCNodeName ? g_instance.attr.attr_common.PGXCNodeName
                                                     : g_instance.comm_cxt.localinfo_cxt.g_self_nodename,
            "00000",
            u_sess->debug_query_id,
            (elevel == LOG) ? "[LIBCOMM] LOG" : "[LIBCOMM] WARNING",
            msg);
    }

    (void)fflush(stdout);
}

static inline int mc_clock_gettime(struct timespec* ts)
{
    return clock_gettime(CLOCK_MONOTONIC, ts);
}

#define MC_NS_IN_SEC 1000000000ULL

uint64_t mc_timers_us(void)
{
    uint64_t ns_monotonic;
    struct timespec ts = {0};

    (void)mc_clock_gettime(&ts);
    ns_monotonic = (uint64)((ts.tv_sec * MC_NS_IN_SEC) + (uint64)ts.tv_nsec) / 1000;
    return ns_monotonic;
}

uint64 mc_timers_ms(void)
{
    return mc_timers_us() / 1000;
}

void comm_ipc_log_get_time(char *now_date, int time_len)
{
    const int MS_LEN = 4;
    const int BUF_LEN = MS_LEN * 2;
    const int MS_OFFSET = 19;
    struct timeval tv = {0};  /* make time string start, like setup_formatted_log_time */
    struct pg_tm* localtime = NULL;
    pg_time_t stamp_time;
    char msbuf[BUF_LEN];
    errno_t ss_rc = 0;

    (void)gettimeofday(&tv, NULL);
    stamp_time = (pg_time_t)tv.tv_sec;
    localtime = pg_localtime(&stamp_time, log_timezone);
    /* leave room for milliseconds. */
    if (localtime != NULL) {
        (void)pg_strftime(now_date, time_len,
            "%Y-%m-%d %H:%M:%S",
            localtime);
    }

    /* 'paste' milliseconds into place. */ 
    ss_rc = snprintf_s(msbuf, sizeof(msbuf), MS_LEN, ".%03d", (int)(tv.tv_usec / 1000));
    securec_check_ss(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(now_date + MS_OFFSET, time_len - MS_OFFSET - 1, msbuf, MS_LEN);
    securec_check(ss_rc, "\0", "\0");
}

WakeupPipe::WakeupPipe()
{
    InitPipe(m_normal_wakeup_pipes);
}

void WakeupPipe::DoWakeup()
{
    int errno_tmp = errno;
    if ((epoll_ctl(m_epfd, EPOLL_CTL_ADD, m_normal_wakeup_pipes[WAKEUP_PIPE_START], &m_ev)) && (errno != EEXIST)) {
        mc_elog(ERROR, "Trace: Failed to add wakeup fd to internal epfd (errno=%d %m)", errno);
    }
    errno = errno_tmp;
}

void WakeupPipe::InitPipe(int *input_pipes)
{
    if (input_pipes == NULL) {
        mc_elog(ERROR, "Trace: WakeupPipe::InitPipe failed. invalid args.");
        return;
    }
    input_pipes[WAKEUP_PIPE_START] = INVALID_FD;
    input_pipes[WAKEUP_PIPE_END] = INVALID_FD;

    if (pipe(input_pipes)) {
        mc_elog(ERROR, "Trace: wakeup pipe create failed (errno=%d %m)", errno);
    }
    if (write(input_pipes[1], "^", 1) != 1) {
        mc_elog(ERROR, "Trace: wakeup pipe write failed(errno=%d %m)", errno);
    }
    mc_elog(LOG, "Trace: created wakeup epfd[%d], pipe [RD=%d, WR=%d]",
        m_epfd, input_pipes[0], input_pipes[1]);

    m_ev.events = EPOLLIN;
    m_ev.data.fd = input_pipes[0];
}

void WakeupPipe::RemoveWakeupFd()
{
    (void)epoll_ctl(m_epfd, EPOLL_CTL_DEL, m_normal_wakeup_pipes[WAKEUP_PIPE_START], NULL);
}

void WakeupPipe::ClosePipe(int *input_pipes)
{
    close(input_pipes[WAKEUP_PIPE_START]);
    close(input_pipes[WAKEUP_PIPE_END]);
    input_pipes[WAKEUP_PIPE_START] = INVALID_FD;
    input_pipes[WAKEUP_PIPE_END] = INVALID_FD;
}

WakeupPipe::~WakeupPipe()
{
    ClosePipe(m_normal_wakeup_pipes);
}
