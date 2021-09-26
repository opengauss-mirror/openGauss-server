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
 * libcomm_util.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/libcomm_utils/libcomm_util.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _UTILS_UTIL_H_
#define _UTILS_UTIL_H_

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <poll.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <pthread.h>
#include "libcomm_message.h"
#include "libcomm_common.h"

#define MAXSTACKSIZE 1048576
#define PACKETLEN 8192

#include <sys/syscall.h>
#ifndef gettid
#define gettid() syscall(SYS_gettid)
#endif

#define COMM_DEBUG_LOG(format, ...) \
    g_instance.comm_cxt.commutil_cxt.g_debug_mode ? (mc_elog(LOG, "[DEBUG]" format, ##__VA_ARGS__), 1) : 0;

#define COMM_DEBUG_CALL(call_this) g_instance.comm_cxt.commutil_cxt.g_debug_mode ? (call_this, 1) : 0;

#define COMM_STAT_CALL(mailbox, call_this) mailbox->statistic ? (call_this, 1) : 0;

#define COMM_STAT_TIME() g_instance.comm_cxt.commutil_cxt.g_stat_mode ? (uint32)mc_timers_ms() : 0;

#define COMM_STAT_TIME_US() g_instance.comm_cxt.commutil_cxt.g_stat_mode ? (uint32)mc_timers_us() : 0;

#define MAILBOX_ELOG(mailbox, elevel, format, ...)              \
    do {                                                        \
        struct node_sock* elog_node_sock = NULL;                \
        if (mailbox->is_producer == 1)                          \
            elog_node_sock = g_instance.comm_cxt.g_s_node_sock; \
        else                                                    \
            elog_node_sock = g_instance.comm_cxt.g_r_node_sock; \
        mc_elog(elevel,                                         \
            format " Mailbox[%d][%d][%d] state:%s, "            \
                   "node[%s], query[%lu], remote version[%d].", \
            ##__VA_ARGS__,                                      \
            (mailbox)->idx,                                     \
            (mailbox)->streamid,                                \
            (mailbox)->local_version,                           \
            stream_stat_string((mailbox)->state),               \
            elog_node_sock[(mailbox)->idx].remote_nodename,     \
            (mailbox)->query_id,                                \
            (mailbox)->remote_version);                         \
    } while (0)

#define COMM_TIMER_TIME() g_instance.comm_cxt.commutil_cxt.g_timer_mode ? mc_timers_us() : 0;

#define COMM_TIMER_INIT()                 \
    uint64_t t_begin = COMM_TIMER_TIME(); \
    uint64_t t_last = t_begin;            \
    uint64_t t_just = 0;                  \
    int timer_cnt = 0;

#define COMM_TIMER_COPY_INIT(t)           \
    uint64_t t_begin = (t);               \
    uint64_t t_last = t_begin;            \
    uint64_t t_just = 0;                  \
    int timer_cnt = 0;

#define COMM_TIMER_LOG(format, ...)                                       \
    do {                                                                  \
        if (g_instance.comm_cxt.commutil_cxt.g_timer_mode) {              \
            t_just = COMM_TIMER_TIME();                                   \
            mc_elog(LOG,                                                  \
                "[TIMER]" format "(part%d used:%luns, all used:%luns). ", \
                ##__VA_ARGS__,                                            \
                ++timer_cnt,                                              \
                t_just - t_last,                                          \
                t_just - t_begin);                                        \
            t_last = t_just;                                              \
        }                                                                 \
    } while (0)

#define REMOTE_NAME(node_sock, idx) ((idx) >= 0 ? (node_sock)[(idx)].remote_nodename : "Unknown")

typedef union {
    struct sockaddr_in v4;
    struct sockaddr_in6 v6;
    struct sockaddr sa;
} sockaddr_storage_t;

// the message type define
// this message used stream id=0 logic connecion
typedef enum {
    LIBCOMM_PKG_TYPE_UNKNOW,
    LIBCOMM_PKG_TYPE_CONNECT,
    LIBCOMM_PKG_TYPE_ACCEPT,
    LIBCOMM_PKG_TYPE_DELAY_REQUEST,
    LIBCOMM_PKG_TYPE_DELAY_REPLY,
    LIBCOMM_PKG_TYPE_MAX
} libcomm_package_type;

// sctp connect package
struct libcomm_connect_package {
    uint16 type;
    uint16 magic_num;
    char node_name[NAMEDATALEN];
    char host[HOST_ADDRSTRLEN];
};

// sctp accept package
struct libcomm_accept_package {
    uint16 type;
    uint16 result;
};

// sctp delay survey package
struct libcomm_delay_package {
    uint16 type;
    uint16 sn;
    uint32 start_time;
    uint32 recv_time;
    uint32 reply_time;
    uint32 finish_time;
};

// we save a sctp delay info 5s, 12 times used 1min
#define MAX_DELAY_ARRAY_INDEX 12
// sctp delay infomation
struct libcomm_delay_info {
    // the array index for delay array
    uint16 current_array_idx;
    uint32 delay[MAX_DELAY_ARRAY_INDEX];
};

// time record used for the statistic information of the mailbox
struct libcomm_time_record {
    uint64 wait_lock_start;
    uint64 wait_data_time;
    uint64 wait_data_start;
    uint64 wait_data_end;
    uint64 time_enter;
    uint64 time_now;
    uint64 t_begin;
};

struct check_cmailbox_option {
    libcomm_time_record *time_record;                      // used to update cmailbox statistics
    int first_cycle;                                        // whether is first cycle in gs_wait_poll
    int poll_error_flag;                                    // error means waked up by other threads
};

typedef struct TimeProfile {
    uint64 start;
    uint64 end;
} TimeProfile;
typedef struct WaitResult {
    int ret;
    bool notify_remote;
} WaitResult;
typedef struct SendOptions {
    unsigned long need_send_len;
    bool block_mode;
    int time_out;
    FCMSG_T* fcmsgs;
    int local_version;
} SendOptions;

void gs_set_usable_memory(long usable_memory);

long gs_get_comm_used_memory();

long gs_get_comm_peak_memory();

void gs_set_memory_pool_size(long mem_pool);

void set_debug_mode(bool mod);

void set_timer_mode(bool mod);

void set_stat_mode(bool mod);

void set_no_delay(bool mod);

int cmp_addr(sockaddr_storage_t* addr1, sockaddr_storage_t* addr2);

void getStacksize(int t, pthread_attr_t pattr);

int setStacksize(pthread_attr_t* pattr, size_t stacksize);

void printfcmsg(const char* caller, struct FCMSG_T* pfcmsg);

uint32 comm_get_cpylen(const char* src, uint32 max_len);

int set_socketopt(int sock, int FL_OR_FD, long arg);

uint64_t mc_timers_ns(void);

uint64_t mc_timers_us(void);

uint64 mc_timers_ms(void);

void comm_ipc_log_get_time(char *now_date, int time_len);

void printf_cmailbox_statistic(c_mailbox* cmailbox, char* nodename);

void printf_pmailbox_statistic(p_mailbox* pmailbox, char* nodename);

void print_socket_info(int sock, struct tcp_info* info, bool sender);

const char* stream_stat_string(int type);

const char* ctrl_msg_string(int type);

const char* msg_oper_string(CommMsgOper msg_oper);

#endif  // _UTILS_UTIL_H_
