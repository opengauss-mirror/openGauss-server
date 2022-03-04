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
 * libcomm_common.h
 *
 * IDENTIFICATION
 *    gausskernel/cbb/communication/libcomm_common.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _GS_LIBCOMM_COMMON_H_
#define _GS_LIBCOMM_COMMON_H_

#include <stddef.h>
#include <errno.h>
#include <unistd.h>
#include <limits.h>
#include <sys/resource.h>

#include "utils/memprot.h"
#include "gs_threadlocal.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include "libcomm_utils/libcomm_message.h"
#include "libcomm.h"
#include "libcomm_utils/libcomm_atomics.h"

#ifdef SCTP_BUFFER_DEBUG
#define BUFF_BLOCK_NUM 100
#endif

// These are preserved for memory management.
//
#define MAXSCTPSTREAMS 65535

// common used definitions.
extern unsigned long IOV_DATA_SIZE;
extern unsigned long IOV_ITEM_SIZE;
extern unsigned long DEFULTMSGLEN;
extern unsigned long LIBCOMM_BUFFER_SIZE;

#define QUEUE_SIZE 65535
#define BIG_REPEAT 1000000
#define MAX_BIND_RETRYS 30
#define BODYSIZE 10
#define MSG_CNT 10
#define MAX_FD_ID 65535
#define MAX_RECV_NUM 50
#define EPOLL_TIMEOUT 10000 /* 10s,unit:ms  */
#define MAX_EPOLL_TIMEOUT_COUNT 60 /* 10min */ 
#define CONNECT_TIMEOUT 600
#define WAITQUOTA_TIMEOUT 600
#define SINGLE_WAITQUOTA 3

#define MAX_MAILBOX_VERSION 65535

#define BROADCAST_NEED_SEND 0
#define BROADCAST_SEND_FINISH 1
#define BROADCAST_CONNECT_CLOSED -1
#define BROADCAST_WAIT_QUOTA -2

#define WAIT_POLL_FLAG_GOT 1
#define WAIT_POLL_FLAG_IDLE 0
#define WAIT_POLL_FLAG_ERROR -1
#define WAIT_POLL_FLAG_WAIT -2

// The maximum number of node and physical connections.
//
#define NODE_NUMBER 2048
#define STREAM_NUMBER 4096

#define IPC_TYPE_LEN 1
#define IPC_MSG_LEN 4
#define IPC_HEAD_LEN (IPC_TYPE_LEN + IPC_MSG_LEN)

#define RECV_NEED_RETRY 0

// some error definition for receiver
#define RECV_MEM_ERROR  -2
#define RECV_NET_ERROR  -1
#define INVALID_SOCK  -1

// The g_c_mailbox and g_p_mailbox are
// the basic structure for keeping infomation of receiver and sender.
//
#define C_MAILBOX(i, j) (*(*(g_instance.comm_cxt.g_c_mailbox + i) + j))
#define P_MAILBOX(i, j) (*(*(g_instance.comm_cxt.g_p_mailbox + i) + j))

#undef MIN
#undef MAX
#define MIN(A, B) ((B) < (A) ? (B) : (A))
#define MAX(A, B) ((B) > (A) ? (B) : (A))
#define HOST_ADDRSTRLEN INET6_ADDRSTRLEN
#define NAMEDATALEN 64

// if a big then b return a-b, else return 0
#define ABS_SUB(a, b) (((a) > (b)) ? ((a) - (b)) : 0)

#define mc_calloc(p, t, a, b) (p) = (t*)calloc((a), (b))
#define mc_free(a) free((a))

#define DEBUG_QUERY_ID (likely(u_sess == NULL) ? t_thrd.comm_cxt.debug_query_id : u_sess->debug_query_id)
#define IS_LOCAL_HOST(host) (strcmp(host, "*") == 0 || strcmp(host, "localhost") == 0 || strcmp(host, "0.0.0.0") == 0)
#define LIBCOMM_INTERFACE_END(wakeup_abnormal, immediate_interrupt)            \
    do {                                                                       \
        if (((wakeup_abnormal) || (immediate_interrupt)) &&                    \
            (t_thrd.postmaster_cxt.ProcessStartupPacketForLogicConn == false)) \
            CHECK_FOR_INTERRUPTS();                                            \
        t_thrd.int_cxt.ImmediateInterruptOK = (immediate_interrupt);           \
    } while (0)
#define IS_NOTIFY_REMOTE(reason) \
            ((reason) != ECOMMTCPTCPDISCONNECT && (reason) != ECOMMTCPREMOETECLOSE && (reason) != ECOMMTCPREJECTSTREAM)

/*
 * we need to notify peer process when
 * assert failed so we can get the state
 * of both sender and receiver
 */
#define LIBCOMM_ASSERT(condition, nidx, sidx, node_role) gs_libcomm_handle_assert(condition, nidx, sidx, node_role)

extern long libpq_used_memory;
extern long libcomm_used_memory;
extern long comm_peak_used_memory;
extern int g_ackchk_time;

extern uint64_t mc_timers_us(void);
extern uint64 mc_timers_ms(void);

/*
 * libcomm malloc define start
 * LIBCOMM_MALLOC: malloc memory from CommunnicatorGlobalMemoryContext without exception
 */
#if defined LIBCOMM_FAULT_INJECTION_ENABLE
#define LIBCOMM_MALLOC(ptr, size, type)                                                                              \
    do {                                                                                                             \
        if (is_comm_fault_injection(LIBCOMM_FI_MALLOC_FAILED)) {                                                     \
            LIBCOMM_ELOG(                                                                                            \
                WARNING, "(LIBCOMM MALLOC)\t[FAULT INJECTION]Failed to malloc, caller: %s:%d.", __FILE__, __LINE__); \
            (ptr) = NULL;                                                                                            \
        } else {                                                                                                     \
            Assert(CurrentMemoryContext == g_instance.comm_cxt.comm_global_mem_cxt ||                                \
                CurrentMemoryContext == g_instance.comm_logic_cxt.comm_logic_mem_cxt);                               \
            (ptr) = (type*)palloc0_noexcept((size));                                                                 \
            if (unlikely(NULL == (ptr))) {                                                                           \
                gs_memprot_reset_beyondchunk();                                                                      \
                mc_elog(WARNING,                                                                                     \
                    "libcomm malloc failed, size:%lu, current used:%ld, caller: %s:%d.",                             \
                    (uint64)(size),                                                                                  \
                    libcomm_used_memory,                                                                             \
                    __FILE__,                                                                                        \
                    __LINE__);                                                                                       \
            } else {                                                                                                 \
                (void)atomic_add(&libcomm_used_memory, (size));                                                      \
                if ((libcomm_used_memory + libpq_used_memory) > comm_peak_used_memory)                               \
                    comm_peak_used_memory = (libcomm_used_memory + libpq_used_memory);                               \
            }                                                                                                        \
        }                                                                                                            \
    } while (0)
#elif defined MEMORY_CONTEXT_CHECKING
#define LIBCOMM_MALLOC(ptr, size, type)                                              \
    do {                                                                             \
        Assert(CurrentMemoryContext == g_instance.comm_cxt.comm_global_mem_cxt ||    \
            CurrentMemoryContext == g_instance.comm_logic_cxt.comm_logic_mem_cxt);   \
        if (gs_memory_enjection())                                                   \
            (ptr) = NULL;                                                            \
        else                                                                         \
            (ptr) = (type*)palloc0_noexcept((size));                                 \
        if (unlikely(NULL == (ptr))) {                                               \
            gs_memprot_reset_beyondchunk();                                          \
            mc_elog(WARNING,                                                         \
                "libcomm malloc failed, size:%lu, current used:%ld, caller: %s:%d.", \
                (uint64)(size),                                                      \
                libcomm_used_memory,                                                 \
                __FILE__,                                                            \
                __LINE__);                                                           \
        } else {                                                                     \
            (void)atomic_add(&libcomm_used_memory, (size));                          \
            if ((libcomm_used_memory + libpq_used_memory) > comm_peak_used_memory)   \
                comm_peak_used_memory = (libcomm_used_memory + libpq_used_memory);   \
        }                                                                            \
    } while (0)
#else
#define LIBCOMM_MALLOC(ptr, size, type)                                              \
    do {                                                                             \
        Assert(CurrentMemoryContext == g_instance.comm_cxt.comm_global_mem_cxt ||    \
            CurrentMemoryContext == g_instance.comm_logic_cxt.comm_logic_mem_cxt);   \
        (ptr) = (type*)palloc0_noexcept((size));                                     \
        if (unlikely(NULL == (ptr))) {                                               \
            gs_memprot_reset_beyondchunk();                                          \
            mc_elog(WARNING,                                                         \
                "libcomm malloc failed, size:%lu, current used:%ld, caller: %s:%d.", \
                (uint64)(size),                                                      \
                libcomm_used_memory,                                                 \
                __FILE__,                                                            \
                __LINE__);                                                           \
            errno = ECOMMTCPMEMALLOC;                                               \
        } else {                                                                     \
            (void)atomic_add(&libcomm_used_memory, (size));                          \
            if ((libcomm_used_memory + libpq_used_memory) > comm_peak_used_memory)   \
                comm_peak_used_memory = (libcomm_used_memory + libpq_used_memory);   \
        }                                                                            \
    } while (0)
#endif

#define LIBCOMM_FREE(ptr, size)                                                         \
    do {                                                                                \
        if (likely(NULL != (ptr))) {                                                    \
            Assert(CurrentMemoryContext == g_instance.comm_cxt.comm_global_mem_cxt ||   \
                CurrentMemoryContext == g_instance.comm_logic_cxt.comm_logic_mem_cxt);  \
            pfree_ext((ptr));                                                           \
            (void)atomic_sub(&libcomm_used_memory, (size));                             \
            (ptr) = NULL;                                                               \
        }                                                                               \
    } while (0)

#define LIBCOMM_ELOG(elevel, format, ...)       \
    do {                                        \
        mc_elog(elevel, format, ##__VA_ARGS__); \
    } while (0)

#define LIBCOMM_SYSTEM_CALL(function)                                  \
    do {                                                               \
        if (function != 0) {                                           \
            mc_elog(WARNING, "system call failed, errno:%d. ", errno); \
            Assert(0 != 0);                                            \
        }                                                              \
    } while (0)

#ifdef USE_SSL

#define LIBCOMM_FIND_SSL(ssl, sock, errstr)                                           \
    do {                                                                              \
        if (g_instance.attr.attr_network.comm_enable_SSL) {                           \
            (ssl) = LibCommClientGetSSLForSocket(sock);                               \
            if ((ssl) == NULL) {                                                      \
                LIBCOMM_ELOG(WARNING, "%s%d", (errstr), (sock));                      \
                return -1;                                                            \
            }                                                                         \
        }                                                                             \
    } while (0)

#endif

#define LIBCOMM_PTHREAD_RWLOCK_INIT(lock, attr) LIBCOMM_SYSTEM_CALL(pthread_rwlock_init(lock, attr))

#define LIBCOMM_PTHREAD_RWLOCK_WRLOCK(lock) LIBCOMM_SYSTEM_CALL(pthread_rwlock_wrlock(lock))

#define LIBCOMM_PTHREAD_RWLOCK_RDLOCK(lock) LIBCOMM_SYSTEM_CALL(pthread_rwlock_rdlock(lock))

#define LIBCOMM_PTHREAD_RWLOCK_UNLOCK(lock) LIBCOMM_SYSTEM_CALL(pthread_rwlock_unlock(lock))

#define LIBCOMM_PTHREAD_COND_DESTORY(cond) LIBCOMM_SYSTEM_CALL(pthread_cond_destroy(cond))

#define LIBCOMM_PTHREAD_MUTEX_INIT(mutex, pshared) LIBCOMM_SYSTEM_CALL(pthread_mutex_init(mutex, pshared))

#define LIBCOMM_PTHREAD_MUTEX_DESTORY(mutex) LIBCOMM_SYSTEM_CALL(pthread_mutex_destroy(mutex))

#define LIBCOMM_PTHREAD_MUTEX_LOCK(mutex) LIBCOMM_SYSTEM_CALL(pthread_mutex_lock(mutex))

#define LIBCOMM_PTHREAD_MUTEX_UNLOCK(mutex) LIBCOMM_SYSTEM_CALL(pthread_mutex_unlock(mutex))

#define LIBCOMM_PTHREAD_COND_SIGNAL(cond) LIBCOMM_SYSTEM_CALL(pthread_cond_signal(cond))

#define LIBCOMM_PTHREAD_COND_BROADCAST(cond) LIBCOMM_SYSTEM_CALL(pthread_cond_broadcast(cond))

// Using posix semaphore to notify quota checking thread
// when quota of stream is changed.
struct binary_semaphore {
    pthread_mutex_t mutex;  // locker
    pthread_cond_t cond;    // variable condition for notifying
    int b_flag;             // flag using to avoid fake notification
    int waiting_count;      // waiting threads count
    int b_destroy;
    int destroy_wait;       // destroy need wait someone done
    int init();
    int destroy(bool do_destroy = false);
    void reset();
    void post();
    void post_all();
    int wait();
    void destroy_wait_add();
    void destroy_wait_sub();
    int timed_wait(int timeout);
};

// hash keys and hash entries
// hash entry key, consist of node index, plan index and plan node index of
// a query plan
struct stream_with_node_key {
    int node_idx;
    TcpStreamKey stream_key;
};

// entry of hash_table,
// the father class of stream_entry, int_int_entry and host_port_int_entry.
struct hash_entry {
    struct binary_semaphore sem;
    int _init();
    void _destroy();
    void _signal();
    void _signal_all();
    void _wait();
    void _hold_destroy();
    void _release_destroy();
    // The parameter timeout should be in second, if it is minus or zero, the function is the same as _wait.
    int _timewait(int timeout);
};

// hash entry of  stream information
// (1) using for looking up stream index by node index, plan index and plan node index
struct stream_entry : public hash_entry {
    int val;  // stream_id
};

// hash entry: listening sctp port <-> node index, tcp ctl port	<->	node index
// (1)using for looking up node index by sctp port
// (2)using for looking up node index by control tcp port
struct int_int_entry : public hash_entry {
    int val;
};

struct tid_entry {
    int tid;
    struct int_int_entry entry;
};

struct sock_ver_entry {
    int ver;
    struct int_int_entry entry;
};

// internal fd with version id
struct sock_id {
    int fd;
    int id;
};

struct sock_id_int_entry : public hash_entry {
    int val;
};

struct sock_id_entry {
    sock_id key;
    struct sock_id_int_entry entry;
};

// Handler of epoller, including fd and epoll events.
struct mc_poller_hndl {
    int fd;
    int id;
    uint32_t events;
};

struct int_poller_hndl_entry : public hash_entry {
    struct mc_poller_hndl val;
};

struct char_key {
    char name[NAMEDATALEN];
};

struct char_int_entry : public hash_entry {
    int val;
};

struct nodename_entry {
    struct char_key key;
    struct char_int_entry entry;
};

struct ip_key {
    char ip[HOST_LEN_OF_HTAB];
    int port;
};

struct node_state {
    int conn_state;
    int node_idx;
};

struct ip_node_state_entry : public hash_entry {
    struct node_state val;
};

struct ip_state_entry {
    struct ip_key key;
    struct ip_node_state_entry entry;
};

typedef enum {
    MSG_NULL = 0,
    MSG_CONNECT,
    MSG_ACCEPT,
    MSG_DATA,
    MSG_QUOTA,
    MSG_TID,
    MSG_DISCONN,
    MSG_CLOSED,
    MSG_UNKNOW
} MSG_TYPE;

typedef struct {
    uint8 type;
    uint8 magic_num;
    uint16 version;
    uint16 logic_id;
    uint32 msg_len;
    uint32 checksum;
} MsgHead;

// connection information of node
struct node_connection {
    pthread_rwlock_t rwlock;
    struct sockaddr_storage ss;
    char remote_host[HOST_ADDRSTRLEN];
    int port;
    int ss_len;
    int socket;
    int socket_id;
    int64 last_rcv_time;
    bool ip_changed;
    long comm_bytes;
    int comm_count;
    int assoc_id;
    MsgHead msg_head;
    int head_read_cursor;
    struct mc_lqueue_item* iov_item;
};

// structure used for keeping control tcp port & socket, sctp port and socket
enum { CTRL_TCP_SOCK, CTRL_TCP_PORT, CTRL_TCP_SOCK_ID };
struct node_sock {
    char remote_host[HOST_ADDRSTRLEN];
    char remote_nodename[NAMEDATALEN];
    struct sockaddr_storage to_ss;
    // following variable is used to concurrent control when call mc_tcp_write
    //
    pthread_mutex_t _slock;
    pthread_mutex_t _tlock;

    bool ip_changed;
    int ctrl_tcp_sock;
    int ctrl_tcp_port;
    // we use the id for DFX, when the ctrl_tcp_sock is changed, the fd increase 1.
    // when the id is 65535, it will be set to 0 again.
    //
    int ctrl_tcp_sock_id;
    int libcomm_reply_sock;
    int libcomm_reply_sock_id;
    void reset_all();
    void init();
    void clear();
    void destroy();
    void lock();
    void unlock();
    void close_socket(int flag);
    void close_socket_nl(int flag);  // close without lock
    void set(int val, int flag);
    void set_nl(int val, int flag);  // set without lock
    int get(int flag, int* id);
    int get_nl(int flag, int* id) const;  // get without lock
};

// structure of sctp connection to nodes, using by data receiver
struct local_receivers {
    struct node_connection* receiver_conn;
    struct node_connection server_listen_conn;
    int server_ctrl_tcp_port;
};

// using by data sender
struct local_senders {
    struct node_connection* sender_conn;
};

typedef struct {
    int socket;
    int node_idx;
    int streamid;
    int version;
    struct mc_lqueue_item* iov_item;
} LibcommRecvInfo;

typedef struct {
    int socket;
    int socket_id;
    int node_idx;
    int streamid;
    int version;
    int msg_len;
    char* msg;
} LibcommSendInfo;

typedef int (*LibcommRecvFunc)(LibcommRecvInfo* recv_info);
typedef int (*LibcommSendFunc)(LibcommSendInfo* send_info);
typedef int (*LibcommConnectFunc)(libcommaddrinfo* sctp_addrinfo, int node_idx);
typedef int (*LibcommAcceptFunc)(int fd, struct sockaddr* sa, socklen_t* salenptr);
typedef int (*LibcommListenFunc)();
typedef int (*LibcommBlockSendFunc)(LibcommSendInfo* send_info);
typedef int (*LibcommSendAckFunc)(int fd, const void* data, int size);
typedef int (*LibcommCheckSocketFunc)(int fd);

typedef struct {
    LibcommRecvFunc recv_data;
    LibcommSendFunc send_data;
    LibcommConnectFunc connect;
    LibcommAcceptFunc accept;
    LibcommListenFunc listen;
    LibcommBlockSendFunc block_send;
    LibcommSendAckFunc send_ack;
    LibcommCheckSocketFunc check_socket;
} LibcommAdaptLayer;

// structure for keep status of sctp stream
struct mailbox {
    uint8 state;          // The state of stream, 1~6 : INIT,READY,RESUME,HOLD,CLOSED
    uint8 is_producer;    // this mailbox used for producer: 1, consumer: 0
    uint16 close_reason;  // close reason last time

    uint16 idx;             // node index
    uint16 streamid;        // stream id
    uint16 local_version;   // version of mailbox, it will added 1 when the mailbox is closed.
                            // in libcomm we use this value to check the validity of current logic connection,
                            // whether closed by other thread or not
    uint16 remote_version;  // version of remote mailbox, send data or control message with it,
                            // remote need check if this and the newest version of remote mailbox are equal

    int ctrl_tcp_sock;           // control tcp socket
    pthread_mutex_t sinfo_lock;  // lock for read and write the structure variable
    uint64 query_id;             // query index (debug_query_id)
    TcpStreamKey stream_key;    // stream key

    pid_t local_thread_id;  // local stream thread id
    pid_t peer_thread_id;   // remote stream thread id
    binary_semaphore* semaphore;
    unsigned long bufCAP;  // The remained quota of the stream, the value is between 0~quota
#ifdef SCTP_BUFFER_DEBUG
    struct mc_lqueue* buff_q_tmp;  // buffer data for debug
#endif
};

// structure for keep status of sctp stream for PRODUCER threads
struct pmailbox_statistic {
    uint64 start_time;    // get the time in clock_gettime(unit:ms) when this producer thread ready to send
    uint64 connect_time;  // build connection used time(unit:ms)

    uint64 first_send_time;  // the time when send first data(unit:ms)
    uint64 last_send_time;   // the time when send last data(unit:ms)

    uint64 wait_quota_overhead;  // total overhead when this thread wait quota(unit:ms)
    uint64 os_send_overhead;     // total overhead when this thread wait send return(unit:ms)

    uint64 total_send_time;  // totale time when running in libcomm(unit:ms)
    uint64 call_send_count;  // the number of enter gs_send

    uint64 producer_elapsed_time;  // total time of producer prepare data(interval between gs_send)(unit:ms)

    uint64 send_bytes;  // total recv packet bytes in this producer thread
};

struct p_mailbox : public mailbox {
    pmailbox_statistic* statistic;  // producer thread statistic information
};

// structure for keep status of sctp stream for CONSUMER threads
struct cmailbox_statistic {
    uint64 start_time;      // get the time in clock_gettime(unit:ms) when this consumer thread ready to receive
    uint64 wait_data_time;  // total overhead of waitting data(unit:ms)

    uint64 first_poll_time;  // the first time call gs_wait_poll(unit:ms)
    uint64 last_poll_time;   // the last time call gs_recv(unit:ms)

    uint64 first_recv_time;  // the time when received first data(unit:ms)
    uint64 last_recv_time;   // the time when received last data(unit:ms)

    uint64 total_poll_time;  // total time when running in libcomm(unit:ms)
    uint64 call_poll_count;  // the number of enter gs_wait_poll

    uint64 wait_lock_time;         // total time cost on wait cmailbox lock in gs_wait_poll(unit:ms)
    uint64 consumer_elapsed_time;  // total time of consumer fetch data(interval between last gs_wait_poll end and
                                   // current gs_wait_poll start)(unit:ms)

    uint64 gs_recv_time;       // totol time of consumer cost on gs_recv
    uint64 total_signal_time;  // totale time when calling pthread_cond_signal in gs_push_cmailbox_buffer(unit:ms)

    uint64 recv_bytes;  // total recv packet bytes in this consumer thread

    uint64 recv_loop_time;   // totol time of between receiver_loop fetch data from recv buffer and push into cmailbox
    uint32 recv_loop_count;  // times of msg buffer pushed into cmailbox by receiver loop
};

struct c_mailbox : public mailbox {
    struct mc_lqueue* buff_q;       // buffer queue for caching received data from the counterpart stream
    cmailbox_statistic* statistic;  // consumer thread statistic information
	
    /* This field is added by the CN DN logical link function to efficiently notify users. */
    SessionInfo *session_info_ptr;
};


// Resolves system errors and native	errors to	human-readable string.
const char* mc_strerror(int errnum);
extern int gs_internal_recv(const sock_id fd_id, int node_idx, int flags);
extern void gs_accept_ctrl_conntion(struct sock_id* t_fd_id, struct FCMSG_T* fcmsgr);
extern void gs_receivers_flow_handle_tid_request(FCMSG_T* fcmsgr);
extern void gs_receivers_flow_handle_ready_request(FCMSG_T* fcmsgr);
extern void gs_receivers_flow_handle_close_request(FCMSG_T* fcmsgr);
extern void gs_receivers_flow_handle_assert_fail_request(FCMSG_T* fcmsgr);
extern void gs_senders_flow_handle_tid_request(FCMSG_T* fcmsgr);
extern void gs_senders_flow_handle_init_request(FCMSG_T* fcmsgr);
extern void gs_senders_flow_handle_ready_request(FCMSG_T* fcmsgr);
extern void gs_senders_flow_handle_resume_request(FCMSG_T* fcmsgr);
extern void gs_senders_flow_handle_close_request(FCMSG_T* fcmsgr);
extern void gs_senders_flow_handle_assert_fail_request(FCMSG_T* fcmsgr);
extern void gs_senders_flow_handle_stop_query_request(FCMSG_T* fcmsgr);
extern void SetupCommSignalHook();
extern void gs_libcomm_handle_assert(bool condition, int nidx, int sidx, int node_role);
extern void gs_r_release_comm_memory();
extern void libcomm_free_iov_item(struct mc_lqueue_item** iov_item, int size);
extern int libcomm_malloc_iov_item(struct mc_lqueue_item** iov_item, int size);
extern int gs_send_msg_by_unix_domain(const void* msg, int msg_len);
extern int gs_s_build_tcp_ctrl_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx, bool is_reply);
extern int gs_send_ctrl_msg_by_socket(LibCommConn *conn, FCMSG_T* msg, int node_idx);
extern void gs_set_reply_sock(int node_idx);
extern int libcomm_tcp_recv(LibcommRecvInfo* recv_info);
extern int gs_accept_data_conntion(struct iovec* iov, sock_id fd_id);
extern bool is_tcp_mode();
extern void NotifyListener(struct c_mailbox* cmailbox, bool err_occurs, const char* caller_name);

// declarations of functions
extern int gs_update_fd_to_htab_socket_version(struct sock_id* fd_id);
extern void gs_s_close_bad_ctrl_tcp_sock(struct sock_id* fd_id, int close_reason, bool clean_epoll, int node_idx);
extern void gs_r_close_bad_ctrl_tcp_sock(struct sock_id* fd_id, int close_reason);
extern void gs_s_close_bad_data_socket(struct sock_id* fd_id, int close_reason, int node_idx);
extern void gs_r_close_bad_data_socket(int node_idx, sock_id fd_id, int close_reason, bool is_lock);
extern int gs_map_sock_id_to_node_idx(const sock_id fd_id, int idx);
extern void gs_update_connection_state(ip_key addr, int result, bool is_signal, int node_idx);
extern int gs_send_ctrl_msg_without_lock(struct node_sock* ns, FCMSG_T* msg, int node_idx, int role);
extern int gs_send_ctrl_msg(struct node_sock* ns, FCMSG_T* msg, int role);
extern void gs_close_timeout_connections(libcommaddrinfo** libcomm_addrinfo, int addr_num,
                                         int node_idx, int streamid);
extern int gs_clean_connection(libcommaddrinfo** libcomm_addrinfo, int addr_num, int error_index,
                               int re, bool TempImmediateInterruptOK);
extern void gs_s_close_logic_connection(struct p_mailbox* pmailbox, int close_reason, FCMSG_T* msg);
extern void gs_check_all_producers_mailbox(const gsocket* gs_sock_array, int nproducer, int* producer);
extern bool gs_s_form_start_ctrl_msg(p_mailbox* pmailbox, FCMSG_T* msg);
extern int gs_check_all_mailbox(libcommaddrinfo** libcomm_addrinfo, int addr_num, int re,
                                bool TempImmediateInterruptOK, int timeout);
extern int gs_push_local_buffer(int streamid, const char* message, int m_len, int cmailbox_version);
extern bool gs_r_quota_notify(c_mailbox* cmailbox, FCMSG_T* msg);
extern int gs_get_stream_id(int node_idx);
extern int gs_handle_data_delay_message(int idx, struct mc_lqueue_item* q_item, uint16 msg_type);
extern int gs_push_cmailbox_buffer(c_mailbox* cmailbox, struct mc_lqueue_item* q_item, int version);
extern bool gs_s_check_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx, bool is_reply, int type);
extern int gs_s_get_connection_state(ip_key addr, int node_idx, int type);
extern void gs_r_close_logic_connection(struct c_mailbox* cmailbox, int close_reason, FCMSG_T* msg);
extern void gs_poll_signal(binary_semaphore* sem);

extern int LibCommClientReadBlock(LibCommConn *conn, void* data, int size, int flags);
extern int LibCommClientWriteBlock(LibCommConn *conn, void* data, int size);
extern int LibCommClientCheckSocket(LibCommConn * conn);
#ifdef USE_SSL

#define CLIENT_CERT_FILE "client.crt"
#define CLIENT_KEY_FILE "client.key"
#define ROOT_CERT_FILE "root.crt"
#define ROOT_CRL_FILE "root.crl"

#define MAXPATH 1024
#define MAXBUFSIZE 256

typedef enum{
    LIBCOMM_POLLING_FAILED = 0,
    LIBCOMM_POLLING_READING, /* These two indicate that one may	  */
    LIBCOMM_POLLING_WRITING, /* use select before polling again.   */
    LIBCOMM_POLLING_SYSCALL,
    LIBCOMM_POLLING_OK,
    LIBCOMM_POLLING_ACTIVE /* unused; keep for awhile for backwards
                          * compatibility */
} LibcommPollingStatusType;

extern LibcommPollingStatusType LibCommClientSecureOpen(LibCommConn * conn, bool isUnidirectional = true);
extern void LibCommClientSSLClose(LibCommConn *conn);
extern int LibCommClientWaitTimed(int forRead, int forWrite, LibCommConn *conn, time_t finish_time);
extern int LibCommClientSecureInit();
extern ssize_t LibCommClientSSLWrite(SSL *ssl, const void* ptr, size_t len);
extern ssize_t LibCommClientSSLRead(SSL *ssl, void* ptr, size_t len);
extern SSL* LibCommClientGetSSLForSocket(int socket);

extern void comm_initialize_SSL();
extern void comm_ssl_close(SSL_INFO* port);
extern int comm_ssl_open_server(SSL_INFO* port, int fd);
extern libcomm_sslinfo** comm_ssl_find_port(libcomm_sslinfo** head, int sock);
extern SSL* comm_ssl_find_ssl_by_fd(int sock);
#endif

extern int LibCommInitChannelConn(int nodeNum);

#include "libcomm_utils/libcomm_lock_free_queue.h"

typedef enum {
    WAKEUP_PIPE_START = 0,
    WAKEUP_PIPE_END,
    WAKEUP_PIPE_SIZE
} WAKEUP_PIPE_IDX;

class WakeupPipe {
public:
    WakeupPipe(void);
    virtual ~WakeupPipe() = 0;
    void InitPipe(int *input_pipes);
    void ClosePipe(int *input_pipes);
    void DoWakeup();
    inline const bool IsWakeupFd(int fd)
    {
        return (fd == m_normal_wakeup_pipes[WAKEUP_PIPE_START]);
    };
    void RemoveWakeupFd();
    int m_epfd;
protected:
    inline void SetWakeupEpfd(int epfd)
    {
        m_epfd = epfd;
    };
    struct epoll_event m_ev;

private:
    int m_normal_wakeup_pipes[WAKEUP_PIPE_SIZE];
};

class CommEpollFd : public WakeupPipe, public BaseObject {
public:
    CommEpollFd(int epfd, int size);
    ~CommEpollFd();

    void Init(int epfd);
    void DeInit();
    int EpollCtl(int op, int fd, const struct epoll_event *event);
    bool IsLogicFd(uint64_t data);
    inline knl_session_context *GetSessionBaseOnEvent(const struct epoll_event* ev)
    {
        return (ev) ? (knl_session_context*)ev->data.ptr : NULL;
    };
    int RegisterNewSession(knl_session_context* session);
    int ResetSession(const knl_session_context* session);
    int UnRegisterSession(const knl_session_context* session);
    void PushReadyEvent(SessionInfo *session_info, bool err_occurs);
    const bool CheckError(const SessionInfo *session_info);
    const bool CheckEvent(const SessionInfo *session_info);
    int EpollWait(int epfd, epoll_event *events, int maxevents, int timeout);
    int GetReadyEvents();
    ArrayLockFreeQueue<SessionInfo> m_ready_events_queue;

private:
    int m_max_session_size;
    FdCollection *m_comm_fd_collection;

    /* for EpollWait */
    struct epoll_event *m_events;
    int m_maxevents;
    int m_timeout;
    struct epoll_event *m_ready_events;

    /* number of total ready fds(physic + logic) */
    int m_all_ready_fds;
};

extern CommEpollFd *GetCommEpollFd(int epfd);
extern void LogicEpollCreate(int epfd, int size);
extern void CloseLogicEpfd(int epfd);
#endif  //_GS_LIBCOMM_COMMON_H_
