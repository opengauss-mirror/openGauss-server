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
 *---------------------------------------------------------------------------------------
 *
 * sctp_common.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_common.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _GS_SCTP_COMMON_H_
#define _GS_SCTP_COMMON_H_

#include <stddef.h>
#include <errno.h>
#include <unistd.h>
#include <limits.h>
#include <sys/resource.h>

#include "utils/memprot.h"
#include "gs_threadlocal.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include "sctp_utils/sctp_message.h"
#include "libcomm.h"
#include "sctp_utils/sctp_atomics.h"

#ifdef SCTP_BUFFER_DEBUG
#define BUFF_BLOCK_NUM 100
#endif

// These are preserved for memory management.
//
#define MAXSCTPSTREAMS 65535

// common used definitions.
//
#define IOV_DATA_SIZE 8192
#define IOV_ITEM_SIZE IOV_DATA_SIZE + sizeof(struct iovec) + sizeof(mc_lqueue_element)
#define QUEUE_SIZE 65535
// default message size is 8192 bytes.
//
#define DEFULTMSGLEN 8192
#define REPEAT 10
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

#define mc_calloc(p, t, a, b) (p) = (t*)calloc((a), (b))
#define mc_free(a) free((a))

extern long libpq_used_memory;
extern long libcomm_used_memory;
extern long comm_peak_used_memory;

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
            Assert(CurrentMemoryContext == g_instance.comm_cxt.comm_global_mem_cxt);                                 \
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
        Assert(CurrentMemoryContext == g_instance.comm_cxt.comm_global_mem_cxt);     \
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
        Assert(CurrentMemoryContext == g_instance.comm_cxt.comm_global_mem_cxt);     \
        (ptr) = (type*)palloc0_noexcept((size));                                     \
        if (unlikely(NULL == (ptr))) {                                               \
            gs_memprot_reset_beyondchunk();                                          \
            mc_elog(WARNING,                                                         \
                "libcomm malloc failed, size:%lu, current used:%ld, caller: %s:%d.", \
                (uint64)(size),                                                      \
                libcomm_used_memory,                                                 \
                __FILE__,                                                            \
                __LINE__);                                                           \
            errno = ECOMMSCTPMEMALLOC;                                               \
        } else {                                                                     \
            (void)atomic_add(&libcomm_used_memory, (size));                          \
            if ((libcomm_used_memory + libpq_used_memory) > comm_peak_used_memory)   \
                comm_peak_used_memory = (libcomm_used_memory + libpq_used_memory);   \
        }                                                                            \
    } while (0)
#endif

#define LIBCOMM_FREE(ptr, size)                                                      \
    do {                                                                             \
        if (likely(NULL != (ptr))) {                                                 \
            Assert(CurrentMemoryContext == g_instance.comm_cxt.comm_global_mem_cxt); \
            pfree_ext((ptr));                                                        \
            (void)atomic_sub(&libcomm_used_memory, (size));                          \
            (ptr) = NULL;                                                            \
        }                                                                            \
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
    int destroy_wait;  // destroy need wait someone done
    int init()
    {
        atomic_set(&b_flag, 0);
        atomic_set(&waiting_count, 0);
        atomic_set(&b_destroy, 0);
        atomic_set(&destroy_wait, 0);
        int err = pthread_cond_init(&cond, NULL);
        if (err != 0)
            return err;
        err = pthread_mutex_init(&mutex, NULL);
        if (err != 0) {
            LIBCOMM_PTHREAD_COND_DESTORY(&cond);
            return err;
        }
        return err;
    };

    int destroy(bool do_destroy = false)
    {
        const int ret = 0;
        atomic_set(&b_destroy, 1);
        while (destroy_wait != 0) {
            post();
            usleep(100);
        }
        if (do_destroy) {
            LIBCOMM_PTHREAD_COND_DESTORY(&cond);
            LIBCOMM_PTHREAD_MUTEX_DESTORY(&mutex);
        }
        return ret;
    };
    void reset()
    {
        atomic_set(&b_flag, 0);
        while (destroy_wait != 0) {
            post();
            usleep(100);
        }
    };
    void post()
    {
        LIBCOMM_PTHREAD_MUTEX_LOCK(&mutex);
        /* thread will poll up when someone has posted before */
        atomic_set(&b_flag, 1);
        if (waiting_count > 0) {
            LIBCOMM_PTHREAD_COND_SIGNAL(&cond);
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&mutex);
    };
    void post_all()
    {
        LIBCOMM_PTHREAD_MUTEX_LOCK(&mutex);
        atomic_set(&b_flag, 1);
        if (waiting_count > 0) {
            LIBCOMM_PTHREAD_COND_BROADCAST(&cond);
        }
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&mutex);
    };
    int wait()
    {
        if (b_flag) {
            /* reset b_flag is no one is waitting */
            if (waiting_count == 0)
                atomic_set(&b_flag, 0);
            return 0;
        }

        int ret = 0;
        LIBCOMM_PTHREAD_MUTEX_LOCK(&mutex);
        while (!b_flag) {
            atomic_add(&waiting_count, 1);
            ret = pthread_cond_wait(&cond, &mutex);
            atomic_sub(&waiting_count, 1);
        }

        if (b_destroy)
            ret = -1;
        /* reset b_flag is no one is waitting */
        if (waiting_count == 0)
            atomic_set(&b_flag, 0);
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&mutex);

        return ret;
    };

    void destroy_wait_add()
    {
        atomic_add(&destroy_wait, 1);
    };

    void destroy_wait_sub()
    {
        atomic_sub(&destroy_wait, 1);
    };

    // The parameter timeout should be in second, if it is minus or zero, the function
    // is the same as _wait.
    int timed_wait(int timeout)
    {
        int ret = -1;

        if (timeout <= 0)
            ret = wait();
        else {
            if (b_flag) {
                /* reset b_flag is no one is waitting */
                if (waiting_count == 0)
                    atomic_set(&b_flag, 0);
                return 0;
            }

            LIBCOMM_PTHREAD_MUTEX_LOCK(&mutex);
            if (b_flag) {
                atomic_set(&b_flag, 0);
                LIBCOMM_PTHREAD_MUTEX_UNLOCK(&mutex);
                ret = 0;
                return ret;
            }
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += timeout;
            ts.tv_nsec = 0;
            atomic_add(&waiting_count, 1);

            ret = pthread_cond_timedwait(&cond, &mutex, &ts);

            atomic_sub(&waiting_count, 1);
            if (b_flag) {
                /* reset b_flag is no one is waitting */
                if (waiting_count == 0)
                    atomic_set(&b_flag, 0);
                ret = 0;
            }

            if (b_destroy)
                ret = -1;
            LIBCOMM_PTHREAD_MUTEX_UNLOCK(&mutex);
        }

        return ret;
    }
};

// hash keys and hash entries
// hash entry key, consist of node index, plan index and plan node index of
// a query plan
struct stream_with_node_key {
    int node_idx;
    SctpStreamKey stream_key;
};

// entry of hash_table,
// the father class of stream_entry, int_int_entry and host_port_int_entry.
struct hash_entry {
    struct binary_semaphore sem;
    int _init()
    {
        return sem.init();
    };
    void _destroy()
    {
        sem.destroy(true);
    };
    void _signal()
    {
        sem.post();
    };
    void _signal_all()
    {
        sem.post_all();
    };
    void _wait()
    {
        sem.wait();
    };
    void _hold_destroy()
    {
        sem.destroy_wait_add();
    };
    void _release_destroy()
    {
        sem.destroy_wait_sub();
    };
    // The parameter timeout should be in second, if it is minus or zero, the function
    // is the same as _wait.
    int _timewait(int timeout)
    {
        return sem.timed_wait(timeout);
    };
};

// hash entry of  stream information
// (1) using for looking up stream index by node index, plan index and plan node index
struct stream_entry : hash_entry {
    int val;  // stream_id
};

// hash entry: listening sctp port <-> node index, tcp ctl port	<->	node index
// (1)using for looking up node index by sctp port
// (2)using for looking up node index by control tcp port
struct int_int_entry : hash_entry {
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

struct sock_id_int_entry : hash_entry {
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

struct int_poller_hndl_entry : hash_entry {
    struct mc_poller_hndl val;
};

struct char_key {
    char name[NAMEDATALEN];
};

struct char_int_entry : hash_entry {
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

struct ip_node_state_entry : hash_entry {
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
    comm_connect_t assoc_id;
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
    int sctp_reply_sock;
    void reset_all()
    {
        ctrl_tcp_sock = -1;
        ctrl_tcp_port = -1;
        ctrl_tcp_sock_id = 0;
        sctp_reply_sock = -1;
        errno_t ss_rc = 0;
        ss_rc = memset_s(remote_host, HOST_ADDRSTRLEN, 0x0, HOST_ADDRSTRLEN);
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(remote_nodename, NAMEDATALEN, 0x0, NAMEDATALEN);
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(&to_ss, sizeof(struct sockaddr_storage), 0x0, sizeof(struct sockaddr_storage));
        securec_check(ss_rc, "\0", "\0");
    };
    void init()
    {
        reset_all();
        LIBCOMM_PTHREAD_MUTEX_INIT(&_slock, 0);
        LIBCOMM_PTHREAD_MUTEX_INIT(&_tlock, 0);
    };
    void clear()
    {
        reset_all();
    };
    void destroy()
    {
        clear();
        LIBCOMM_PTHREAD_MUTEX_DESTORY(&_slock);
        LIBCOMM_PTHREAD_MUTEX_DESTORY(&_tlock);
    };
    void lock()
    {
        LIBCOMM_PTHREAD_MUTEX_LOCK(&_tlock);
    };
    void unlock()
    {
        LIBCOMM_PTHREAD_MUTEX_UNLOCK(&_tlock);
    };
    void close_socket(int flag)
    {
        lock();
        close_socket_nl(flag);
        unlock();
    };
    void close_socket_nl(int flag)  // close without lock
    {
        switch (flag) {
            case CTRL_TCP_SOCK:
                if (ctrl_tcp_sock >= 0) {
                    close(ctrl_tcp_sock);
                    ctrl_tcp_sock = -1;
                    ctrl_tcp_sock_id = -1;
                }
                break;
            default:
                break;
        }
    };
    void set(int val, int flag)
    {
        lock();
        set_nl(val, flag);
        unlock();
    };
    void set_nl(int val, int flag)  // set without lock
    {
        switch (flag) {
            case CTRL_TCP_SOCK:
                ctrl_tcp_sock = val;
                break;
            case CTRL_TCP_PORT:
                ctrl_tcp_port = val;
                break;
            case CTRL_TCP_SOCK_ID:
                ctrl_tcp_sock_id = val;
                break;
            default:
                break;
        }
    };
    int get(int flag, int* id)
    {
        int val = -1;
        lock();
        val = get_nl(flag, id);
        unlock();
        return val;
    };
    int get_nl(int flag, int* id)  // get without lock
    {
        int val = -1;
        switch (flag) {
            case CTRL_TCP_SOCK:
                val = ctrl_tcp_sock;
                if (id != NULL)
                    *id = ctrl_tcp_sock_id;
                break;
            case CTRL_TCP_PORT:
                val = ctrl_tcp_port;
                break;
            case CTRL_TCP_SOCK_ID:
                val = ctrl_tcp_sock_id;
                if (id != NULL)
                    *id = ctrl_tcp_sock_id;
                break;
            default:
                break;
        }
        return val;
    };
};

// structure of sctp connection to nodes, using by data receiver
struct local_receivers {
    struct node_connection* receiver_conn;
    struct node_connection server_listen_sctp_conn;
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

    // version of remote mailbox, send data or control message with it, 
    // remote need check if this and the newest version of remote mailbox are equal
    uint16 remote_version;

    int ctrl_tcp_sock;           // control tcp socket
    pthread_mutex_t sinfo_lock;  // lock for read and write the structure variable
    uint64 query_id;             // query index (debug_query_id)
    SctpStreamKey stream_key;    // SCTP stream key

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

struct p_mailbox : mailbox {
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
    uint64 consumer_elapsed_time;  // total time of consumer fetch data(interval between last gs_wait_poll end and current gs_wait_poll start)(unit:ms)

    uint64 gs_recv_time;       // totol time of consumer cost on gs_recv
    uint64 total_signal_time;  // totale time when calling pthread_cond_signal in gs_push_cmailbox_buffer(unit:ms)

    uint64 recv_bytes;  // total recv packet bytes in this consumer thread

    uint64 recv_loop_time;   // totol time of between receiver_loop fetch data from recv buffer and push into cmailbox
    uint32 recv_loop_count;  // times of msg buffer pushed into cmailbox by receiver loop
};

struct c_mailbox : mailbox {
    struct mc_lqueue* buff_q;       // buffer queue for caching received data from the counterpart stream
    cmailbox_statistic* statistic;  // consumer thread statistic information
};

// Resolves system errors and native	errors to	human-readable string.
const char* mc_strerror(int errnum);

#endif //_GS_SCTP_COMMON_H_

