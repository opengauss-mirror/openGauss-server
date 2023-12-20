/*-------------------------------------------------------------------------
 *
 * commproxy_interface.h
 *      api for CommProxy interface
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/communication/commproxy_interface.h
 *
 * NOTES
 *      Libnet or default tcp socket programming
 *
 *-------------------------------------------------------------------------
 */
#ifndef COMMPROXY_INTERFACE_H
#define COMMPROXY_INTERFACE_H

#include <unistd.h>

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/epoll.h>
#include <poll.h>
#include <iostream>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/stack.hpp>
#include <string>
#include <list>
#include <string.h>
#include "communication/commproxy_basic.h"
#include "communication/libnet_extern.h"
#include "postgres_ext.h"
#include "postgres.h"
#include "nodes/pg_list.h"
#include "port.h"
#include "pgtime.h"
#include "utils/memutils.h"
#include "knl/knl_instance.h"
#include "knl/knl_thread.h"
extern knl_instance_context g_instance;


/******************************************************************
 *      compile options      *
 ******************************************************************/
#define COMM_OPT_ON 1
#define COMM_OPT_OFF 0
#define COMM_SEND_QUEUE_WITH_DPDP_MPSC COMM_OPT_OFF

/***********************************************************************************
 * Socket function related interface and data structures
 ***********************************************************************************
 */
/*
 * Notice:
 * 1. only proxy conns have comm_sock_desc struct to save infos
 * 2. kernel proxy and libnet proxy maybe both used, eg. some communicators use libnet, other communicators use kernel
 * 3. if epoll fd is proxy, its listen fd can be noproxy fd but must be same phy_proto_type
 */
typedef enum CommConnType {
    CommConnKernel = 0,
    CommConnLibnet,
    /* proxy conn may have different protocol type */
    CommConnProxy,
    CommConnMax
} CommConnType;
extern CommConnType CommGetConnType(int sockfd);

#define AmIProxyModeType(t) (t == CommConnProxy)
#define AmISupportProxyMode() (g_comm_controller != NULL)
/* for some storage socket api, we not support with proxy and libnet mode */
#define AmINoKernelModeSockfd(fd) (CommGetConnType(fd) != CommConnKernel)
#define AmIProxyModeSockfd(fd) (CommGetConnType(fd) == CommConnProxy)

#define AmIKernelModeSockfd(fd) (CommGetConnType(fd) == CommConnKernel)
#define AmILibnetNoProxyModeSockfd(fd) (CommGetConnType(fd) == CommConnLibnet)

extern void comm_init_api_hook();

extern CommSocketOption *CommGetConnOption();
extern void CommSetConnOption(CommConnSocketRole role, CommConnAttr attr, CommConnNumaMap map, int group_id);
extern void CommSetEpollOption(CommEpollType type);
extern void CommSetPollOption(bool no_proxy, bool no_ctx_switch, int threshold);
extern CommPollOption *CommGetPollOption();

extern void CommFilterOptionByIpPort(char *ip, int port, CommSocketOption *option);

extern int comm_socket(int domain, int type, int protocol);
extern int comm_connect(int sockfd, const struct sockaddr* addr, socklen_t addrlen);
extern int comm_bind(int sockfd, const struct sockaddr* addr, socklen_t addrlen);
extern int comm_listen(int sockfd, int backlog);
extern int comm_accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen);
extern int comm_accept4(int sockfd, struct sockaddr *addr, socklen_t *addrlen, int flags);

extern int comm_close(int sockfd);
extern int comm_closesocket(int sockfd);
extern int comm_shutdown(int sockfd, int how);
extern int comm_setsockopt(int sockfd, int level, int option_name, const void* option_value, socklen_t option_len);
extern int comm_getsockopt(int sockfd, int level, int option_name, void* option_value, socklen_t* option_len);
extern int comm_getpeername(int sockfd, struct sockaddr* addr, socklen_t* addrlen);
extern int comm_gethostname(char* hostname, size_t size);
extern int comm_getsockname(int sockfd, struct sockaddr* addr, socklen_t* addrlen);
extern int comm_ioctl(int fd, int request, ...);
extern int comm_fcntl(int fd, int request, long arg = 0);

/* IO api */
extern ssize_t comm_read(int sockfd, void* buf, size_t count);
extern ssize_t comm_write(int sockfd, const void* buf, size_t count);
extern ssize_t comm_send(int sockfd, const void* buf, size_t len, int flags = 0);
extern ssize_t comm_recv(int sockfd, void* buf, size_t len, int flags = 0);

extern ssize_t comm_sendto(
    int sockfd, const void* buf, size_t len, int flags, const struct sockaddr* dest_addr, socklen_t addrlen);
extern ssize_t comm_recvfrom(
    int sockfd, void* buf, size_t len, int flags, struct sockaddr* src_addr, socklen_t* addrlen);

extern ssize_t comm_sendmsg(int sockfd, const struct msghdr* msg, int flags);
extern ssize_t comm_recvmsg(int sockfd, struct msghdr* msg, int flags);

/* select & poll api */
extern int comm_poll(struct pollfd* fds, nfds_t nfds, int timeout);
extern int comm_select(int fds, fd_set* readfds, fd_set* writefds, fd_set* exceptfds, struct timeval* timeout);

/* epoll api */
extern int comm_epoll_create(int size);
extern int comm_epoll_create1(int flag);
extern int comm_epoll_ctl(int epfd, int op, int fd, struct epoll_event* event);
extern int comm_epoll_wait(int epfd, struct epoll_event* events, int maxevents, int timeout);
extern int comm_epoll_pwait(int epfd, struct epoll_event* events, int maxevents, int timeout, const sigset_t *sigmask);


/* gaussdb kernel and gaussdbnetwork */
typedef pthread_t ThreadId;


/***********************************************************************************
 * Start comm library related interface and data structures
 ***********************************************************************************
 */
typedef enum ErrorLevel {
    COMM_DEBUG2 = 0,
    COMM_DEBUG1 = 1,
    COMM_DEBUG_DESC = 2,
    COMM_INFO = 3,
    COMM_INFO_DESC = 4,
    COMM_LOG = 5,
    COMM_WARNING = 6,
    COMM_ERROR = 7,

    ERROR_MAX
} ErrorLevel;

extern ErrorLevel min_debug_level;

#define MAX_LTRAN_CLIENT 4
#define LOCAL_PORT_NUM 2
#define COMM_DEBUG COMM_DEBUG1
#define MAX_CONTINUOUS_COUNT 10
#define INIT_TX_ALLOC_BUFF_NUM 5

#if (!defined WITH_OPENEULER_OS) && (!defined OPENEULER_MAJOR)
extern int gettimeofday(struct timeval* tp, struct timezone* tzp);
#endif
//extern THR_LOCAL knl_thrd_context t_thrd;

#define COMM_ELOG(elevel, format, ...)                     \
    do {                                                   \
        if (elevel < min_debug_level) {                    \
            break;                                         \
        }                                                  \
        struct timeval tv; \
        pg_time_t stamp_time; \
        (void)gettimeofday(&tv, NULL); \
        stamp_time = (pg_time_t) tv.tv_sec; \
        const char* tag = NULL;                            \
        char _out_buff[2048];                              \
        switch (elevel) {                                  \
            case COMM_DEBUG2:                              \
                tag = "DEBUG2";                            \
                break;                                     \
            case COMM_DEBUG:                               \
                tag = "DEBUG";                             \
                break;                                     \
            case COMM_INFO:                                \
                tag = "INFO";                              \
                break;                                     \
            case COMM_LOG:                                 \
                tag = "LOG";                               \
                break;                                     \
            case COMM_WARNING:                             \
                tag = "WARNING";                           \
                break;                                     \
            case COMM_ERROR:                               \
                tag = "ERROR";                             \
                break;                                     \
            case COMM_DEBUG_DESC:                          \
                tag = "";                                  \
                break;                                     \
            case COMM_INFO_DESC:                           \
                tag = "";                                  \
                break;                                     \
            default:                                       \
                tag = "UNKNOW";                            \
        }                                                  \
        if (strlen(tag) == 0) {                            \
            printf(format, ##__VA_ARGS__);                 \
        } else {                                           \
            sprintf(_out_buff, "[COMMPROXY] ");            \
            sprintf(_out_buff + 12, "%7s: ", tag);         \
            sprintf(_out_buff + 21, "%18lu ", t_thrd.proc_cxt.MyProcPid); \
            if (session_timezone != NULL) { \
                (void)pg_strftime(_out_buff + 40, 128, "%Y-%m-%d %H:%M:%S     %Z ", pg_localtime(&stamp_time, session_timezone)); \
                sprintf(_out_buff + 40 + 19, ".%03d ", (int)(tv.tv_usec / 1000)); \
            } \
            sprintf(_out_buff + 40 + 24, format, ##__VA_ARGS__);\
            printf("%s", _out_buff);                       \
        }                                                  \
        fflush(stdout); \
    } while (0)

#define MAX_LTRAN_CORE_NUM 2
#define MAX_PROXY_CORE_NUM 8
#define CPU_NUM_PER_NUMA 2

#define MAX_PORT_NUM 4
#define THREADPOOL_TOTAL_GROUP 8
#define THREADPOOL_GROUPIRQ_NUMBER 4
#define THREADPOOL_GROUPCPU_NUMBER 32
#define THREADPOOL_TOTAL_CPU_NUMBER (THREADPOOL_GROUPCPU_NUMBER * THREADPOOL_TOTAL_GROUP)

#define THREADPOOL_TOTAL_WORKER_NUMBER (28 * 4 * 2)

#define MAX_SERVING_WORKERS 1024
#define MAX_LSTACK_CONF 256

#define DEFAULT_WORKER_BIND_MODE BIND_CPU_MODE_GROUP
#define MAX_STAT_PACKET_LENGTH (128 * 1024 * 1024)

#define MAX_IP_POR_STR_LEN 50
#define MAX_FILTER_NUM 10
#define MAX_FILTER_STR_LEN 30
typedef struct CommProxyFilter {
    bool s_isvaild;
    char s_noproxy[MAX_FILTER_NUM][MAX_FILTER_STR_LEN];
    int s_noproxy_num;
} CommProxyFilter;

/*
 * cpu core number start with 1, like as htop, we use 0 as invalid
 * but user set config with 0-127, we add 1 when parse config
 */
typedef struct CommProxyInstance {
    /* read from config file */
    int s_comm_type;
    int s_comms[THREADPOOL_TOTAL_GROUP][MAX_PROXY_CORE_NUM];
    char s_listen[MAX_FILTER_NUM][MAX_FILTER_STR_LEN];
    char s_connect[MAX_FILTER_NUM][MAX_FILTER_STR_LEN];

    /* generate by rules */
    int s_comm_nums;
    int s_listen_cnt;
    int s_connect_cnt;
    CommConnNumaMap s_gen_numa_map;
} CommProxyInstance;

typedef struct CommPureInstance {
    int s_comm_type;
    int s_comm_node; /* pure connect is single thread, we bind to node rather than core */
    char s_listen[MAX_FILTER_NUM][MAX_FILTER_STR_LEN];
    char s_connect[MAX_FILTER_NUM][MAX_FILTER_STR_LEN];

    /* generate by rules */
    int s_listen_cnt;
    int s_connect_cnt;
    CommConnNumaMap s_gen_numa_map;
} CommPureInstance;

typedef struct CommLstackConfig {
    char lstack_conf[MAX_LSTACK_CONF];
    int local_port[LOCAL_PORT_NUM];
    bool start_ltran;
    int ltran_num;
    int ltran_node[MAX_LTRAN_CLIENT];
    int ltran_cores[MAX_LTRAN_CLIENT][2]; /* per ltran needs up/down 2 cores */
} CommLstackConfig;

typedef enum CommSendXlogMode {
    CommSendXlogNormal = 0,
    CommSendXlogWaitIn,
    CommSendXlogProxy
} CommSendXlogMode;
typedef struct CommProxyConfig {
    int s_comm_type;
    bool s_enable_libnet;
    int s_debug_level;
    bool s_enable_dfx;

    CommLstackConfig s_comm_lstack_config;
    int s_comm_proxy_group_cnt;
    int s_numa_num;
    CommProxyInstance s_comm_proxy_groups[MAX_COMM_PROXY_GROUP_CNT];

    int s_comm_pure_group_cnt;
    CommPureInstance s_comm_pure_groups[MAX_COMM_PURE_GROUP_CNT];

    char s_repl_direct_conn[MAX_FILTER_STR_LEN];

    bool s_bypass_standby_flush;

    /*
     * 0: normal mode
     * 1: send_all_data_mode
     * 2: proxy mode
     */
    int s_send_xlog_mode;
} CommProxyConfig;

typedef struct CoreBindConfig {
    bool enable_libnet;
    bool start_ltran;
    int numa_node[MAX_LTRAN_CLIENT];
    char lstack_conf[MAX_LSTACK_CONF];
    int ltran[MAX_LTRAN_CORE_NUM];
    int comm[THREADPOOL_TOTAL_GROUP][MAX_PROXY_CORE_NUM];
    char worker[THREADPOOL_TOTAL_GROUP][THREADPOOL_GROUPCPU_NUMBER];
    int worker_per_comm;
    int comm_num[THREADPOOL_TOTAL_GROUP];
    int ltran_num;
    int worker_num[THREADPOOL_TOTAL_GROUP];
    ErrorLevel debug_level;
    int ltran_client_num;
    int local_port[LOCAL_PORT_NUM];

    /* temp config */
    char repl_direct_conn[MAX_FILTER_STR_LEN];
} CoreBindConfig;

typedef struct ThreadAttr {
    int s_threads_num;
    int s_group_num;
} ThreadAttr;

typedef struct CommProxyInvokeResult {
    int s_ret;
} CommProxyInvokeResult;

extern CoreBindConfig g_core_bind_config;
extern CommProxyConfig g_comm_proxy_config;

extern bool CommProxyNeedSetup();
extern bool IsCommProxyStartUp();
extern void CommProxyStartUp();

extern void ParseThreadPoolAttr(ThreadAttr *thread_attr);
extern bool ParseCommProxyAttr(CommProxyConfig* config);

extern int CommLibNetPrepareEnv();
extern int CommCheckLtranProcess();

/***********************************************************************************
 * Base data type define and atomic interface related
 ***********************************************************************************
 */
//extern bool g_start_ltran_ctl
typedef unsigned int uint32;
typedef signed int int32;
extern uint64 comm_atomic_add_fetch_u64(volatile uint64* ptr, uint32 inc);
extern uint32 comm_atomic_add_fetch_u32(volatile uint32* ptr, uint32 inc);
extern uint64 comm_atomic_fetch_sub_u64(volatile uint64* ptr, uint32 inc);
extern uint32 comm_atomic_fetch_sub_u32(volatile uint32* ptr, uint32 inc);
extern uint64 comm_atomic_read_u64(volatile uint64* ptr);
extern uint32 comm_atomic_read_u32(volatile uint32* ptr);
extern void comm_atomic_wirte_u32(volatile uint32* ptr, uint32 val);
extern bool comm_compare_and_swap_32(volatile int32* dest, int32 oldval, int32 newval);

#if defined(__aarch64__)
#define gaussdb_memory_barrier_dsb(opt) __asm__ __volatile__("DMB " #opt::: "memory")
#define gaussdb_memory_barrier() gaussdb_memory_barrier_dsb(ish)
#define gaussdb_read_barrier() gaussdb_memory_barrier_dsb(ishld)
#define gaussdb_write_barrier() gaussdb_memory_barrier_dsb(ishst)

#if defined (__USE_NUMA)
#define gaussdb_numa_memory_bind(i) \
    do { \
        struct bitmask *nodeMask = numa_allocate_nodemask(); \
        numa_bitmask_setbit(nodeMask, (i)); \
        numa_bind(nodeMask); \
        numa_free_nodemask(nodeMask); \
    } while (0);

#define gaussdb_numa_memory_unbind() \
    do { \
        numa_bind(numa_all_nodes_ptr); \
    } while (0);
#else
#define gaussdb_numa_memory_bind(i)
#define gaussdb_numa_memory_unbind()
#endif

#elif defined(__x86_64__)
#define gaussdb_memory_barrier() \
    __asm__ __volatile__ ("lock; addl $0,0(%%rsp)" : : : "memory")

#define gaussdb_read_barrier() gaussdb_memory_barrier()
#define gaussdb_write_barrier() gaussdb_memory_barrier()

#define gaussdb_numa_memory_bind(i)
#define gaussdb_numa_memory_unbind()

#elif defined(__i386__)
#define gaussdb_memory_barrier() \
    __asm__ __volatile__ ("lock; addl $0,0(%%esp)" : : : "memory")

#define gaussdb_numa_memory_bind(i)
#define gaussdb_numa_memory_unbind()

#else
#define gaussdb_memory_barrier() __asm__ __volatile__("" ::: "memory")
#define gaussdb_read_barrier() gaussdb_memory_barrier()
#define gaussdb_write_barrier() gaussdb_memory_barrier()

#define gaussdb_numa_memory_bind(i)
#define gaussdb_numa_memory_unbind()

#endif


#define WaitCondPositive(cond, interval) \
    {                                    \
        while (!(cond)) {                \
            pg_usleep(interval);         \
        }                                \
    }

#define WaitCondPositiveWithTimeout(cond, interval, timeout) \
        {                                    \
            int timegap = 0;                 \
            while (!(cond)) {                \
                pg_usleep(interval);         \
                timegap += interval;         \
                if (timegap >= timeout) {    \
                    break;                   \
                }                            \
            }                                \
        }


#define MAX_PROXY_THREAD_NUMS 64
#define PROXY_RET_OK 0
#define PROXY_RET_FAIL 1
#define PROXY_RET_NOTIMPL 2

#define comm_static_set(x, v) \
    do { \
        (x) = (v); \
    } while (0);

#define comm_static_inc(x) \
    do { \
        (x)++; \
    } while (0);

#define comm_static_dec(x) \
    do { \
        (x)--; \
    } while (0);

#define comm_static_add(x, v) \
    do { \
        (x) = (x) + (v); \
    } while (0);

#define comm_static_sub(x, v) \
    do { \
        (x) = (x) - (v); \
    } while (0);

#define comm_static_atomic_inc(x) \
    do { \
        comm_atomic_add_fetch_u32((x), 1); \
    } while (0)

#define comm_static_atomic_dec(x) \
    do { \
        comm_atomic_fetch_sub_u32((x), 1); \
    } while (0)

#define comm_static_atomic_get(x) \
    comm_atomic_read_u32((x))

#define init_req_static(req) \
    do { \
        (req)->s_prepare_num = 0; \
        (req)->s_ready_num = 0; \
        (req)->s_endnum = 0; \
        (req)->s_skipped_num = 0; \
    } while (0);

#define COMM_DEBUG_EXEC(x) x;


typedef struct {
    int (*socket_fn)(int domain, int type, int protocol);
    int (*accept_fn)(int s, struct sockaddr*, socklen_t*);
    int (*accept4_fn)(int s, struct sockaddr*, socklen_t*, int flags);
    int (*bind_fn)(int s, const struct sockaddr*, socklen_t);
    int (*listen_fn)(int s, int backlog);
    int (*connect_fn)(int s, const struct sockaddr* name, socklen_t namelen);
    int (*getpeername_fn)(int s, struct sockaddr* name, socklen_t* namelen);
    int (*getsockname_fn)(int s, struct sockaddr* name, socklen_t* namelen);
    int (*setsockopt_fn)(int s, int level, int optname, const void* optval, socklen_t optlen);
    int (*getsockopt_fn)(int s, int level, int optname, void* optval, socklen_t* optlen);
    int (*close_fn)(int fd);
    int (*shutdown_fn)(int fd, int how);
    pid_t (*fork_fn)(void);
    ssize_t (*read_fn)(int fd, void* mem, size_t len);
    ssize_t (*write_fn)(int fd, const void* data, size_t len);
    int (*fcntl_fn)(int fd, int cmd, ...);
    int (*epoll_create_fn)(int size);
    int (*epoll_create1_fn)(int flags);
    int (*epoll_ctl_fn)(int epfd, int op, int fd, struct epoll_event* event);
    int (*epoll_wait_fn)(int epfd, struct epoll_event* events, int maxevents, int timeout);
    int (*epoll_pwait_fn)(int epfd, struct epoll_event* events, int maxevents, int timeout, const sigset_t *sigmask);
    int (*sigaction_fn)(int signum, const struct sigaction* act, struct sigaction* oldact);
    int (*poll_fn)(struct pollfd* fds, nfds_t nfds, int timeout);
    ssize_t (*send_fn)(int sockfd, const void* buf, size_t len, int flags);
    ssize_t (*recv_fn)(int sockfd, void* buf, size_t len, int flags);
    ssize_t (*addr_recv_fn)(int sockfd, void* buf, size_t len, int flags);
} gs_api_t;

extern const gs_api_t g_comm_socket_api[CommConnMax];

typedef enum IPAddrType {
    IpAddrTypeAny = 0,
    IpAddrTypeLoopBack = 1,
    IpAddrTypeBroadCast = 2,
    IpAddrTypeLocal = 3,
    IpAddrTypeOther = 4
} IPAddrType;

typedef enum CommFdTypeOpt {
    CommFdTypeSetHostOrLibnet = 0,
    CommFdTypeSetHost = 1,
    CommFdTypeSetLibNet = 2,
    CommFdTypeAddInprogress = 3,
    CommFdTypeDelHost = 4,
} CommFdType;

typedef enum CommQueueChannel {
    ChannelTX = 0,
    ChannelRX,

    /* Indicate the max number */
    TotalChannel
} CommQueueChannel;

typedef enum CommExecStep {
    comm_exec_step_uninit = 0,
    comm_exec_step_start,
    comm_exec_step_prepare,
    comm_exec_step_process,
    comm_exec_step_ready,
    comm_exec_step_done,
} CommExecStep;

typedef enum CommWaitNextStatus {
    CommWaitNextContinue = 0,
    CommWaitNextBreak
} CommWaitNextStatus;

typedef enum SocketRequestType {
    sockreq_socket = 1,
    sockreq_close,

    sockreq_accept,
    sockreq_accept4,
    sockreq_poll,
    sockreq_select,
    sockreq_bind,
    sockreq_listen,
    sockreq_setsockopt,
    sockreq_getsockopt,
    sockreq_getsockname,
    sockreq_getpeername,
    sockreq_fcntl,
    sockreq_epoll_create,
    sockreq_epollctl,
    sockreq_epollwait,
    sockreq_recv,
    sockreq_send,
    sockreq_connect,
    sockreq_shutdown,
    sockreq_max
} SocketRequestType;

typedef enum SocketHookType {
    socket_hook_before,
    socket_hook_after,
    socket_hook_max
} SocketHookType;

typedef enum CommProxyerStatus {
    CommProxyerUninit = 0,
    CommProxyerStart,
    CommProxyerReady,
    CommProxyerError,
    CommProxyerClosing,
    CommProxyerDie
} CommProxyerStatus;

/*
 * Communicator data structures forward declaration
 */
class CommSockDesc;
class ThreadPoolCommunicator;
struct Packet;
struct SocketRequest;

/*
 * Communicator Event definition
 */
typedef enum EventType {
    EventUknown = -1,
    EventRecv,
    EventListen,
    EventOut,
    EventThreadPoolListen,
} EventType;

typedef struct EventDataHeader {
    volatile int s_event_type;

    /* for datafd to listener/recv */
    CommSockDesc* s_sock;
    CommSockDesc* s_listener;
    epoll_data_t s_origin_event_data_ptr;
    uint32 s_origin_events;
    bool s_epoll_oneshot_active;
} EventDataHeader;

#define CommConnTypeShift 2
typedef enum EpollInputStatus {
    /* if epoll fd is kernel mode, we only support listen kernel fd */
    epoll_kernel_fd_kernel = CommConnKernel << CommConnTypeShift | CommConnKernel,

    /* if epoll fd is kernel proxy fd, we can listen kernel fd or kernel proxy fd */
    epoll_proxy_fd_kernel = CommConnProxy << CommConnTypeShift | CommConnKernel,
    epoll_proxy_fd_kernel_proxy = CommConnProxy << CommConnTypeShift | CommConnProxy,

    /* if epoll fd is libnet fd, we only support listen libnet fd */
    epoll_libnet_fd_libnet = CommConnLibnet << CommConnTypeShift | CommConnLibnet,
} EpollInputStatus;

/***********************************************************************************
 * CommAdapter related data structures
 ***********************************************************************************
 */
typedef void (*CommProcessSendFuncPtr)(ThreadPoolCommunicator *comm);

typedef int (*CommWorkerSendFuncPtr)(CommSockDesc* comm_sock, const char* buffer, int length);
typedef void (*CommProxySendFuncPtr)(ThreadPoolCommunicator *comm);
typedef int (*CommWorkerRecvFuncPtr)(CommSockDesc* sock_desc, char *buffer, int exp_length);
typedef void (*CommProxyRecvFuncPtr)(CommSockDesc* sock_desc);

typedef struct CommTransporter {
    CommWorkerSendFuncPtr comm_worker_send;
    CommProxySendFuncPtr comm_proxy_send;
    CommWorkerRecvFuncPtr comm_worker_recv;
    CommProxyRecvFuncPtr comm_proxy_recv;
} CommTransporter;

typedef enum CommTransportMode {
    CommModeDefault = 0,
    CommModeDoubleQueue,
    CommModeRingBuffer,
} CommTransportMode;

typedef enum CommEpollMode {
    CommEpollModeWakeup = 0,
    CommEpollModeSockList,
} CommEpollMode;

typedef enum CommProxyNotifyMode {
    CommProxyNotifyModeSem = 0,
    CommProxyNotifyModeSpinLoop,
    CommProxyNotifyModePolling,
} CommProxyNotifyMode;

typedef enum CommProxySendMode {
    CommProxySendModeReadyQueue = 0,
    CommProxySendModePolling,
} CommProxySendMode;

typedef enum SendStatus {
    SendStatusReady = 0,
    SendStatusPart,
    SendStatusError,
    SendStatusSuccess
} SockSendStatus;
typedef enum SockRecvStatus {
    RecvStatusReady = 0,
    RecvStatusClose,
    RecvStatusRetry,
    RecvStatusError,
    RecvStatusSuccess
} SockRecvStatus;

typedef struct CommSockDelay {
    int trigger_nums;
    int cur_delay_time;
    int max_trigger_nums;
    int min_trigger_nums;
} CommSockDelay;

/***********************************************************************************
 * CommSocket related data structures
 ***********************************************************************************
 */
#define InvalidCommFd -1;
#define MaxPacketLength 1024
/* socket buffer 256*1024 */
#define DefaultSocketRingBufferSize (16 * 1024 * 1024)
/* ring buffer set max to 128MB,  */
#define MaxSocketRingBufferSize (128 * 1024 * 1024)
#define MaxPbufNum (65536)   //must be 2^n

class CommTransPort : public BaseObject
{
public:
    int m_fd;
    CommQueueChannel m_type;
    ErrorLevel m_debug_level;

public:
    virtual bool CheckBufferEmpty() = 0;
    virtual bool CheckBufferFull() = 0;
    virtual size_t GetBufferDataSize() = 0;
    virtual int GetDataFromBuffer(char* s, size_t length) = 0;
    virtual int PutDataToBuffer(const char *s, size_t length) = 0;
    virtual SendStatus SendDataFromBuffer(int fd, ThreadPoolCommunicator *comm) = 0;
    virtual SockRecvStatus RecvDataToBuffer(int fd, ThreadPoolCommunicator *comm) = 0;
    virtual void PrintBufferDetail(const char *info) = 0;
    virtual bool NeedWait(bool block) = 0;
    virtual void WaitBufferEmpty() = 0;
    virtual void Init(int fd, CommQueueChannel type) = 0;
    virtual void DeInit() = 0;

    CommTransPort() {};
    virtual ~CommTransPort() {};
};

typedef enum TransportStatus {
    TransportNormal = 0,
    TransportBusy,
    TransportChange,
    TransportChangeDone
} TransportStatus;

typedef struct RingBufferGroup {
    char *m_comm_buffer;
    volatile uint32_t m_comm_buffer_size;
    volatile uint32_t m_read_pointer;
    volatile uint32_t m_write_pointer;
} RingBufferGroup;

class CommRingBuffer: public CommTransPort
{
public:
    RingBufferGroup *m_buffer;
    RingBufferGroup *m_enlarge_buffer;

    pthread_mutex_t m_mutex;
    pthread_cond_t m_cv;
    bool m_isempty;
    volatile TransportStatus m_status;
    CommSockDelay m_delay;
    /* proxyer notify worker to recv/send data */
    CommProxyNotifyMode m_notify_mode;

public:
    CommRingBuffer();
    ~CommRingBuffer();
    void Init(int fd, CommQueueChannel type);
    void DeInit();
    bool CheckBufferEmpty();
    bool CheckBufferFull();
    size_t GetBufferFreeBytes(int r, int w, int size);
    size_t GetBufferDataSize();
    int CopyDataFromBuffer(char* s, size_t length, size_t offset);
    int GetDataFromBuffer(char* s, size_t length);
    int PutDataToBuffer(const char *s, size_t length);
    SendStatus SendDataFromBuffer(int fd, ThreadPoolCommunicator *comm);
    SockRecvStatus RecvDataToBuffer(int fd, ThreadPoolCommunicator *comm);
    void PrintBufferDetail(const char *info);
    bool NeedWait(bool block);
    void NotifyWorker(SockRecvStatus rtn_status);
    void WaitBufferEmpty();
    void SetNotifyMode(CommProxyNotifyMode mode);
    bool EnlargeBufferSize(uint32 length);

    inline void ResetDelay()
    {
        m_delay.cur_delay_time = 0;
    };
};

#ifdef USE_LIBNET
class CommRingBufferLibnet: public CommRingBuffer
{
public:
    uint32_t m_buffaddr_pointer;
    uint32_t m_curre_buff_num;
    uint32_t m_minus_buff_num_counter;
    uint32_t m_local_read_pointer;
    volatile uint32_t m_curr_data_len;
    struct pkt_buff_info *m_comm_buffer;

public:
    CommRingBufferLibnet();
    ~CommRingBufferLibnet();
    void Init(int fd, CommQueueChannel type);
    void DeInit();
    size_t GetBufferDataSize();
    void CommReleaseUsedBuff(void *mem);
    int CopyDataFromBuffer(char* s, size_t length, size_t offset);
    int GetDataFromBuffer(char* s, size_t length);
    int PutDataToBuffer(const char *s, size_t length);
    SendStatus SendDataFromBuffer(int fd, ThreadPoolCommunicator *comm);
    SockRecvStatus RecvDataToBuffer(int fd, ThreadPoolCommunicator *comm);
};
#endif

class CommPacketBuffer: public CommTransPort
{
public:
    boost::lockfree::queue<Packet *> *m_data_queue;
    Packet* m_recv_packet;
    sem_t m_data_queue_sem;

public:
    CommPacketBuffer();
    ~CommPacketBuffer();
    void Init(int fd, CommQueueChannel type);
    void DeInit();

public:
    bool CheckBufferEmpty();
    bool CheckBufferFull();
    size_t GetBufferFreeBytes(int r, int w);
    size_t GetBufferDataSize();
    int GetDataFromBuffer(char* s, size_t length);
    int PutDataToBuffer(const char *s, size_t length);
    SendStatus SendDataFromBuffer(int fd, ThreadPoolCommunicator *comm);
    SockRecvStatus RecvDataToBuffer(int fd, ThreadPoolCommunicator *comm);
    void PrintBufferDetail(const char *info);
    bool NeedWait(bool block);
    void WaitBufferEmpty();

public:
    inline Packet* GetHeadPacket()
    {
        Packet *pkt_point = NULL;
        m_data_queue->pop(pkt_point);
        if (pkt_point == NULL) {
            return NULL;
        }
        return pkt_point;
    };

    inline void PopHeadPacket()
    {
        Packet *p = NULL;
        m_data_queue->pop(p);
    };

    inline Packet* RemoveHeadPacket()
    {
        Packet* p = NULL;
        if (m_data_queue->pop(p)) {
            return p;
        }

        return NULL;
    };

    inline void AddTailPacket(Packet* p)
    {
        m_data_queue->push(p);
    };

private:
    void BackPktBuff(Packet* pack);
    void ClearQueuePacket();
};

typedef enum CommSockType {
    /* For report error */
    CommSockUnknownType = -1,

    /* Operates at server side */
    CommSockServerFd, /* returned by comm_socket()       */
    CommSockEpollFd,  /* returned by comm_epoll_create() */
    CommSockServerDataFd,   /* returned by comm_poll()/comm_accept()/comm_connect() */

    /* communicator library internal type */
    CommSockCommEpollFd,
    CommSockCommServerFd,

    CommSockClientDataFd,

    CommSockFakeLibosFd,
} CommSockType;

typedef struct CommFdInfoNode {
    int fd;
    int fdNums;
    struct CommFdInfoNode *next;
} CommFdInfoNode;

#define AmCommDataFd(comm_sock) ((comm_sock)->m_fd_type == CommSockClientDataFd || (comm_sock)->m_fd_type == CommSockServerDataFd || (comm_sock)->m_fd_type == CommSockFakeLibosFd)
#define AmCommServerFd(comm_sock) ((comm_sock)->m_fd_type == CommSockServerFd)

#define IgnoreErrorNo(x) ((x) == 0 || (x) == EAGAIN || (x) == EWOULDBLOCK || (x) == EINTR || (x) == EINPROGRESS)
typedef enum CommSockStatus {
    CommSockStatusUninit = 0, /* created by socket */
    CommSockStatusReady, /* created by connect/accept/bind */
    CommSockStatusListening,   /* called by listen */
    CommSockStatusRegistToEpoll,  /* called by comm_epoll_ctl add() */
    CommSockStatusRemovedFromEpoll,  /* called by comm_epoll_ctl del() */
    CommSockStatusClosedByPeer,  /* called by recv return 0 add del from epoll() */
    CommSockStatusClosedBySelf,  /* called by comm_close() */
    CommSockStatusNeedRetry,
    CommSockStatusNormal,
    CommSockStatusProcessedData, /* fd has recv/send data */
    CommSockStatusDie,
    CommSockStatusWorkerGetClose
} CommSockStatus;

typedef enum CommSockPollStatus {
    CommSockPollUnRegist = 0,
    CommSockPollRegisted,
    CommSockPollReady,
} CommSockPollStatus;

typedef enum CommSockListenType {
    CommSockListenNone = 0,
    CommSockListenByPoll,
    CommSockListenByEpoll,
} CommSockListenType;

typedef enum CommSockProcessType {
    CommSockProcessInit = 0,
    CommSockProcessProxyRecving,
    CommSockProcessProxyRecvend,
    CommSockProcessProxySending,
    CommSockProcessProxySendend,
} CommSockProcessType;

typedef enum CommSockConnectStatus {
    CommSockConnectUninit = 0,
    CommSockConnectInprogressBlock,
    CommSockConnectInprogressNonBlock,
    CommSockConnectInprogressNonBlockPollListen,
    CommSockConnectSuccess,
    CommSockConnectFail,
} CommSockConnectStatus;
#define ConnectEnd(x) ((x) == CommSockConnectSuccess || (x) == CommSockConnectFail)

#define COMM_MIN_DELAY_USEC 10
#define COMM_MAX_DELAY_USEC 100000
typedef struct CommSockNode {
    CommSockDesc *s_comm_sock;
    struct CommSockNode *s_next_node;
    struct CommSockNode *s_prev_node;
} CommSockNode;

class CommSockList : public BaseObject {
public:
    List* m_sock_list;
    CommSockDesc *m_comm_epoll_sock;

public:
    CommSockList(CommSockDesc *epoll_sock);
    ~CommSockList();

    void addCommSock(CommSockDesc *comm_sock);
    void delCommSock(CommSockDesc *_comm_sock);
    int getCommEpollEvents(struct epoll_event* events, int events_num);
    bool findCommSock(bool with_lock, int fd);

private:
    pthread_mutex_t m_mutex;
};

/*
 * - brief: CommSockDesc is the backend supporting data structure at Comm-Poroxy,
 *          it is searched/located by user side fd as key (s_fd_key), normally, we
 *          have several case for CommSockDesc serverfd, epoll_fd, datafd
 */
class CommSockDesc : public BaseObject {
public:
    /* "sock fd" of curernt object served */
    int m_fd;

    /* type socket the current of CommSockDesc */
    CommSockType m_fd_type;
    ThreadPoolCommunicator* m_communicator; /* valid in datafd */
    int m_group_id;                         /* valid in epollfd & datafd */

    CommSockListenType m_listen_type;

    /* some status maybe happen at sametime, TODO:bit and/or */
    CommSockStatus m_status;
    int m_errno;
    bool m_not_need_del_from_epoll;

    /*
     * for datafd, if accept, parent is serverfd
     *             if connect, parent is null
     * for internal serverfd, parent is logical serverfd
     * for logical serverfd and epoll fd, parent is null
     */
    CommSockDesc *m_parent_comm_sock;

    /****
     * - brief: CommServerFd maintaince fields
     */
    int m_server_fd_cnt;     /* number of actual fd curernt serverfd contained */
    int* m_server_fd_arrays; /* array of curernt serverfd contained */
    volatile uint32_t m_server_income_ready_num;
    int m_last_index;

    /****
     * - brief: CommInternalServerFd
     */
    struct pollfd* m_fds;
    int m_nfds;
    CommSockPollStatus m_poll_status;

    /****
     * - brief: CommEpollFd maintaince fields
     */
    boost::lockfree::queue<struct epoll_event*>* m_epoll_queue; /* (n)p(1)c eventg queue */
    volatile uint32 m_events_num;
    sem_t m_epoll_queue_sem;                                    /* PV operation sem */
    /* no-threadpool mode */
    CommSockList *m_epoll_list;
    bool m_need_kernel_call;
    /*
     * epoll get close status by m_status, but m_status we assume it only changed by proxy
     * so we need a flag to avoid catch EPOLLERR event repeat
     * because worker process is async, err will release sock resource, for next process it will core or other exception
     * for EVENTIN/EVENTOUT, if oneshot, we will only catch once
     *              if no-oneshot, we catch more than once is normal
     * the reason we set catch flag not remove form m_epoll_list is to be consistent with the kernel call behavior
     */
    bool m_epoll_catch_error;

    /*
     * Support a EpollFD at application level registered with proxy-fd and regular fd,
     * We normally see such kind of case in Postmaster's ServerLoop() where unix domain
     * and network-fd(via proxy) is registerred here
     */
    int m_epoll_real_fd;

    /****
     * - brief: CommDataFd maintaince fields
     */
    EventDataHeader m_event_header;
    int m_worker_id;
    bool m_send_idle;
    CommTransPort* m_transport[TotalChannel];
    CommTransportMode m_trans_mode;

    /* for poll data check */
    unsigned int m_data_threshold[TotalChannel];

    /* for recv data delay */
    volatile CommSockProcessType m_process;
    bool m_block;
    /* for noblock mode to check connect success */
    std::mutex m_connect_mutex;
    std::condition_variable m_connect_cv;
    CommSockConnectStatus m_connect_status;

    /*
     * CommProy Dfx support
     * - brief: static per fd s/r packet cnt for each length
     */
    bool m_enable_packet_static;
    uint64 *m_static_pkt_cnt_by_length[TotalChannel];
    uint64  m_wakeup_cnt;

public:
    CommSockDesc();
    virtual ~CommSockDesc();
    void Init(int fd, CommSockType type, ThreadPoolCommunicator* comm, CommSockDesc* parent);
    void DeInit();

    inline int GetCurrentFd() const
    {
        return m_fd;
    };

    inline bool CheckActive()
    {
        return (m_worker_id >= 0);
    };

    inline int GetWorkerId() const
    {
        Assert(m_worker_id >= 0);
        return m_worker_id;
    };

    inline size_t GetQueueCnt(int channel)
    {
        return m_transport[channel]->GetBufferDataSize();
    };

    inline void PushEpollEvent(struct epoll_event* listen_event)
    {
        comm_atomic_add_fetch_u32(&m_events_num, 1);
        bool need_post = m_epoll_queue->empty();
        m_epoll_queue->push(listen_event);
        if (need_post) {
            sem_post(&m_epoll_queue_sem);
        }
    };

    inline struct epoll_event* PopEpollEvent()
    {
        struct epoll_event* ev = NULL;
        bool ret = m_epoll_queue->pop(ev);
        if (ret) {
            Assert(ev != NULL);
            comm_atomic_fetch_sub_u32(&m_events_num, 1);
        }

        return ev;
    };

    inline size_t GetEventNums()
    {
        return comm_atomic_read_u32(&m_events_num);
    };

    /*
     * recv queue we need clear if connection close
     * send queue can not clear, send is async, data should be sent over
     */
    void ClearSockQueue();
    void WaitQueueEmpty(int channel);
    void BackPktBuff(int channel, Packet* pack);

    void InitDataStatic();
    void PrintDataStatic();
    void UpdateDataStatic(size_t len, int type);

    void SetPollThreshold(int channel, int threshold);
};

typedef struct ReqStatic {
    volatile unsigned int s_prepare_num;
    volatile unsigned int s_ready_num;
    volatile unsigned int s_endnum;
    volatile unsigned int s_skipped_num;
} ReqStatic;

/*
 * Users can specify multiple proxy combinations.
 * Each proxy can set different filtering parameters with core config filters
 */
class CommController : public BaseObject {
public:
    ThreadPoolCommunicator** m_communicators[MAX_COMM_PROXY_GROUP_CNT];
    int m_communicator_nums[MAX_COMM_PROXY_GROUP_CNT];

    ThreadPoolCommunicator** m_special_communicators;
    int m_special_communicator_nums;

    /* in proxy wait process, controller use poll and epoll wait to be compatible with both realfd and logical fd */
    phy_proto_type_t m_default_proto_type;
    gs_api_t m_default_comm_api;

    /* Global reference to mapping fd -> CommSockDesc */
    CommSockDesc** m_sockcomm_map_arr;

    CommTransportMode m_transport_mode;

    CommEpollMode m_epoll_mode;
    CommProxyNotifyMode m_notify_mode;
    CommProxySendMode m_ready_send_mode;

    volatile int m_ready_comm_cnt;

    int m_proxy_recv_loop_cnt;
    int m_proxy_send_loop_cnt;

    CommProcessSendFuncPtr m_process_send_func;

    /*
     * CommProxy Dfx Support
     *
     * - brief: statistic information for accept/poll/epoll events
     */
    ReqStatic m_accept_static;
    ReqStatic m_poll_static;
    ReqStatic m_epoll_recv_static;
    ReqStatic m_epoll_listen_static;
    ReqStatic m_epoll_all_static;
    volatile unsigned int m_active_worker_num;

public:
    CommController();
    ~CommController();

    int Init();
    int InitCommProxyGroup(int proxy_id, CommProxyInstance *proxy_instance);
    int GetCommPollEvents(    struct pollfd* fdarray, unsigned long nfds, int poll_ready_num);
    void RegistPollEvents(    struct pollfd* fdarray, unsigned long nfds);
    void ProxyWaitProcess(SocketRequestType* param);

    int GetNextDummyFd();
    bool IsServerfd(int fd);

    int WaitCommReady(int proxy_id);
    void ProxyerStatusReadyCallBack(CommProxyerStatus status);

    void SetCommSockNotifyMode(int fd, CommProxyNotifyMode mode);

    void SetCommSockIdle(int sockfd_key);
    void SetCommSockActive(int sockfd_key, int workerid);

    void BindThreadToGroupExcludeProxy(int fd);

    ThreadPoolCommunicator* GetCommunicator(int fd);
    ThreadPoolCommunicator* GetCommunicatorByGroup(int proxy_id, int group_id);
    int GetCommSockGroupId(int fd);

    int CommPollFdMarkSwitch(struct pollfd* fdarray, int nfds);

    inline CommSockDesc* FdGetCommSockDesc(int sockfd)
    {
        if (sockfd < 0 || !m_sockcomm_map_arr[sockfd] || m_sockcomm_map_arr[sockfd]->m_fd_type == CommSockUnknownType) {
            return NULL;
        }

        return m_sockcomm_map_arr[sockfd];
    };

    inline CommSockDesc* const* FdGetCommSockDescPtr(int sockfd)
    {
        if (sockfd < 0 || !m_sockcomm_map_arr[sockfd] || m_sockcomm_map_arr[sockfd]->m_fd_type == CommSockUnknownType) {
            return NULL;
        }

        return &m_sockcomm_map_arr[sockfd];
    };

    inline CommSockDesc* FdGetCommSockDescOrigin(int sockfd)
    {
        if (sockfd < 0) {
            return NULL;
        }
        return m_sockcomm_map_arr[sockfd];
    };

    inline void AddCommSockMap(CommSockDesc* comm)
    {
        int fd = comm->m_fd;
        if (m_sockcomm_map_arr[fd] == NULL) {
            m_sockcomm_map_arr[fd] = comm;
        }
        gaussdb_memory_barrier();
    };

    inline void DelCommSockMap(CommSockDesc* comm_sock)
    {
        ereport(LOG, (errmodule(MOD_COMM_PROXY), errmsg("delete commsock from map in DelCommSockMap.")));
        int fd = comm_sock->m_fd;
        if (fd < 0 || m_sockcomm_map_arr[fd] == NULL) {
            ereport(ERROR, (errmodule(MOD_COMM_PROXY),
                errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("fd:%d is already deleted or not be created.", fd),
                errdetail("N/A"),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
            return;
        }
        m_sockcomm_map_arr[fd] = NULL;
        gaussdb_memory_barrier();
    };

private:
    int m_dummy_fd_min;
    int m_dummy_fd_max;
    int m_dummy_fd_next;
};

extern CommController* g_comm_controller;

extern int gs_api_init(gs_api_t* gs_api, phy_proto_type_t proto);

/* memory api */
inline void* comm_malloc(size_t size)
{
    return palloc(size);
}

inline void* comm_malloc0(size_t size)
{
    return palloc0(size);
}

inline void* comm_repalloc(void* pointer, size_t size)
{
    return repalloc(pointer, size);
}

#define comm_free(ptr) \
    do {               \
        pfree(ptr);    \
    } while (0)

#define comm_free_ext(ptr) \
    do {                   \
        pfree(ptr);        \
        ptr = NULL;        \
    } while (0)

extern void CommStartProxyer(ThreadId *thread_id, ThreadPoolCommunicator *comm);
extern void CommStartProxyStatThread(CommController *controller);
extern void SetupCommProxySignalHook();

extern void init_commsock_recv_delay(CommSockDelay *delay);
extern void reset_commsock_recv_delay(CommSockDelay *delay);
extern void perform_commsock_recv_delay(CommSockDelay *delay);
extern CommWaitNextStatus comm_proxy_process_wait(struct timeval st, int timeout, bool ctx_switch, sem_t* s_queue_sem = NULL);
extern bool CommLibNetIsLibosFd(phy_proto_type_t proto_type, int fd);
extern int CommLibNetSetFdType(int fd, CommFdTypeOpt type);
extern IPAddrType CommLibNetGetIPType(unsigned int ip);
extern bool CommLibNetIsNoProxyLibosFd(int fd);
extern bool CommConfigSocketOption(CommConnSocketRole role, struct sockaddr *addr);
extern bool CommConfigSocketOption(CommConnSocketRole role, const char *ip, int port);


/*------------------------ Comm Proxy inteface definition -----------------------------*/
extern int comm_proxy_socket(int domain, int type, int protocol);
extern int comm_proxy_close(int fd);
extern int comm_proxy_shutdown(int fd, int how);
extern int comm_proxy_accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen);
extern int comm_proxy_accept4(int sockfd, struct sockaddr* addr, socklen_t* addrlen, int flags);
extern int comm_proxy_connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen);
extern int comm_proxy_bind(int sockfd, const struct sockaddr* ServerAddr, socklen_t addrlen);
extern int comm_proxy_listen(int sockfd, int backlog);
extern int comm_proxy_setsockopt(int sockfd, int level, int optname, const void* optval, socklen_t optlen);
extern int comm_proxy_getsockopt(int sockfd, int level, int optname, void* optval, socklen_t* optlen);
extern int comm_proxy_getsockname(int sockfd, struct sockaddr* addr, socklen_t* addrlen);
extern int comm_proxy_getpeername(int sockfd, struct sockaddr* addr, socklen_t* addrlen);
extern int comm_proxy_fcntl(int sockfd, int cmd, ...);
extern int comm_proxy_poll(struct pollfd* fdarray, unsigned long nfds, int timeout);
extern int comm_proxy_epoll_create(int size);
extern int comm_proxy_epoll_create1(int flag);
extern int comm_proxy_epoll_ctl(int epfd, int op, int fd, struct epoll_event* event);
extern int comm_proxy_epoll_wait(int epfd, struct epoll_event* events, int maxevents, int timeout);
extern int comm_proxy_epoll_pwait(int epfd, struct epoll_event* events, int maxevents, int timeout, const sigset_t *sigmask);
extern ssize_t comm_proxy_send(int socketfd, const void* buffer, size_t length, int flags);
extern ssize_t comm_proxy_recv(int socketfd, void* buffer, size_t length, int flags);
extern void comm_change_event(CommSockDesc* comm_sock, EventType event_type, struct epoll_event* ev);
extern ssize_t comm_proxy_addr_recv(int sockfd, void *buf, size_t len, int flags);
extern pid_t comm_proxy_fork(void);
extern ssize_t comm_proxy_read(int s, void* mem, size_t len);
extern ssize_t comm_proxy_write(int s, const void* mem, size_t size);
extern int comm_proxy_sigaction(int signum, const struct sigaction* act, struct sigaction* oldact);

typedef struct SockDescCreateAttr {
    CommSockType type;
    ThreadPoolCommunicator* comm;
    CommSockDesc* parent;
    union {
        struct {
            int comm_num;
        } server;
        struct {
            int size;
            int group_id;
        } epoll;
        int other;
    } extra_data;
} SockDescCreateAttr;
extern CommSockDesc* CommCreateSockDesc(int fd, SockDescCreateAttr* attr);
extern void CommReleaseSockDesc(CommSockDesc **comm_sock);

/***********************************************************************************
 * CommMonitor related data structures
 ***********************************************************************************
 */
extern void ProcessCommMonitorMessage(int fd);

typedef enum ParseMonitorType {
    ParseMonitorTypeCheck = 0,
    ParseMonitorTypeSet
} ParseMonitorType;

typedef struct ParseMonitorCmd {
    int (*func)(char *recv_buffer, char *send_buffer, ParseMonitorType type); /* parse function */
    const char* name;          /* mode name to use from command line as an argument */
    const char* shorts;        /* short name */
    const char* note;          /* mode description */
} ParseMonitorCmd;

extern ParseMonitorCmd g_parse_monitor_cmd[];

#define COMM_API_____________________
extern bool LibnetAddConnectWaitEntry(int epfd, int sock, int forRead, int forWrite, int opt);

#define MAX_SOCKET_FD_NUM (5)
#define MAX_PKT_TRANS_CNT (5000 * 1000) /* 29k */
extern unsigned long int g_static_pkt_cnt_by_length[TotalChannel][MAX_PKT_TRANS_CNT];
extern int g_static_pkt_cnt_idx[TotalChannel];
extern void comm_update_packet_static(size_t len, int type);

#define WalSenderEnter 0
#define WalSenderWakeup 1
#define WalSenderCnt 2
extern unsigned long int g_static_walsender_wakeup[WalSenderCnt][MAX_PKT_TRANS_CNT];
extern int g_static_walsender_cnt_idx[WalSenderCnt];


extern void comm_update_walsender_static(int type, int len);

#define WalRcvWriterWrite 0
#define WalRcvWriterExpectWrite 1
#define WalRcvWriterFlush 2
#define WalRcvWriterStat 3
#define WalRcvWriterStatMax (5000 * 1000) /* per 3min tpcc has 290k io */
extern unsigned long int g_static_walrcvwriter_iostat[WalRcvWriterStat][WalRcvWriterStatMax];
extern int g_static_walrcvwriter_idx[WalRcvWriterStat];

extern void comm_update_walrcvwriter_static(int type, int len);

extern bool CommGetIpPortFromSockAddr(struct sockaddr *addr, char *ip, int *port);

extern unsigned long int g_fd_pkt_length_by_loop[MAX_SOCKET_FD_NUM][TotalChannel][MAX_PKT_TRANS_CNT];
extern int g_fd_pkt_length_idx[MAX_SOCKET_FD_NUM][TotalChannel];
extern void comm_reset_packet_length_static();
extern void comm_update_packet_length_static(int fd, size_t len, int type);

extern void comm_print_packet_static();
extern void comm_print_walsender_static();
extern void comm_print_walrcvwriter_static();
extern void comm_print_packet_lenght_static();

extern int comm_add_static_fd(int fd);

#endif /* COMMPROXY_INTERFACE_H */
