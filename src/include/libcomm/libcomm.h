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
 * ---------------------------------------------------------------------------------------
 * 
 * libcomm.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/libcomm/libcomm.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef _GS_LIBCOMM_H_
#define _GS_LIBCOMM_H_

#include <stdio.h>
#include <netinet/in.h>
#include "sys/epoll.h"
#ifndef WIN32
#include <pthread.h>
#else
#include "pthread-win32.h"
#endif
#include <stdlib.h>
#include "c.h"
#include "cipher.h"
#include "utils/palloc.h"

#ifdef USE_SSL
#include "openssl/err.h"
#include "openssl/ssl.h"
#include "openssl/rand.h"
#include "openssl/ossl_typ.h"
#include "openssl/obj_mac.h"
#include "openssl/dh.h"
#include "openssl/bn.h"
#include "openssl/x509.h"
#include "openssl/x509_vfy.h"
#include "openssl/opensslconf.h"
#include "openssl/crypto.h"
#include "openssl/bio.h"
#endif /* USE_SSL */

#define ECOMMTCPARGSINVAL 1001
#define ECOMMTCPMEMALLOC 1002
#define ECOMMTCPCVINIT 1003
#define ECOMMTCPCVDESTROY 1004
#define ECOMMTCPLOCKINIT 1005
#define ECOMMTCPLOCKDESTROY 1006
#define ECOMMTCPNODEIDFD 1007
#define ECOMMTCPNODEIDXTCPFD 1008
#define ECOMMTCPSETSTREAMIDX 1009
#define ECOMMTCPBUFFQSIZE 1010
#define ECOMMTCPQUTOASZIE 1011
#define ECOMMTCPSEMINIT 1012
#define ECOMMTCPSEMPOST 1013
#define ECOMMTCPSEMWAIT 1014
#define ECOMMTCPEPOLLINIT 1015
#define ECOMMTCPEPOLLHNDL 1016
#define ECOMMTCPSCTPADRINIT 1017
#define ECOMMTCPSCTPLISTEN 1018
#define ECOMMTCPCMAILBOXINIT 1019
#define ECOMMTCPNODEIDXSCTPPORT 1020
#define ECOMMTCPSTREAMIDX 1021
#define ECOMMTCPTCPFD 1022
#define ECOMMTCPEPOLLEVNT 1023
#define ECOMMTCPCTRLMSG 1024
#define ECOMMTCPCTRLMSGWR 1025
#define ECOMMTCPCTRLMSGRD 1026
#define ECOMMTCPSTREAMIDXINVAL 1027
#define ECOMMTCPINVALNODEID 1028
#define ECOMMTCPCTRLMSGSIZE 1029
#define ECOMMTCPCVSIGNAL 1030
#define ECOMMTCPEPOLLLST 1031
#define ECOMMTCPCTRLCONN 1032
#define ECOMMTCPSND 1033
#define ECOMMTCPEPOLLCLOSE 1034
#define ECOMMTCPEPOLLTIMEOUT 1035
#define ECOMMTCPHASHENTRYDEL 1036
#define ECOMMTCPTHREADSTOP 1037
#define ECOMMTCPMAILBOXCLOSE 1038
#define ECOMMTCPSTREAMSTATE 1039
#define ECOMMTCPSCTPFDINVAL 1040
#define ECOMMTCPNODATA 1041
#define ECOMMTCPTHREADSTART 1042
#define ECOMMTCPHASHENTRYADD 1043
#define ECOMMTCPBUILDSCTPASSOC 1044
#define ECOMMTCPWRONGSTREAMKEY 1045
#define ECOMMTCPRELEASEMEM 1046
#define ECOMMTCPTCPDISCONNECT 1047
#define ECOMMTCPDISCONNECT 1048
#define ECOMMTCPREMOETECLOSE 1049
#define ECOMMTCPAPPCLOSE 1050
#define ECOMMTCPLOCALCLOSEPOLL 1051
#define ECOMMTCPPEERCLOSEPOLL 1052 
#define ECOMMTCPCONNFAIL 1054
#define ECOMMTCPSTREAMCONNFAIL 1055
#define ECOMMTCPREJECTSTREAM 1056
#define ECOMMTCPCONNTIMEOUT 1057
#define ECOMMTCPWAITQUOTAFAIL 1058
#define ECOMMTCPWAITPOLLERROR 1059
#define ECOMMTCPPEERCHANGED 1060
#define ECOMMTCPGSSAUTHFAIL 1061
#define ECOMMTCPSENDTIMEOUT 1062
#define ECOMMTCPNOTINTERNALIP 1063

// Structure definitions.
// Stream key is using to associate producers and consumers
//
#define HOST_ADDRSTRLEN INET6_ADDRSTRLEN
#define HOST_LEN_OF_HTAB 64
#define NAMEDATALEN 64
#define MSG_TIME_LEN 30
#define MAX_DN_NODE_NUM 8192
#define MAX_CN_NODE_NUM 1024
#define MAX_CN_DN_NODE_NUM (MAX_DN_NODE_NUM + MAX_CN_NODE_NUM)  //(MaxCoords+MaxDataNodes)
#define MIN_CN_DN_NODE_NUM (1 + 1)                              //(1 CN + 1 DN)
#define DOUBLE_NAMEDATALEN 128

#define SEC_TO_MICRO_SEC 1000

typedef enum {
    LIBCOMM_NONE,
    LIBCOMM_SEND_CTRL,
    LIBCOMM_RECV_CTRL,
    LIBCOMM_RECV_LOOP,
    LIBCOMM_AUX
} LibcommThreadTypeDef;

/* send and recv message type */
typedef enum
{
    SEND_SOME = 0,
    SECURE_READ,
    SECURE_WRITE,
    READ_DATA,
    READ_DATA_FROM_LOGIC
} CommMsgOper;

typedef enum
{
    POSTMASTER = 0,
    GS_SEND_flow,
    GS_RECV_FLOW,
    GS_RECV_LOOP,
} CommThreadUsed;

typedef struct CommStreamKey {
    uint64 queryId;       /* Plan id of current query. */
    uint32 planNodeId;    /* Plan node id of stream node. */
    uint32 producerSmpId; /* Smp id for producer. */
    uint32 consumerSmpId; /* Smp id for consumer. */
} TcpStreamKey;

// struct of libcomm logic addr
// idx gives the node idx of backend
// sid gives the logic conn idx with specific node
// ver gives the version of this logic addr, once it is closed ver++
// type is GSOCK_TYPE
typedef struct {
    uint16 idx;
    uint16 sid;
    uint16 ver;
    uint16 type;
} gsocket;

struct StreamConnInfo;

// statistic structure
//
typedef struct {
    char remote_node[NAMEDATALEN];
    char remote_host[HOST_ADDRSTRLEN];
    int idx;
    int stream_id;
    const char* stream_state;
    int tcp_sock;
    uint64 query_id;
    TcpStreamKey stream_key;
    long quota_size;
    unsigned long buff_usize;
    long bytes;
    long time;
    long speed;
    unsigned long local_thread_id;
    unsigned long peer_thread_id;
} CommRecvStreamStatus;

typedef struct {
    char remote_node[NAMEDATALEN];
    char remote_host[HOST_ADDRSTRLEN];
    int idx;
    int stream_id;
    const char* stream_state;
    int tcp_sock;
    int packet_count;
    int quota_count;
    uint64 query_id;
    TcpStreamKey stream_key;
    long bytes;
    long time;
    long speed;
    long quota_size;
    long wait_quota;
    long send_overhead;
    unsigned long local_thread_id;
    unsigned long peer_thread_id;
} CommSendStreamStatus;

typedef struct {
    long recv_speed;
    long send_speed;
    int recv_count_speed;
    int send_count_speed;
    long buffer;
    long mem_libcomm;
    long mem_libpq;
    int postmaster;
    int gs_sender_flow;
    int gs_receiver_flow;
    int gs_receiver_loop;
    int stream_conn_num;
} CommStat;


typedef struct {
    char remote_node[NAMEDATALEN];
    char remote_host[HOST_ADDRSTRLEN];
    int idx;
    int stream_num;
    uint32 min_delay;
    uint32 dev_delay;
    uint32 max_delay;
} CommDelayInfo;

#ifdef USE_SSL
typedef struct SSL_INFO {
    SSL* ssl;
    X509* peer;
    char* peer_cn;
    unsigned long count;
    int sock;
} SSL_INFO;
typedef struct libcommsslinfo {
    SSL_INFO node;
    struct libcommsslinfo* next;
} libcomm_sslinfo;
#endif
typedef struct {
    int socket;
    char *libcommhost;              /* the machine on which the server is running */
    char *sslcert;                  /* client certificate filename */
    char *sslcrl;                   /* certificate revocation list filename */
    char *sslkey;                   /* client key filename */
    char *sslrootcert;              /* root certificte filename */
    char *remote_nodename;          /* remote datanode name */
    bool sigpipe_so;                /* have we masked SIGPIPE via SO_NOSIGPIPE? */
#ifdef USE_SSL
    SSL *ssl;
    X509 *peer;                     /* X509 cert of server */
#endif
    char* sslmode;                  /* SSL mode (require,prefer,allow,disable) */
    bool sigpipe_flag;              /* can we mask SIGPIPE via MSG_NOSIGNAL? */
    unsigned char cipher_passwd[CIPHER_LEN + 1];
} LibCommConn;
// sctp address infomation
//
typedef struct libcommaddrinfo {
    char* host;                       // host ip
    char nodename[NAMEDATALEN];       // datanode name
    int ctrl_port;                    // control tcp listening port
    int listen_port;                    // listening port
    int status;                       // status of the address info,
                                      // -1:closed, 0:need send, 1:send finish
    int nodeIdx;                      // datanode index, like PGXCNodeId
    CommStreamKey streamKey;             // stream key, use plan id,plan node id, producer smp id and consumer smp id
    unsigned int qid;                 // query index
    bool parallel_send_mode;          // this connection use parallel mode to send
    int addr_list_size;               // how many node in addr info list, only the head node set it
    libcommaddrinfo* addr_list_next;  // point to next addr info node, libcomm use it to build addr info list
    gsocket gs_sock;                  // libcomm logic addr
} libcomm_addrinfo;

/* cn and dn send and recv message log */
typedef struct MessageIpcLog
{
    char type;              /* the incomplete message type parsed last time */
    int msg_cursor;
    int msg_len;            /* When the message parsed last time is incomplete, record the true length of the message */

    int len_cursor;         /* When msglen is parsed to be less than 4 bytes, the received bytes count are recorded */
    uint32 len_cache;

    /* For consecutive parses to the same message, record the msgtype, length, and last parsed time and same message count. */
    char last_msg_type;     /* the type of the previous message */
    int last_msg_len;       /* the message of the previous message */
    int last_msg_count;     /* same message count */
    char last_msg_time[MSG_TIME_LEN]; // the time of the previous message
}MessageIpcLog;

typedef struct MessageCommLog
{
    MessageIpcLog recv_ipc_log;
    MessageIpcLog send_ipc_log;
}MessageCommLog;

typedef enum {
    GSOCK_INVALID,
    GSOCK_PRODUCER,
    GSOCK_CONSUMER,
    GSOCK_DAUL_CHANNEL,
} GSOCK_TYPE;

extern gsocket gs_invalid_gsock;
#define GS_INVALID_GSOCK gs_invalid_gsock

void mc_elog(int elevel, const char* fmt, ...) __attribute__((format(printf, 2, 3)));

#define LIBCOMM_DEBUG_LOG(format, ...) \
    do {                               \
        ;                              \
    } while (0)

// the role of current node
//
typedef enum { ROLE_PRODUCER, ROLE_CONSUMER, ROLE_MAX_TYPE } SctpNodeRole;

// the connection state of IP+PORT
//
typedef enum { CONNSTATEFAIL, CONNSTATECONNECTING, CONNSTATESUCCEED } ConnectionState;

// the channel type
//
typedef enum { DATA_CHANNEL, CTRL_CHANNEL } ChannelType;

typedef bool (*wakeup_hook_type)(TcpStreamKey key, StreamConnInfo connInfo);

// Set basic initialization information for communication,
// calling by each datanode for initializing the communication layer
//
extern int gs_set_basic_info(const char* local_host,  // local ip of the datanode, it can be used "localhost" for simple
    const char* local_node_name,                      // local node name of the datanode, like PGXCNodeName
    int node_num,      // total number of datanodes, maximum value is 1024, it could be set in postgresql.conf with
                       // parameter name comm_max_datanode
    char* sock_path);  // unix domain path

// Connect to destination datanode, and get the sctp stream index for sending
// called by Sender
//
// return: stream index
//
extern int gs_connect(libcommaddrinfo** sctp_addrinfo,  // destination address
    int addr_num,                                       // connection number
    int timeout                                         // timeout threshold, default is 10 min
);

// Regist call back function of receiver for waking up Consumer in executor,
// if a sender connect successfully to receiver, wake up the consumer once
//
extern void gs_connect_regist_callback(wakeup_hook_type wakeup_callback);

// Send message through sctp channel using a sctp stream
// called by Sender
//
// return: transmitted data size
//
extern int gs_send(gsocket* gs_sock,  // destination address
    char* message,                    // message to send
    int m_len,                        // message length
    int time_out,                     // timeout threshold, default is -1
    bool block_mode                   // is wait quota
);

extern int gs_broadcast_send(struct libcommaddrinfo* sctp_addrinfo, char* message, int m_len, int time_out);

// Receive message from an array of sctp channels,
// however, we will get message data from just one channel,
// and copy the data into buffer.
//
// return: received data size
//
extern int gs_recv(
    gsocket* gs_sock,  // array of node index, which labeled the datanodes will send data to this datanode
    void* buff,        // buffer to copy data message, allocated by caller
    int buff_size      // size of buffer
);

// Simulate linux poll interface,
// to notify the Consumer thread data have been received into the inner buffer, you can come to take it
//
// return: the number of mailbox which has data
//
extern int gs_wait_poll(gsocket* gs_sock_array,  // array of producers node index
    int nproducer,                               // number of producers
    int* producer,                               // producers number triggers poll
    int timeout,                                 // time out in seconds, 0 for block mode
    bool close_expected                          // is logic connection closed by remote is an expected result
);

/* Handle the same message and print message receiving and sending log */
extern void gs_comm_ipc_print(MessageIpcLog *ipc_log, char *remotenode, gsocket *gs_sock, CommMsgOper msg_oper);

/* Send and receive data between nodes, for performance problem location */
extern MessageCommLog* gs_comm_ipc_performance(MessageCommLog *msgLog,
    void *ptr,
    int n,
    char *remotenode,
    gsocket *gs_sock,
    CommMsgOper logType);

// Receiver close sctp stream
//
extern int gs_r_close_stream(int sctp_idx,  // node index
    int sctp_sid,                           // stream index
    int version  // stream key(the plan id and plan node id), associated the pair of  Consumer and Producer
);

extern int gs_r_close_stream(gsocket* gsock);

// Sender close sctp stream
//
extern int gs_s_close_stream(int sctp_idx,  // node index
    int sctp_sid,                           // node index
    int version  // stream key(the plan id and plan node id), associated the pair of  Consumer and Producer
);

extern void gs_close_gsocket(gsocket* gsock);

extern void gs_poll_close();

extern int gs_s_close_stream(gsocket* gsock);

extern int gs_poll(int time_out);

extern int gs_poll_create();

extern int gs_close_all_stream_by_debug_id(uint64 query_id);

extern bool gs_stop_query(gsocket* gsock, uint32 remote_pid);

// Shudown the communication layer
//
extern void gs_shutdown_comm();

// Do internal cancel when cancelling is requested
//
extern void gs_r_cancel();

// set t_thrd proc workingVersionNum
extern void gs_set_working_version_num(int num);

// export communication layer status
//
extern void gs_log_comm_status();

// check if the kernel version is reliable
//
extern int gs_check_SLESSP2_version();

// get the assigned stream number
//
extern int gs_get_stream_num();

// get the error information
//
const char* gs_comm_strerror();

// get communication layer stream status at receiver as a tuple for pg_comm_recv_stream
//
extern bool get_next_recv_stream_status(CommRecvStreamStatus* stream_status);

// get communication layer stream status at sender as a tuple for pg_comm_send_stream
//
extern bool get_next_send_stream_status(CommSendStreamStatus* stream_status);

// get communication layer stream status as a tuple for pg_comm_status
//
extern bool gs_get_comm_stat(CommStat* comm_stat);

// get communication layer sctp delay infomation as a tuple for pg_comm_delay
//
extern bool get_next_comm_delay_info(CommDelayInfo* delay_info);

extern void gs_set_debug_mode(bool mod);

extern void gs_set_stat_mode(bool mod);

extern void gs_set_timer_mode(bool mod);

extern void gs_set_no_delay(bool mod);

extern void gs_set_ackchk_time(int mod);

extern void gs_set_libcomm_used_rate(int rate);

extern void init_libcomm_cpu_rate();

// set t_thrd proc workingVersionNum
extern void gs_set_working_version_num(int num);

// get availabe memory of communication layer
//
extern long gs_get_comm_used_memory(void);

extern long gs_get_comm_peak_memory(void);

extern Size gs_get_comm_context_memory(void);

extern int gs_release_comm_memory();

// interface function for postmaster read msg by unix domain
extern int gs_recv_msg_by_unix_domain(int fd, gsocket* gs_sock);

// check mailbox version for connection reused by poolmgr
extern bool gs_test_libcomm_conn(gsocket* gs_sock);

// reset cmailbox for pooler reuse
extern void gs_clean_cmailbox(gsocket gs_sock);

extern bool gs_check_mailbox(uint16 version1, uint16 version2);
extern void gs_r_reset_cmailbox(struct c_mailbox* cmailbox, int close_reason);
extern void gs_s_reset_pmailbox(struct p_mailbox* pmailbox, int close_reason);
extern bool gs_mailbox_build(int idx);
extern void gs_mailbox_destory(int idx);

// for capacity expansion
extern void gs_change_capacity(int newval);

extern int gs_get_cur_node();

extern void commSenderFlowMain();
extern void commReceiverFlowMain();
extern void commAuxiliaryMain();
extern void commPoolCleanerMain();
extern void commReceiverMain(void* tid_callback);

extern void gs_init_adapt_layer();
extern void gs_senders_struct_set();
extern void init_comm_buffer_size();
extern void gs_set_local_host(const char* host);
extern void gs_broadcast_poll();
extern void gs_set_kerberos_keyfile();
extern void gs_receivers_struct_init(int ctrl_port, int data_port);
extern void gs_set_comm_session();
extern int gs_get_node_idx(char* node_name);
extern int gs_memory_pool_queue_initial_success(uint32 index);
extern struct mc_lqueue_item* gs_memory_pool_queue_pop(char* iov);
extern bool gs_memory_pool_queue_push(char* item);

extern ThreadId startCommSenderFlow(void);
extern ThreadId startCommReceiverFlow();
extern ThreadId startCommAuxiliary();
extern ThreadId startCommReceiver(int* tid);
extern void startCommReceiverWorker(ThreadId* threadid);


extern void gs_init_hash_table();
extern int mc_tcp_connect_nonblock(const char* host, int port);

/* Network adaptation interface of the libcomm for the ThreadPoolListener. */
extern void CommResourceInit();
extern int CommEpollCreate(int size);
extern int CommEpollCtl(int epfd, int op, int fd, struct epoll_event *event);
extern int CommEpollWait(int epfd, struct epoll_event *event, int maxevents, int timeout);
extern int CommEpollClose(int epfd);
extern void InitCommLogicResource();
extern void ProcessCommLogicTearDown();

class CommEpollFd;
struct HTAB;

typedef struct LogicFd {
    int idx;
    int streamid;
} LogicFd;

typedef struct CommEpFdInfo {
    /* hash key first */
    int epfd;
    CommEpollFd *comm_epfd;
} CommEpFdInfo;

typedef struct CommFd {
    int fd;
    gsocket logic_fd;
} CommFd;

struct knl_session_context;
class CommEpollFd;
typedef struct SessionInfo {
    LogicFd logic_fd;                    // HASH key first

    CommFd commfd;
    int wakeup_cnt;
    int handle_wakeup_cnt;
    volatile bool err_occurs;
    CommEpollFd *comm_epfd_ptr;
    knl_session_context *session_ptr;
    int is_idle;
} SessionInfo;

#ifndef NO_COMPILE_CLASS_FDCOLLECTION
class FdCollection : public BaseObject {
public:
    FdCollection();
    ~FdCollection();
    void Init();
    void DeInit();
    int AddEpfd(int epfd, int size);
    int DelEpfd(int epfd);
    void CleanEpfd();
    inline CommEpollFd* GetCommEpollFd(int epfd);

    int AddLogicFd(CommEpollFd *comm_epfd, void *session_ptr);
    int DelLogicFd(const LogicFd *logic_fd);
    void CleanLogicFd();
    SessionInfo* GetSessionInfo(const LogicFd *logic_fd);

private:
    inline bool IsEpfdValid(int fd);

    /* SockfdAPI** m_sockfd_map */
    HTAB *m_epfd_htab;
    pthread_mutex_t m_epfd_htab_lock;

    /* logicfd info  */
    HTAB *m_logicfd_htab;
    pthread_mutex_t m_logicfd_htab_lock;
    int m_logicfd_nums;
};
#endif

extern void WakeupSession(SessionInfo *session_info, bool err_occurs, const char* caller_name);

/*
 * LIBCOMM_CHECK is defined when make commcheck
 */
#ifdef LIBCOMM_CHECK
#define LIBCOMM_FAULT_INJECTION_ENABLE
#define LIBCOMM_SPEED_TEST_ENABLE
#else
#undef LIBCOMM_FAULT_INJECTION_ENABLE
#undef LIBCOMM_SPEED_TEST_ENABLE
#endif

/*
 * Libcomm Speed Test Framework
 * Before start: 1, Must run a stream query to get DN connections.
 *               2, add new guc params to $GAUSSHOME/bin/cluster_guc.conf
 * Start: Use gs_guc reload to set test thread num.
 * For example: gs_guc reload -Z datanode -N all -I all -c "comm_test_thread_num=1"
 * Stop: Use gs_guc reload to set test thread num=0.
 */
#ifdef LIBCOMM_SPEED_TEST_ENABLE
extern void gs_set_test_thread_num(int newval);
extern void gs_set_test_msg_len(int newval);
extern void gs_set_test_send_sleep(int newval);
extern void gs_set_test_send_once(int newval);
extern void gs_set_test_recv_sleep(int newval);
extern void gs_set_test_recv_once(int newval);
#endif

/*Libcomm fault injection Framework
 * Before start: 1, enable LIBCOMM_FAULT_INJECTION_ENABLE.
 *               2, add new guc params to $GAUSSHOME/bin/cluster_guc.conf
 * Start: Use gs_guc reload to set FI num.
 * For instance: gs_guc reload -Z datanode -N all -I all -c "comm_fault_injection=6"
 * Stop: Use gs_guc reload to set comm_fault_injection=0.
 */
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
typedef enum {
    LIBCOMM_FI_NONE,
    LIBCOMM_FI_R_TCP_DISCONNECT,
    LIBCOMM_FI_R_SCTP_DISCONNECT,
    LIBCOMM_FI_S_TCP_DISCONNECT,
    LIBCOMM_FI_S_SCTP_DISCONNECT,
    LIBCOMM_FI_RELEASE_MEMORY,
    LIBCOMM_FI_FAILOVER,  //
    LIBCOMM_FI_NO_STREAMID,
    LIBCOMM_FI_CONSUMER_REJECT,
    LIBCOMM_FI_R_APP_CLOSE,    //
    LIBCOMM_FI_S_APP_CLOSE,    //
    LIBCOMM_FI_CANCEL_SIGNAL,  //
    LIBCOMM_FI_CLOSE_BY_VIEW,  //
    LIBCOMM_FI_GSS_TCP_FAILED,
    LIBCOMM_FI_GSS_SCTP_FAILED,
    LIBCOMM_FI_R_PACKAGE_SPLIT,
    LIBCOMM_FI_MALLOC_FAILED,
    LIBCOMM_FI_MC_TCP_READ_FAILED,
    LIBCOMM_FI_MC_TCP_READ_BLOCK_FAILED,
    LIBCOMM_FI_MC_TCP_READ_NONBLOCK_FAILED,
    LIBCOMM_FI_MC_TCP_WRITE_FAILED,
    LIBCOMM_FI_MC_TCP_WRITE_NONBLOCK_FAILED,
    LIBCOMM_FI_MC_TCP_ACCEPT_FAILED,
    LIBCOMM_FI_MC_TCP_LISTEN_FAILED,
    LIBCOMM_FI_MC_TCP_CONNECT_FAILED,
    LIBCOMM_FI_FD_SOCKETVERSION_FAILED,
    LIBCOMM_FI_SOCKID_NODEIDX_FAILED,
    LIBCOMM_FI_POLLER_ADD_FD_FAILED,
    LIBCOMM_FI_NO_NODEIDX,
    LIBCOMM_FI_CREATE_POLL_FAILED,
    LIBCOMM_FI_DYNAMIC_CAPACITY_FAILED,
    LIBCOMM_FI_MAX
} LibcommFaultInjection;

extern void gs_set_fault_injection(int newval);
extern void set_comm_fault_injection(int type);
extern bool is_comm_fault_injection(LibcommFaultInjection type);
#endif

#ifdef USE_SPQ
constexpr uint16 SPQ_QE_CONNECTION = 0;
constexpr uint16 SPQ_QC_CONNECTION = 1;
struct QCConnKey {
    uint64 query_id;
    uint32 plan_node_id;
    uint16 node_id;
    uint16 type;
};
struct QCConnEntry {
    QCConnKey key;
    uint64 streamcap;
    gsocket forward;
    gsocket backward;
    int scannedPageNum;
    int internal_node_id;
};
struct BackConnInfo {
    uint16 node_idx;
    uint16 version;
    uint64 streamcap;
    uint64 query_id;
    CommStreamKey stream_key;
    gsocket *backward;
};
extern int gs_r_build_reply_connection(BackConnInfo* fcmsgr, int local_version, uint16 *sid);
#endif

#endif  //_GS_LIBCOMM_H_
