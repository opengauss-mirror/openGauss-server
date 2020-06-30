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
 * mc_sctp.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_core/mc_sctp.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/uio.h>  // for iovec{} and readv/writev
#include <sys/wait.h>
#include <sys/param.h>
#include <sys/socket.h>  // basic socket definitions
#include <netdb.h>
#include <net/if.h>
#include <arpa/inet.h>  // inet(3) functions
#include "mc_sctp.h"
#include "sctp_utils/sctp_err.h"
#include "sctp_utils/sctp_platform.h"

#if ENABLE_MULTIPLE_NODES
/*
 * Common getsockopt() layer
 * If the NEW getsockopt() API fails this function will fall back to using
 * the old API
 */
#ifdef SCTP_DEBUG
static int mc_sctp_getaddrs(int sd, comm_assoc_t id, int optname_new, struct sockaddr** addrs, int* size)
{
    int cnt, err;
    socklen_t len;
    size_t bufsize = 4096; /* enough for most cases */
    *size = bufsize;
    errno_t ss_rc = 0;
    struct comm_getaddrs* getaddrs = NULL;
    LIBCOMM_MALLOC(getaddrs, bufsize, comm_getaddrs);
    if (getaddrs == NULL) {
        return -1;
    }

    for (;;) {
        char* new_buf = NULL;

        len = bufsize;
        getaddrs->id = id;
        err = getsockopt(sd, SOL_SCTP, optname_new, getaddrs, &len);
        if (err == 0) {
            /* got it */
            break;
        }
        if (errno != ENOMEM) {
            /* unknown error */
            LIBCOMM_FREE(getaddrs, bufsize);
            return -1;
        }
        /* expand buffer */
        if (bufsize > 128 * 1024) {
            /* this is getting ridiculous */
            LIBCOMM_FREE(getaddrs, bufsize);
            errno = ENOBUFS;
            return -1;
        }
        LIBCOMM_MALLOC(new_buf, bufsize + 4096, char);
        if (NULL == new_buf) {
            LIBCOMM_FREE(getaddrs, bufsize);
            return -1;
        }
        ss_rc = memcpy_s(new_buf, bufsize + 1, getaddrs, bufsize);
        securec_check(ss_rc, "\0", " \0");
        if (getaddrs != NULL) {
            LIBCOMM_FREE(getaddrs, bufsize);
        }
        bufsize += 4096;
        *size = bufsize;
        getaddrs = (struct comm_getaddrs*)new_buf;
    }

    /* we skip traversing the list, allocating a new buffer etc. and enjoy
     * a simple hack */
    cnt = getaddrs->num;
    ss_rc = memmove_s(getaddrs, bufsize, getaddrs + 1, len);
    securec_check(ss_rc, "\0", "\0");
    *addrs = (struct sockaddr*)getaddrs;

    return cnt;
}  // mc_sctp_getaddrs

/* Get all peer address on a socket.  This is a new SCTP API
 * described in the section 8.3 of the Sockets API Extensions for SCTP.
 * This is implemented using the getsockopt() interface.
 */
int mc_sctp_getpaddrs(int sd, comm_assoc_t id, struct sockaddr** addrs)
{
    return mc_sctp_getaddrs(sd, id, COMM_GET_PEER_ADDRESS, addrs);
}  // mc_sctp_getpaddrs

/* Get all locally bound address on a socket.  This is a new SCTP API
 * described in the section 8.5 of the Sockets API Extensions for SCTP.
 * This is implemented using the getsockopt() interface.
 */
int mc_sctp_getladdrs(int sd, comm_assoc_t id, struct sockaddr** addrs, int* bufsize)
{
    return mc_sctp_getaddrs(sd, id, SCTP_GET_LOCAL_ADDRS, addrs, bufsize);
}  //  mc_sctp_getladdrs

/* Frees all resources allocated by sctp_getladdrs().  This is a new SCTP API
 * described in the section 8.6 of the Sockets API Extensions for SCTP.
 */
int mc_sctp_freeladdrs(struct sockaddr* addrs, int size)
{
    LIBCOMM_FREE(addrs, size);
    return 0;
}  //  mc_sctp_freeladdrs
#endif
// create a SCTP socket and returns socket fd
//
int mc_sctp_sock(int domain, int socket_type)
{
    int sock;
    int error = -1;
    int type = socket_type;  // socket_type

    // create a socket of sctp
    //
    if ((sock = socket(domain, type, IPPROTO_SCTP)) < 0) {
        LIBCOMM_ELOG(WARNING, "(sctp socket init)\tFailed to create sctp socket:%s.", mc_strerror(errno));
        return -1;
    }
    // set attributes of the socket : FD_CLOEXEC, SO_REUSEADDR
    //
    error = set_socketopt(sock, 1, FD_CLOEXEC);
    if (error < 0) {
        LIBCOMM_ELOG(WARNING, "(sctp socket init)\tFailed to set socket option FD_CLOEXEC:%s.", mc_strerror(errno));
    }

    if (error >= 0) {
        const int on = 1;
        error = mc_sctp_setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
        if (error < 0) {
            LIBCOMM_ELOG(
                WARNING, "(sctp socket init)\tFailed to set socket option SO_REUSEADDR:%s.", mc_strerror(errno));
        }
    }

    if (error < 0) {
        (void)mc_sctp_sock_close(sock);
        return -1;
    }

    return sock;
}

// set max number of streams in sctp channel
//
int mc_sctp_set_sock_initmsg(int sock, int num_oustreams, int max_instreams)
{
    mc_assert(sock > 0);

    int error = -1;
    struct comm_init_message initmsg;
    errno_t ss_rc = 0;
    ss_rc = memset_s(&initmsg, sizeof(initmsg), 0x0, sizeof(struct comm_init_message));
    securec_check(ss_rc, "\0", "\0");

    // set the number of streams per SCTP association (default is only 10).
    initmsg.num_output = num_oustreams;
    initmsg.max_input = max_instreams;

    error = mc_sctp_setsockopt(sock, SOL_SCTP, COMM_INIT_MESSAGE, (char*)&initmsg, sizeof(struct comm_init_message));
    if (error != 0) {
        LIBCOMM_ELOG(
            WARNING, "(sctp set socket attr)\tFailed to set socket option:%s.", mc_strerror(errno));
    }

    return error;
}  // mc_sctp_set_sock_initmsg

int mc_sctp_set_sock_subscribe(int sock)
{
    mc_assert(sock > 0);

    int error;
    struct comm_event_subscribe subscribe;
    errno_t ss_rc = 0;
    ss_rc = memset_s(&subscribe, sizeof(subscribe), 0, sizeof(struct comm_event_subscribe));
    securec_check(ss_rc, "\0", "\0");
    subscribe.io_event = 1;
    subscribe.connect_event = 1;
    subscribe.address_event = 1;
    subscribe.failure_event = 1;
    subscribe.error_event = 1;
    subscribe.shutdown_event = 1;
    subscribe.delivery_event = 1;

    // becareful, the different kernel version has different size of struct subscribe,
    // now running on kernel>=2.6.32, size of struct subscribe is 9.
    //
    error = mc_sctp_setsockopt(sock, SOL_SCTP, COMM_EVENT_INFO, (const void*)&subscribe, sizeof(subscribe));
    if (error != 0) {
        LIBCOMM_ELOG(
            WARNING, "(sctp set socket events)\tFailed to set socket option SCTP_EVENTS:%s.", mc_strerror(errno));
    }

    return error;
}  // mc_sctp_set_sock_subscribe

// bind sctp socket with ip address
//
int mc_sctp_bind(int sock, struct sockaddr_storage* saddr, int in_len)
{
    mc_assert(sock > 0);

    int error = 0;
    int i = 0;

    // we try to do bind with MAX_BIND_RETRYS times,
    // which is defined in src/common.h.
    //
    do {
        if (i > 0) {
            (void)sleep(5);  // sleep a while before new try
        }
        error = bind(sock, (struct sockaddr*)saddr, in_len);  // bind the sctp socket to the assigned address
        if (error != 0) {
            char host_s[NI_MAXHOST], serv_s[NI_MAXSERV];
            int err =
                getnameinfo((struct sockaddr*)saddr, in_len, host_s, NI_MAXHOST, serv_s, NI_MAXSERV, NI_NUMERICHOST);
            gai_assert(err);

            LIBCOMM_ELOG(WARNING,
                "(sctp bind)\tBind(socket[%d], addr:port[%s:%s]):%s  --  attempt %d/%d.",
                sock,
                host_s,
                serv_s,
                mc_strerror(errno),
                i + 1,
                MAX_BIND_RETRYS);
            if (errno != EADDRINUSE) {
                LIBCOMM_ELOG(
                    WARNING, "(sctp bind)\tFailed to bind to host:port[%s:%s]:%s.", host_s, serv_s, mc_strerror(errno));
                return -1;
            }
        }

        i++;

        if (i >= MAX_BIND_RETRYS) {
            LIBCOMM_ELOG(WARNING, "(sctp bind)\tMaximum bind attempts. Die now.");
            return -1;
        }
    } while (error < 0 && i < MAX_BIND_RETRYS);

    return error;
}

// let SCTP socket listens for connections
//
int mc_sctp_listen(int sock, int listen_count)
{
    int error = 0;

    // Mark socket as being able to accept new associations
    //
    error = listen(sock, listen_count);
    if (error == 0) {
        return 0;
    }

    LIBCOMM_ELOG(WARNING, "(sctp listen)\tFailed to listen on socket[%d]:%s.", sock, mc_strerror(errno));

    // close the sctp socket
    //
    return -1;
}

// init a sctp sockaddr_storage with host and port
//
int mc_sctp_addr_init(const char* host, int port, struct sockaddr_storage* ss, int* in_len)
{
    mc_assert(port >= 0);

    struct sockaddr_in* t_addr = NULL;
    int len = 0;
    int error = 0;

    // do not termination process when revieve SIGPIPE
    //
    (void)signal(SIGPIPE, SIG_IGN);

    if (strcmp(host, "localhost") == 0) {
        host = "0.0.0.0";
    }

    t_addr = (struct sockaddr_in*)ss;
    t_addr->sin_family = AF_INET;
    t_addr->sin_port = htons(port);
    error = inet_pton(AF_INET, host, &t_addr->sin_addr);
    len = sizeof(struct sockaddr_in);
#ifdef __FreeBSD__
    t_addr->sin_len = len;
#endif

    *in_len = len;
    return (error == 1) ? 0 : error;
}

// init a sctp socket for server or client
//
static int mc_sctp_sock_init(
    struct sockaddr_storage* s_loc, int sin_len, int num_ostreams, int max_instreams, int socket_type)
{
    int sctp_socket = -1;
    int error = -1;

    // create sctp socket
    //
    sctp_socket = mc_sctp_sock(s_loc->ss_family, socket_type);
    // set the initial message attributes of the sctp socket
    //
    if (sctp_socket >= 0) {
        error = mc_sctp_set_sock_initmsg(sctp_socket, num_ostreams, max_instreams);
    }

    // set the subscribe message attributes of the sctp socket
    //
    if (error == 0) {
        error = mc_sctp_set_sock_subscribe(sctp_socket);
    }

    // bind the sctp socket to assigned address
    //
    if (error == 0) {
        error = mc_sctp_bind(sctp_socket, s_loc, sin_len);
    }

    if (error == 0) {
        int no_delay = (g_instance.comm_cxt.commutil_cxt.g_no_delay) ? 1 : 0;
        error = mc_sctp_setsockopt(sctp_socket, SOL_SCTP, COMM_NO_DELAY, (char*)&no_delay, sizeof(no_delay));
    }

    if (sctp_socket > 0 && error != 0) {
        (void)mc_sctp_sock_close(sctp_socket);
        return error;
    }

    return sctp_socket;
}

// init a sctp server with socket address storage
//
int mc_sctp_server_init(struct sockaddr_storage* s_loc, int sin_len)
{
    int sctp_socket = -1;
    int error = -1;
    int socket_type = SOCK_STREAM;  // use stream model before

    // initialize a sctp socket
    //
    sctp_socket = mc_sctp_sock_init(s_loc, sin_len, gs_get_stream_num(), gs_get_stream_num(), socket_type);
    if (sctp_socket > 0) {
        // do listen
        //
        error = mc_sctp_listen(sctp_socket, SCTP_LISTENQ);
        // set nonblock
        //
        if (error == 0) {
            error = mc_sctp_set_nonblock(sctp_socket);
        }

        if (error != 0) {
            error = mc_sctp_sock_close(sctp_socket);
            mc_assert(error == 0);
            sctp_socket = -1;
        }
    }
    return sctp_socket;
}  // mc_sctp_server_init

// init a sctp client with socket address storage
//
int mc_sctp_client_init(struct sockaddr_storage* s_loc, int sin_len)
{
    mc_assert(s_loc != NULL);

    int sctp_socket;
    int socket_type = SOCK_STREAM;  // use stream model before

    // initialize a sctp socket
    //
    sctp_socket = mc_sctp_sock_init(s_loc, sin_len, gs_get_stream_num(), gs_get_stream_num(), socket_type);
    // set recvmsg timeout to 1000 useconds
    //
    if (sctp_socket >= 0) {
        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 1000;
        if (mc_sctp_setsockopt(sctp_socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0) {
            LIBCOMM_ELOG(WARNING, "(sctp client init)\tFailed to set so_rcvtimeo:%s.", mc_strerror(errno));
        }
    }
    return sctp_socket;
}

int mc_sctp_connect(int sock, struct sockaddr_storage* s_loc, int sin_len)
{
    int error = 0;
    do {
        error = connect(sock, (struct sockaddr*)s_loc, sin_len);
    } while (error < 0 && errno == EINTR);
    if (error == 0) {
        (void)mc_sctp_set_nonblock(sock);
    }
    return error;
}

int mc_sctp_accept(int sock, struct sockaddr* sa, socklen_t* salenptr)
{
    int new_fd;
    int error = 0;
accept_again:
    if ((new_fd = accept4(sock, sa, salenptr, SOCK_CLOEXEC)) < 0) {
#ifdef EPROTO
        if (errno == EPROTO || errno == ECONNABORTED) {
#else
        if (errno == ECONNABORTED || errno == EINTR) {
#endif
            goto accept_again;
        } else {
            errno_assert(errno);
        }
    }
    if (new_fd > 0) {
        error = mc_sctp_set_sock_initmsg(new_fd, gs_get_stream_num(), gs_get_stream_num());
        // set the subscribe message attributes of the sctp socket
        //
        if (error == 0) {
            error = mc_sctp_set_sock_subscribe(new_fd);
        }

        if (error == 0) {
            int no_delay = (g_instance.comm_cxt.commutil_cxt.g_no_delay) ? 1 : 0;
            error = mc_sctp_setsockopt(new_fd, SOL_SCTP, COMM_NO_DELAY, (char*)&no_delay, sizeof(no_delay));
        }

        // set nonblock
        //
        if (error == 0) {
            error = mc_sctp_set_nonblock(new_fd);
        }
    }

    return (error == 0 ? new_fd : error);
}

static void mc_init_iov(struct iovec *iov, char *message, int msglen)
{
    iov->iov_base = message;
    iov->iov_len = msglen;
}

static void mc_init_outmsg(struct msghdr *msg, struct iovec *iov,  char *cmsg, size_t cmsg_size)
{
    msg->msg_name = NULL;
    msg->msg_namelen = 0;
    msg->msg_iov = iov;
    msg->msg_iovlen = 1;
    msg->msg_control = cmsg;
    msg->msg_controllen = cmsg_size;
    msg->msg_flags = 0;
}

static void mc_init_cmsg(struct cmsghdr *cmsg, int cmsg_level, int cmsg_type, size_t cmsg_len)
{
    cmsg->cmsg_level = cmsg_level;
    cmsg->cmsg_type = cmsg_type;
    cmsg->cmsg_len = cmsg_len;
}

// send message to sctp socket with given stream.
// sock			: client sctp socket(local)
// stream		: sctp stream
// order			: packet's order
// timetolive		: time to live in the net
// flags			: the same as the flag of sendmsg
// to_rem		: the address the remote destination
// message		: the message to send
// ppid			: the thread pid of the sender thread
// m_len			: the length of the message to send
//
int mc_sctp_send(int sock, int stream, int order, int timetolive, int flags, struct sockaddr_storage* to_rem,
    char* message, unsigned long ppid, int m_len)
{
    int error = 0;
    int msglen = 0;

    // build control message
    //
    char outcmsg[CMSG_SPACE(sizeof(struct comm_buff_info))];

    struct comm_buff_info* sinfo = NULL;
    struct cmsghdr* cmsg = NULL;
    struct iovec iov;
    struct msghdr outmsg = { 0 };
    errno_t ss_rc = 0;

    msglen = m_len;
    mc_init_iov(&iov, message, msglen);
    mc_init_outmsg(&outmsg, &iov, outcmsg, sizeof(outcmsg));

    cmsg = CMSG_FIRSTHDR(&outmsg);
    mc_init_cmsg(cmsg, IPPROTO_SCTP, COMM_BUFF, CMSG_LEN(sizeof(struct comm_buff_info)));

    outmsg.msg_controllen = cmsg->cmsg_len;

    sinfo = (struct comm_buff_info*)CMSG_DATA(cmsg);
    ss_rc = memset_s(sinfo, sizeof(struct comm_buff_info), 0x0, sizeof(struct comm_buff_info));
    securec_check(ss_rc, "\0", "\0");
    sinfo->ipid = ppid;
    sinfo->istream = stream;
    sinfo->itypes = 0;

    if (!order) {
        sinfo->itypes = COMM_UNORDERED;
    }
    if (timetolive) {
        sinfo->itime = timetolive;
    }

    COMM_DEBUG_LOG("(sctp sendmsg)\tStream[%u] flags[0x%x] ppid[%lu].", sinfo->istream, sinfo->itypes, ppid);

    // do send message
    //
    error = sendmsg(sock, &outmsg, flags);

    if (error < 0 && (errno == ENOBUFS || errno == EINTR || errno == EAGAIN)) {
        COMM_DEBUG_LOG("(sctp sendmsg)\tENOBUFS on stream[%d], error[%d]:%s.", stream, error, mc_strerror(errno));
        return 0;
    }

    if (error < 0) {
        LIBCOMM_ELOG(WARNING, "(sctp sendmsg)\tFailed to send msg, error[%d]:%s.", error, mc_strerror(errno));
    }

    COMM_DEBUG_LOG("(sctp sendmsg)\tSendmsg on socket[%d], stream[%d]:%4d bytes.", sock, sinfo->istream, error);
    return error;
}  // mc_sctp_send

// receive message from SCTP socket
//
int mc_sctp_recv(int sock, struct msghdr* inmessage, int flags)
{
    mc_assert(sock > 0);
    mc_assert(inmessage != NULL);
    return recvmsg(sock, inmessage, flags);
}
// send ack message to remote counterpart
//
int mc_sctp_send_ack(int sock, char* msg, int msg_len)
{
    mc_assert(msg != NULL);
    mc_assert(sock >= 0);
    mc_assert(msg_len > 0);
    struct msghdr outmsg;
    struct iovec iov;
    int flags = MSG_NOSIGNAL | MSG_WAITALL;

    errno_t ss_rc = 0;
    ss_rc = memset_s(&outmsg, sizeof(outmsg), 0x0, sizeof(outmsg));
    securec_check(ss_rc, "\0", "\0");

    outmsg.msg_name = NULL;
    outmsg.msg_namelen = 0;
    outmsg.msg_iov = &iov;
    iov.iov_base = (void*)msg;
    iov.iov_len = msg_len;
    outmsg.msg_iovlen = 1;
    outmsg.msg_controllen = 0;

    return sendmsg(sock, &outmsg, flags);
}
// recv ack message to remote counterpart to confirm
//
int mc_sctp_recv_ack(int sock, char* msg, int msg_len)
{
    mc_assert(msg != NULL);
    mc_assert(sock >= 0);
    mc_assert(msg_len > 0);
    int flags = MSG_WAITALL;
    int error = -1;
    struct iovec iov;
    struct msghdr inmsg;
    char incmsg[CMSG_SPACE(sizeof(struct comm_buff_info))];
    errno_t ss_rc = 0;
    iov.iov_base = msg;
    iov.iov_len = msg_len;
    for (;;) {
        ss_rc = memset_s(&inmsg, sizeof(inmsg), 0x0, sizeof(inmsg));
        securec_check(ss_rc, "\0", "\0");
        inmsg.msg_name = NULL;
        inmsg.msg_namelen = 0;
        inmsg.msg_iov = &iov;
        inmsg.msg_iovlen = 1;
        inmsg.msg_control = incmsg;
        inmsg.msg_controllen = sizeof(incmsg);
        error = recvmsg(sock, &inmsg, flags);
        if (error <= 0 || !(MSG_NOTIFICATION & inmsg.msg_flags)) {
            break;
        }
    }
    return error;
}

typedef enum McXidType {
    MC_SID = 0,
    MC_PPID,
} McXid;

/*
 * function name: mc_sctp_get_xid
 * description	: get sid or ppid from received sctp message by type.
 * arguments	: _in_ msg: the point for sctp message.
 *				: _in_ type: the type of get xid
 * return value	: sid/ppid
 */
static unsigned int mc_sctp_get_xid(struct msghdr* msg, McXid type)
{
    struct cmsghdr* scmsg = NULL;
    comm_msg_data_t* data = NULL;
    int xid = -1;
    // for now, we only return the streamid of first valid
    // chunk pointed by the first header in msg
    //
    for (scmsg = CMSG_FIRSTHDR(msg); scmsg != NULL; scmsg = CMSG_NXTHDR(msg, scmsg)) {
        if ((scmsg->cmsg_level == IPPROTO_SCTP) && (scmsg->cmsg_type == COMM_BUFF)) {
            data = (comm_msg_data_t*)CMSG_DATA(scmsg);
            if (type == MC_SID) {
                xid = (data->buff.istream);
            } else if (type == MC_PPID) {
                xid = (data->buff.ipid);
            }

            break;
        } else {
            data = (comm_msg_data_t*)CMSG_DATA(scmsg);
            LIBCOMM_ELOG(WARNING,
                "sizeof(comm_msg_data_t)[%lu], sizeof(comm_buff_info)[%lu], fake sid[%d].",
                sizeof(comm_msg_data_t),
                sizeof(struct comm_buff_info),
                (data->buff.istream));

            continue;
        }
    }

    return xid;
}

/*
 * function name	: mc_sctp_get_sid
 * description	: get sid from received sctp message.
 * arguments		: _in_ msg: the point for sctp message.
 * return value	:
 * 				  -1: failed.
 * 				  >=0: ppid of this message.
 */
unsigned int mc_sctp_get_sid(struct msghdr* msg)
{
    int sid;
    sid = mc_sctp_get_xid(msg, MC_SID);

    return sid;
}

/*
 * function name	: mc_sctp_get_ppid
 * description	: get ppid from received sctp message.
 * arguments		: _in_ msg: the point for sctp message.
 * return value	:
 * 				  -1: failed.
 * 				  >=0: ppid of this message.
 */
unsigned int mc_sctp_get_ppid(struct msghdr* msg)
{
    int ppid;
    ppid = mc_sctp_get_xid(msg, MC_PPID);

    return ppid;
}

// set socket to NON-BLOCKING to epoll on it
//
int mc_sctp_set_nonblock(int sock)
{
    return set_socketopt(sock, 0, O_NONBLOCK);
}

// close SCTP socket
//
int mc_sctp_sock_close(int sock)
{
    int error = 0;
    LIBCOMM_ELOG(WARNING, "(sctp close)\tClose socket[%d].", sock);
    error = close(sock);

    if (error != 0) {
        LIBCOMM_ELOG(WARNING, "(sctp close)\tFailed to close socket[%d]:%s.", sock, strerror(errno));
        return error;
    }
    return 0;

}  // mc_sctp_socket_close

// set sctp socket option
//
int mc_sctp_setsockopt(int sock, int level, int optname, const void* optval, socklen_t optlen)
{
    return setsockopt(sock, level, optname, optval, optlen);
}

// get sctp socket option
//
int mc_sctp_getsockopt(int sock, int optname, void* optval, socklen_t* optlen)
{
    return getsockopt(sock, SOL_SCTP, optname, optval, optlen);
}

int mc_sctp_check_socket(int sock)
{
    int flags = MSG_DONTWAIT;
    int error = -1;
    bool is_sock_err = false;
    struct iovec iov;
    struct msghdr inmsg;

    if (sock < 0) {
        return -1;
    }

    // The same as  mc_sctp_recv_assoc_id, we do not use mc_calloc and mc_free in this function,
    // because if we do not have enough usable memory, it may be waiting here and in gs_internal_recv,
    // and may be none of them will be waked up by mc_free.
    //
    LIBCOMM_MALLOC(iov.iov_base, IOV_DATA_SIZE, void);
    if (iov.iov_base == NULL) {
        LIBCOMM_ELOG(WARNING,
            "(sctp check socket)\tFailed to malloc for recv message from socket[%d]:%s.",
            sock,
            strerror(errno));
        return -1;
    }
    iov.iov_len = IOV_DATA_SIZE;
    char incmsg[CMSG_SPACE(sizeof(struct comm_buff_info))];
    errno_t ss_rc = 0;
    ss_rc = memset_s(&inmsg, sizeof(inmsg), 0x0, sizeof(inmsg));
    securec_check(ss_rc, "\0", "\0");
    inmsg.msg_name = NULL;
    inmsg.msg_namelen = 0;
    inmsg.msg_iov = &iov;
    inmsg.msg_iovlen = 1;
    inmsg.msg_control = incmsg;
    inmsg.msg_controllen = sizeof(incmsg);

    while (false == is_sock_err) {
        error = recvmsg(sock, &inmsg, flags);

        if (error < 0) {
            // no data to recieve, and no socket error
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else if (errno == EINTR) {  // need retry
                continue;
            } else {  // other errno means really error
                is_sock_err = true;
                break;
            }
        }

        // remote has closed
        if (error == 0) {
            is_sock_err = true;
            break;
        }

        // recieved notification
        if (MSG_NOTIFICATION & inmsg.msg_flags) {
            union comm_notification* sn = (union comm_notification*)inmsg.msg_iov[0].iov_base;
            switch (sn->sn_header.sn_type) {
                case COMM_SHUTDOWN_EVENT:
                    mc_sctp_print_message(&inmsg, error);
                    is_sock_err = true;
                    break;
                case COMM_CONN_CHANGE:
                    switch (sn->conn_change.conn_state) {
                        case COMM_LOST:
                        case COMM_SHUTDOWN:
                            mc_sctp_print_message(&inmsg, error);
                            is_sock_err = true;
                            break;
                        case COMM_RESTART:
                            mc_sctp_print_message(&inmsg, error);
                            is_sock_err = true;
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
        }
    }
    LIBCOMM_FREE(iov.iov_base, IOV_DATA_SIZE);

    if (is_sock_err) {
        return -1;
    }

    return 0;
}

#ifdef SCTP_DEBUG
static union comm_notification* mc_sctp_get_sctp_notification(struct msghdr* msg)
{
    if (!(msg->msg_flags & MSG_NOTIFICATION)) {
        return NULL;
    }

    union comm_notification* sn = (union comm_notification*)msg->msg_iov->iov_base;
    if (COMM_CONN_CHANGE != sn->sn_header.sn_type) {
        return NULL;
    }

    return sn;
}

// get associtaion from sctp message
//
static int mc_sctp_get_assoc_id(struct msghdr* msg, comm_assoc_t* assoc_id)
{
    union comm_notification* sn = NULL;
    if ((sn = mc_sctp_get_sctp_notification(msg)) == NULL) {
        return -1;
    }

    if (COMM_UP == sn->conn_change.conn_state) {
        *assoc_id = sn->conn_change.conn_connect_id;
        return 0;
    }

    return -1;
}
#endif

/*
 * function name	: mc_sctp_recv_block_mode
 * description	: receive sctp message in block mode.
 * arguments		: _in_ fd: the socket fd.
 *				  _in_ msg: the point for message.
 *				  _in_ msg_len: the len of message.
 * return value	:
 * 				  -1: failed.
 * 				  0: succeed.
 */
int mc_sctp_recv_block_mode(int fd, char* msg, int msg_len)
{
    int error = 0;
    int recv_bytes = 0;

    for (;;) {
        error = mc_sctp_recv_ack(fd, msg + recv_bytes, msg_len - recv_bytes);
        if (error > 0) {
            recv_bytes += error;
        } else if (error == 0) {
            break;
        } else if (error < 0 && errno != EAGAIN && errno != EINTR) {
            break;
        }

        if (recv_bytes == msg_len) {
            break;
        }
    }

    return (recv_bytes == msg_len ? 0 : -1);
}

/*
 * function name	: mc_sctp_send_block_mode
 * description	: send sctp message in block mode used streamid=0.
 * arguments		: _in_ fd: the socket fd.
 *				  _in_ msg: the point for message.
 *				  _in_ msg_len: the len of message.
 * return value	:
 * 				  -1: failed.
 * 				  0: succeed.
 */
int mc_sctp_send_block_mode(int fd, const void* data, int size)
{
    int error = 0;
    int send_bytes = 0;

    for (;;) {
        error = mc_sctp_send_ack(fd, (char*)data + send_bytes, size - send_bytes);
        if (error > 0) {
            send_bytes += error;
        } else if (error == 0) {
            break;
        } else if (error < 0 && errno != EAGAIN && errno != EINTR) {
            break;
        }

        if (send_bytes == size) {
            break;
        }
    }

    return (send_bytes == size ? send_bytes : -1);
}

#ifdef SCTP_DEBUG
// Display an IPv4 address in readable format.
//
#define NIPQUAD(addr) \
    ((unsigned char*)&addr)[0], ((unsigned char*)&addr)[1], ((unsigned char*)&addr)[2], ((unsigned char*)&addr)[3]

// Display an IPv6 address in readable format.
//
#define NIP6(addr)                                                                                                  \
    ntohs((addr).s6_addr16[0]), ntohs((addr).s6_addr16[1]), ntohs((addr).s6_addr16[2]), ntohs((addr).s6_addr16[3]), \
        ntohs((addr).s6_addr16[4]), ntohs((addr).s6_addr16[5]), ntohs((addr).s6_addr16[6]), ntohs((addr).s6_addr16[7])

int mc_sctp_check_association(int sock, comm_assoc_t assoc_id)
{
    struct sockaddr *laddrs = NULL, *paddrs = NULL;
    int n_laddrs, n_paddrs, i, is_equal = -1;
    int laddr_buf_size, paddr_buf_size;
    struct sockaddr* lsa_addr = NULL;

    n_laddrs = mc_sctp_getladdrs(sock, assoc_id, &laddrs, &laddr_buf_size);
    if (n_laddrs <= 0) {
        LIBCOMM_ELOG(WARNING, "mc_sctp_getladdrs:%s.", strerror(errno));
        return -1;
    }
    void* laddr_buf = (void*)laddrs;

    n_paddrs = mc_sctp_getpaddrs(sock, assoc_id, &paddrs, &paddr_buf_size);
    if (n_paddrs <= 0) {
        LIBCOMM_ELOG(WARNING, "mc_sctp_getpaddrs:%s.", strerror(errno));
        return -1;
    }
    void* paddr_buf = (void*)paddrs;

#ifdef SCTP_DEBUG
    struct sockaddr_in* in_addr = NULL;
    struct sockaddr_in6* in6_addr = NULL;
    struct sockaddr* psa_addr = NULL;
    for (i = 0; i < n_laddrs; i++) {
        lsa_addr = (struct sockaddr*)laddr_buf;
        if (AF_INET == lsa_addr->sa_family) {
            in_addr = (struct sockaddr_in*)lsa_addr;
            LIBCOMM_ELOG(LOG, "LOCAL ADDR %d.%d.%d.%d PORT %d", NIPQUAD(in_addr->sin_addr), ntohs(in_addr->sin_port));
            laddr_buf = (void*)((char*)laddr_buf + sizeof(struct sockaddr_in));
        } else {
            in6_addr = (struct sockaddr_in6*)lsa_addr;
            LIBCOMM_ELOG(LOG,
                "LOCAL ADDR %04x:%04x:%04x:%04x:%04x:%04x:%04x:%04x PORT %d",
                NIP6(in6_addr->sin6_addr),
                ntohs(in6_addr->sin6_port));
            laddr_buf = (void*)((char*)laddr_buf + sizeof(struct sockaddr_in6));
        }
    }
    for (i = 0; i < n_paddrs; i++) {
        psa_addr = (struct sockaddr*)paddr_buf;
        if (AF_INET == psa_addr->sa_family) {
            in_addr = (struct sockaddr_in*)psa_addr;
            LIBCOMM_ELOG(LOG, "PEER ADDR %d.%d.%d.%d PORT %d", NIPQUAD(in_addr->sin_addr), ntohs(in_addr->sin_port));
            paddr_buf = (void*)((char*)paddr_buf + sizeof(struct sockaddr_in));
        } else {
            in6_addr = (struct sockaddr_in6*)psa_addr;
            LIBCOMM_ELOG(LOG,
                "PEER ADDR %04x:%04x:%04x:%04x:%04x:%04x:%04x:%04x PORT %d",
                NIP6(in6_addr->sin6_addr),
                ntohs(in6_addr->sin6_port));
            paddr_buf = (void*)((char*)paddr_buf + sizeof(struct sockaddr_in6));
        }
    }
#endif
    laddr_buf = (void*)laddrs;
    paddr_buf = (void*)paddrs;
    if (n_laddrs == n_paddrs) {
        for (i = 0; i < n_laddrs; i++) {
            lsa_addr = (struct sockaddr*)laddr_buf;
            if (AF_INET == lsa_addr->sa_family) {
                is_equal = cmp_addr((sockaddr_storage_t*)laddr_buf, (sockaddr_storage_t*)paddr_buf);
                if (is_equal != 0) {
                    break;
                }
                laddr_buf = (void*)((char*)laddr_buf + sizeof(struct sockaddr_in));
                paddr_buf = (void*)((char*)paddr_buf + sizeof(struct sockaddr_in));
            } else {
                is_equal = cmp_addr((sockaddr_storage_t*)laddr_buf, (sockaddr_storage_t*)paddr_buf);
                if (is_equal != 0) {
                    break;
                }
                laddr_buf = (void*)((char*)laddr_buf + sizeof(struct sockaddr_in6));
                paddr_buf = (void*)((char*)paddr_buf + sizeof(struct sockaddr_in6));
            }
        }
    } else {
        is_equal = -1;
    }

    (void)mc_sctp_freeladdrs(laddrs, laddr_buf_size);
    (void)mc_sctp_freeladdrs(paddrs, paddr_buf_size);
    if (is_equal != 0) {
        return 0;
    } else {
        LIBCOMM_ELOG(WARNING, "Receiver address and the sender address are the same.");
        return -1;
    }
}
#endif

static void mc_sctp_print_cmsg(comm_msg_type type, comm_msg_data_t* data)
{
    switch (type) {
        case COMM_INIT:
            LIBCOMM_ELOG(LOG, "INIBUFF");
            LIBCOMM_ELOG(LOG, "   num_output %d.", data->init.num_output);
            LIBCOMM_ELOG(LOG, "   max_input %d.", data->init.max_input);
            LIBCOMM_ELOG(LOG, "   max_attempts %d.", data->init.max_attempts);
            LIBCOMM_ELOG(LOG, "   max_timeout %d.", data->init.max_timeout);
            break;
        case COMM_BUFF:
            LIBCOMM_ELOG(LOG, "BUFF");
            LIBCOMM_ELOG(LOG, "   istream %u.", data->buff.istream);
            LIBCOMM_ELOG(LOG, "   issn %u.", data->buff.issn);
            LIBCOMM_ELOG(LOG, "   itypes 0x%x.", data->buff.itypes);
            LIBCOMM_ELOG(LOG, "   ipid %u.", data->buff.ipid);
            LIBCOMM_ELOG(LOG, "   ictx %x.", data->buff.ictx);
            LIBCOMM_ELOG(LOG, "   itsn     %u.", data->buff.itsn);
            LIBCOMM_ELOG(LOG, "   ctsn  %u.", data->buff.ctsn);
            LIBCOMM_ELOG(LOG, "   id  %u.", data->buff.id);

            break;

        default:
            LIBCOMM_ELOG(LOG, "UNKNOWN CMSG: %d.", type);
            break;
    }
}

void mc_sctp_print_message(struct msghdr* msg, size_t msg_len)
{
    comm_msg_data_t* data = NULL;
    struct cmsghdr* cmsg = NULL;
    int i, done;
    char save;
    union comm_notification* sn = NULL;

    for (cmsg = CMSG_FIRSTHDR(msg); cmsg != NULL; cmsg = CMSG_NXTHDR(msg, cmsg)) {
        data = (comm_msg_data_t*)CMSG_DATA(cmsg);
        mc_sctp_print_cmsg((comm_msg_type)cmsg->cmsg_type, data);
    }

    if (!(MSG_NOTIFICATION & msg->msg_flags)) {
        int idx = 0;
        // Make sure that everything is printable and that we
        // are NUL terminated...
        LIBCOMM_ELOG(WARNING, "DATA(%d):  ", (int)msg_len);
        while (msg_len > 0) {
            char* txt = NULL;
            int len;

            txt = (char*)msg->msg_iov[idx].iov_base;
            len = msg->msg_iov[idx].iov_len;

            save = txt[msg_len - 1];
            if (len > (int)msg_len) {
                txt[(len = msg_len) - 1] = '\0';
            }

            if ((msg_len -= len) > 0) {
                idx++;
            }

            for (i = 0; i < len - 1; ++i) {
                if (!isprint(txt[i])) {
                    txt[i] = '.';
                }
            }

            LIBCOMM_ELOG(WARNING, "%s.", txt);
            txt[msg_len - 1] = save;

            done = !strcmp(txt, "exit");
            if (done) {
                break;
            }
        }
    } else {
        sn = (union comm_notification*)msg->msg_iov[0].iov_base;
        switch (sn->sn_header.sn_type) {
            case COMM_CONN_CHANGE:
                switch (sn->conn_change.conn_state) {
                    case COMM_UP:
                        LIBCOMM_ELOG(WARNING, "NOTIFICATION: COMM_CONN_CHANGE - COMM_UP");
                        break;
                    case COMM_LOST:
                        LIBCOMM_ELOG(WARNING, "NOTIFICATION: COMM_CONN_CHANGE - COMM_LOST");
                        break;
                    case COMM_RESTART:
                        LIBCOMM_ELOG(WARNING, "NOTIFICATION: COMM_CONN_CHANGE - RESTART");
                        break;
                    case COMM_SHUTDOWN:
                        LIBCOMM_ELOG(WARNING, "NOTIFICATION: COMM_CONN_CHANGE - SHUTDOWN_COMP");
                        break;
                    case COMM_CANT_CONNECT:
                        LIBCOMM_ELOG(WARNING, "NOTIFICATION: COMM_CONN_CHANGE - CANT_STR_ASSOC");
                        break;
                    default:
                        LIBCOMM_ELOG(
                            WARNING, "NOTIFICATION: COMM_CONN_CHANGE - UNEXPECTED(%d)", sn->conn_change.conn_state);
                        break;
                }
                break;
            case COMM_SHUTDOWN_EVENT:
                LIBCOMM_ELOG(WARNING, "NOTIFICATION: COMM_SHUTDOWN_EVENT");
                break;
            case COMM_ADDR_CHANGE:
                LIBCOMM_ELOG(WARNING, "NOTIFICATION: COMM_ADDR_CHANGE");
                break;
            case COMM_FAILED:
                LIBCOMM_ELOG(WARNING, "NOTIFICATION: COMM_FAILED");
                break;
            case COMM_ERROR:
                LIBCOMM_ELOG(WARNING, "NOTIFICATION: COMM_ERROR");
                break;
            default:
                LIBCOMM_ELOG(WARNING, "NOTIFICATION: UNEXPECTED(%d)", sn->sn_header.sn_type);
                break;
        }
    }
}
#endif
