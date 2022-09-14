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
 * mc_tcp.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_core/mc_tcp.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "mc_tcp.h"
#include "libcomm_utils/libcomm_util.h"
#include "libcomm_utils/libcomm_err.h"

#ifdef USE_SSL
#include "knl/knl_session.h"
#include "libpq/libpq-be.h"
#include "libcomm/libcomm.h"
#include "../libcomm_common.h"
#endif

void mc_tcp_setsockopt(int fd, int level, int optname, const void* optval, socklen_t optlen)
{
    if (setsockopt(fd, level, optname, optval, optlen) < 0) {
        errno_assert(errno);
    }
}

void mc_tcp_set_keepalive_param(int idle, int intvl, int count)
{
    g_instance.comm_cxt.mctcp_cxt.mc_tcp_keepalive_idle = (idle > 0) ? idle : 0;
    g_instance.comm_cxt.mctcp_cxt.mc_tcp_keepalive_interval = (intvl > 0) ? intvl : 0;
    g_instance.comm_cxt.mctcp_cxt.mc_tcp_keepalive_count = (count > 0) ? count : 0;
}

void mc_tcp_set_user_timeout(int timeout)
{
    g_instance.comm_cxt.mctcp_cxt.mc_tcp_user_timeout = (timeout > 0) ? timeout : 0;
}

void mc_tcp_set_timeout_param(int conn_timeout, int send_timeout)
{
    g_instance.comm_cxt.mctcp_cxt.mc_tcp_connect_timeout = (conn_timeout > 0) ? conn_timeout : 0;
    g_instance.comm_cxt.mctcp_cxt.mc_tcp_send_timeout = (send_timeout > 0) ? send_timeout : 0;
}

int mc_tcp_get_connect_timeout()
{
    return g_instance.comm_cxt.mctcp_cxt.mc_tcp_connect_timeout;
}

void mc_tcp_set_timeout(int fd, int timeo)
{
    if (timeo == 0) {
        return;
    }

    struct timeval timeout = {timeo, 0};
    socklen_t len = sizeof(timeout);
    mc_tcp_setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout, len);
    mc_tcp_setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, len);
}

void mc_tcp_set_keepalive(int fd)
{
    int on = 1;
    int idle = g_instance.comm_cxt.mctcp_cxt.mc_tcp_keepalive_idle;
    int interval = g_instance.comm_cxt.mctcp_cxt.mc_tcp_keepalive_interval;
    int count = g_instance.comm_cxt.mctcp_cxt.mc_tcp_keepalive_count;
    int tcp_user_timeout = g_instance.comm_cxt.mctcp_cxt.mc_tcp_user_timeout;

    mc_tcp_setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (char*)&on, sizeof(on));
    mc_tcp_setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, (char*)&idle, sizeof(idle));
    mc_tcp_setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, (char*)&interval, sizeof(interval));
    mc_tcp_setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, (char*)&count, sizeof(count));
#ifdef TCP_USER_TIMEOUT
    mc_tcp_setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, (char*)&tcp_user_timeout, sizeof(tcp_user_timeout));
#endif
}

int mc_tcp_get_peer_name(int fd, char* host, int* port)
{
    struct sockaddr peeraddr = {0};
    socklen_t len = sizeof(struct sockaddr);
    int ret = getpeername(fd, (struct sockaddr*)&peeraddr, &len);
    if (ret < 0) {
        return ret;
    }

    if (AF_INET == peeraddr.sa_family) {
        struct sockaddr_in* paddr = (struct sockaddr_in*)&peeraddr;
        *port = ntohs(paddr->sin_port);
        if (inet_ntop(AF_INET, &(paddr->sin_addr), host, HOST_ADDRSTRLEN) == NULL) {
            ret = -2;
        }
    } else if (AF_INET6 == peeraddr.sa_family) {
        struct sockaddr_in6* paddr = (struct sockaddr_in6*)&peeraddr;
        *port = ntohs(paddr->sin6_port);
        if (inet_ntop(AF_INET6, &(paddr->sin6_addr), host, HOST_ADDRSTRLEN) == NULL) {
            ret = -3;
        }
    } else {
        ret = -4;
    }

    return ret;
}

// set socket to NON-BLOCKING to epoll on it
//
int mc_tcp_set_nonblock(int fd)
{
    return set_socketopt(fd, 0, O_NONBLOCK);
}

int mc_tcp_set_cloexec(int fd)
{
    return set_socketopt(fd, 1, FD_CLOEXEC);
}

int mc_tcp_accept(int fd, struct sockaddr* sa, socklen_t* salenptr)
{
    int new_fd;
again:
    if ((new_fd = accept4(fd, sa, salenptr, SOCK_CLOEXEC)) < 0) {
#ifdef EPROTO
        if (errno == EPROTO || errno == ECONNABORTED) {
#else
        if (errno == ECONNABORTED) {
#endif
            goto again;
        } else {
            errno_assert(errno);
        }
    }

    if (new_fd > 0) {
        (void)mc_tcp_set_nonblock(new_fd);  // set non-block

        int no_delay = (g_instance.comm_cxt.commutil_cxt.g_no_delay) ? 1 : 0;
        mc_tcp_setsockopt(new_fd, IPPROTO_TCP, TCP_NODELAY, &no_delay, sizeof(no_delay));  // set no delay
        mc_tcp_set_keepalive(new_fd);
    }
    return (new_fd);
}

int mc_tcp_bind(int fd, const struct sockaddr* sa, socklen_t salen)
{
    int error = -1;
    int retry_time = MAX_BIND_RETRYS;
    sockaddr_in* addr = (sockaddr_in*)sa;

    while (retry_time--) {
        error = bind(fd, sa, salen);
        if (error == 0) {
            break;
        }

        uint16 port = ntohs(addr->sin_port);

        LIBCOMM_ELOG(WARNING,
            "(mc tcp listen)\tFailed to bind host:port[%s:%hu], errno[%d]:%s."
            "Maybe port %hu is used, run 'netstat -anop|grep %hu' or "
            "'lsof -i:%hu'(need root) to see who is using.",
            inet_ntoa(addr->sin_addr),
            port,
            errno,
            mc_strerror(errno),
            port,
            port,
            port);

        (void)sleep(5);
    }
    return error;
}

static int mc_tcp_do_connect(int fd, const struct sockaddr* sa, socklen_t salen)
{
    int error = connect(fd, sa, salen);
    return error;
}

static void mc_tcp_do_listen(int fd, int backlog)
{
    if (listen(fd, backlog) < 0) {
        errno_assert(errno);
    }
}

int mc_tcp_read_block(int fd, void* data, int size, int flags)
{
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_MC_TCP_READ_BLOCK_FAILED)) {
        LIBCOMM_ELOG(WARNING, "(mc tcp read block)\t[FAULT INJECTION]Failed to read block for %d.", fd);
        return -1;
    }
#endif
    uint64 time_enter, time_now;
    time_enter = mc_timers_ms();
    ssize_t nbytes = 0;
    int rc = 0;

#ifdef USE_SSL
    SSL *ssl = NULL;

    LIBCOMM_FIND_SSL(ssl, fd, "(mc tcp read block)\tNot find ssl for sock ");
#endif

    // In our application, if we do not get an integrated message, we must continue receiving.
    //
    while (nbytes != size) {
#ifdef USE_SSL
        if (g_instance.attr.attr_network.comm_enable_SSL) {
            rc = LibCommClientSSLRead(ssl, (char*)data + nbytes, size - nbytes);
        } else
#endif
        {
            rc = recv(fd, (char*)data + nbytes, size - nbytes, flags);
        }

        if (rc > 0) {
            if (((char*)data)[0] == '\0') {
                LIBCOMM_ELOG(ERROR, "(mc tcp read block)\tIllegal message from sock %d.", fd);
                return -1;
            }

            nbytes = nbytes + rc;

        } else if (rc == 0) { //  Orderly shutdown by the other peer.
            nbytes = 0;
            break;
        } else if (rc < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                time_now = mc_timers_ms();
                // If can not receive data after 600 seconds later,
                // we think this connection has problem
                if (((time_now - time_enter) >
                        ((uint64)(unsigned)g_instance.comm_cxt.mctcp_cxt.mc_tcp_send_timeout * SEC_TO_MICRO_SEC)) &&
                    (time_now > time_enter)) {
                    errno = ECOMMTCPSENDTIMEOUT;
                    return -1;
                }
                (void)usleep(1);
                continue;
            } else {
                nbytes = -1;
                break;
            }
        }
    }
    /* Orderly shutdown by the other peer or Signalise peer failure. */
    if ((nbytes == 0) || (nbytes == -1 && (errno == ECONNRESET || errno == ECONNREFUSED || errno == ETIMEDOUT ||
                                              errno == EHOSTUNREACH))) {
        return -1;
    }

    return (size_t)nbytes;
}

int mc_tcp_read_nonblock(int fd, void* data, int size, int flags)
{
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_MC_TCP_READ_NONBLOCK_FAILED)) {
        LIBCOMM_ELOG(WARNING, "(mc tcp read nonblock)\t[FAULT INJECTION]Failed to read nonblock for %d.", fd);
        return -1;
    }
#endif

    ssize_t nbytes = 0;

#ifdef USE_SSL
    if (g_instance.attr.attr_network.comm_enable_SSL) {
        SSL *ssl = NULL;
        LIBCOMM_FIND_SSL(ssl, fd, "(mc tcp read nonblock)\tNot find ssl for sock ");
        nbytes = LibCommClientSSLRead(ssl, data, size);
    } else
#endif /* USE_SSL */
    {
        nbytes = recv(fd, data, size, flags);
    }

    //  Several errors are OK. When speculative read is being done we may not
    //  be able to read a single byte to the socket. Also, SIGSTOP issued
    //  by a debugging tool can result in EINTR error.
    //
    if (nbytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        return 0;
    }

    //  Signalise peer failure.
    //
    if (nbytes == -1) {
        return -1;
    }

    //  Orderly shutdown by the other peer.
    //
    if (nbytes == 0) {
        return -1;
    }

    return (size_t)nbytes;
}

int mc_tcp_check_socket(int sock)
{
    char temp_buf[IOV_DATA_SIZE] = {0};
    bool is_sock_err = false;
    int error = -1;

    if (sock < 0) {
        return -1;
    }

#ifdef USE_SSL
    SSL *ssl = NULL;

    LIBCOMM_FIND_SSL(ssl, sock, "(mc tcp check socket)\tNot find ssl for sock ");
#endif

    LIBCOMM_ELOG(LOG, "mc_tcp_check_socket start.");

    while (false == is_sock_err) {
#ifdef USE_SSL
        if (g_instance.attr.attr_network.comm_enable_SSL) {
            error = LibCommClientSSLRead(ssl, temp_buf, IOV_DATA_SIZE);
        } else
#endif
        {
            error = recv(sock, temp_buf, IOV_DATA_SIZE, 0);
        }
        if (error < 0) {
            // no data to recieve, and no socket error
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            // need retry
            else if (errno == EINTR) {
                continue;
            }
            // other errno means really error
            else {
                is_sock_err = true;
                break;
            }
        }

        // remote has closed
        if (error == 0) {
            is_sock_err = true;
            break;
        }

        if (error > 0) {
            // something fault data
            continue;
        }
    }

    if (is_sock_err) {
        return -1;
    }

    return 0;
}

int mc_tcp_write_block(int fd, const void* data, int size)
{
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_MC_TCP_WRITE_FAILED)) {
        LIBCOMM_ELOG(WARNING, "(mc tcp write)\t[FAULT INJECTION]Failed to write for %d.", fd);
        shutdown(fd, SHUT_RDWR);
        return -1;
    }
#endif
    ssize_t nbytes;
    ssize_t nSend = 0;
    const int flags = 0;
    uint64 time_enter, time_now;
    time_enter = mc_timers_ms();

#ifdef USE_SSL
    SSL *ssl = NULL;

    LIBCOMM_FIND_SSL(ssl, fd, "(mc tcp write block)\tNot find ssl for sock ");
#endif

    //  Several errors are OK. When speculative write is being done we may not
    //  be able to write a single byte to the socket. Also, SIGSTOP issued
    //  by a debugging tool can result in EINTR error.
    //
    while (nSend != size) {
#ifdef USE_SSL
        if (g_instance.attr.attr_network.comm_enable_SSL) {
            nbytes = LibCommClientSSLWrite(ssl, (void*)((char*)data + nSend), size - nSend);
        } else
#endif
        {
            nbytes = send(fd, (const void*)((char*)data + nSend), size - nSend, flags);
        }

        if (nbytes <= 0) {
            if (nbytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR || errno == ENOBUFS)) {
                time_now = mc_timers_ms();
                // If can not receive data after 600 seconds later,
                // we think this connection has problem
                if (((time_now - time_enter) >
                        ((uint64)(unsigned)g_instance.comm_cxt.mctcp_cxt.mc_tcp_send_timeout * SEC_TO_MICRO_SEC)) &&
                    (time_now > time_enter)) {
                    errno = ECOMMTCPSENDTIMEOUT;
                    return -1;
                }
                continue;
            }

            return -1;
        } else {
            nSend += nbytes;
        }
    }

    return (size_t)nSend;
}

int mc_tcp_write_noblock(int fd, const void* data, int size)
{
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_MC_TCP_WRITE_NONBLOCK_FAILED)) {
        LIBCOMM_ELOG(WARNING, "(mc tcp write noblock)\t[FAULT INJECTION]Failed to write nonblock for %d.", fd);
        shutdown(fd, SHUT_RDWR);
        return -1;
    }
#endif
    ssize_t nbytes;
    const int flags = 0;

#ifdef USE_SSL
    SSL *ssl = NULL;

    LIBCOMM_FIND_SSL(ssl, fd, "(mc tcp write nonblock)\tNot find ssl for sock ");
#endif

    //  Several errors are OK. When speculative write is being done we may not
    //  be able to write a single byte to the socket. Also, SIGSTOP issued
    //  by a debugging tool can result in EINTR error.
    //
#ifdef USE_SSL
    if (g_instance.attr.attr_network.comm_enable_SSL) {
        nbytes = LibCommClientSSLWrite(ssl, data, size);
    } else
#endif
    {
        nbytes = send(fd, data, size, flags);
    }

    if (nbytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR || errno == ENOBUFS)) {
        return 0;
    }

    if (nbytes <= 0) {
        return -1;
    }

    return (size_t)nbytes;
}

// initialize Socket
//
int mc_tcp_socket(int family, int type, int protocol)
{
    int error = socket(family, type, protocol);

    return (error);
}

void mc_tcp_close(int fd)
{
    int error = 0;
    LIBCOMM_ELOG(WARNING, "(mc tcp close)\tClose socket[%d].", fd);
    error = close(fd);

    if (error != 0) {
        LIBCOMM_ELOG(WARNING, "(mc tcp close)\tFailed to close socket[%d]:%s.", fd, strerror(errno));
    }
    return;
}

/* init a sctp mc_tcp_addr_init with host and port */
int mc_tcp_addr_init(const char* host, int port, struct sockaddr_storage* ss, int* in_len)
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

int mc_tcp_connect_nonblock(const char* host, int port)
{
    int sockfd, n;

    struct addrinfo hints = {0};
    struct addrinfo *res = NULL;

    errno_t ss_rc = memset_s(&hints, sizeof(hints), 0, sizeof(struct addrinfo));
    securec_check(ss_rc, "\0", "\0");

    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char serv[NI_MAXSERV];
    int rc = snprintf_s(serv, NI_MAXSERV, NI_MAXSERV - 1, "%d", port);
    securec_check_ss(rc, "\0", "\0");

    do {
        if (strcmp(host, "localhost") == 0) {
            n = getaddrinfo(NULL, serv, &hints, &res);
        } else {
            n = getaddrinfo(host, serv, &hints, &res);
        }
    } while (n == EAI_AGAIN);

    if (n != 0) {
        return -1;
    }

    sockfd = mc_tcp_socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sockfd < 0) {
        freeaddrinfo(res);
        return -1;
    }

    if (mc_tcp_set_nonblock(sockfd) < 0) {
        freeaddrinfo(res);
        return -1;
    }

    errno = 0;
    mc_tcp_do_connect(sockfd, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);

    return sockfd;
}

int mc_tcp_connect(const char* host, int port)
{
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_MC_TCP_CONNECT_FAILED)) {
        LIBCOMM_ELOG(
            WARNING, "(mc tcp connect)\t[FAULT INJECTION]Failed to do control tcp listen for %s:%d.", host, port);
        return -1;
    }
#endif
    mc_assert(port > 0);

    errno = 0;
    int sockfd, n, error = 0;

    struct addrinfo hints = {0};
    struct addrinfo *res = NULL, *ressave = NULL;

    errno_t ss_rc = memset_s(&hints, sizeof(hints), 0, sizeof(struct addrinfo));
    securec_check(ss_rc, "\0", "\0");

    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char serv[NI_MAXSERV];
    int rc = snprintf_s(serv, NI_MAXSERV, NI_MAXSERV - 1, "%d", port);
    securec_check_ss(rc, "\0", "\0");

retry:
    if (strcmp(host, "localhost") == 0) {
        n = getaddrinfo(NULL, serv, &hints, &res);
    } else {
        n = getaddrinfo(host, serv, &hints, &res);
    }

    if (n == EAI_AGAIN) {
        goto retry;
    }

    if (n != 0) {
        LIBCOMM_ELOG(WARNING,
            "(mc tcp connect)\tFailed to get address infomation for host:port[%s:%s] error[%d]:%s.",
            host,
            serv,
            n,
            mc_strerror(errno));
        return -1;
    }

    ressave = res;

    do {
        sockfd = mc_tcp_socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (sockfd < 0) {
            continue;  // ignore this one
        }
        // set tcp connection socket properties
        //
        int no_delay = (g_instance.comm_cxt.commutil_cxt.g_no_delay) ? 1 : 0;
        mc_tcp_setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &no_delay, sizeof(no_delay));
        mc_tcp_set_keepalive(sockfd);
        mc_tcp_set_timeout(sockfd, g_instance.comm_cxt.mctcp_cxt.mc_tcp_connect_timeout);

        error = mc_tcp_set_cloexec(sockfd);

        if (error == 0 && (mc_tcp_do_connect(sockfd, res->ai_addr, res->ai_addrlen) == 0)) {
            error = mc_tcp_set_nonblock(sockfd);  // set non-block
            if (error == 0) {
                break;  // success
            }
        }
        LIBCOMM_ELOG(WARNING,
            "(mc tcp connect)\tFailed to build TCP connect to host:port[%s:%s], family[%d],error[%d]:%s.",
            host,
            serv,
            res->ai_family,
            errno,
            mc_strerror(errno));

        mc_tcp_close(sockfd);  // ignore this one
        sockfd = -1;

    } while ((res = res->ai_next) != NULL);

    if (ressave != NULL) {
        freeaddrinfo(ressave);
    }

    return (sockfd);
}

int mc_tcp_listen(const char* host, int port, socklen_t* addrlenp)
{
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    if (is_comm_fault_injection(LIBCOMM_FI_MC_TCP_LISTEN_FAILED)) {
        LIBCOMM_ELOG(
            WARNING, "(mc tcp listen)\t[FAULT INJECTION]Failed to do control tcp listen for %s:%d.", host, port);
        return -1;
    }
#endif

    errno = 0;
    int listenfd, n, error = 0;
    const int on = 1;
    struct addrinfo hints = {0};
    struct addrinfo *res = NULL, *ressave = NULL;
    errno_t ss_rc;
    int rc;

    char serv[NI_MAXSERV];
    rc = snprintf_s(serv, NI_MAXSERV, NI_MAXSERV - 1, "%d", port);
    securec_check_ss(rc, "\0", "\0");

    ss_rc = memset_s(&hints, sizeof(hints), 0, sizeof(struct addrinfo));
    securec_check(ss_rc, "\0", "\0");

    hints.ai_flags = AI_PASSIVE;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

retry:
    if (strcmp(host, "localhost") == 0) {
        n = getaddrinfo(NULL, serv, &hints, &res);
    } else {
        n = getaddrinfo(host, serv, &hints, &res);
    }

    if (n == EAI_AGAIN) {
        goto retry;
    }

    if (n != 0 || res == NULL) {
        LIBCOMM_ELOG(WARNING,
            "(mc tcp listen)\tFailed to get address infomation for host:port[%s:%s] error[%d]:%s.",
            host,
            serv,
            n,
            mc_strerror(errno));
        return -1;
    }

    ressave = res;

    do {

        listenfd = mc_tcp_socket(res->ai_family, res->ai_socktype, res->ai_protocol);

        if (listenfd < 0) {
            continue;  // error, try next one
        }
        // set tcp connection socket properties
        //
        mc_tcp_setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
        error = mc_tcp_set_cloexec(listenfd);
        if (error == 0) {
            error = mc_tcp_set_nonblock(listenfd);
        }

        if (error == 0 && (mc_tcp_bind(listenfd, res->ai_addr, res->ai_addrlen) == 0)) {
            break;  // success
        }
        mc_tcp_close(listenfd);  // bind error, close and try next one
        listenfd = -1;
    } while ((res = res->ai_next) != NULL);

    if (listenfd != -1) {
        mc_tcp_do_listen(listenfd, TCP_LISTENQ);

        if (addrlenp != NULL && res != NULL) {
            *addrlenp = res->ai_addrlen;  // return size of protocol address
        }
        COMM_DEBUG_LOG("(mc tcp listen)\tControl tcp listen for %s:%s on socket[%d].", host, serv, listenfd);
    } else {
        LIBCOMM_ELOG(WARNING, "(mc tcp listen)\tFailed to do control tcp listen for %s:%s.", host, serv);
    }

    freeaddrinfo(ressave);
    return (listenfd);
}
