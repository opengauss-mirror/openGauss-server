/* ---------------------------------------------------------------------------------------
 *
 *  pqcomm.cpp
 *        Communication functions between the Frontend and the Backend
 *
 * These routines handle the low-level details of communication between
 * frontend and backend.  They just shove data across the communication
 * channel, and are ignorant of the semantics of the data --- or would be,
 * except for major brain damage in the design of the old COPY OUT protocol.
 * Unfortunately, COPY OUT was designed to commandeer the communication
 * channel (it just transfers data without wrapping it into messages).
 * No other messages can be sent while COPY OUT is in progress; and if the
 * copy is aborted by an ereport(ERROR), we need to close out the copy so that
 * the frontend gets back into sync.  Therefore, these routines have to be
 * aware of COPY OUT state.  (New COPY-OUT is message-based and does *not*
 * set the DoingCopyOut flag.)
 *
 * NOTE: generally, it's a bad idea to emit outgoing messages directly with
 * pq_putbytes(), especially if the message would require multiple calls
 * to send.  Instead, use the routines in pqformat.c to construct the message
 * in a buffer and then emit it in one call to pq_putmessage.  This ensures
 * that the channel will not be clogged by an incomplete message if execution
 * is aborted by ereport(ERROR) partway through the message.  The only
 * non-libpq code that should call pq_putbytes directly is old-style COPY OUT.
 *
 * At one time, libpq was shared between frontend and backend, but now
 * the backend's "backend/libpq" is quite separate from "interfaces/libpq".
 * All that remains is similarities of names to trap the unwary...
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/heartbeat/libpq/pqcomm.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
/* ------------------------
 * INTERFACE ROUTINES
 *
 * setup/teardown:
 *		StreamServerPort	- Open postmaster's server port
 *		StreamConnection	- Create new connection with client
 *		StreamClose			- Close a client/backend connection
 *		TouchSocketFile		- Protect socket file against /tmp cleaners
 *		pq_init			- initialize libpq at backend startup
 *		pq_comm_reset	- reset libpq during error recovery
 *		pq_close		- shutdown libpq at backend exit
 *
 * low-level I/O:
 *		pq_getbytes		- get a known number of bytes from connection
 *		pq_getstring	- get a null terminated string from connection
 *		pq_getmessage	- get a message with length word from connection
 *		pq_getbyte		- get next byte from connection
 *		pq_peekbyte		- peek at next byte from connection
 *		pq_putbytes		- send bytes to connection (not flushed until pq_flush)
 *		pq_flush		- flush pending output
 *
 * message-level I/O (and old-style-COPY-OUT cruft):
 *		pq_putmessage	- send a normal message (suppressed in COPY OUT mode)
 *		pq_startcopyout - inform libpq that a COPY OUT transfer is beginning
 *		pq_endcopyout	- end a COPY OUT transfer
 *
 * ------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "pg_config.h"

#include <signal.h>
#include <fcntl.h>
#include <grp.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>
#ifdef HAVE_UTIME_H
#include <utime.h>
#endif
#include "libpq/ip.h"
#include "replication/heartbeat/libpq/pqcomm.h"
#include "replication/heartbeat/libpq/libpq.h"
#include "replication/heartbeat/libpq/libpq-be.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "pgstat.h"

namespace PureLibpq {
#define MAXGTMPATH 256
#define TCP_SOCKET_ERROR_EPIPE (-2)
#define TCP_SOCKET_ERROR_NO_MESSAGE (-3)
#define TCP_SOCKET_ERROR_NO_BUFFER (-4)
#define TCP_SOCKET_ERROR_INVALID_IP (-5)
#define TCP_MAX_RETRY_TIMES 3

/* Internal functions */
static int internal_putbytes(Port *myport, const char *s, size_t len);
static int internal_flush(Port *myport);

/*
 * StreamServerPort -- open a "listening" port to accept connections.
 *
 * Successfully opened sockets are added to the ListenSocket[] array,
 * at the first position that isn't -1.
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */
int StreamServerPort(int family, char *hostName, unsigned short portNumber, int ListenSocket[], int MaxListen)
{
    const int PORT_STR_LEN = 32;
    const int FAMILY_DESC_LEN = 64;
    int fd = -1;
    int err;
    int maxconn;
    int ret;
    char portNumberStr[PORT_STR_LEN];
    const char *familyDesc = NULL;
    char familyDescBuf[FAMILY_DESC_LEN];
    char *service = NULL;
    struct addrinfo *addrs = NULL;
    struct addrinfo *addr = NULL;
    struct addrinfo hint;
    int listen_index = 0;
    int added = 0;
    errno_t rc = 0;

#if !defined(WIN32) || defined(IPV6_V6ONLY)
    int one = 1;
#endif

    /* Initialize hint structure */
    rc = memset_s(&hint, sizeof(hint), 0, sizeof(hint));
    securec_check(rc, "\0", "\0");

    hint.ai_family = family;
    hint.ai_flags = AI_PASSIVE;
    hint.ai_socktype = SOCK_STREAM;

    rc = snprintf_s(portNumberStr, sizeof(portNumberStr), sizeof(portNumberStr) - 1, "%d", portNumber);
    securec_check_ss(rc, "\0", "\0");
    service = portNumberStr;

    ret = pg_getaddrinfo_all(hostName, service, &hint, &addrs);
    if (ret || (addrs == NULL)) {
        if (hostName != NULL)
            ereport(LOG, (errmsg("could not translate host name \"%s\", service \"%s\" to address: %s", hostName,
                                 service, gai_strerror(ret))));
        else
            ereport(LOG, (errmsg("could not translate service \"%s\" to address: %s", service, gai_strerror(ret))));
        if (addrs != NULL)
            pg_freeaddrinfo_all(hint.ai_family, addrs);
        return STATUS_ERROR;
    }

    for (addr = addrs; addr != NULL; addr = addr->ai_next) {
        if (!IS_AF_UNIX(family) && IS_AF_UNIX(addr->ai_family)) {
            /*
             * Only set up a unix domain socket when they really asked for it.
             * The service/port is different in that case.
             */
            continue;
        }

        /* See if there is still room to add 1 more socket. */
        for (; listen_index < MaxListen; listen_index++) {
            if (ListenSocket[listen_index] == PGINVALID_SOCKET)
                break;
        }
        if (listen_index >= MaxListen) {
            ereport(LOG, (errmsg("could not bind to all requested addresses: MAXLISTEN (%d) exceeded", MaxListen)));
            break;
        }

        /* set up family name for possible error messages */
        switch (addr->ai_family) {
            case AF_INET:
                familyDesc = "IPv4";
                break;
#ifdef HAVE_IPV6
            case AF_INET6:
                familyDesc = "IPv6";
                break;
#endif
            default:
                rc = snprintf_s(familyDescBuf, sizeof(familyDescBuf), sizeof(familyDescBuf) - 1,
                                "unrecognized address family %d", addr->ai_family);
                securec_check_ss(rc, "\0", "\0");
                familyDesc = familyDescBuf;
                break;
        }

        if ((fd = socket(addr->ai_family, SOCK_STREAM, 0)) < 0) {
            ereport(LOG, (errmsg("could not create %s socket:", familyDesc)));
            continue;
        }

#ifndef WIN32

        /*
         * Without the SO_REUSEADDR flag, a new postmaster can't be started
         * right away after a stop or crash, giving "address already in use"
         * error on TCP ports.
         *
         * On win32, however, this behavior only happens if the
         * SO_EXLUSIVEADDRUSE is set. With SO_REUSEADDR, win32 allows multiple
         * servers to listen on the same address, resulting in unpredictable
         * behavior. With no flags at all, win32 behaves as Unix with
         * SO_REUSEADDR.
         */
        if (!IS_AF_UNIX(addr->ai_family)) {
            if ((setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&one, sizeof(one))) == -1) {
                ereport(LOG, (errmsg("setsockopt(SO_REUSEADDR) failed.")));
                close(fd);
                continue;
            }
        }
#endif

#ifdef IPV6_V6ONLY
        if (addr->ai_family == AF_INET6) {
            if (setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, (char *)&one, sizeof(one)) == -1) {
                ereport(LOG, (errmsg("setsockopt(IPV6_V6ONLY) failed.")));
                close(fd);
                continue;
            }
        }
#endif

        /*
         * Note: This might fail on some OS's, like Linux older than
         * 2.4.21-pre3, that don't have the IPV6_V6ONLY socket option, and map
         * ipv4 addresses to ipv6.	It will show ::ffff:ipv4 for all ipv4
         * connections.
         */
        err = bind(fd, addr->ai_addr, addr->ai_addrlen);
        if (err < 0) {
            ereport(LOG, (errmsg("could not bind %s socket: Is another instance already running on port %d?"
                                 " If not, wait a few seconds and retry.",
                                 familyDesc, (int)portNumber)));

            close(fd);
            continue;
        }

        const int MAX_CONNECTIONS = 512;

        /*
         * Select appropriate accept-queue length limit.  PG_SOMAXCONN is only
         * intended to provide a clamp on the request on platforms where an
         * overly large request provokes a kernel error (are there any?).
         */
        maxconn = MAX_CONNECTIONS;

        err = listen(fd, maxconn);
        if (err < 0) {
            ereport(LOG, (errmsg("could not listen on %s socket.", familyDesc)));
            close(fd);
            continue;
        }
        ListenSocket[listen_index] = fd;
        added++;
    }

    pg_freeaddrinfo_all(hint.ai_family, addrs);

    if (!added)
        return STATUS_ERROR;

    return STATUS_OK;
}

int SetSocketNoBlock(int isocketId)
{
    int iFlag = 0;
    int ret = 0;
    uint32 uFlag = 0;

    iFlag = fcntl(isocketId, F_GETFL, 0);
    if (iFlag < 0) {
        ereport(LOG, (errmsg("Get socket info is failed(socketId = %d,errno = %d,errinfo = %s).", isocketId, errno,
                             strerror(errno))));
        return STATUS_ERROR;
    }

    uFlag = (uint32)iFlag;
    uFlag |= O_NONBLOCK;

    ret = fcntl(isocketId, F_SETFL, uFlag);
    if (ret < 0) {
        ereport(LOG, (errmsg("Set socket block is failed(socketId = %d,errno = %d,errinfo = %s).", isocketId, errno,
                             strerror(errno))));
        return STATUS_ERROR;
    }

    return STATUS_OK;
}

/*
 * StreamConnection -- create a new connection with client using
 *		server port.  Set port->sock to the FD of the new connection.
 *
 * ASSUME: that this doesn't need to be non-blocking because
 *		the Postmaster uses select() to tell when the server master
 *		socket is ready for accept().
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */
int StreamConnection(int server_fd, Port *port)
{
    /* accept connection and fill in the client (remote) address */
    port->raddr.salen = sizeof(port->raddr.addr);
    if ((port->sock = accept(server_fd, (struct sockaddr *)&port->raddr.addr, (socklen_t *)&port->raddr.salen)) < 0) {
        ereport(LOG, (errmsg("could not accept new connection.")));

        /*
         * If accept() fails then postmaster.c will still see the server
         * socket as read-ready, and will immediately try again.  To avoid
         * uselessly sucking lots of CPU, delay a bit before trying again.
         * (The most likely reason for failure is being out of kernel file
         * table slots; we can do little except hope some will get freed up.)
         */
        return STATUS_ERROR;
    }

#ifdef SCO_ACCEPT_BUG

    /*
     * UnixWare 7+ and OpenServer 5.0.4 are known to have this bug, but it
     * shouldn't hurt to catch it for all versions of those platforms.
     */
    if (port->raddr.addr.ss_family == 0)
        port->raddr.addr.ss_family = AF_UNIX;
#endif

    /* fill in the server (local) address */
    port->laddr.salen = sizeof(port->laddr.addr);
    if (getsockname(port->sock, (struct sockaddr *)&port->laddr.addr, (socklen_t *)&port->laddr.salen) < 0) {
        ereport(LOG, (errmsg("getsockname() failed !")));
        return STATUS_ERROR;
    }

    /* select NODELAY and KEEPALIVE options if it's a TCP connection */
    if (!IS_AF_UNIX(port->laddr.addr.ss_family)) {
        int on;

#ifdef TCP_NODELAY
        on = 1;
        if (setsockopt(port->sock, IPPROTO_TCP, TCP_NODELAY, (char *)&on, sizeof(on)) < 0) {
            ereport(LOG, (errmsg("setsockopt(TCP_NODELAY) failed.")));
            return STATUS_ERROR;
        }
#endif
        on = 1;
        if (setsockopt(port->sock, SOL_SOCKET, SO_KEEPALIVE, (char *)&on, sizeof(on)) < 0) {
            ereport(LOG, (errmsg("setsockopt(SO_KEEPALIVE) failed.")));
            return STATUS_ERROR;
        }

        on = SetSocketNoBlock(port->sock);
        if (on != STATUS_OK) {
            ereport(LOG, (errmsg("SetSocketNoBlock failed.")));
            return STATUS_ERROR;
        }
    }

    return STATUS_OK;
}

/*
 * StreamClose -- close a client/backend connection
 *
 * NOTE: this is NOT used to terminate a session; it is just used to release
 * the file descriptor in a process that should no longer have the socket
 * open.  (For example, the postmaster calls this after passing ownership
 * of the connection to a child process.)  It is expected that someone else
 * still has the socket open.  So, we only want to close the descriptor,
 * we do NOT want to send anything to the far end.
 */
void StreamClose(int sock)
{
    close(sock);
}

/* --------------------------------
 *		pq_recvbuf - load some bytes into the input buffer
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int pq_recvbuf(Port *myport)
{
    errno_t rc;
    if (myport->PqRecvPointer > 0) {
        if (myport->PqRecvLength > myport->PqRecvPointer) {
            /* still some unread data, left-justify it in the buffer */
            rc = memmove_s(myport->PqRecvBuffer, myport->PqRecvLength, myport->PqRecvBuffer + myport->PqRecvPointer,
                           myport->PqRecvLength - myport->PqRecvPointer);
            securec_check(rc, "\0", "\0");
            myport->PqRecvLength -= myport->PqRecvPointer;
            myport->PqRecvPointer = 0;
        } else {
            myport->PqRecvLength = myport->PqRecvPointer = 0;
        }
    }

    /* Can fill buffer from myport->PqRecvLength and upwards */
    for (;;) {
        int r;
        int retry_times = 0;

        PGSTAT_INIT_TIME_RECORD();
        PGSTAT_START_TIME_RECORD();
    retry:
        r = recv(myport->sock, myport->PqRecvBuffer + myport->PqRecvLength, PQ_BUFFER_SIZE - myport->PqRecvLength,
                 MSG_DONTWAIT);
        END_NET_RECV_INFO(r);
        myport->last_call = LastCall_RECV;

        if (r < 0) {
            myport->last_errno = errno;
            if (errno == EINTR)
                continue; /* Ok if interrupted */

            if (errno == EPIPE) {
                return TCP_SOCKET_ERROR_EPIPE;
            }

            /*
             * The socket's file descriptor is marked O_NONBLOCK and no data is waiting to be received;
             * or MSG_OOB is set and no out-of-band data is available and either the socket's file descriptor
             * is marked  O_NONBLOCK or the socket does not support blocking to await out-of-band data.
             */
            if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
                if (retry_times++ >= TCP_MAX_RETRY_TIMES) {
                    return TCP_SOCKET_ERROR_NO_MESSAGE;
                }
                goto retry;
            }

            /*
             * Careful: an ereport() that tries to write to the client would
             * cause recursion to here, leading to stack overflow and core
             * dump!  This message must go *only* to the postmaster log.
             */
            ereport(COMMERROR, (errmsg("could not receive data from client: err=%d", errno)));
            return TCP_SOCKET_ERROR_EPIPE;
        } else {
            myport->last_errno = 0;
        }
        if (r == 0) {
            /*
             * EOF detected.  We used to write a log message here, but it's
             * better to expect the ultimate caller to do that.
             */
            return EOF;
        }
        /* r contains number of bytes read, so just incr length */
        myport->PqRecvLength += r;
        return 0;
    }
}

/* --------------------------------
 *		pq_getbyte	- get a single byte from connection, or return EOF
 * --------------------------------
 */
int pq_getbyte(Port *myport)
{
    int ret;
    while (myport->PqRecvPointer >= myport->PqRecvLength) {
        ret = pq_recvbuf(myport);
        if (ret == 0) {
            continue;
        } else {
            return ret; /* Failed to recv data */
        }
    }
    return (unsigned char)myport->PqRecvBuffer[myport->PqRecvPointer++];
}

/* --------------------------------
 *		pq_getbytes		- get a known number of bytes from connection
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_getbytes(Port *myport, char *s, size_t len)
{
    size_t amount;
    int ret;
    errno_t rc;

    while (len > 0) {
        while (myport->PqRecvPointer >= myport->PqRecvLength) {
            ret = pq_recvbuf(myport);
            if (ret != 0) {
                return ret; /* Failed to recv data */
            }
        }
        amount = myport->PqRecvLength - myport->PqRecvPointer;
        if (amount > len)
            amount = len;
        rc = memcpy_s(s, len, myport->PqRecvBuffer + myport->PqRecvPointer, amount);
        securec_check(rc, "\0", "\0");
        myport->PqRecvPointer += amount;
        s += amount;
        len -= amount;
    }
    return 0;
}

/* --------------------------------
 *      pq_putbytes     - send bytes to connection (not flushed until pq_flush)
 *
 *      returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_putbytes(Port *myport, const char *s, size_t len)
{
    return internal_putbytes(myport, s, len);
}

static int internal_putbytes(Port *myport, const char *s, size_t len)
{
    size_t amount;
    int ret;
    errno_t rc;

    while (len > 0) {
        /* If buffer is full, then flush it out */
        if (myport->PqSendPointer >= PQ_BUFFER_SIZE) {
            ret = internal_flush(myport);
            if (ret != 0) {
                return ret;
            }
        }
        amount = PQ_BUFFER_SIZE - myport->PqSendPointer;
        if (amount > len)
            amount = len;
        rc = memcpy_s(myport->PqSendBuffer + myport->PqSendPointer, PQ_BUFFER_SIZE - myport->PqSendPointer, s, amount);
        securec_check(rc, "\0", "\0");
        myport->PqSendPointer += amount;
        s += amount;
        len -= amount;
    }
    return 0;
}

/* --------------------------------
 *		pq_flush		- flush pending output
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_flush(Port *myport)
{
    int res;

    /* No-op if reentrant call */
    res = internal_flush(myport);
    return res;
}

static int internal_flush(Port *myport)
{
    static THR_LOCAL int last_reported_send_errno = 0;

    char *bufptr = myport->PqSendBuffer;
    char *bufend = myport->PqSendBuffer + myport->PqSendPointer;

    while (bufptr < bufend) {
        int r;
        int retry_times = 0;
    resend:
        errno = 0;
        PGSTAT_INIT_TIME_RECORD();
        PGSTAT_START_TIME_RECORD();
        r = send(myport->sock, bufptr, bufend - bufptr, MSG_DONTWAIT);
        END_NET_SEND_INFO(r);
        myport->last_call = LastCall_SEND;
        if (r <= 0) {
            myport->last_errno = errno;
            if (errno == EINTR)
                continue; /* Ok if we were interrupted */

            if (errno == EPIPE) {
                return TCP_SOCKET_ERROR_EPIPE;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (retry_times++ >= TCP_MAX_RETRY_TIMES) {
                    return TCP_SOCKET_ERROR_NO_MESSAGE;
                }
                goto resend;
            }

            /*
             * Careful: an ereport() that tries to write to the client would
             * cause recursion to here, leading to stack overflow and core
             * dump!  This message must go *only* to the postmaster log.
             *
             * If a client disconnects while we're in the midst of output, we
             * might write quite a bit of data before we get to a safe query
             * abort point.  So, suppress duplicate log messages.
             */
            if (errno != last_reported_send_errno) {
                last_reported_send_errno = errno;
                ereport(COMMERROR, (errmsg("could not send data to client.")));
            }

            /*
             * We drop the buffered data anyway so that processing can
             * continue, even though we'll probably quit soon.
             */
            myport->PqSendPointer = 0;
            return EOF;
        } else {
            myport->last_errno = 0;
        }

        last_reported_send_errno = 0; /* reset after any successful send */
        bufptr += r;
        myport->PqSendPointer = myport->PqSendPointer + r;
    }

    myport->PqSendPointer = 0;
    return 0;
}

/* --------------------------------
 *		pq_putmessage	- send a normal message (suppressed in COPY OUT mode)
 *
 *		If msgtype is not '\0', it is a message type code to place before
 *		the message body.  If msgtype is '\0', then the message has no type
 *		code (this is only valid in pre-3.0 protocols).
 *
 *		len is the length of the message body data at *s.  In protocol 3.0
 *		and later, a message length word (equal to len+4 because it counts
 *		itself too) is inserted by this routine.
 *
 *		All normal messages are suppressed while old-style COPY OUT is in
 *		progress.  (In practice only a few notice messages might get emitted
 *		then; dropping them is annoying, but at least they will still appear
 *		in the postmaster log.)
 *
 *		We also suppress messages generated while pqcomm.c is busy.  This
 *		avoids any possibility of messages being inserted within other
 *		messages.  The only known trouble case arises if SIGQUIT occurs
 *		during a pqcomm.c routine --- quickdie() will try to send a warning
 *		message, and the most reasonable approach seems to be to drop it.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_putmessage(Port *myport, char msgtype, const char *s, size_t len)
{
    uint32 n32;
    int ret;
    if (msgtype) {
        ret = internal_putbytes(myport, &msgtype, 1);
        if (ret != 0) {
            return ret;
        }
    }

    n32 = htonl((uint32)(len + sizeof(uint32)));
    ret = internal_putbytes(myport, (char *)&n32, sizeof(uint32));
    if (ret != 0) {
        return ret;
    }

    ret = internal_putbytes(myport, s, len);
    if (ret != 0) {
        return ret;
    }

    return 0;
}

void CloseAndFreePort(Port *port)
{
    if (port != NULL) {
        if (port->sock >= 0) {
            close(port->sock);
            port->sock = -1;
        }
        pfree(port);
    }
}

}  // namespace PureLibpq
