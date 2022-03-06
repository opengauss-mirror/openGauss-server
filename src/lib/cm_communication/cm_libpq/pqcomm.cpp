/* -------------------------------------------------------------------------
 *
 * pqcomm.c
 *	  Communication functions between the Frontend and the Backend
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
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *	$PostgreSQL: pgsql/src/backend/libpq/pqcomm.c,v 1.198 2008/01/01 19:45:49 momjian Exp $
 *
 * -------------------------------------------------------------------------
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
#include "cm/cm_c.h"
#include "cm/ip.h"
#include "cm/libpq.h"
#include "cm/libpq-be.h"
#include "cm/elog.h"

#define MAXGTMPATH 256

#ifndef NO_SOCKET
const int NO_SOCKET = -1;
#endif

extern int tcp_keepalives_idle;
extern int tcp_keepalives_interval;
extern int tcp_keepalives_count;

/*
 * Buffers for low-level I/O
 */

/* Internal functions */
static int internal_putbytes(Port* myport, const char* s, size_t len);
static int internal_flush(Port* myport);

/*
 * Streams -- wrapper around Unix socket system calls
 *
 *
 *		Stream functions are used for vanilla TCP connection protocol.
 */

/*
 * StreamServerPort -- open a "listening" port to accept connections.
 *
 * Successfully opened sockets are added to the ListenSocket[] array,
 * at the first position that isn't -1.
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */

int StreamServerPort(int family, char* hostName, unsigned short portNumber, int ListenSocket[], int MaxListen)
{
    int fd = 0;
    int err;
    int maxconn;
    int ret;
    char portNumberStr[32];
    const char* familyDesc = NULL;
    char familyDescBuf[64];
    char* service = NULL;
    struct addrinfo* addrs = NULL;
    struct addrinfo* addr = NULL;
    struct addrinfo hint;
    int listen_index = 0;
    int added = 0;
    errno_t rc = 0;

#if !defined(WIN32) || defined(IPV6_V6ONLY)
    int one = 1;
#endif

    /* Initialize hint structure */
    rc = memset_s(&hint, sizeof(hint), 0, sizeof(hint));
    securec_check_errno(rc, );

    hint.ai_family = family;
    hint.ai_flags = AI_PASSIVE;
    hint.ai_socktype = SOCK_STREAM;

    {
        rc = snprintf_s(portNumberStr, sizeof(portNumberStr), sizeof(portNumberStr) - 1, "%d", portNumber);
        securec_check_intval(rc, );
        service = portNumberStr;
    }

    ret = cmpg_getaddrinfo_all(hostName, service, &hint, &addrs);
    if (ret || (addrs == NULL)) {
        if (hostName != NULL) {
            write_runlog(LOG,
                "could not translate host name \"%s\", service \"%s\" to address: %s",
                hostName,
                service,
                gai_strerror(ret));
        } else {
            write_runlog(LOG, "could not translate service \"%s\" to address: %s", service, gai_strerror(ret));
        }
        if (addrs != NULL) {
            cmpg_freeaddrinfo_all(addrs);
        }
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
            if (ListenSocket[listen_index] == -1) {
                break;
            }
        }
        if (listen_index >= MaxListen) {
            write_runlog(LOG, "could not bind to all requested addresses: MAXLISTEN (%d) exceeded\n", MaxListen);
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
                rc = snprintf_s(familyDescBuf,
                    sizeof(familyDescBuf),
                    sizeof(familyDescBuf) - 1,
                    "unrecognized address family %d",
                    addr->ai_family);
                securec_check_intval(rc, );
                familyDesc = familyDescBuf;
                break;
        }

        if ((fd = socket(addr->ai_family, SOCK_STREAM, 0)) < 0) {
            write_runlog(LOG, "could not create %s socket: \n", familyDesc);
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
            if ((setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&one, sizeof(one))) == -1) {
                write_runlog(LOG, "setsockopt(SO_REUSEADDR) failed: \n");
                close(fd);
                continue;
            }
        }
#endif

#ifdef IPV6_V6ONLY
        if (addr->ai_family == AF_INET6) {
            if (setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, (char*)&one, sizeof(one)) == -1) {
                write_runlog(LOG, "setsockopt(IPV6_V6ONLY) failed: \n");
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
            write_runlog(LOG,
                "could not bind %s socket: Is another instance already running on port %d?  If not, wait a few seconds "
                "and retry.\n",
                familyDesc,
                (int)portNumber);

            close(fd);
            continue;
        }

#define CM_MAX_CONNECTIONS 1024

        /*
         * Select appropriate accept-queue length limit.  PG_SOMAXCONN is only
         * intended to provide a clamp on the request on platforms where an
         * overly large request provokes a kernel error (are there any?).
         */
        maxconn = CM_MAX_CONNECTIONS * 2;

        err = listen(fd, maxconn);
        if (err < 0) {
            write_runlog(LOG, "could not listen on %s socket: \n", familyDesc);
            close(fd);
            continue;
        }
        ListenSocket[listen_index] = fd;
        added++;
    }

    cmpg_freeaddrinfo_all(addrs);

    if (!added) {
        return STATUS_ERROR;
    }

    return STATUS_OK;
}

int SetSocketNoBlock(int isocketId)
{
    int iFlag = 0;
    int ret = 0;
    uint32 uFlag = 0;

    iFlag = fcntl(isocketId, F_GETFL, 0);
    if (iFlag < 0) {
        write_runlog(LOG,
            "Get socket info is failed(socketId = %d,errno = %d,errinfo = %s).",
            isocketId,
            errno,
            strerror(errno));
        return STATUS_ERROR;
    }

    uFlag = (uint32)iFlag;
    uFlag |= O_NONBLOCK;

    ret = fcntl(isocketId, F_SETFL, uFlag);
    if (ret < 0) {
        write_runlog(LOG,
            "Set socket block is failed(socketId = %d,errno = %d,errinfo = %s).",
            isocketId,
            errno,
            strerror(errno));
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
int StreamConnection(int server_fd, Port* port)
{
    /* accept connection and fill in the client (remote) address */
    port->raddr.salen = sizeof(port->raddr.addr);
    if ((port->sock = accept(server_fd, (struct sockaddr*)&port->raddr.addr, (socklen_t*)&port->raddr.salen)) < 0) {
        write_runlog(LOG, "could not accept new connection: \n");

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
    if (port->raddr.addr.ss_family == 0) {
        port->raddr.addr.ss_family = AF_UNIX;
    }
#endif

    /* fill in the server (local) address */
    port->laddr.salen = sizeof(port->laddr.addr);
    if (getsockname(port->sock, (struct sockaddr*)&port->laddr.addr, (socklen_t*)&port->laddr.salen) < 0) {
        write_runlog(LOG, "getsockname() failed !\n");
        return STATUS_ERROR;
    }

    /* select NODELAY and KEEPALIVE options if it's a TCP connection */
    if (!IS_AF_UNIX(port->laddr.addr.ss_family)) {
        int on;

#ifdef TCP_NODELAY
        on = 1;
        if (setsockopt(port->sock, IPPROTO_TCP, TCP_NODELAY, (char*)&on, sizeof(on)) < 0) {
            write_runlog(LOG, "setsockopt(TCP_NODELAY) failed\n");
            return STATUS_ERROR;
        }
#endif
        on = 1;
        if (setsockopt(port->sock, SOL_SOCKET, SO_KEEPALIVE, (char*)&on, sizeof(on)) < 0) {
            write_runlog(LOG, "setsockopt(SO_KEEPALIVE) failed\n");
            return STATUS_ERROR;
        }

        on = SetSocketNoBlock(port->sock);
        if (STATUS_OK != on) {
            write_runlog(LOG, "SetSocketNoBlock failed\n");
            return STATUS_ERROR;
        }
        /*
         * Also apply the current keepalive parameters.  If we fail to set a
         * parameter, don't error out, because these aren't universally
         * supported.  (Note: you might think we need to reset the GUC
         * variables to 0 in such a case, but it's not necessary because the
         * show hooks for these variables report the truth anyway.)
         */
        (void)pq_setkeepalivesidle(tcp_keepalives_idle, port);
        (void)pq_setkeepalivesinterval(tcp_keepalives_interval, port);
        (void)pq_setkeepalivescount(tcp_keepalives_count, port);
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
 * Low-level I/O routines begin here.
 *
 * These routines communicate with a frontend client across a connection
 * already established by the preceding routines.
 * --------------------------------
 */

/* --------------------------------
 *		pq_recvbuf - load some bytes into the input buffer
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int pq_recvbuf(Port* myport)
{
    errno_t rc;
    if (myport->PqRecvPointer > 0) {
        if (myport->PqRecvLength > myport->PqRecvPointer) {
            /* still some unread data, left-justify it in the buffer */
            rc = memmove_s(myport->PqRecvBuffer,
                myport->PqRecvLength,
                myport->PqRecvBuffer + myport->PqRecvPointer,
                myport->PqRecvLength - myport->PqRecvPointer);
            securec_check_errno(rc, );
            myport->PqRecvLength -= myport->PqRecvPointer;
            myport->PqRecvPointer = 0;
        } else {
            myport->PqRecvLength = myport->PqRecvPointer = 0;
        }
    }

    /* Can fill buffer from myport->PqRecvLength and upwards */
    for (;;) {
        int r;

        r = recv(myport->sock,
            myport->PqRecvBuffer + myport->PqRecvLength,
            PQ_BUFFER_SIZE - myport->PqRecvLength,
            MSG_DONTWAIT);
        myport->last_call = CM_LastCall_RECV;

        if (r < 0) {
            myport->last_errno = errno;
            if (errno == EINTR)
                continue; /* Ok if interrupted */

            if (EPIPE == errno) {
                return TCP_SOCKET_ERROR_EPIPE;
            }

            /*
             * The  socket's file descriptor is marked O_NONBLOCK and no data is waiting to be received; or MSG_OOB is
             * set and no out-of-band data is available and either the socket's file descriptor is marked  O_NONBLOCK or
             * the socket does not support blocking to await out-of-band data.
             */
            if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
                return TCP_SOCKET_ERROR_NO_MESSAGE;
            }

            /*
             * Careful: an ereport() that tries to write to the client would
             * cause recursion to here, leading to stack overflow and core
             * dump!  This message must go *only* to the postmaster log.
             */
            write_runlog(ERROR, "could not receive data from client: err=%d\n", errno);
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
 * pq_getbyte - get a single byte from connection, or return EOF
 * --------------------------------
 */
int pq_getbyte(Port* myport)
{
    int ret;
    while (myport->PqRecvPointer >= myport->PqRecvLength) {
        ret = pq_recvbuf(myport);
        if (0 == ret) {
            continue;
        } else {
            return ret; /* Failed to recv data */
        }
    }
    return (unsigned char)myport->PqRecvBuffer[myport->PqRecvPointer++];
}

/* --------------------------------
 * pq_getbytes - get a known number of bytes from connection
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_getbytes(Port* myport, char* s, size_t len, size_t* recvlen)
{
    size_t amount;
    *recvlen = 0;
    int ret;
    errno_t rc;

    while (len > 0) {
        while (myport->PqRecvPointer >= myport->PqRecvLength) {
            ret = pq_recvbuf(myport);
            if (0 != ret && TCP_SOCKET_ERROR_NO_MESSAGE != ret) {
                return ret; /* Failed to recv data */
            }
        }
        amount = myport->PqRecvLength - myport->PqRecvPointer;
        if (amount > len) {
            amount = len;
        }
        rc = memcpy_s(s, len, myport->PqRecvBuffer + myport->PqRecvPointer, amount);
        securec_check_errno(rc, );
        myport->PqRecvPointer += amount;
        s += amount;
        len -= amount;
        *recvlen += amount;
    }
    return 0;
}

#ifdef NOT_USED
/* --------------------------------
 *		pq_discardbytes		- throw away a known number of bytes
 *
 *		same as pq_getbytes except we do not copy the data to anyplace.
 *		this is used for resynchronizing after read errors.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int pq_discardbytes(Port* myport, size_t len)
{
    size_t amount;
    int ret;

    while (len > 0) {
        while (myport->PqRecvPointer >= myport->PqRecvLength) {
            ret = pq_recvbuf(myport);
            if (0 == ret) {
                /* If nothing in buffer, then recv some */
                continue;
            } else {
                return ret; /* Failed to recv data */
            }
        }
        amount = myport->PqRecvLength - myport->PqRecvPointer;
        if (amount > len) {
            amount = len;
        }
        myport->PqRecvPointer += amount;
        len -= amount;
    }
    return 0;
}
#endif /* NOT_USED */

/* --------------------------------
 *		pq_getmessage	- get a message with length word from connection
 *
 *		The return value is placed in an expansible StringInfo, which has
 *		already been initialized by the caller.
 *		Only the message body is placed in the StringInfo; the length word
 *		is removed.  Also, s->cursor is initialized to zero for convenience
 *		in scanning the message contents.
 *
 *		If maxlen is not zero, it is an upper limit on the length of the
 *		message we are willing to accept.  We abort the connection (by
 *		returning EOF) if client tries to send more than that.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_getmessage(Port* myport, CM_StringInfo s, int maxlen)
{
    int32 len = 0;
    size_t recvlen = 0;
    int ret = 0;

    /* Read message length word */
    if (0 == s->msglen) {
        ret = pq_getbytes(myport, (char*)&len, (size_t)4, &recvlen);
        if (ret != 0) {
            if (EOF == ret) {
                write_runlog(ERROR, "unexpected EOF within message length word\n");
            }
            return ret;
        }

        len = ntohl(len);

        if (len < 4 || (maxlen > 0 && len > maxlen)) {
            write_runlog(ERROR, "invalid message length");
            return EOF;
        }

        len -= 4; /* discount length itself */
        s->msglen = len;
        if (len > 0) {
            /*
             * Allocate space for message.	If we run out of room (ridiculously
             * large message), we will elog(ERROR), but we want to discard the
             * message body so as not to lose communication sync.
             */
            if (0 != CM_enlargeStringInfo(s, len)) {
                return EOF;
            }
        }
    }

    if (s->msglen > s->len) {

        /* And grab the message */
        ret = pq_getbytes(myport, s->data + s->len, s->msglen - s->len, &(recvlen));
        if (ret != 0) {
            write_runlog(ERROR, "incomplete message from client, ret=%d\n", ret);
            return ret;
        }
        s->len = s->len + recvlen;
        /* Place a trailing null per StringInfo convention */
        s->data[s->len] = '\0';
    }

    return 0;
}

static int internal_putbytes(Port* myport, const char* s, size_t len)
{
    size_t amount;
    int ret;
    errno_t rc;

    while (len > 0) {
        /* If buffer is full, then flush it out */
        if (myport->PqSendPointer >= PQ_BUFFER_SIZE) {
            ret = internal_flush(myport);
            if (0 != ret) {
                return ret;
            }
        }
        amount = PQ_BUFFER_SIZE - myport->PqSendPointer;
        if (amount > len) {
            amount = len;
        }
        rc = memcpy_s(myport->PqSendBuffer + myport->PqSendPointer, PQ_BUFFER_SIZE - myport->PqSendPointer, s, amount);
        securec_check_errno(rc, );
        myport->PqSendPointer += amount;
        s += amount;
        len -= amount;
    }
    return 0;
}

/* --------------------------------
 * pq_flush- flush pending output
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pq_flush(Port* myport)
{
    int res;

    /* No-op if reentrant call */
    res = internal_flush(myport);
    return res;
}

static int internal_flush(Port* myport)
{
    static THR_LOCAL int last_reported_send_errno = 0;

    char* bufptr = myport->PqSendBuffer;
    char* bufend = myport->PqSendBuffer + myport->PqSendPointer;

    while (bufptr < bufend) {
        int r;
    resend:
        errno = 0;
        r = send(myport->sock, bufptr, bufend - bufptr, MSG_DONTWAIT);
        myport->last_call = CM_LastCall_SEND;
        if (r <= 0) {
            myport->last_errno = errno;
            if (errno == EINTR) {
                continue; /* Ok if we were interrupted */
            }

            if (EPIPE == errno) {
                return TCP_SOCKET_ERROR_EPIPE;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
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
                write_runlog(ERROR, "could not send data to client: \n");
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
 * Message-level I/O routines begin here.
 *
 * These routines understand about the old-style COPY OUT protocol.
 * --------------------------------
 */

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
int pq_putmessage(Port* myport, char msgtype, const char* s, size_t len)
{
    uint32 n32;
    int ret;
    if (msgtype) {
        ret = internal_putbytes(myport, &msgtype, 1);
        if (ret != 0) {
            return ret;
        }
    }

    n32 = htonl((uint32)(len + 4));
    ret = internal_putbytes(myport, (char*)&n32, 4);
    if (ret != 0) {
        return ret;
    }

    ret = internal_putbytes(myport, s, len);
    if (ret != 0) {
        return ret;
    }

    return 0;
}

/*
 * Support for TCP Keepalive parameters
 */

int pq_getkeepalivesidle(Port* port)
{
#ifdef TCP_KEEPIDLE
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return 0;
    }

    if (port->keepalives_idle != 0) {
        return port->keepalives_idle;
    }

    if (port->default_keepalives_idle == 0) {
        ACCEPT_TYPE_ARG3 size = sizeof(port->default_keepalives_idle);

        if (getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPIDLE, (char*)&port->default_keepalives_idle, &size) < 0) {
            write_runlog(LOG, "getsockopt(TCP_KEEPIDLE) failed:\n");
            port->default_keepalives_idle = -1; /* don't know */
        }
    }

    return port->default_keepalives_idle;
#else
    return 0;
#endif
}

int pq_setkeepalivesidle(int idle, Port* port)
{
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return STATUS_OK;
    }

#ifdef TCP_KEEPIDLE
    if (idle == port->keepalives_idle) {
        return STATUS_OK;
    }

    if (port->default_keepalives_idle <= 0) {
        if (pq_getkeepalivesidle(port) < 0) {
            if (idle == 0) {
                return STATUS_OK; /* default is set but unknown */
            } else {
                return STATUS_ERROR;
            }
        }
    }

    if (idle == 0) {
        idle = port->default_keepalives_idle;
    }

    if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPIDLE, (char*)&idle, sizeof(idle)) < 0) {
        write_runlog(LOG, "setsockopt(TCP_KEEPIDLE) failed:\n");
        return STATUS_ERROR;
    }

    port->keepalives_idle = idle;
#else
    if (idle != 0) {
        write_runlog(LOG, "setsockopt(TCP_KEEPIDLE) not supported");
        return STATUS_ERROR;
    }
#endif

    return STATUS_OK;
}

int pq_getkeepalivesinterval(Port* port)
{
#ifdef TCP_KEEPINTVL
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return 0;
    }

    if (port->keepalives_interval != 0) {
        return port->keepalives_interval;
    }

    if (port->default_keepalives_interval == 0) {
        ACCEPT_TYPE_ARG3 size = sizeof(port->default_keepalives_interval);

        if (getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL, (char*)&port->default_keepalives_interval, &size) < 0) {
            write_runlog(LOG, "getsockopt(TCP_KEEPINTVL) failed:\n");
            port->default_keepalives_interval = -1; /* don't know */
        }
    }

    return port->default_keepalives_interval;
#else
    return 0;
#endif
}

int pq_setkeepalivesinterval(int interval, Port* port)
{
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return STATUS_OK;
    }

#ifdef TCP_KEEPINTVL
    if (interval == port->keepalives_interval) {
        return STATUS_OK;
    }

    if (port->default_keepalives_interval <= 0) {
        if (pq_getkeepalivesinterval(port) < 0) {
            if (interval == 0) {
                return STATUS_OK; /* default is set but unknown */
            } else {
                return STATUS_ERROR;
            }
        }
    }

    if (interval == 0) {
        interval = port->default_keepalives_interval;
    }

    if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL, (char*)&interval, sizeof(interval)) < 0) {
        write_runlog(LOG, "setsockopt(TCP_KEEPINTVL) failed: \n");
        return STATUS_ERROR;
    }

    port->keepalives_interval = interval;
#else
    if (interval != 0) {
        write_runlog(LOG, "setsockopt(TCP_KEEPINTVL) not supported\n");
        return STATUS_ERROR;
    }
#endif

    return STATUS_OK;
}

int pq_getkeepalivescount(Port* port)
{
#ifdef TCP_KEEPCNT
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return 0;
    }

    if (port->keepalives_count != 0) {
        return port->keepalives_count;
    }

    if (port->default_keepalives_count == 0) {
        ACCEPT_TYPE_ARG3 size = sizeof(port->default_keepalives_count);

        if (getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT, (char*)&port->default_keepalives_count, &size) < 0) {
            write_runlog(LOG, "getsockopt(TCP_KEEPCNT) failed: \n");
            port->default_keepalives_count = -1; /* don't know */
        }
    }

    return port->default_keepalives_count;
#else
    return 0;
#endif
}

int pq_setkeepalivescount(int count, Port* port)
{
    if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family)) {
        return STATUS_OK;
    }

#ifdef TCP_KEEPCNT
    if (count == port->keepalives_count) {
        return STATUS_OK;
    }

    if (port->default_keepalives_count <= 0) {
        if (pq_getkeepalivescount(port) < 0) {
            if (count == 0) {
                return STATUS_OK; /* default is set but unknown */
            } else {
                return STATUS_ERROR;
            }
        }
    }

    if (count == 0) {
        count = port->default_keepalives_count;
    }

    if (port->sock != NO_SOCKET) {
        if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT, (char*)&count, sizeof(count)) < 0) {
            write_runlog(LOG, "setsockopt(TCP_KEEPCNT) failed: \n");
            return STATUS_ERROR;
        }

        port->keepalives_count = count;
    }

#else
    if (count != 0) {
        write_runlog(LOG, "setsockopt(TCP_KEEPCNT) not supported\n");
        return STATUS_ERROR;
    }
#endif

    return STATUS_OK;
}
