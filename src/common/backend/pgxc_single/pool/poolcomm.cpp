/* -------------------------------------------------------------------------
 *
 * poolcomm.c
 *
 *	  Communication functions between the pool manager and session
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * -------------------------------------------------------------------------
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stddef.h>
#include "c.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "pgxc/pool_comm.h"
#include "storage/ipc.h"
#include "utils/elog.h"
#include "miscadmin.h"
#include "pgstat.h"

static int pool_recvbuf(PoolPort* port);
static int pool_discardbytes(PoolPort* port, size_t len);

#ifdef HAVE_UNIX_SOCKETS

#define POOLER_UNIXSOCK_PATH(path, port, sockdir)                                                         \
    do {                                                                                                  \
        int rcs;                                                                                          \
        const char* unixSocketDir = NULL;                                                                 \
        const char* pghost = gs_getenv_r("PGHOST");                                                       \
        if (check_client_env(pghost) == NULL) {                                                           \
            pghost = NULL;                                                                                \
        }                                                                                                 \
        if (sockdir != NULL && (*sockdir) != '\0') {                                                      \
            unixSocketDir = sockdir;                                                                      \
        } else {                                                                                          \
            if (pghost != NULL && (*pghost) != '\0') {                                                    \
                unixSocketDir = pghost;                                                                   \
            } else {                                                                                      \
                unixSocketDir = DEFAULT_PGSOCKET_DIR;                                                     \
            }                                                                                             \
        }                                                                                                 \
        rcs = snprintf_s(path, sizeof(path), sizeof(path) - 1, "%s/.s.PGPOOL.%d", unixSocketDir, (port)); \
        securec_check_ss(rcs, "\0", "\0");                                                                \
    } while (0)

static void StreamDoUnlink(int code, Datum arg);

static int Lock_AF_UNIX(unsigned short port, const char* unixSocketName);
#endif

/*
 * Open server socket on specified port to accept connection from sessions
 */
int pool_listen(unsigned short port, const char* unixSocketName)
{
    int fd = -1;
    int len;
    int rcs = 0;
    struct sockaddr_un unix_addr = {0};

#ifdef HAVE_UNIX_SOCKETS
    if (Lock_AF_UNIX(port, unixSocketName) < 0)
        return -1;

    /* create a Unix domain stream socket */
    if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
        return -1;

    /* fill in socket address structure */
    rcs = memset_s(&unix_addr, sizeof(unix_addr), 0, sizeof(unix_addr));
    securec_check(rcs, "\0", "\0");
    unix_addr.sun_family = AF_UNIX;

    rcs = strcpy_s(unix_addr.sun_path, sizeof(unix_addr.sun_path), u_sess->pgxc_cxt.sock_path);
    securec_check(rcs, "\0", "\0");
    len = sizeof(unix_addr.sun_family) + strlen(unix_addr.sun_path) + 1;

#ifdef F_SETFD
    if (fcntl(fd, F_SETFD, FD_CLOEXEC) == -1) {
        closesocket(fd);
        return -1;
    }
#endif /* F_SETFD */

    /* bind the name to the descriptor */
    if (bind(fd, (struct sockaddr*)&unix_addr, len) < 0) {
        closesocket(fd);
        return -1;
    }

    /* tell kernel we're a server */
    if (listen(fd, 5) < 0) {
        closesocket(fd);
        return -1;
    }

    /* Arrange to unlink the socket file at exit */
    on_proc_exit(StreamDoUnlink, 0);

    return fd;
#else
    /* need to support for non-unix platform */
    Assert(false);
    return -1;
#endif
}

/* StreamDoUnlink()
 * Shutdown routine for pooler connection
 * If a Unix socket is used for communication, explicitly close it.
 */
#ifdef HAVE_UNIX_SOCKETS
static void StreamDoUnlink(int code, Datum arg)
{
    Assert(u_sess->pgxc_cxt.sock_path[0]);
    unlink(u_sess->pgxc_cxt.sock_path);
}
#endif /* HAVE_UNIX_SOCKETS */

#ifdef HAVE_UNIX_SOCKETS
static int Lock_AF_UNIX(unsigned short port, const char* unixSocketName)
{
    POOLER_UNIXSOCK_PATH(u_sess->pgxc_cxt.sock_path, port, unixSocketName);

    CreateSocketLockFile(u_sess->pgxc_cxt.sock_path, true);

    unlink(u_sess->pgxc_cxt.sock_path);

    return 0;
}
#endif

/*
 * Connect to pooler listening on specified port
 */
int pool_connect(unsigned short port, const char* unixSocketName)
{
    int fd = -1;
    int len;
    int rc;
    struct sockaddr_un unix_addr = {0};

#ifdef HAVE_UNIX_SOCKETS
    /* create a Unix domain stream socket */
    if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
        return -1;

    /* fill socket address structure w/server's addr */
    POOLER_UNIXSOCK_PATH(u_sess->pgxc_cxt.sock_path, port, unixSocketName);

    rc = memset_s(&unix_addr, sizeof(unix_addr), 0, sizeof(unix_addr));
    securec_check(rc, "\0", "\0");
    unix_addr.sun_family = AF_UNIX;
    rc = strcpy_s(unix_addr.sun_path, sizeof(unix_addr.sun_path), u_sess->pgxc_cxt.sock_path);
    securec_check(rc, "\0", "\0");
    len = sizeof(unix_addr.sun_family) + strlen(unix_addr.sun_path) + 1;

#ifdef F_SETFD
    if (fcntl(fd, F_SETFD, FD_CLOEXEC) == -1) {
        closesocket(fd);
        return -1;
    }
#endif /* F_SETFD */

    if (connect(fd, (struct sockaddr*)&unix_addr, len) < 0) {
        closesocket(fd);
        return -1;
    }

    return fd;
#else
    /* need to support for non-unix platform */
    ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("pool manager only supports UNIX socket")));
    return -1;
#endif
}

/*
 * Get one byte from the buffer, read data from the connection if buffer is empty
 */
int pool_getbyte(PoolPort* port)
{
    while (port->RecvPointer >= port->RecvLength) {
        if (pool_recvbuf(port)) /* If nothing in buffer, then recv some */
            return EOF;         /* Failed to recv data */
    }
    return (unsigned char)port->RecvBuffer[port->RecvPointer++];
}

/*
 * Get one byte from the buffer if it is not empty
 */
int pool_pollbyte(PoolPort* port)
{
    if (port->RecvPointer >= port->RecvLength) {
        return EOF; /* Empty buffer */
    }
    return (unsigned char)port->RecvBuffer[port->RecvPointer++];
}

/*
 * Read pooler protocol message from the buffer.
 */
int pool_getmessage(PoolPort* port, StringInfo s, int maxlen)
{
    int32 len;

    resetStringInfo(s);

    /* Read message length word */
    if (pool_getbytes(port, (char*)&len, 4) == EOF) {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("unexpected EOF within message length word:%m")));
        return EOF;
    }

    len = ntohl(len);
    if (len < 4 || (maxlen > 0 && len > maxlen)) {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid message length")));
        return EOF;
    }

    len -= 4; /* discount length itself */

    if (len > 0) {
        /*
         * Allocate space for message.	If we run out of room (ridiculously
         * large message), we will elog(ERROR)
         */
        PG_TRY();
        {
            enlargeStringInfo(s, len);
        }
        PG_CATCH();
        {
            if (pool_discardbytes(port, len) == EOF)
                ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("incomplete message from client:%m")));
            PG_RE_THROW();
        }
        PG_END_TRY();

        /* And grab the message */
        if (pool_getbytes(port, s->data, len) == EOF) {
            ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("incomplete message from client:%m")));
            return EOF;
        }
        s->len = len;
        /* Place a trailing null per StringInfo convention */
        s->data[len] = '\0';
    }

    return 0;
}

/* --------------------------------
 * pool_getbytes - get a known number of bytes from connection
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pool_getbytes(PoolPort* port, char* s, size_t len)
{
    size_t amount;
    int rcs = 0;

    while (len > 0) {
        while (port->RecvPointer >= port->RecvLength) {
            if (pool_recvbuf(port)) /* If nothing in buffer, then recv some */
                return EOF;         /* Failed to recv data */
        }
        amount = port->RecvLength - port->RecvPointer;
        if (amount > len) {
            amount = len;
        }
        if (amount > 0) {
            rcs = memcpy_s(s, amount, port->RecvBuffer + port->RecvPointer, amount);
            securec_check(rcs, "\0", "\0");
        }
        port->RecvPointer += amount;
        s += amount;
        len -= amount;
    }
    return 0;
}

/* --------------------------------
 * pool_discardbytes - discard a known number of bytes from connection
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int pool_discardbytes(PoolPort* port, size_t len)
{
    size_t amount;

    while (len > 0) {
        while (port->RecvPointer >= port->RecvLength) {
            if (pool_recvbuf(port)) /* If nothing in buffer, then recv some */
                return EOF;         /* Failed to recv data */
        }
        amount = port->RecvLength - port->RecvPointer;
        if (amount > len) {
            amount = len;
        }
        port->RecvPointer += amount;
        len -= amount;
    }
    return 0;
}

/* --------------------------------
 * pool_recvbuf - load some bytes into the input buffer
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int pool_recvbuf(PoolPort* port)
{
    if (port->RecvPointer > 0) {
        if (port->RecvLength > port->RecvPointer) {
            /* still some unread data, left-justify it in the buffer */
            int rcs = memmove_s(port->RecvBuffer,
                port->RecvLength - port->RecvPointer,
                port->RecvBuffer + port->RecvPointer,
                port->RecvLength - port->RecvPointer);
            securec_check(rcs, "\0", "\0");

            port->RecvLength -= port->RecvPointer;
            port->RecvPointer = 0;
        } else
            port->RecvLength = port->RecvPointer = 0;
    }

    /* Can fill buffer from PqRecvLength and upwards */
    for (;;) {
        int r;

        PGSTAT_INIT_TIME_RECORD();
        PGSTAT_START_TIME_RECORD();
        r = recv(Socket(*port), port->RecvBuffer + port->RecvLength, POOL_BUFFER_SIZE - port->RecvLength, 0);
        END_NET_RECV_INFO(r);

        if (r < 0) {
            if (errno == EINTR) {
                continue; /* Ok if interrupted */
            }

            /*
             * Report broken connection
             */
            ereport(LOG, (errcode_for_socket_access(), errmsg("could not receive data from client: %m")));
            return EOF;
        }
        if (r == 0) {
            /*
             * EOF detected.  We used to write a log message here, but it's
             * better to expect the ultimate caller to do that.
             */
            return EOF;
        }
        /* r contains number of bytes read, so just incr length */
        port->RecvLength += r;
        return 0;
    }
}

/*
 * Put a known number of bytes into the connection buffer
 */
int pool_putbytes(PoolPort* port, const char* s, size_t len)
{
    size_t amount;

    while (len > 0) {
        /* If buffer is full, then flush it out */
        if (port->SendPointer >= POOL_BUFFER_SIZE)
            if (pool_flush(port))
                return EOF;
        amount = POOL_BUFFER_SIZE - port->SendPointer;
        if (amount > len) {
            amount = len;
        }
        if (amount > 0) {
            errno_t ss_rc = memcpy_s(port->SendBuffer + port->SendPointer, amount, s, amount);
            securec_check(ss_rc, "\0", "\0");
        }
        port->SendPointer += amount;
        s += amount;
        len -= amount;
    }
    return 0;
}

/* --------------------------------
 *		pool_flush		- flush pending output
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int pool_flush(PoolPort* port)
{
    char* bufptr = port->SendBuffer;
    char* bufend = port->SendBuffer + port->SendPointer;

    while (bufptr < bufend) {
        int r;
        errno = 0;
        PGSTAT_INIT_TIME_RECORD();
        PGSTAT_START_TIME_RECORD();
        r = send(Socket(*port), bufptr, bufend - bufptr, 0);
        END_NET_SEND_INFO(r);

        if (r <= 0) {
            if (errno == EINTR) {
                continue; /* Ok if we were interrupted */
            }

            if (errno != u_sess->pgxc_cxt.last_reported_send_errno) {
                u_sess->pgxc_cxt.last_reported_send_errno = errno;

                /*
                 * Handle a seg fault that may later occur in proc array
                 * when this fails when we are already shutting down
                 * If shutting down already, do not call.
                 */
                if (!t_thrd.proc_cxt.proc_exit_inprogress)
                    return 0;
            }

            /*
             * We drop the buffered data anyway so that processing can
             * continue, even though we'll probably quit soon.
             */
            port->SendPointer = 0;
            return EOF;
        }

        u_sess->pgxc_cxt.last_reported_send_errno = 0; /* reset after any successful send */
        bufptr += r;
    }

    port->SendPointer = 0;
    return 0;
}

/*
 * Put the pooler protocol message into the connection buffer
 */
int pool_putmessage(PoolPort* port, char msgtype, const char* s, size_t len)
{
    uint n32;

    if (pool_putbytes(port, &msgtype, 1))
        return EOF;

    n32 = htonl((uint32)(len + 4));
    if (pool_putbytes(port, (char*)&n32, 4))
        return EOF;

    if (pool_putbytes(port, s, len))
        return EOF;

    return 0;
}

/* message code('f'), size(8), node_count */
#define SEND_MSG_BUFFER_SIZE (9 + g_instance.attr.attr_network.MaxConnections * sizeof(char))
/* message code('s'), result */
#define SEND_RES_BUFFER_SIZE 5
#define SEND_PID_BUFFER_SIZE (5 + (g_instance.attr.attr_network.MaxConnections - 1) * THREADID_LEN)

/*
    The following two functions are called in pool_sendconnDefs and pool_recvconnDefs separately
    for send and receive message for serveral times.
*/
int pool_internal_sendconnDefs(int u_sock, PoolConnDef* connDef, int count)
{
    int ret;
    struct iovec iov[1];
    struct msghdr msg = {0};
    uint n32;
    struct cmsghdr* cmptr = NULL;
    int controllen = sizeof(struct cmsghdr) + count * sizeof(int);
    /* send message buffer includes: 9 bytes(constant) + connInfos array */
    int send_msg_buffer_size = 9 + count * sizeof(PoolConnInfo);
    char* buf = (char*)malloc(send_msg_buffer_size);
    char nbuf = '0';
    errno_t ss_rc = 0;
    if (buf == NULL) {
        // if we are here, the counterpart must block in recvmsg(),
        // so we should send null message to invoke it
        //
        ereport(WARNING, (errmsg("could not malloc buffer for sending connDefs.")));
        iov[0].iov_base = &nbuf;
        iov[0].iov_len = 1;
        msg.msg_iov = iov;
        msg.msg_iovlen = 1;
        msg.msg_name = NULL;
        msg.msg_namelen = 0;
        msg.msg_control = NULL;
        msg.msg_controllen = 0;
    } else {
        buf[0] = 'f';
        n32 = htonl((uint32)8);
        ss_rc = memcpy_s(buf + 1, sizeof(uint), &n32, 4);
        securec_check(ss_rc, "\0", "\0");
        n32 = htonl((uint32)count);
        ss_rc = memcpy_s(buf + 5, sizeof(uint), &n32, 4);
        securec_check(ss_rc, "\0", "\0");
        if (count > 0) {
            /* Copy the connHosts into buffer */
            ss_rc = memcpy_s(buf + 9, count * sizeof(PoolConnInfo), connDef->connInfos, count * sizeof(PoolConnInfo));
            securec_check(ss_rc, "\0", "\0");
        }
        iov[0].iov_base = buf;
        iov[0].iov_len = send_msg_buffer_size;
        msg.msg_iov = iov;
        msg.msg_iovlen = 1;
        msg.msg_name = NULL;
        msg.msg_namelen = 0;
    }

    if (count == 0) {
        msg.msg_control = NULL;
        msg.msg_controllen = 0;
    } else {
        cmptr = (cmsghdr*)malloc(controllen);
        if (cmptr == NULL) {
            // if we are here, the counterpart must block in recvmsg(),
            // so we should send null message to invoke it
            //
            ereport(WARNING, (errmsg("could not malloc cmsghdr for sending connDefs.")));
            // count must be zero when send null message
            //
            if (buf != NULL) {
                n32 = htonl((uint32)0);
                ss_rc = memcpy_s(buf + 5, sizeof(uint), &n32, 4);
                securec_check(ss_rc, "\0", "\0");
            }
            msg.msg_control = NULL;
            msg.msg_controllen = 0;
        } else {
            cmptr->cmsg_level = SOL_SOCKET;
            cmptr->cmsg_type = SCM_RIGHTS;
            cmptr->cmsg_len = controllen;
            msg.msg_control = (caddr_t)cmptr;
            msg.msg_controllen = controllen;
            if (count > 0) {
                /* the fd to pass */
                ss_rc = memcpy_s(CMSG_DATA(cmptr), count * sizeof(int), connDef->fds, count * sizeof(int));
                securec_check(ss_rc, "\0", "\0");
            }
        }
    }
    ret = sendmsg(u_sock, &msg, 0);
    if (ret != send_msg_buffer_size) {
        if (cmptr != NULL) {
            free(cmptr);
            cmptr = NULL;
        }

        ereport(WARNING, (errcode_for_socket_access(), errmsg("could not send connDefs:%m.")));
        /* send null message to invoke recvmsg, or it will be blocked forever */
        if (ret < 0) {
            msg.msg_control = NULL;
            msg.msg_controllen = 0;
            msg.msg_flags = 0;

            // count must be zero when send null message
            //
            if (buf != NULL) {
                n32 = htonl((uint32)0);
                ss_rc = memcpy_s(buf + 5, sizeof(uint), &n32, 4);
                securec_check(ss_rc, "\0", "\0");
            }

            if (sendmsg(u_sock, &msg, 0) < 0) {
                ereport(WARNING, (errcode_for_socket_access(), errmsg("failed to send null msg:%m.")));
            }
        }
        if (buf != NULL) {
            free(buf);
            buf = NULL;
        }
        return EOF;
    }

    if (cmptr != NULL) {
        free(cmptr);
        cmptr = NULL;
    }
    if (buf != NULL) {
        free(buf);
        buf = NULL;
    }
    return ret;
}

int pool_internal_recvconnDefs(int u_sock, PoolConnDef* connDef, int count)
{
// free buffer and struct cmsghdr memory
//
#define FREE_BC_MEM()        \
    do {                     \
        if (buf != NULL) {   \
            free(buf);       \
            buf = NULL;      \
        }                    \
        if (cmptr != NULL) { \
            free(cmptr);     \
            cmptr = NULL;    \
        }                    \
    } while (0)

    int r, ret;
    errno_t ss_rc = 0;
    uint n32;
    struct iovec iov[1];
    struct msghdr msg = {0};
    int controllen = sizeof(struct cmsghdr) + count * sizeof(int);
    struct cmsghdr* cmptr = NULL;
    /* receive message buffer includes: 9 bytes(constant) + connInfos array */
    int recv_msg_buffer_size = 9 + count * sizeof(PoolConnInfo);  // it must be the same as send_msg_buffer_size
    char* buf = (char*)malloc(recv_msg_buffer_size);
    if (buf == NULL) {
        ereport(WARNING, (errmsg("could not malloc buffer for receiving connDefs.")));
        return EOF;
    }
    cmptr = (cmsghdr*)malloc(controllen);
    if (cmptr == NULL) {
        FREE_BC_MEM();
        ereport(WARNING, (errmsg("could not malloc cmsghdr for receiving connDefs.")));
        return EOF;
    }

    ss_rc = memset_s(cmptr, controllen, -1, controllen);
    securec_check(ss_rc, "\0", "\0");

    iov[0].iov_base = buf;
    iov[0].iov_len = recv_msg_buffer_size;
    msg.msg_iov = iov;
    msg.msg_iovlen = 1;
    msg.msg_name = NULL;
    msg.msg_namelen = 0;
    msg.msg_control = (caddr_t)cmptr;
    msg.msg_controllen = controllen;

    r = recvmsg(u_sock, &msg, 0);
    if (r < 0) {
        FREE_BC_MEM();
        /*
         * Report broken connection
         */
        ereport(WARNING, (errcode_for_socket_access(), errmsg("could not receive data from client: %m")));
        ret = -2;
        return ret;
    } else if (r == 0) {
        FREE_BC_MEM();
        ret = EOF;
        return ret;
    } else if (r != recv_msg_buffer_size) {
        FREE_BC_MEM();
        ereport(WARNING,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("incomplete message from client:%d,%d", recv_msg_buffer_size, r)));
        ret = -2;
        return ret;
    }

    /* Verify response */
    if (buf[0] != 'f') {
        ereport(WARNING, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("unexpected message code:%c", buf[0])));
        FREE_BC_MEM();
        ret = EOF;
        return ret;
    }

    ss_rc = memcpy_s(&n32, sizeof(uint), buf + 1, 4);
    securec_check(ss_rc, "\0", "\0");
    n32 = ntohl(n32);
    if (n32 != 8) {
        FREE_BC_MEM();
        ereport(WARNING, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid message size")));
        ret = EOF;
        return ret;
    }

    /*
     * If connection count is 0 it means pool does not have connections
     * to  fulfill request. Otherwise number of returned connections
     * should be equal to requested count. If it not the case consider this
     * a protocol violation. (Probably connection went out of sync)
     */
    ss_rc = memcpy_s(&n32, sizeof(uint), buf + 5, 4);
    securec_check(ss_rc, "\0", "\0");
    n32 = ntohl(n32);
    if (n32 == 0) {
        FREE_BC_MEM();
        ereport(
            LOG, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("failed to recieve fds of connections from pooler")));
        ret = EOF;
        return ret;
    }

    if (n32 != (uint)count) {
        FREE_BC_MEM();
        ereport(WARNING, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("unexpected connection count")));
        ret = EOF;
        return ret;
    }

    if (count > 0) {
        /* Get the connInfos info from buffer */
        ss_rc = memcpy_s(connDef->connInfos, count * sizeof(PoolConnInfo), buf + 9, count * sizeof(PoolConnInfo));
        securec_check(ss_rc, "\0", "\0");
        /* Get fds info from msg */
        ss_rc = memcpy_s(connDef->fds, count * sizeof(int), CMSG_DATA(cmptr), count * sizeof(int));
        securec_check(ss_rc, "\0", "\0");
    }
    FREE_BC_MEM();
    return r;
}

/*
    The number of file descriptors (socket fd) to send using unix domain socket is limited in kernel in
    file "linux-2.6.32.63/include/scm.h" by following hard coded.

    #define SCM_MAX_FD	253

    It may be different in deferent linux version. However, it is usually between 250 and 255.

    We have two choices to support large clusters which have nodes more than 255.
    1)Duplicate the fds using dup() at first, then put them in buffer and send it using normal send(), receive it using
   recv(). However, There is a problem. If we send the duplicated fds successly, but unknown error happens during
   calling recv(), we can not close the duplicated fds. It may cause hang !!! 2)Calling the typical method serveral
   times to send all fds. Although, it is much more complicated in coding, we can control everything in this way. If
   recv() failed, we can close the received fds and the kernel will close the other duplicated fds for us.
*/
/*
 * Build up a message carrying file descriptors or process numbers and send them over specified
 * connection
 */
#define MAX_FDS_NUM 200
int pool_sendconnDefs(PoolPort* port, PoolConnDef* connDef, int count)
{
    int ret;
    int i = 0;
    int n_send_times = (count % MAX_FDS_NUM == 0) ? (count / MAX_FDS_NUM) : (count / MAX_FDS_NUM + 1);
    int u_sock = Socket(*port);
    PoolConnDef inner_connDef;
    inner_connDef.fds = connDef->fds;
    inner_connDef.connInfos = connDef->connInfos;
    int inner_cnt = (count > MAX_FDS_NUM) ? MAX_FDS_NUM : count;

    n_send_times = (n_send_times == 0) ? 1 : n_send_times;

    for (i = 0; i < n_send_times; i++) {
        // not the last time of sending, the number of fds must be MAX_FDS_NUM
        //
        if (i != n_send_times - 1) {
            ret = pool_internal_sendconnDefs(u_sock, &inner_connDef, inner_cnt);
            if (ret > 0) {
                // set the point offset
                //
                inner_connDef.fds += inner_cnt;
                inner_connDef.connInfos += inner_cnt;
            } else {
                ereport(
                    WARNING, (errcode_for_socket_access(), errmsg("failed to send connDefs at the time:%d.", i + 1)));
                return EOF;
            }
            continue;
        }
        // the last time
        //
        inner_cnt = count - (n_send_times - 1) * MAX_FDS_NUM;
        ret = pool_internal_sendconnDefs(u_sock, &inner_connDef, inner_cnt);
        if (ret < 0) {
            ereport(WARNING,
                (errcode_for_socket_access(), errmsg("failed to send connDefs at the time:%d.", n_send_times)));
            return EOF;
        }
    }
    return 0;
}

/*
 * Read a message from the specified connection carrying file descriptors
 */
int pool_recvconnDefs(PoolPort* port, PoolConnDef* connDef, int count)
{
    int ret;
    int i = 0;
    errno_t ss_rc;
    int n_recv_times = (count % MAX_FDS_NUM == 0) ? (count / MAX_FDS_NUM) : (count / MAX_FDS_NUM + 1);
    int u_sock = Socket(*port);
    PoolConnDef inner_connDef;
    inner_connDef.fds = connDef->fds;
    inner_connDef.connInfos = connDef->connInfos;
    int inner_cnt = (count > MAX_FDS_NUM) ? MAX_FDS_NUM : count;
    bool b_failed = false;
    n_recv_times = (n_recv_times == 0) ? 1 : n_recv_times;

    // for closing the fds when receive failed.
    //
    if (count > 0) {
        ss_rc = memset_s(connDef->fds, count * sizeof(int), -1, count * sizeof(int));
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(connDef->connInfos, count * sizeof(PoolConnInfo), 0, count * sizeof(PoolConnInfo));
        securec_check(ss_rc, "\0", "\0");
    }
    for (i = 0; i < n_recv_times; i++) {
        // not the last time of sending, the number of fds must be MAX_FDS_NUM
        //
        if (i != n_recv_times - 1) {
            ret = pool_internal_recvconnDefs(u_sock, &inner_connDef, inner_cnt);
            if (ret > 0) {
                // set the point offset
                inner_connDef.fds += inner_cnt;
                inner_connDef.connInfos += inner_cnt;
            } else {
                ereport(WARNING,
                    (errcode_for_socket_access(), errmsg("failed to receive connDefs at the time: %d.", i + 1)));
                b_failed = true;
                if (ret < -1)
                    goto failure;
            }
            continue;
        }
        // the last time
        //
        inner_cnt = count - (n_recv_times - 1) * MAX_FDS_NUM;
        ret = pool_internal_recvconnDefs(u_sock, &inner_connDef, inner_cnt);
        if (ret < 0) {
            ereport(WARNING,
                (errcode_for_socket_access(), errmsg("failed to receive connDefs at the time:%d.", n_recv_times)));
            b_failed = true;
            if (ret < -1) { // fd count is 0
                goto failure;
            }
        }
    }

failure:
    if (b_failed) {
        // close the received fds, or it may cause hang
        //
        for (i = 0; i < count; i++) {
            if (connDef->fds[i] > 0) {
                close(connDef->fds[i]);
                connDef->fds[i] = -1;
            }
        }
        return EOF;
    }

    return 0;
}

/*
 * Send result to specified connection
 */
int pool_sendres(PoolPort* port, int res)
{
    char buf[SEND_RES_BUFFER_SIZE];
    uint n32;

    /* Header */
    buf[0] = 's';
    /* Result */
    n32 = htonl(res);
    int rcs = memcpy_s(buf + 1, sizeof(uint), &n32, 4);
    securec_check(rcs, "\0", "\0");
    PGSTAT_INIT_TIME_RECORD();
    PGSTAT_START_TIME_RECORD();
    int ret = send(Socket(*port), &buf, SEND_RES_BUFFER_SIZE, 0);
    END_NET_SEND_INFO(ret);
    if (ret != SEND_RES_BUFFER_SIZE) {
        if (ret < 0) {
            ereport(ERROR, (errcode_for_socket_access(), errmsg("pooler failed to send res: %m")));
        }
        return EOF;
    }

    return 0;
}

/*
 * Read result from specified connection.
 * Return 0 at success or EOF at error.
 */
int pool_recvres(PoolPort* port)
{
    int r;
    const int res = 0;
    int rcs = 0;
    uint n32;
    char buf[SEND_RES_BUFFER_SIZE];

    PGSTAT_INIT_TIME_RECORD();
    PGSTAT_START_TIME_RECORD();
    r = recv(Socket(*port), &buf, SEND_RES_BUFFER_SIZE, 0);
    END_NET_RECV_INFO(r);
    if (r < 0) {
        /*
         * Report broken connection
         */
        ereport(ERROR, (errcode_for_socket_access(), errmsg("could not receive data from client: %m")));
        goto failure;
    } else if (r == 0) {
        goto failure;
    } else if (r != SEND_RES_BUFFER_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("incomplete message from client")));
        goto failure;
    }

    /* Verify response */
    if (buf[0] != 's') {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("unexpected message code")));
        goto failure;
    }

    rcs = memcpy_s(&n32, sizeof(uint), buf + 1, 4);
    securec_check(rcs, "\0", "\0");
    n32 = ntohl(n32);
    if (n32 != 0) {
        return EOF;
    }

    return res;

failure:
    return EOF;
}

#define THREADID_LEN sizeof(ThreadId)
/*
 * Read a message from the specified connection carrying pid numbers
 * of transactions interacting with pooler
 */
int pool_recvpids(PoolPort* port, ThreadId** pids)
{
    int r, i;
    int rcs = 0;
    uint n32;
    char buf[SEND_PID_BUFFER_SIZE];

    /*
     * Buffer size is upper bounded by the maximum number of connections,
     * as in the pooler each connection has one Pooler Agent.
     */
    PGSTAT_INIT_TIME_RECORD();
    PGSTAT_START_TIME_RECORD();
    r = recv(Socket(*port), &buf, SEND_PID_BUFFER_SIZE, 0);
    END_NET_RECV_INFO(r);
    if (r < 0) {
        /*
         * Report broken connection
         */
        ereport(ERROR, (errcode_for_socket_access(), errmsg("could not receive data from client: %m")));
        goto failure;
    } else if (r == 0) {
        goto failure;
    } else if (r != (int)SEND_PID_BUFFER_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("incomplete message from client")));
        goto failure;
    }

    /* Verify response */
    if (buf[0] != 'p') {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("unexpected message code")));
        goto failure;
    }

    rcs = memcpy_s(&n32, sizeof(uint), buf + 1, 4);
    securec_check(rcs, "\0", "\0");
    n32 = ntohl(n32);
    if (n32 == 0) {
        elog(WARNING, "No transaction to abort");
        return n32;
    }

    *pids = (ThreadId*)palloc(sizeof(ThreadId) * n32);

    for (i = 0; (uint)i < n32; i++) {
        ThreadId tid;
        uint32 h32;
        uint32 l32;
        char pidBuf[THREADID_LEN];
        errno_t ss_rc = 0;

        ss_rc = memcpy_s(pidBuf, THREADID_LEN, buf + 5 + i * THREADID_LEN, THREADID_LEN);
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memcpy_s(&h32, sizeof(uint32), pidBuf, 4);
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memcpy_s(&l32, sizeof(uint32), pidBuf + 4, 4);
        securec_check(ss_rc, "\0", "\0");
        h32 = ntohl(h32);
        l32 = ntohl(l32);

        tid = h32;
        tid <<= 32;
        tid |= l32;

        (*pids)[i] = tid;
    }
    return n32;

failure:
    return 0;
}

/*
 * Send a message containing pid numbers to the specified connection
 */
int pool_sendpids(PoolPort* port, ThreadId* pids, int count)
{
    int res = 0;
    int i;
    char buf[SEND_PID_BUFFER_SIZE];
    uint n32;
    uint32 hPid, lPid;
    errno_t ss_rc = 0;

    buf[0] = 'p';
    n32 = htonl((uint32)count);

    ss_rc = memcpy_s(buf + 1, sizeof(uint), &n32, 4);
    securec_check(ss_rc, "\0", "\0");

    for (i = 0; i < count; i++) {
        int n;
        char pidBuf[THREADID_LEN];

        hPid = (uint32)(pids[i] >> 32);
        n = htonl(hPid);
        ss_rc = memcpy_s(pidBuf, sizeof(int), &n, 4);
        securec_check(ss_rc, "\0", "\0");

        lPid = (uint32)pids[i];
        n = htonl(lPid);
        ss_rc = memcpy_s(pidBuf + 4, sizeof(int), &n, 4);
        securec_check(ss_rc, "\0", "\0");

        ss_rc = memcpy_s(buf + 5 + i * THREADID_LEN, THREADID_LEN, pidBuf, THREADID_LEN);
        securec_check(ss_rc, "\0", "\0");
    }
    PGSTAT_INIT_TIME_RECORD();
    PGSTAT_START_TIME_RECORD();
    int ret = send(Socket(*port), &buf, SEND_PID_BUFFER_SIZE, 0);
    END_NET_SEND_INFO(ret);
    if (ret != (ssize_t)SEND_PID_BUFFER_SIZE) {
        if (ret < 0) {
            ereport(ERROR, (errcode_for_socket_access(), errmsg("pooler failed to send pids: %m")));
        }
        res = EOF;
    }

    return res;
}
