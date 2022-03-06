/* -------------------------------------------------------------------------
 *
 * fe-connect.c
 *	  functions related to setting up a connection to the backend
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/interfaces/libpq/fe-connect.c,v 1.371 2008/12/15 10:28:21 mha Exp $
 *
 * -------------------------------------------------------------------------
 */

#include <arpa/inet.h>
#include <ctype.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "gssapi/gssapi_krb5.h"
#include "cm/libpq-fe.h"
#include "cm/libpq-int.h"
#include "cm/cm_c.h"
#include "cm/cm_ip.h"
#include "cm/cm_msg.h"
#include "cm/ip.h"
#include "cm/elog.h"

/*
 * fall back options if they are not specified by arguments or defined
 * by environment variables.
 */
#define DefaultHost "localhost"

/* ----------
 * Definition of the conninfo parameters and their fallback resources.
 *
 * CMPQconninfoOptions[] is a constant static array that we use to initialize
 * a dynamically allocated working copy.  All the "val" fields in
 * CMPQconninfoOptions[] *must* be NULL.	In a working copy, non-null "val"
 * fields point to malloc'd strings that should be freed when the working
 * array is freed (see CMPQconninfoFree).
 * ----------
 */
static const CMPQconninfoOption CMPQconninfoOptions[] = {{"connect_timeout", NULL},
    {"host", NULL},
    {"hostaddr", NULL},
    {"port", NULL},
    {"localhost", NULL},
    {"localport", NULL},
    {"node_id", NULL},
    {"node_name", NULL},
    {"remote_type", NULL},
    {"postmaster", NULL},
    {"user", NULL},
    /* Terminating entry --- MUST BE LAST */
    {NULL, NULL}};

static bool connectOptions1(CM_Conn* conn, const char* conninfo);
static int connectCMStart(CM_Conn* conn);
static int connectCMComplete(CM_Conn* conn);
static CM_Conn* makeEmptyCM_Conn(void);
static void freeCM_Conn(CM_Conn* conn);
static void closeCM_Conn(CM_Conn* conn);
static CMPQconninfoOption* conninfo_parse(const char* conninfo, PQExpBuffer errorMessage, bool use_defaults);
static char* conninfo_getval(CMPQconninfoOption* connOptions, const char* keyword);
static int CMGssContinue(CM_Conn* conn);
static int CMGssStartup(CM_Conn* conn);
static char* gs_getenv_with_check(const char* envKey, CM_Conn* conn);
bool pg_fe_set_noblock(pgsocket sock)
{
#if !defined(WIN32)
    return (fcntl(sock, F_SETFL, O_NONBLOCK) != -1);
#else
    unsigned long ioctlsocket_ret = 1;

    /* Returns non-0 on failure, while fcntl() returns -1 on failure */
    return (ioctlsocket(sock, FIONBIO, &ioctlsocket_ret) == 0);
#endif
}

CM_Conn* PQconnectCM(const char* conninfo)
{
    CM_Conn* conn = PQconnectCMStart(conninfo);

    if ((conn != NULL) && conn->status != CONNECTION_BAD) {
        (void)connectCMComplete(conn);
    } else if (conn != NULL) {
        closeCM_Conn(conn);
        freeCM_Conn(conn);
        conn = NULL;
    }

    return conn;
}

/*
 * PQconnectCMStart
 *
 * Returns a CM_Conn*.  If NULL is returned, a malloc error has occurred, and
 * you should not attempt to proceed with this connection.	If the status
 * field of the connection returned is CONNECTION_BAD, an error has
 * occurred. In this case you should call CMPQfinish on the result, (perhaps
 * inspecting the error message first).  Other fields of the structure may not
 * be valid if that occurs.  If the status field is not CONNECTION_BAD, then
 * this stage has succeeded - call CMPQconnectPoll, using select(2) to see when
 * this is necessary.
 *
 * See CMPQconnectPoll for more info.
 */
CM_Conn* PQconnectCMStart(const char* conninfo)
{
    CM_Conn* conn = NULL;

    /*
     * Allocate memory for the conn structure
     */
    conn = makeEmptyCM_Conn();
    if (conn == NULL) {
        return NULL;
    }

    /*
     * Parse the conninfo string
     */
    if (!connectOptions1(conn, conninfo)) {
        return conn;
    }

    /*
     * Connect to the database
     */
    if (!connectCMStart(conn)) {
        /* Just in case we failed to set it in connectCMStart */
        conn->status = CONNECTION_BAD;
    }

    return conn;
}

/*
 *		connectOptions1
 *
 * Internal subroutine to set up connection parameters given an already-
 * created CM_Conn and a conninfo string.
 *
 * Returns true if OK, false if trouble (in which case errorMessage is set
 * and so is conn->status).
 */
static bool connectOptions1(CM_Conn* conn, const char* conninfo)
{
    CMPQconninfoOption* connOptions = NULL;
    char* tmp = NULL;

    /*
     * Parse the conninfo string
     */
    connOptions = conninfo_parse(conninfo, &conn->errorMessage, true);
    if (connOptions == NULL) {
        conn->status = CONNECTION_BAD;
        /* errorMessage is already set */
        return false;
    }

    /*
     * Move option values into conn structure
     *
     * XXX: probably worth checking strdup() return value here...
     */
    tmp = conninfo_getval(connOptions, "hostaddr");
    conn->pghostaddr = tmp != NULL ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "host");
    conn->pghost = tmp != NULL ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "port");
    conn->pgport = tmp != NULL ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "localhost");
    conn->pglocalhost = tmp != NULL ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "localport");
    conn->pglocalport = tmp != NULL ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "connect_timeout");
    conn->connect_timeout = tmp != NULL ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "user");
    conn->pguser = tmp != NULL ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "node_id");
    conn->node_id = tmp != NULL ? atoi(tmp) : 0;
    tmp = conninfo_getval(connOptions, "node_name");
    conn->gc_node_name = tmp != NULL ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "postmaster");
    conn->is_postmaster = tmp != NULL ? atoi(tmp) : 0;
    tmp = conninfo_getval(connOptions, "remote_type");
    conn->remote_type = tmp != NULL ? atoi(tmp) : CM_NODE_DEFAULT;

    /*
     * Free the option info - all is in conn now
     */
    CMPQconninfoFree(connOptions);
    connOptions = NULL;

    return true;
}

/* ----------
 * connectNoDelay -
 * Sets the TCP_NODELAY socket option.
 * Returns 1 if successful, 0 if not.
 * ----------
 */
static int connectNoDelay(CM_Conn* conn)
{
#ifdef TCP_NODELAY
    int on = 1;

    if (setsockopt(conn->sock, IPPROTO_TCP, TCP_NODELAY, (char*)&on, sizeof(on)) < 0) {
        appendCMPQExpBuffer(&conn->errorMessage, "could not set socket to TCP no delay mode: \n");
        return 0;
    }
#endif

    return 1;
}

/* ----------
 * connectFailureMessage -
 * create a friendly error message on connection failure.
 * ----------
 */
static void connectFailureMessage(CM_Conn* conn)
{
    appendCMPQExpBuffer(&conn->errorMessage,
        "could not connect to server: \n"
        "\tIs the server running on host \"%s\" and accepting\n"
        "\tTCP/IP connections on port %s?\n",
        conn->pghostaddr != NULL ? conn->pghostaddr : (conn->pghost != NULL ? conn->pghost : "???"),
        conn->pgport);
}

/* ----------
 * connectCMStart -
 * Begin the process of making a connection to the backend.
 *
 * Returns 1 if successful, 0 if not.
 * ----------
 */
static int connectCMStart(CM_Conn* conn)
{
    int portnum = 0;
    char portstr[128];
    struct addrinfo* addrs = NULL;
    struct addrinfo hint = {0};
    const char* node = NULL;
    int ret;
    errno_t rc = 0;

    if (conn == NULL) {
        return 0;
    }

    /* Ensure our buffers are empty */
    conn->inStart = conn->inCursor = conn->inEnd = 0;
    conn->outCount = 0;

    /*
     * Determine the parameters to pass to CM_getaddrinfo_all.
     */

    /* Initialize hint structure */
    rc = memset_s(&hint, sizeof(hint), 0, sizeof(hint));
    securec_check_errno(rc, );
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_family = AF_UNSPEC;

    /* Set up port number as a string */
    if (conn->pgport != NULL && conn->pgport[0] != '\0') {
        portnum = atoi(conn->pgport);
    }
    rc = snprintf_s(portstr, sizeof(portstr), sizeof(portstr) - 1, "%d", portnum);
    securec_check_ss_c(rc, "\0", "\0");

    if (conn->pghostaddr != NULL && conn->pghostaddr[0] != '\0') {
        /* Using pghostaddr avoids a hostname lookup */
        node = conn->pghostaddr;
        hint.ai_family = AF_UNSPEC;
        hint.ai_flags = AI_NUMERICHOST;
    } else if (conn->pghost != NULL && conn->pghost[0] != '\0') {
        /* Using pghost, so we have to look-up the hostname */
        node = conn->pghost;
        hint.ai_family = AF_UNSPEC;
    } else {
        /* Without Unix sockets, default to localhost instead */
        node = "localhost";
        hint.ai_family = AF_UNSPEC;
    }

    /* Use CM_getaddrinfo_all() to resolve the address */
    ret = cm_getaddrinfo_all(node, portstr, &hint, &addrs);
    if (ret || (addrs == NULL)) {
        if (node != NULL) {
            appendCMPQExpBuffer(
                &conn->errorMessage, "could not translate host name \"%s\" to address: %s\n", node, gai_strerror(ret));
        } else {
            appendCMPQExpBuffer(&conn->errorMessage,
                "could not translate Unix-domain socket path \"%s\" to address: %s\n",
                portstr,
                gai_strerror(ret));
        }
        if (addrs != NULL) {
            cm_freeaddrinfo_all(hint.ai_family, addrs);
        }
        goto connect_errReturn;
    }

    /*
     * Set up to try to connect, with protocol 3.0 as the first attempt.
     */
    conn->addrlist = addrs;
    conn->addr_cur = addrs;
    conn->addrlist_family = hint.ai_family;
    conn->status = CONNECTION_NEEDED;

    /*
     * The code for processing CONNECTION_NEEDED state is in CMPQconnectPoll(),
     * so that it can easily be re-executed if needed again during the
     * asynchronous startup process.  However, we must run it once here,
     * because callers expect a success return from this routine to mean that
     * we are in PGRES_POLLING_WRITING connection state.
     */
    if (CMPQconnectPoll(conn) == PGRES_POLLING_WRITING) {
        return 1;
    }

connect_errReturn:
    if (conn->sock >= 0) {
        close(conn->sock);
        conn->sock = -1;
    }
    conn->status = CONNECTION_BAD;
    return 0;
}

/*
 * connectCMComplete
 *
 * Block and complete a connection.
 *
 * Returns 1 on success, 0 on failure.
 */
static int connectCMComplete(CM_Conn* conn)
{
    CMPostgresPollingStatusType flag = PGRES_POLLING_WRITING;
    time_t finish_time = ((time_t)-1);

    if (conn == NULL || conn->status == CONNECTION_BAD) {
        return 0;
    }

    /*
     * Set up a time limit, if connect_timeout isn't zero.
     */
    if (conn->connect_timeout != NULL) {
        int timeout = atoi(conn->connect_timeout);

        if (timeout > 0) {
            /*
             * Rounding could cause connection to fail; need at least 2 secs
             */
            if (timeout < 2) {
                timeout = 2;
            }
            /* calculate the finish time based on start + timeout */
            finish_time = time(NULL) + timeout;
        }
    }

    for (;;) {
        /*
         * Wait, if necessary.	Note that the initial state (just after
         * PQconnectCMStart) is to wait for the socket to select for writing.
         */
        switch (flag) {
            case PGRES_POLLING_OK:
                /* Reset stored error messages since we now have a working connection */
                resetCMPQExpBuffer(&conn->errorMessage);
                return 1; /* success! */

            case PGRES_POLLING_READING:
                if (cmpqWaitTimed(1, 0, conn, finish_time)) {
                    conn->status = CONNECTION_BAD;
                    return 0;
                }
                break;

            case PGRES_POLLING_WRITING:
                if (cmpqWaitTimed(0, 1, conn, finish_time)) {
                    conn->status = CONNECTION_BAD;
                    return 0;
                }
                break;

            default:
                /* Just in case we failed to set it in CMPQconnectPoll */
                conn->status = CONNECTION_BAD;
                return 0;
        }

        /*
         * Now try to advance the state machine.
         */
        flag = CMPQconnectPoll(conn);
    }
}

/* ----------------
 * CMPQconnectPoll
 *
 * Poll an asynchronous connection.
 *
 * Returns a CMClientPollingStatusType.
 * Before calling this function, use select(2) to determine when data
 * has arrived..
 *
 * You must call CMPQfinish whether or not this fails.
 */
CMPostgresPollingStatusType CMPQconnectPoll(CM_Conn* conn)
{
    errno_t rc = EOK;

    if (conn == NULL) {
        return PGRES_POLLING_FAILED;
    }

    /* Get the new data */
    switch (conn->status) {
            /*
             * We really shouldn't have been polled in these two cases, but we
             * can handle it.
             */
        case CONNECTION_BAD:
            return PGRES_POLLING_FAILED;
        case CONNECTION_OK:
            return PGRES_POLLING_OK;

            /* These are reading states */
        case CONNECTION_AWAITING_RESPONSE:
        case CONNECTION_AUTH_OK: {
            /* Load waiting data */
            int flushResult;
            /*
             * If data remains unsent, send it.  Else we might be waiting for the
             * result of a command the backend hasn't even got yet.
             */
            while ((flushResult = cmpqFlush(conn)) > 0) {
                if (cmpqWait(false, true, conn)) {
                    flushResult = -1;
                    break;
                }
            }

            int n = cmpqReadData(conn);

            if (n < 0) {
                goto error_return;
            }
            if (n == 0) {
                return PGRES_POLLING_READING;
            }

            break;
        }

            /* These are writing states, so we just proceed. */
        case CONNECTION_STARTED:
        case CONNECTION_MADE:
            break;

        case CONNECTION_NEEDED:
            break;

        default:
            appendCMPQExpBuffer(&conn->errorMessage,
                "invalid connection state, "
                "probably indicative of memory corruption\n");
            goto error_return;
    }

keep_going: /* We will come back to here until there is
             * nothing left to do. */
    switch (conn->status) {
        case CONNECTION_NEEDED: {
            /*
             * Try to initiate a connection to one of the addresses
             * returned by cm_getaddrinfo_all().  conn->addr_cur is the
             * next one to try. We fail when we run out of addresses
             * (reporting the error returned for the *last* alternative,
             * which may not be what users expect :-().
             */
            while (conn->addr_cur != NULL) {
                struct addrinfo* addr_cur = conn->addr_cur;

                /* Remember current address for possible error msg */
                rc = memcpy_s(&conn->raddr.addr, sizeof(conn->raddr.addr), addr_cur->ai_addr, addr_cur->ai_addrlen);
                securec_check_c(rc, "\0", "\0");
                conn->raddr.salen = addr_cur->ai_addrlen;

                /* Open a socket */
                conn->sock = socket(addr_cur->ai_family, SOCK_STREAM, 0);
                if (conn->sock < 0) {
                    /*
                     * ignore socket() failure if we have more addresses
                     * to try
                     */
                    if (addr_cur->ai_next != NULL) {
                        conn->addr_cur = addr_cur->ai_next;
                        continue;
                    }
                    appendCMPQExpBuffer(&conn->errorMessage, "could not create socket: \n");
                    break;
                }

                if (conn->pglocalhost == NULL) {
                    appendCMPQExpBuffer(&conn->errorMessage, "could not found localhost, localhost is null \n");
                    break;
                }

                struct sockaddr_in localaddr;

                rc = memset_s(&localaddr, sizeof(sockaddr_in), 0, sizeof(sockaddr_in));
                securec_check_errno(rc, );
                localaddr.sin_family = AF_INET;
                localaddr.sin_addr.s_addr = inet_addr(conn->pglocalhost);
                /* Any local port will do. */
                localaddr.sin_port = 0;

                rc = bind(conn->sock, (struct sockaddr*)&localaddr, sizeof(localaddr));
                if (rc != 0) {
                    appendCMPQExpBuffer(
                        &conn->errorMessage, "could not bind localhost:%s, result is %d \n", conn->pglocalhost, rc);
                    break;
                }

#ifdef F_SETFD
                if (fcntl(conn->sock, F_SETFD, FD_CLOEXEC) == -1) {
                    appendCMPQExpBuffer(&conn->errorMessage, "could not set socket(FD_CLOEXEC): %d\n", SOCK_ERRNO);
                    closesocket(conn->sock);
                    conn->sock = -1;
                    conn->addr_cur = addr_cur->ai_next;
                    continue;
                }
#endif /* F_SETFD */

                /*
                 * Random_Port_Reuse need set SO_REUSEADDR on.
                 * Random_Port_Reuse must not use bind interface,
                 * because socket owns a random port private when used bind interface.
                 * SO_REUSEPORT solve this problem in kernel 3.9.
                 */
                if (!IS_AF_UNIX(addr_cur->ai_family)) {
                    int on = 1;

                    if ((setsockopt(conn->sock, SOL_SOCKET, SO_REUSEADDR, (char*)&on, sizeof(on))) == -1) {
                        appendCMPQExpBuffer(&conn->errorMessage, "setsockopt(SO_REUSEADDR) failed: %d\n", SOCK_ERRNO);
                        closesocket(conn->sock);
                        conn->sock = -1;
                        conn->addr_cur = addr_cur->ai_next;
                        continue;
                    }
                }

                /*
                 * Select socket options: no delay of outgoing data for
                 * TCP sockets, nonblock mode, close-on-exec. Fail if any
                 * of this fails.
                 */
                if (!IS_AF_UNIX(addr_cur->ai_family)) {
                    if (!connectNoDelay(conn)) {
                        close(conn->sock);
                        conn->sock = -1;
                        conn->addr_cur = addr_cur->ai_next;
                        continue;
                    }
                }

                if (
#ifndef WIN32
                    !IS_AF_UNIX(addr_cur->ai_family) &&
#endif
                    !pg_fe_set_noblock(conn->sock)) {
                    appendCMPQExpBuffer(
                        &conn->errorMessage, "could not set socket to non-blocking mode: %d\n", SOCK_ERRNO);
                    close(conn->sock);
                    conn->sock = -1;
                    conn->addr_cur = addr_cur->ai_next;
                    continue;
                }

                /*
                 * Start/make connection.  This should not block, since we
                 * are in nonblock mode.  If it does, well, too bad.
                 */
                if (connect(conn->sock, addr_cur->ai_addr, addr_cur->ai_addrlen) < 0) {
                    if (SOCK_ERRNO == EINPROGRESS || SOCK_ERRNO == EWOULDBLOCK || SOCK_ERRNO == EINTR ||
                        SOCK_ERRNO == 0) {
                        /*
                         * This is fine - we're in non-blocking mode, and
                         * the connection is in progress.  Tell caller to
                         * wait for write-ready on socket.
                         */
                        conn->status = CONNECTION_STARTED;
                        return PGRES_POLLING_WRITING;
                    }
                    /* otherwise, trouble */
                } else {
                    /*
                     * Hm, we're connected already --- seems the "nonblock
                     * connection" wasn't.  Advance the state machine and
                     * go do the next stuff.
                     */
                    conn->status = CONNECTION_STARTED;
                    goto keep_going;
                }

                /*
                 * This connection failed --- set up error report, then
                 * close socket (do it this way in case close() affects
                 * the value of errno...).	We will ignore the connect()
                 * failure and keep going if there are more addresses.
                 */
                connectFailureMessage(conn);
                if (conn->sock >= 0) {
                    close(conn->sock);
                    conn->sock = -1;
                }

                /*
                 * Try the next address, if any.
                 */
                conn->addr_cur = addr_cur->ai_next;
            } /* loop over addresses */

            /*
             * Ooops, no more addresses.  An appropriate error message is
             * already set up, so just set the right status.
             */
            goto error_return;
        }

        case CONNECTION_STARTED: {
            int optval;
            size_t optlen = sizeof(optval);

            /*
             * Write ready, since we've made it here, so the connection
             * has been made ... or has failed.
             */

            /*
             * Now check (using getsockopt) that there is not an error
             * state waiting for us on the socket.
             */

            if (getsockopt(conn->sock, SOL_SOCKET, SO_ERROR, (char*)&optval, (socklen_t*)&optlen) == -1) {
                appendCMPQExpBuffer(&conn->errorMessage, libpq_gettext("could not get socket error status: \n"));
                goto error_return;
            } else if (optval != 0) {
                /*
                 * When using a nonblocking connect, we will typically see
                 * connect failures at this point, so provide a friendly
                 * error message.
                 */
                connectFailureMessage(conn);

                /*
                 * If more addresses remain, keep trying, just as in the
                 * case where connect() returned failure immediately.
                 */
                if (conn->addr_cur->ai_next != NULL) {
                    if (conn->sock >= 0) {
                        close(conn->sock);
                        conn->sock = -1;
                    }
                    conn->addr_cur = conn->addr_cur->ai_next;
                    conn->status = CONNECTION_NEEDED;
                    goto keep_going;
                }
                goto error_return;
            }

            /* Fill in the client address */
            conn->laddr.salen = sizeof(conn->laddr.addr);
            if (getsockname(conn->sock, (struct sockaddr*)&conn->laddr.addr, (socklen_t*)&conn->laddr.salen) < 0) {
                appendCMPQExpBuffer(&conn->errorMessage, "could not get client address from socket:\n");
                goto error_return;
            }

            /*
             * Make sure we can write before advancing to next step.
             */
            conn->status = CONNECTION_MADE;
            return PGRES_POLLING_WRITING;
        }

        case CONNECTION_MADE: {
            CM_StartupPacket* sp = (CM_StartupPacket*)malloc(sizeof(CM_StartupPacket));
            if (sp == NULL) {
                appendCMPQExpBuffer(&conn->errorMessage, "malloc failed, size: %ld \n", sizeof(CM_StartupPacket));
                goto error_return;
            }
            int packetlen = sizeof(CM_StartupPacket);

            rc = memset_s(sp, sizeof(CM_StartupPacket), 0, sizeof(CM_StartupPacket));
            securec_check_errno(rc, );

            if (conn->pguser != NULL) {
                rc = strncpy_s(sp->sp_user, SP_USER, conn->pguser, SP_USER - 1);
                securec_check_errno(rc, );
                sp->sp_user[SP_USER - 1] = '\0';
            }

            if (conn->pglocalhost != NULL) {
                rc = strncpy_s(sp->sp_host, SP_HOST, conn->pglocalhost, SP_HOST - 1);
                securec_check_errno(rc, );
                sp->sp_host[SP_HOST - 1] = '\0';
            }

            /*
             * Build a startup packet. We tell the CM server/proxy our
             * PGXC Node name and whether we are a proxy or not.
             *
             * When the connection is made from the proxy, we let the CM
             * server know about it so that some special headers are
             * handled correctly by the server.
             */
            rc = strncpy_s(sp->sp_node_name, SP_NODE_NAME, conn->gc_node_name, SP_NODE_NAME - 1);
            securec_check_errno(rc, );
            sp->sp_node_name[SP_NODE_NAME - 1] = '\0';
            sp->sp_remotetype = conn->remote_type;
            sp->node_id = conn->node_id;
            sp->sp_ispostmaster = conn->is_postmaster;

            /*
             * Send the startup packet.
             *
             * Theoretically, this could block, but it really shouldn't
             * since we only got here if the socket is write-ready.
             */
            if (CMPQPacketSend(conn, 'A', (char*)sp, packetlen) != STATUS_OK) {
                appendCMPQExpBuffer(&conn->errorMessage, "could not send startup packet: \n");
                free(sp);
                goto error_return;
            }

            conn->status = CONNECTION_AWAITING_RESPONSE;

            /* Clean up startup packet */
            free(sp);

            return PGRES_POLLING_READING;
        }

            /*
             * Handle authentication exchange: wait for postmaster messages
             * and respond as necessary.
             */
        case CONNECTION_AWAITING_RESPONSE: {
            char beresp;
            int msgLength;
            int avail;

            /*
             * Scan the message from current point (note that if we find
             * the message is incomplete, we will return without advancing
             * inStart, and resume here next time).
             */
            conn->inCursor = conn->inStart;

            /* Read type byte */
            if (cmpqGetc(&beresp, conn)) {
                /* We'll come back when there is more data */
                return PGRES_POLLING_READING;
            }

            /*
             * Validate message type: we expect among (a default request without authentication,
             * an error, a kerberos authentication) here.  Anything else probably means
             * it's not CM on the other end at all.
             */
            if (!(beresp == 'R' || beresp == 'E' || beresp == 'P')) {
                appendCMPQExpBuffer(&conn->errorMessage,
                    "expected authentication request from "
                    "server, but received %c\n",
                    beresp);
                goto error_return;
            }

            /* Read message length word */
            if (cmpqGetInt(&msgLength, 4, conn)) {
                /* We'll come back when there is more data */
                return PGRES_POLLING_READING;
            }
            /*
             * Try to validate message length before using it.
             * Authentication requests can't be very large, although GSS
             * auth requests may not be that small.  Errors can be a
             * little larger, but not huge.  If we see a large apparent
             * length in an error, it means we're really talking to a
             * pre-3.0-protocol server; cope.
             */
            if (beresp == 'R' && (msgLength < 4 || msgLength > 2000)) {
                appendCMPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("expected authentication request from "
                                  "server, but received %c\n"),
                    beresp);
                goto error_return;
            }

            /* Handle errors. */
            if (beresp == 'E') {
                if (cmpqGets_append(&conn->errorMessage, conn)) {
                    /* We'll come back when there is more data */
                    return PGRES_POLLING_READING;
                }
                /* OK, we read the message; mark data consumed */
                conn->inStart = conn->inCursor;
                goto error_return;
            }

            msgLength -= 4;
            if (msgLength <= 0) {
                goto error_return;
            }
            if (beresp == 'P') {
                int llen = msgLength;
                conn->gss_inbuf.length = llen;
                FREE_AND_RESET(conn->gss_inbuf.value);
                conn->gss_inbuf.value = malloc(llen);
                if (conn->gss_inbuf.value == NULL) {
                    appendCMPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("failed to allocate the gss_inbuf memory:"
                                      "out of memory: request_size=%d.\n"),
                        llen);
                    goto error_return;
                }
                cmpqGetnchar((char*)conn->gss_inbuf.value, llen, conn);
                /* OK, we successfully read the message; mark data consumed */
                conn->inStart = conn->inCursor;
                rc = CMGssContinue(conn);
                if (rc != STATUS_OK) {
                    FREE_AND_RESET(conn->gss_inbuf.value);
                    goto error_return;
                }
                goto keep_going;
            }

            avail = conn->inEnd - conn->inCursor;
            if (avail < msgLength) {
                /*
                 * Before returning, try to enlarge the input buffer if
                 * needed to hold the whole message; see notes in
                 * pqParseInput3.
                 */
                if (cmpqCheckInBufferSpace((size_t)(conn->inCursor + msgLength), conn))
                    goto error_return;
                /* We'll come back when there is more data */
                return PGRES_POLLING_READING;
            }

            /* Get the type of request. */
            int areq = 0;
            if (cmpqGetInt(&areq, 4, conn)) {
                /* We'll come back when there are more data */
                return PGRES_POLLING_READING;
            }

            if (areq == CM_AUTH_REQ_OK) {
                /* OK, we successfully read the message; mark data consumed */
                conn->inStart = conn->inCursor;
                /* We are done with authentication exchange */
                conn->status = CONNECTION_AUTH_OK;
                /* Look to see if we have more data yet. */
                goto keep_going;
            } else if (areq == CM_AUTH_REQ_GSS) {
                /* OK, we successfully read the message; mark data consumed */
                resetCMPQExpBuffer(&conn->errorMessage);
                conn->inStart = conn->inCursor;
                rc = CMGssStartup(conn);
                if (rc != STATUS_OK)
                    goto error_return;
                goto keep_going;
            } else if (areq == CM_AUTH_REQ_GSS_CONT) {
                int llen = msgLength - 4;
                if (llen <= 0) {
                    goto error_return;
                }
                conn->gss_inbuf.length = llen;
                FREE_AND_RESET(conn->gss_inbuf.value);
                conn->gss_inbuf.value = malloc(llen);
                if (conn->gss_inbuf.value == NULL) {
                    appendCMPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("failed to allocate memory for gss_inbuf:"
                                      "out of memory: request size=%d.\n"),
                        llen);
                    goto error_return;
                }
                cmpqGetnchar((char*)conn->gss_inbuf.value, llen, conn);
                /* OK, we successfully read the message; mark data consumed */
                conn->inStart = conn->inCursor;
                rc = CMGssContinue(conn);
                if (rc != STATUS_OK) {
                    FREE_AND_RESET(conn->gss_inbuf.value);
                    goto error_return;
                }
                cmpqFlush(conn);
                goto keep_going;
            } else {
                goto error_return;
            }
        }

        case CONNECTION_AUTH_OK: {
            /* We can release the address list now. */
            cm_freeaddrinfo_all(conn->addrlist_family, conn->addrlist);
            conn->addrlist = NULL;
            conn->addr_cur = NULL;

            /* Otherwise, we are open for business! */
            conn->status = CONNECTION_OK;
            return PGRES_POLLING_OK;
        }

        default:
            appendCMPQExpBuffer(&conn->errorMessage,
                "invalid connection state %c, "
                "probably indicative of memory corruption\n",
                conn->status);
            goto error_return;
    }

    /* Unreachable */

error_return:
    /*
     * We used to close the socket at this point, but that makes it awkward
     * for those above us if they wish to remove this socket from their own
     * records (an fd_set for example).  We'll just have this socket closed
     * when CMPQfinish is called (which is compulsory even after an error, since
     * the connection structure must be freed).
     */
    conn->status = CONNECTION_BAD;
    return PGRES_POLLING_FAILED;
}

/*
 * makeEmptyCM_Conn
 *	 - create a CM_Conn data structure with (as yet) no interesting data
 */
static CM_Conn* makeEmptyCM_Conn(void)
{
    CM_Conn* conn = NULL;
    errno_t rc = 0;

    conn = (CM_Conn*)malloc(sizeof(CM_Conn));
    if (conn == NULL) {
        write_runlog(DEBUG1, "[conn abnormal] Out of memory for CmServer_conn!\n");
        return conn;
    }

    /* Zero all pointers and booleans */
    rc = memset_s(conn, sizeof(CM_Conn), 0, sizeof(CM_Conn));
    securec_check_errno(rc, FREE_AND_RESET(conn));

    conn->status = CONNECTION_BAD;
    conn->result = NULL;

    /*
     * We try to send at least 8K at a time, which is the usual size of pipe
     * buffers on Unix systems.  That way, when we are sending a large amount
     * of data, we avoid incurring extra kernel context swaps for partial
     * bufferloads.  The output buffer is initially made 16K in size, and we
     * try to dump it after accumulating 8K.
     *
     * With the same goal of minimizing context swaps, the input buffer will
     * be enlarged anytime it has less than 8K free, so we initially allocate
     * twice that.
     */
    conn->inBufSize = 16 * 1024;
    conn->inBuffer = (char*)malloc(conn->inBufSize);
    conn->outBufSize = 16 * 1024;
    conn->outBuffer = (char*)malloc(conn->outBufSize);
    initCMPQExpBuffer(&conn->errorMessage);
    initCMPQExpBuffer(&conn->workBuffer);

    if (conn->inBuffer == NULL || conn->outBuffer == NULL || PQExpBufferBroken(&conn->errorMessage) ||
        PQExpBufferBroken(&conn->workBuffer)) {
        /* out of memory already :-( */
        write_runlog(LOG, "[conn abnormal] Out of memory for inBuffer and outBuffer!\n");
        freeCM_Conn(conn);
        conn = NULL;
    }

    return conn;
}

/*
 * freeCM_Conn
 *	 - free an idle (closed) CM_Conn data structure
 *
 * NOTE: this should not overlap any functionality with closeCM_Conn().
 * Clearing/resetting of transient state belongs there; what we do here is
 * release data that is to be held for the life of the CM_Conn structure.
 * If a value ought to be cleared/freed during PQreset(), do it there not here.
 */
static void freeCM_Conn(CM_Conn* conn)
{
    FREE_AND_RESET(conn->pghost);
    FREE_AND_RESET(conn->pghostaddr);
    FREE_AND_RESET(conn->pgport);
    FREE_AND_RESET(conn->pglocalhost);
    FREE_AND_RESET(conn->pglocalport);
    FREE_AND_RESET(conn->pguser);
    FREE_AND_RESET(conn->connect_timeout);
    FREE_AND_RESET(conn->gc_node_name);
    FREE_AND_RESET(conn->inBuffer);
    FREE_AND_RESET(conn->outBuffer);
    FREE_AND_RESET(conn->result);
    termCMPQExpBuffer(&conn->errorMessage);
    termCMPQExpBuffer(&conn->workBuffer);

    OM_uint32 lmin_s = 0;
    gss_release_name(&lmin_s, &conn->gss_targ_nam);

    free(conn);
}

/*
 * closeCM_Conn
 * - properly close a connection to the backend
 *
 * This should reset or release all transient state, but NOT the connection
 * parameters.  On exit, the CM_Conn should be in condition to start a fresh
 * connection with the same parameters (see PQreset()).
 */
static void closeCM_Conn(CM_Conn* conn)
{
    /*
     * Note that the protocol doesn't allow us to send Terminate messages
     * during the startup phase.
     */
    if (conn->sock >= 0 && conn->status == CONNECTION_OK) {
        /*
         * Try to send "close connection" message to backend. Ignore any
         * error.
         *
         * Force length word for backends may try to read that in a generic
         * code
         */
        cmpqPutMsgStart('X', true, conn);
        cmpqPutMsgEnd(conn);
        cmpqFlush(conn);
    }

    /*
     * Close the connection, reset all transient state, flush I/O buffers.
     */
    if (conn->sock >= 0) {
        close(conn->sock);
    }
    conn->sock = -1;
    conn->status = CONNECTION_BAD; /* Well, not really _bad_ - just
                                    * absent */
    cm_freeaddrinfo_all(conn->addrlist_family, conn->addrlist);
    conn->addrlist = NULL;
    conn->addr_cur = NULL;
    conn->inStart = conn->inCursor = conn->inEnd = 0;
    conn->outCount = 0;
}

/*
 * CMPQfinish: properly close a connection to the backend. Also frees
 * the CM_Conn data structure so it shouldn't be re-used after this.
 */
void CMPQfinish(CM_Conn* conn)
{
    if (conn != NULL) {
        closeCM_Conn(conn);
        freeCM_Conn(conn);
        conn = NULL;
    }
}

/*
 * pqPacketSend() -- convenience routine to send a message to server.
 *
 * pack_type: the single-byte message type code.  (Pass zero for startup
 * packets, which have no message type code.)
 *
 * buf, buf_len: contents of message.  The given length includes only what
 * is in buf; the message type and message length fields are added here.
 *
 * RETURNS: STATUS_ERROR if the write fails, STATUS_OK otherwise.
 * SIDE_EFFECTS: may block.
 *
 * Note: all messages sent with this routine have a length word, whether
 * it's protocol 2.0 or 3.0.
 */
int CMPQPacketSend(CM_Conn* conn, char pack_type, const void* buf, size_t buf_len)
{
    int ret = 0;
    if (conn == NULL) {
        write_runlog(ERROR, "CMPQPacketSend failed conn is null");
        return STATUS_ERROR;
    }

    /* Start the message. */
    if (cmpqPutMsgStart(pack_type, true, conn)) {
        write_runlog(ERROR, "Start the message failed");
        return STATUS_ERROR;
    }

    /* Send the message body. */
    if (cmpqPutnchar((const char*)buf, buf_len, conn)) {
        write_runlog(ERROR, "Send the message body failed");
        return STATUS_ERROR;
}
    /* Finish the message. */
    ret = cmpqPutMsgEnd(conn);
    if (ret < 0) {
        write_runlog(LOG, "cmpqPutMsgEnd failed ret=%d\n", ret);
        return STATUS_ERROR;
    }

    /* Flush to ensure backend gets it. */
    ret = cmpqFlush(conn);
    if (ret < 0) {
        write_runlog(LOG, "cmpqFlush failed ret=%d\n", ret);
        return STATUS_ERROR;
    }

    return STATUS_OK;
}

 /**
  * @brief Conninfo parser routine. Defaults are supplied (from a service file, environment variables, etc)
  * for unspecified options, but only if use_defaults is TRUE.
  * 
  * @return CMPQconninfoOption* If successful, a malloc'd CMPQconninfoOption array is returned.
  * If not successful, NULL is returned and an error message is left in errorMessage.
  */
static CMPQconninfoOption* conninfo_parse(const char* conninfo, PQExpBuffer errorMessage, bool use_defaults)
{
    char* pname = NULL;
    char* pval = NULL;
    char* buf = NULL;
    char* cp = NULL;
    char* cp2 = NULL;
    CMPQconninfoOption* options = NULL;
    CMPQconninfoOption* option = NULL;
    errno_t rc;

    /* Make a working copy of CMPQconninfoOptions */
    options = (CMPQconninfoOption*)malloc(sizeof(CMPQconninfoOptions));
    if (options == NULL) {
        printfCMPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
        return NULL;
    }
    rc = memcpy_s(options, sizeof(CMPQconninfoOptions), CMPQconninfoOptions, sizeof(CMPQconninfoOptions));
    securec_check_c(rc, "\0", "\0");

    /* Need a modifiable copy of the input string */
    if ((buf = strdup(conninfo)) == NULL) {
        printfCMPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
        CMPQconninfoFree(options);
        options = NULL;
        return NULL;
    }
    cp = buf;

    while (*cp) {
        /* Skip blanks before the parameter name */
        if (isspace((unsigned char)*cp)) {
            cp++;
            continue;
        }

        /* Get the parameter name */
        pname = cp;
        while (*cp) {
            if (*cp == '=') {
                break;
            }
            if (isspace((unsigned char)*cp)) {
                *cp++ = '\0';
                while (*cp) {
                    if (!isspace((unsigned char)*cp)) {
                        break;
                    }
                    cp++;
                }
                break;
            }
            cp++;
        }

        /* Check that there is a following '=' */
        if (*cp != '=') {
            printfCMPQExpBuffer(
                errorMessage, libpq_gettext("missing \"=\" after \"%s\" in connection info string\n"), pname);
            CMPQconninfoFree(options);
            options = NULL;

            FREE_AND_RESET(buf);
            return NULL;
        }
        *cp++ = '\0';

        /* Skip blanks after the '=' */
        while (*cp) {
            if (!isspace((unsigned char)*cp)) {
                break;
            }
            cp++;
        }

        /* Get the parameter value */
        pval = cp;

        if (*cp != '\'') {
            cp2 = pval;
            while (*cp) {
                if (isspace((unsigned char)*cp)) {
                    *cp++ = '\0';
                    break;
                }
                if (*cp == '\\') {
                    cp++;
                    if (*cp != '\0') {
                        *cp2++ = *cp++;
                    }
                } else {
                    *cp2++ = *cp++;
                }
            }
            *cp2 = '\0';
        } else {
            cp2 = pval;
            cp++;
            for (;;) {
                if (*cp == '\0') {
                    printfCMPQExpBuffer(
                        errorMessage, libpq_gettext("unterminated quoted string in connection info string\n"));
                    CMPQconninfoFree(options);
                    options = NULL;

                    FREE_AND_RESET(buf);
                    return NULL;
                }
                if (*cp == '\\') {
                    cp++;
                    if (*cp != '\0') {
                        *cp2++ = *cp++;
                    }
                    continue;
                }
                if (*cp == '\'') {
                    *cp2 = '\0';
                    cp++;
                    break;
                }
                *cp2++ = *cp++;
            }
        }

        /*
         * Now we have the name and the value. Search for the param record.
         */
        for (option = options; option->keyword != NULL; option++) {
            if (strcmp(option->keyword, pname) == 0) {
                break;
            }
        }
        if (option->keyword == NULL) {
            printfCMPQExpBuffer(errorMessage, libpq_gettext("invalid connection option \"%s\"\n"), pname);
            CMPQconninfoFree(options);
            options = NULL;

            FREE_AND_RESET(buf);
            return NULL;
        }

        /*
         * Store the value
         */
        FREE_AND_RESET(option->val);
        option->val = strdup(pval);
        if (option->val == NULL) {
            printfCMPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
            CMPQconninfoFree(options);
            options = NULL;

            FREE_AND_RESET(buf);
            return NULL;
        }
    }

    /* Done with the modifiable input string */
    FREE_AND_RESET(buf);

    return options;
}

static char* conninfo_getval(CMPQconninfoOption* connOptions, const char* keyword)
{
    CMPQconninfoOption* option = NULL;

    for (option = connOptions; option->keyword != NULL; option++) {
        if (strcmp(option->keyword, keyword) == 0) {
            return option->val;
        }
    }

    return NULL;
}

void CMPQconninfoFree(CMPQconninfoOption* connOptions)
{
    CMPQconninfoOption* option = NULL;

    if (connOptions == NULL) {
        return;
    }

    for (option = connOptions; option->keyword != NULL; option++) {
        FREE_AND_RESET(option->val);
    }
    free(connOptions);
}

CMConnStatusType CMPQstatus(const CM_Conn* conn)
{
    if (conn == NULL) {
        return CONNECTION_BAD;
    }
    return conn->status;
}

char* CMPQerrorMessage(const CM_Conn* conn)
{
    if (conn == NULL) {
        return libpq_gettext("connection pointer is NULL\n");
    }

    return conn->errorMessage.data;
}

/*
 * Continue GSS authentication with next token as needed.
 */
static int CMGssContinue(CM_Conn* conn)
{
    OM_uint32 maj_stat = 0;
    OM_uint32 min_stat = 0;
    OM_uint32 lmin_s = 0;
    char* krbconfig = NULL;
    int retry_count = 0;

retry_init:
    /*
     * This function is used for internal and external connections, do not add lock here.
     * If gss init failed, retry 10 times.
     * Clean the config cache and ticket cache set by hadoop remote read.
     */
#ifndef ENABLE_LITE_MODE
    krb5_clean_cache_profile_path();
#endif


    /* Krb5 config file priority : setpath > env(MPPDB_KRB5_FILE_PATH) > default(/etc/krb5.conf).*/
    krbconfig = gs_getenv_with_check("MPPDB_KRB5_FILE_PATH", conn);
    if (krbconfig == NULL) {
        appendCMPQExpBuffer(&conn->errorMessage, "get env MPPDB_KRB5_FILE_PATH failed.\n");
        return STATUS_ERROR;
    }
#ifndef ENABLE_LITE_MODE
    (void)krb5_set_profile_path(krbconfig);
#endif

    /*
     * The first time come here(with no tickent cache), gss_init_sec_context will send TGS_REQ
     * to kerberos server to get ticket and then cache it in default_ccache_name which configured
     * in MPPDB_KRB5_FILE_PATH.
     */
    maj_stat = gss_init_sec_context(&min_stat,
        GSS_C_NO_CREDENTIAL,
        &conn->gss_ctx,
        conn->gss_targ_nam,
        GSS_C_NO_OID,
        GSS_C_MUTUAL_FLAG,
        0,
        GSS_C_NO_CHANNEL_BINDINGS,
        (conn->gss_ctx == GSS_C_NO_CONTEXT) ? GSS_C_NO_BUFFER : &conn->gss_inbuf,
        NULL,
        &conn->gss_outbuf,
        NULL,
        NULL);

    if (conn->gss_outbuf.length != 0) {
        /*
         * GSS generated data to send to the server. We don't care if it's the
         * first or subsequent packet, just send the same kind of password
         * packet.
         */
        if (CMPQPacketSend(conn, 'p', conn->gss_outbuf.value, conn->gss_outbuf.length) != STATUS_OK) {
            printfCMPQExpBuffer(&conn->errorMessage, libpq_gettext("Send p type packet failed\n"));
            (void)gss_release_buffer(&lmin_s, &conn->gss_outbuf);
            if (conn->gss_ctx != NULL) {
                (void)gss_delete_sec_context(&lmin_s, &conn->gss_ctx, GSS_C_NO_BUFFER);
            }
            if (conn->gss_inbuf.value != NULL) {
                FREE_AND_RESET(conn->gss_inbuf.value);
                conn->gss_inbuf.length = 0;
            }
            return STATUS_ERROR;
        }
        conn->status = CONNECTION_AWAITING_RESPONSE;
    }
    if (conn->gss_inbuf.value != NULL) {
        FREE_AND_RESET(conn->gss_inbuf.value);
        conn->gss_inbuf.length = 0;
    }
    (void)gss_release_buffer(&lmin_s, &conn->gss_outbuf);

    if (maj_stat != GSS_S_COMPLETE && maj_stat != GSS_S_CONTINUE_NEEDED) {
        OM_uint32 qp_min_s = 0;
        OM_uint32 qp_msg_ctx = 0;
        gss_buffer_desc qp_msg;
        gss_display_status(&qp_min_s, maj_stat, GSS_C_GSS_CODE, GSS_C_NO_OID, &qp_msg_ctx, &qp_msg);
        fprintf(stderr, "gss failed: %s\n", (char*)qp_msg.value);
        gss_release_buffer(&qp_min_s, &qp_msg);
        gss_display_status(&qp_min_s, min_stat, GSS_C_MECH_CODE, GSS_C_NO_OID, &qp_msg_ctx, &qp_msg);
        fprintf(stderr, "gss failed: %s\n", (char*)qp_msg.value);
        gss_release_buffer(&qp_min_s, &qp_msg);

        /* Retry 10 times for init context responding to scenarios such as cache renewed by kinit. */
        if (retry_count < 10) {
            (void)usleep(1000);
            retry_count++;
            goto retry_init;
        }

        gss_release_name(&lmin_s, &conn->gss_targ_nam);
        if (conn->gss_ctx != NULL) {
            gss_delete_sec_context(&lmin_s, &conn->gss_ctx, GSS_C_NO_BUFFER);
        }

        printfCMPQExpBuffer(&conn->errorMessage, libpq_gettext("GSSAPI continuation error, more than 10 times\n"));
        return STATUS_ERROR;
    }

    if (maj_stat == GSS_S_COMPLETE) {
        gss_release_name(&lmin_s, &conn->gss_targ_nam);
    }
    if (conn->gss_ctx != NULL) {
        gss_delete_sec_context(&lmin_s, &conn->gss_ctx, GSS_C_NO_BUFFER);
    }

    return STATUS_OK;
}

/*
 * Send initial GSS authentication token
 */
static int CMGssStartup(CM_Conn* conn)
{
    OM_uint32 maj_stat = 0;
    OM_uint32 min_stat = 0;
    int maxlen = -1;
    gss_buffer_desc temp_gbuf;
    char* krbsrvname = NULL;
    char* krbhostname = NULL;
    errno_t rc = EOK;

    if (!((conn->pghost != NULL) && conn->pghost[0] != '\0')) {
        printfCMPQExpBuffer(&conn->errorMessage, libpq_gettext("host name must be specified\n"));
        return STATUS_ERROR;
    }

    if (conn->gss_ctx != NULL) {
        printfCMPQExpBuffer(&conn->errorMessage, libpq_gettext("duplicate GSS authentication request\n"));
        return STATUS_ERROR;
    }

    /*
     * Import service principal name so the proper ticket can be acquired by
     * the GSSAPI system. The PGKRBSRVNAME and KRBHOSTNAME is from
     * the principal.
     */
    krbsrvname = gs_getenv_with_check("PGKRBSRVNAME", conn);
    if (krbsrvname == NULL) {
        printfCMPQExpBuffer(&conn->errorMessage, libpq_gettext("The environment PGKRBSRVNAME is null.\n"));
        return STATUS_ERROR;
    }

    krbhostname = gs_getenv_with_check("KRBHOSTNAME", conn);
    if (krbhostname == NULL) {
        printfCMPQExpBuffer(&conn->errorMessage, libpq_gettext("The environment KRBHOSTNAME null.\n"));
        return STATUS_ERROR;
    }

    if ((MAX_INT32 - strlen(krbhostname)) < (strlen(krbsrvname) + 2)) {
        return STATUS_ERROR;
    }
    maxlen = strlen(krbhostname) + strlen(krbsrvname) + 2;
    temp_gbuf.value = (char*)malloc(maxlen);
    if (temp_gbuf.value == NULL) {
        printfCMPQExpBuffer(&conn->errorMessage, libpq_gettext("out of memory, remote datanode.\n"));
        return STATUS_ERROR;
    }

    rc = snprintf_s((char*)temp_gbuf.value, maxlen, maxlen - 1, "%s/%s", krbsrvname, krbhostname);
    securec_check_ss_c(rc, "", "");
    temp_gbuf.length = strlen((char*)temp_gbuf.value);

    maj_stat = gss_import_name(&min_stat, &temp_gbuf, (gss_OID)GSS_KRB5_NT_PRINCIPAL_NAME, &conn->gss_targ_nam);
    FREE_AND_RESET(temp_gbuf.value);
    if (maj_stat != GSS_S_COMPLETE) {
        printfCMPQExpBuffer(&conn->errorMessage, libpq_gettext("GSSAPI name import error.\n"));
        return STATUS_ERROR;
    }

    /*
     * Initial packet is the same as a continuation packet with no initial
     * context.
     */
    conn->gss_ctx = GSS_C_NO_CONTEXT;

    return CMGssContinue(conn);
}

static void check_backend_env(const char* input_env_value, CM_Conn* conn)
{
    const int MAXENVLEN = 1024;
    const char* danger_character_list[] = {";", "`", "\\", "'", "\"", ">", "<", "$", "&", "|", "!", "\n", NULL};
    int i = 0;

    if (input_env_value == NULL || strlen(input_env_value) >= MAXENVLEN) {
        appendCMPQExpBuffer(&conn->errorMessage, "wrong env value.\n");
        return;
    }

    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr((const char*)input_env_value, danger_character_list[i])) {
            appendCMPQExpBuffer(&conn->errorMessage,
                "env_value(%s) contains invaid symbol(%s).\n",
                input_env_value,
                danger_character_list[i]);
        }
    }
}

static char* gs_getenv_with_check(const char* envKey, CM_Conn* conn)
{
    static char* result = NULL;
    char* envValue = gs_getenv_r(envKey);

    if (envValue != NULL) {
        check_backend_env(envValue, conn);
        result = envValue;
    }

    return result;
}
