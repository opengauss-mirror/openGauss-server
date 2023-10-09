/* ---------------------------------------------------------------------------------------
 *
 *  fe-connect.cpp
 *        functions related to setting up a connection to the backend
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/heartbeat/libpq/fe-connect.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>
#include <time.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include "utils/elog.h"
#include "knl/knl_variable.h"
#include "libpq/ip.h"
#include "replication/heartbeat/libpq/libpq-fe.h"
#include "utils/palloc.h"
#include "utils/timestamp.h"

#define SOCK_ERRNO errno
#define SOCK_ERRNO_SET(e) (errno = (e))

#ifndef FREE_AND_RESET
#define FREE_AND_RESET(ptr) do { \
    if (NULL != (ptr)) { \
        free(ptr);       \
        (ptr) = NULL;    \
    }                    \
} while (0)
#endif

namespace PureLibpq {
typedef struct PQconninfoOption {
    char *keyword; /* The keyword of the option */
    char *val;     /* Option's current value, or NULL */
} PQconninfoOption;

/* ----------
 * Definition of the conninfo parameters and their fallback resources.
 *
 * PQconninfoOptions[] is a constant static array that we use to initialize
 * a dynamically allocated working copy.  All the "val" fields in
 * PQconninfoOptions[] *must* be NULL.	In a working copy, non-null "val"
 * fields point to malloc'd strings that should be freed when the working
 * array is freed (see PQconninfoFree).
 * ----------
 */
static const PQconninfoOption PQconninfoOptions[] = {
    { "connect_timeout", NULL },
    { "host", NULL },
    { "hostaddr", NULL },
    { "port", NULL },
    { "localhost", NULL },
    { "localport", NULL },
    { "node_id", NULL },
    { "node_name", NULL },
    { "remote_type", NULL },
    { "postmaster", NULL },
    { "user", NULL },
    /* Terminating entry --- MUST BE LAST */
    { NULL, NULL }
};

typedef struct ConnParam {
    char *remoteIp;
    int remotePort;
    char *localIp;
    int connTimeout;
} ConnParam;

static PQconninfoOption *conninfo_parse(const char *conninfo, bool use_defaults);
static char *conninfo_getval(PQconninfoOption *connOptions, const char *keyword);
static bool parseConnParam(const char *conninfo, ConnParam *param);
static int internalConnect(ConnParam *param);
static int internalConnect_v6(ConnParam *param);
static void PQconninfoFree(PQconninfoOption *connOptions);
static int connectNoDelay(int sock);
static void ConnParamFree(ConnParam *param);

/*
 *		PQconnect
 *
 * Returns a Port*.  If NULL is returned, a error has occurred.
 *
 */
Port *PQconnect(const char *conninfo)
{
    Port *port = NULL;
    ConnParam *param = (ConnParam *)palloc0(sizeof(ConnParam));
    int sock = 0;

    /*
     * Parse the conninfo string
     */
    if (!parseConnParam(conninfo, param)) {
        ConnParamFree(param);
        return NULL;
    }

    if (strchr(param->remoteIp, ':') != NULL) {
        sock = internalConnect_v6(param);
    } else {
        sock = internalConnect(param);
    }

    ConnParamFree(param);
    if (sock < 0) {
        return NULL;
    }

    port = (Port *)palloc0(sizeof(Port));
    /* fill in the server (remote) address */
    port->raddr.salen = sizeof(port->raddr.addr);
    if (getpeername(sock, (struct sockaddr *)&port->raddr.addr, (socklen_t *)&port->raddr.salen) < 0) {
        ereport(COMMERROR, (errmsg("getsockname() failed.")));
        close(sock);
        pfree(port);
        return NULL;
    }
    port->sock = sock;

    return port;
}

/* Only support IPV4 now */
static int internalConnect(ConnParam *param)
{
    int sock = -1;
    errno_t rc = 0;
    struct sockaddr_in remoteAddr;
    TimestampTz nowFailedTimestamp = 0;
    long secs = 0;
    int msecs = 0;
    rc = memset_s(&remoteAddr, sizeof(remoteAddr), 0, sizeof(remoteAddr));
    securec_check(rc, "", "");
    remoteAddr.sin_family = AF_INET;
    remoteAddr.sin_addr.s_addr = inet_addr(param->remoteIp);
    remoteAddr.sin_port = htons(param->remotePort);

    /* Open a socket */
    sock = socket(remoteAddr.sin_family, SOCK_STREAM, 0);
    if (sock < 0) {
        ereport(COMMERROR, (errmsg("could not create socket.")));
        return sock;
    }

    struct sockaddr_in localaddr;
    rc = memset_s(&localaddr, sizeof(sockaddr_in), 0, sizeof(sockaddr_in));
    securec_check(rc, "", "");
    localaddr.sin_family = AF_INET;
    localaddr.sin_addr.s_addr = inet_addr(param->localIp);
    localaddr.sin_port = 0; /* Any local port will do */

    rc = bind(sock, (struct sockaddr *)&localaddr, sizeof(localaddr));
    if (rc != 0) {
        ereport(COMMERROR, (errmsg("could not bind localhost:%s, result is %d", param->localIp, rc)));
        close(sock);
        return -1;
    }

#ifdef F_SETFD
    if (fcntl(sock, F_SETFD, FD_CLOEXEC) == -1) {
        ereport(COMMERROR, (errmsg("could not set socket(FD_CLOEXEC): %d", SOCK_ERRNO)));
        close(sock);
        return -1;
    }
#endif /* F_SETFD */

    /*
     * Random_Port_Reuse need set SO_REUSEADDR on.
     * Random_Port_Reuse must not use bind interface,
     * because socket owns a random port private when used bind interface.
     * SO_REUSEPORT solve this problem in kernel 3.9.
     */
    if (!IS_AF_UNIX(remoteAddr.sin_family)) {
        int on = 1;

        if ((setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on))) == -1) {
            ereport(COMMERROR, (errmsg("could not set socket(FD_CLOEXEC): %d", SOCK_ERRNO)));
            close(sock);
            return -1;
        }
    }

    /*
     * Select socket options: no delay of outgoing data for
     * TCP sockets, nonblock mode, close-on-exec. Fail if any
     * of this fails.
     */
    if (!IS_AF_UNIX(remoteAddr.sin_family)) {
        if (!connectNoDelay(sock)) {
            close(sock);
            return -1;
        }
    }

    if (param->connTimeout > 0) {
        struct timeval timeout = { 0, 0 };
        timeout.tv_sec = param->connTimeout;
        (void)setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    }

    /*
     * Start/make connection.  This should not block, since we
     * are in nonblock mode.  If it does, well, too bad.
     */
    if (connect(sock, (struct sockaddr *)&remoteAddr, sizeof(struct sockaddr)) < 0) {
        t_thrd.heartbeat_cxt.total_failed_times++;
        nowFailedTimestamp = GetCurrentTimestamp();
        TimestampDifference(t_thrd.heartbeat_cxt.last_failed_timestamp, nowFailedTimestamp, &secs, &msecs);
        if (secs > 1) {
            t_thrd.heartbeat_cxt.last_failed_timestamp = nowFailedTimestamp;
            ereport(COMMERROR, (errmsg("Connect failed, total failed times is %d.",
                t_thrd.heartbeat_cxt.total_failed_times)));
        }
        close(sock);
        return -1;
    }

    return sock;
}

/* support IPV6 */
static int internalConnect_v6(ConnParam *param)
{
    int sock = -1;
    errno_t rc = 0;
    int res = 0;
    char   portstr[MAXPGPATH];
    struct addrinfo *addrs = NULL;
    struct addrinfo *addrs_local = NULL;
    struct addrinfo hint, hint_local;
    errno_t ss_rc = 0;

    /* Initialize hint structure */
    ss_rc = memset_s(&hint, sizeof(hint), 0, sizeof(struct addrinfo));
    securec_check(ss_rc, "\0", "\0");
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_family = AF_UNSPEC;

    snprintf_s(portstr, MAXPGPATH, MAXPGPATH - 1, "%d", param->remotePort);

    /* Use pg_getaddrinfo_all() to resolve the address */
    res = pg_getaddrinfo_all(param->remoteIp, portstr, &hint, &addrs);
    if (res || !addrs) {
        ereport(WARNING, (errmsg("pg_getaddrinfo_all remoteAddr address %s failed\n", param->remoteIp)));
        return -1;
    }

    /* Initialize hint structure */
    ss_rc = memset_s(&hint_local, sizeof(hint_local), 0, sizeof(struct addrinfo));
    securec_check(ss_rc, "\0", "\0");
    hint_local.ai_socktype = SOCK_STREAM;
    hint_local.ai_family = AF_UNSPEC;
    hint_local.ai_flags = AI_PASSIVE;

    /* Use pg_getaddrinfo_all() to resolve the address */
    res = pg_getaddrinfo_all(param->localIp, NULL, &hint_local, &addrs_local);
    if (res || !addrs_local) {
        ereport(WARNING, (errmsg("pg_getaddrinfo_all localAddr address %s failed\n", param->localIp)));
        return -1;
    }

    /* Open a socket */
    sock = socket(addrs->ai_family, SOCK_STREAM, 0);
    if (sock < 0) {
        ereport(COMMERROR, (errmsg("could not create socket.")));
        return sock;
    }

    rc = bind(sock, addrs_local->ai_addr, addrs_local->ai_addrlen);
    if (rc != 0) {
        ereport(COMMERROR, (errmsg("could not bind localhost:%s, result is %d", param->localIp, rc)));
        close(sock);
        return -1;
    }

#ifdef F_SETFD
    if (fcntl(sock, F_SETFD, FD_CLOEXEC) == -1) {
        ereport(COMMERROR, (errmsg("could not set socket(FD_CLOEXEC): %d", SOCK_ERRNO)));
        close(sock);
        return -1;
    }
#endif /* F_SETFD */

    /*
     * Random_Port_Reuse need set SO_REUSEADDR on.
     * Random_Port_Reuse must not use bind interface,
     * because socket owns a random port private when used bind interface.
     * SO_REUSEPORT solve this problem in kernel 3.9.
     */
    if (!IS_AF_UNIX(addrs->ai_family)) {
        int on = 1;

        if ((setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on))) == -1) {
            ereport(COMMERROR, (errmsg("could not set socket(FD_CLOEXEC): %d", SOCK_ERRNO)));
            close(sock);
            return -1;
        }
    }

    /*
     * Select socket options: no delay of outgoing data for
     * TCP sockets, nonblock mode, close-on-exec. Fail if any
     * of this fails.
     */
    if (!IS_AF_UNIX(addrs->ai_family)) {
        if (!connectNoDelay(sock)) {
            close(sock);
            return -1;
        }
    }

    if (param->connTimeout > 0) {
        struct timeval timeout = { 0, 0 };
        timeout.tv_sec = param->connTimeout;
        (void)setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    }

    /*
     * Start/make connection.  This should not block, since we
     * are in nonblock mode.  If it does, well, too bad.
     */
    if (connect(sock, addrs->ai_addr, addrs->ai_addrlen) < 0) {
        ereport(COMMERROR, (errmsg("Connect failed.")));
        close(sock);
        return -1;
    }

    if (addrs) {
        pg_freeaddrinfo_all(hint.ai_family, addrs);
    }
    if (addrs_local) {
        pg_freeaddrinfo_all(hint_local.ai_family, addrs_local);
    }

    return sock;
}

/*
 *		parseConnParam
 *
 * Internal subroutine to set up connection parameters given an already-
 * created ConnParam and a conninfo string.
 *
 * Returns true if OK, false if trouble.
 */
static bool parseConnParam(const char *conninfo, ConnParam *param)
{
    PQconninfoOption *connOptions = NULL;
    char *tmp = NULL;
    const int CONN_TIMED_OUT = 2;
    bool ret = false;

    /*
     * Parse the conninfo string
     */
    connOptions = conninfo_parse(conninfo, true);
    if (connOptions == NULL) {
        return false;
    }

    /*
     * Move option values into conn structure
     *
     * XXX: probably worth checking strdup() return value here...
     */
    tmp = conninfo_getval(connOptions, "host");
    if (tmp == NULL) {
        ereport(COMMERROR, (errmsg("The remote host is NULL.")));
        goto OUTCONN;
    }
    param->remoteIp = pstrdup(tmp);

    tmp = conninfo_getval(connOptions, "port");
    if (tmp == NULL) {
        ereport(COMMERROR, (errmsg("The remote port is NULL.")));
        goto OUTCONN;
    }
    param->remotePort = atoi(tmp);

    tmp = conninfo_getval(connOptions, "localhost");
    if (tmp == NULL) {
        ereport(COMMERROR, (errmsg("The local host is NULL.")));
        goto OUTCONN;
    }
    param->localIp = pstrdup(tmp);

    tmp = conninfo_getval(connOptions, "connect_timeout");
    param->connTimeout = tmp != NULL ? atoi(tmp) : 0;

    /*
     * Rounding could cause connection to fail; need at least 2 secs
     */
    if (param->connTimeout < CONN_TIMED_OUT) {
        param->connTimeout = CONN_TIMED_OUT;
    }

    ret = true;

OUTCONN:
    /*
     * Free the option info - all is in conn now
     */
    PQconninfoFree(connOptions);
    connOptions = NULL;

    return ret;
}

/* ----------
 * connectNoDelay -
 * Sets the TCP_NODELAY socket option.
 * Returns 1 if successful, 0 if not.
 * ----------
 */
static int connectNoDelay(int sock)
{
#ifdef TCP_NODELAY
    int on = 1;

    if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *)&on, sizeof(on)) < 0) {
        ereport(COMMERROR, (errmsg("could not set socket to TCP no delay mode.")));
        return 0;
    }
#endif

    return 1;
}

/*
 * Conninfo parser routine
 *
 * If successful, a malloc'd PQconninfoOption array is returned.
 * If not successful, NULL is returned and an error message is
 * left in errorMessage.
 * Defaults are supplied (from a service file, environment variables, etc)
 * for unspecified options, but only if use_defaults is TRUE.
 */
static PQconninfoOption *conninfo_parse(const char *conninfo, bool use_defaults)
{
    char *pname = NULL;
    char *pval = NULL;
    char *buf = NULL;
    char *cp = NULL;
    char *cp2 = NULL;
    PQconninfoOption *options = NULL;
    PQconninfoOption *option = NULL;
    errno_t rc;

    /* Make a working copy of PQconninfoOptions */
    options = (PQconninfoOption *)malloc(sizeof(PQconninfoOptions));
    if (options == NULL) {
        ereport(COMMERROR, (errmsg("out of memory.")));
        return NULL;
    }
    rc = memcpy_s(options, sizeof(PQconninfoOptions), PQconninfoOptions, sizeof(PQconninfoOptions));
    securec_check_c(rc, "", "");

    /* Need a modifiable copy of the input string */
    if ((buf = strdup(conninfo)) == NULL) {
        ereport(COMMERROR, (errmsg("out of memory.")));
        PQconninfoFree(options);
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
            ereport(COMMERROR, (errmsg("missing \"=\" after \"%s\" in connection info string.", pname)));
            PQconninfoFree(options);
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
                    ereport(COMMERROR, (errmsg("unterminated quoted string in connection info string.")));
                    PQconninfoFree(options);
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
            ereport(COMMERROR, (errmsg("invalid connection option \"%s\".", pname)));
            PQconninfoFree(options);
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
            ereport(COMMERROR, (errmsg("out of memory.")));
            PQconninfoFree(options);
            options = NULL;

            FREE_AND_RESET(buf);
            return NULL;
        }
    }

    /* Done with the modifiable input string */
    FREE_AND_RESET(buf);

    return options;
}

static char *conninfo_getval(PQconninfoOption *connOptions, const char *keyword)
{
    PQconninfoOption *option = NULL;

    for (option = connOptions; option->keyword != NULL; option++) {
        if (strcmp(option->keyword, keyword) == 0) {
            return option->val;
        }
    }

    return NULL;
}

static void PQconninfoFree(PQconninfoOption *connOptions)
{
    PQconninfoOption *option = NULL;

    if (connOptions == NULL) {
        return;
    }

    for (option = connOptions; option->keyword != NULL; option++) {
        FREE_AND_RESET(option->val);
    }
    free(connOptions);
}

static void ConnParamFree(ConnParam *param)
{
    if (param == NULL) {
        return;
    }

    if (param->remoteIp != NULL) {
        pfree(param->remoteIp);
        param->remoteIp = NULL;
    }

    if (param->localIp != NULL) {
        pfree(param->localIp);
        param->localIp = NULL;
    }

    pfree(param);
    param = NULL;
}
}  // namespace PureLibpq
