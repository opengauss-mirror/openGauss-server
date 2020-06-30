/* ---------------------------------------------------------------------------------------
 * 
 * libpq-be.h
 *	  This file contains definitions for structures and externs used
 *	  by the cm server during client coonect.
 *
 *	  Note that this is backend-internal and is NOT exported to clients.
 *	  Structs that need to be client-visible are in pqcomm.h.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/libpq-be.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CM_LIBPQ_BE_H
#define CM_LIBPQ_BE_H

#include "cm/pqcomm.h"
#include "cm/cm_c.h"

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#if defined(HAVE_GSSAPI_H)
#include <gssapi.h>
#else
#include <gssapi/gssapi.h>
#endif

/*
 * This is used by the cm server in its communication with frontends.	It
 * contains all state information needed during this communication before the
 * backend is run.	The Port structure is kept in malloc'd memory and is
 * still available when a backend is running (see u_sess->proc_cxt.MyProcPort).	The data
 * it points to must also be malloc'd, or else palloc'd in TopMostMemoryContext,
 * so that it survives into GTM_ThreadMain execution!
 */

typedef struct Port {
    int sock;                  /* File descriptor */
    CMSockAddr laddr;          /* local addr (postmaster) */
    CMSockAddr raddr;          /* remote addr (client) */
    char* remote_host;         /* name (or ip addr) of remote host */
    char* remote_port;         /* text rep of remote port */
    CM_PortLastCall last_call; /* Last syscall to this port */
    int last_errno;            /* Last errno. zero if the last call succeeds */

    char* user_name;
    int remote_type; /* Type of remote connection */
    uint32 node_id;
    char* node_name;
    bool is_postmaster; /* Is remote a node postmaster? */
    bool startpack_have_processed;
#define PQ_BUFFER_SIZE (16 * 1024)

    char PqSendBuffer[PQ_BUFFER_SIZE];
    int PqSendPointer; /* Next index to store a byte in PqSendBuffer */

    char PqRecvBuffer[PQ_BUFFER_SIZE];
    int PqRecvPointer; /* Next index to read a byte from PqRecvBuffer */
    int PqRecvLength;  /* End of data available in PqRecvBuffer */

    /*
     * TCP keepalive settings.
     *
     * default values are 0 if AF_UNIX or not yet known; current values are 0
     * if AF_UNIX or using the default. Also, -1 in a default value means we
     * were unable to find out the default (getsockopt failed).
     */
    int default_keepalives_idle;
    int default_keepalives_interval;
    int default_keepalives_count;
    int keepalives_idle;
    int keepalives_interval;
    int keepalives_count;

    /*
     * GTM communication error handling.  See libpq-int.h for details.
     */
    int connErr_WaitOpt;
    int connErr_WaitInterval;
    int connErr_WaitCount;

    /* Support kerberos authentication for gtm server */
    char* krbsrvname; /* Kerberos service name */
} Port;

/* TCP keepalives configuration. These are no-ops on an AF_UNIX socket. */

extern int pq_getkeepalivesidle(Port* port);
extern int pq_getkeepalivesinterval(Port* port);
extern int pq_getkeepalivescount(Port* port);

extern int pq_setkeepalivesidle(int idle, Port* port);
extern int pq_setkeepalivesinterval(int interval, Port* port);
extern int pq_setkeepalivescount(int count, Port* port);

extern int StreamServerPort(int family, char* hostName, unsigned short portNumber, int ListenSocket[], int MaxListen);
extern int StreamConnection(int server_fd, Port* port);
extern void StreamClose(int sock);
extern int pq_getbyte(Port* myport);
extern int pq_getmessage(Port* myport, CM_StringInfo s, int maxlen);
extern int pq_putmessage(Port* myport, char msgtype, const char* s, size_t len);
extern int pq_flush(Port* myport);
extern int SetSocketNoBlock(int isocketId);

#endif /* LIBPQ_BE_H */
