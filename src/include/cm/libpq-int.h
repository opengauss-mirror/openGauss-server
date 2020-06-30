/* ---------------------------------------------------------------------------------------
 * 
 * libpq-int.h
 *	  This file contains internal definitions meant to be used only by
 *	  the frontend libpq library, not by applications that call it.
 *
 *	  An application can include this file if it wants to bypass the
 *	  official API defined by libpq-fe.h, but code that does so is much
 *	  more likely to break across PostgreSQL releases than code that uses
 *	  only the official API.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/libpq-int.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CM_LIBPQ_INT_H
#define CM_LIBPQ_INT_H

#include <time.h>
#include <sys/types.h>
#include <sys/time.h>
#include "cm/pqcomm.h"
#include "cm/pqexpbuffer.h"
#include "cm/cm_c.h"
#include "cm/cm_msg.h"

#if defined(HAVE_GSSAPI_H)
#include <gssapi.h>
#else
#include <gssapi/gssapi.h>
#endif

/*
 * cm_Conn stores all the state data associated with a single connection
 * to a backend.
 */
struct cm_conn {
    /* Saved values of connection options */
    char* pghost;     /* the machine on which the server is running */
    char* pghostaddr; /* the IPv4 address of the machine on which
                       * the server is running, in IPv4
                       * numbers-and-dots notation. Takes precedence
                       * over above. */
    char* pgport;     /* the server's communication port */
    char* pglocalhost;
    char* pglocalport;
    char* connect_timeout; /* connection timeout (numeric string) */
    char* pguser;          /* CM username and password, if any */
    int node_id;
    char* gc_node_name; /* PGXC Node Name */
    int remote_type;    /* is this a connection to/from a proxy ? */
    int is_postmaster;  /* is this connection to/from a postmaster instance */

    /* Optional file to write trace info to */
    FILE* Pfdebug;

    /* Status indicators */
    CMConnStatusType status;

    /* Connection data */
    int sock;         /* Unix FD for socket, -1 if not connected */
    CMSockAddr laddr; /* Local address */
    CMSockAddr raddr; /* Remote address */

    /* Error info for cm communication */
    int last_call;
    int last_errno; /* Last errno.  zero if the last call succeeds. */

    /* Transient state needed while establishing connection */
    struct addrinfo* addrlist; /* list of possible backend addresses */
    struct addrinfo* addr_cur; /* the one currently being tried */
    int addrlist_family;       /* needed to know how to free addrlist */

    /* Buffer for data received from backend and not yet processed */
    char* inBuffer; /* currently allocated buffer */
    int inBufSize;  /* allocated size of buffer */
    int inStart;    /* offset to first unconsumed data in buffer */
    int inCursor;   /* next byte to tentatively consume */
    int inEnd;      /* offset to first position after avail data */

    /* Buffer for data not yet sent to backend */
    char* outBuffer; /* currently allocated buffer */
    int outBufSize;  /* allocated size of buffer */
    int outCount;    /* number of chars waiting in buffer */

    /* State for constructing messages in outBuffer */
    int outMsgStart; /* offset to msg start (length word); if -1,
                      * msg has no length word */
    int outMsgEnd;   /* offset to msg end (so far) */

    /* Buffer for current error message */
    PQExpBufferData errorMessage; /* expansible string */

    /* Buffer for receiving various parts of messages */
    PQExpBufferData workBuffer; /* expansible string */

    /* Pointer to the result of last operation */
    CM_Result* result;

    /* Support kerberos authentication for gtm server */
    char* krbsrvname;           /* Kerberos service name */
    gss_ctx_id_t gss_ctx;       /* GSS context */
    gss_name_t gss_targ_nam;    /* GSS target name */
    gss_buffer_desc gss_inbuf;  /* GSS input token */
    gss_buffer_desc gss_outbuf; /* GSS output token */
};

/* === in fe-misc.c === */

/*
 * "Get" and "Put" routines return 0 if successful, EOF if not. Note that for
 * Get, EOF merely means the buffer is exhausted, not that there is
 * necessarily any error.
 */
extern int cmpqCheckOutBufferSpace(size_t bytes_needed, CM_Conn* conn);
extern int cmpqCheckInBufferSpace(size_t bytes_needed, CM_Conn* conn);
extern int cmpqGetc(char* result, CM_Conn* conn);
extern int cmpqGets(PQExpBuffer buf, CM_Conn* conn);
extern int cmpqGets_append(PQExpBuffer buf, CM_Conn* conn);
extern int cmpqPutnchar(const char* s, size_t len, CM_Conn* conn);
extern int cmpqGetnchar(char* s, size_t len, CM_Conn* conn);
extern int cmpqGetInt(int* result, size_t bytes, CM_Conn* conn);
extern int cmpqPutMsgStart(char msg_type, bool force_len, CM_Conn* conn);
extern int cmpqPutMsgEnd(CM_Conn* conn);
extern int cmpqReadData(CM_Conn* conn);
extern int cmpqFlush(CM_Conn* conn);
extern int cmpqWait(int forRead, int forWrite, CM_Conn* conn);
extern int cmpqWaitTimed(int forRead, int forWrite, CM_Conn* conn, time_t finish_time);
extern int cmpqReadReady(CM_Conn* conn);

/*
 * In fe-protocol.c
 */
extern CM_Result* cmpqGetResult(CM_Conn* conn);
extern int cmpqGetError(CM_Conn* conn, CM_Result* result);
extern void cmpqResetResultData(CM_Result* result);

#define SOCK_ERRNO errno
#define SOCK_ERRNO_SET(e) (errno = (e))

#endif /* LIBPQ_INT_H */
