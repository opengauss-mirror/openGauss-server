/* -------------------------------------------------------------------------
 *
 * libpq_be.h
 *	  This file contains definitions for structures and externs used
 *	  by the postmaster during client authentication.
 *
 *	  Note that this is backend-internal and is NOT exported to clients.
 *	  Structs that need to be client-visible are in pqcomm.h.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/libpq-be.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef LIBPQ_BE_H
#define LIBPQ_BE_H

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#ifdef USE_SSL
#include "openssl/ossl_typ.h"
#endif

#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif

#ifdef ENABLE_GSS
#if defined(HAVE_GSSAPI_H)
#include <gssapi.h>
#else
#include <gssapi/gssapi.h>
#endif /* HAVE_GSSAPI_H */
/*
 * GSSAPI brings in headers that set a lot of things in the global namespace on win32,
 * that doesn't match the msvc build. It gives a bunch of compiler warnings that we ignore,
 * but also defines a symbol that simply does not exist. Undefine it again.
 */
#ifdef WIN32_ONLY_COMPILER
#undef HAVE_GETADDRINFO
#endif
#endif /* ENABLE_GSS */

#ifdef ENABLE_SSPI
#define SECURITY_WIN32
#if defined(WIN32) && !defined(WIN32_ONLY_COMPILER)
#include <ntsecapi.h>
#endif
#include <security.h>
#undef SECURITY_WIN32
#ifndef ENABLE_GSS
/*
 * Define a fake structure compatible with GSSAPI on Unix.
 */
typedef struct {
    void* value;
    int length;
} gss_buffer_desc;
#endif
#endif /* ENABLE_SSPI */

#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "libpq/hba.h"
#include "libpq/pqcomm.h"
#include "libpq/sha2.h"
#include "libcomm/libcomm.h"
#include "tcop/dest.h"

typedef enum CAC_state { CAC_OK, CAC_STARTUP, CAC_SHUTDOWN, CAC_RECOVERY, CAC_TOOMANY, CAC_WAITBACKUP, CAC_OOM
    } CAC_state;

/*
 * GSSAPI specific state information
 */
#if defined(ENABLE_GSS) | defined(ENABLE_SSPI)
typedef struct {
    gss_buffer_desc outbuf; /* GSSAPI output token buffer */
#ifdef ENABLE_GSS
    gss_cred_id_t cred; /* GSSAPI connection cred's */
    gss_ctx_id_t ctx;   /* GSSAPI connection context */
    gss_name_t name;    /* GSSAPI client name */
#endif
} pg_gssinfo;
#endif

/*
 * ProtocolExtensionConfig
 *
 * 	All the callbacks implementing a specific wire protocol
 */
typedef struct ProtocolExtensionConfig {
    bool    server_handshake_first;
    void    (*fn_init)(void);
    int     (*fn_start)(struct Port *port);
    void    (*fn_authenticate)(struct Port *port);
    void    (*fn_send_message)(ErrorData *edata);
    void    (*fn_send_cancel_key)(int32 pid, int32 key);
    void    (*fn_comm_reset)(void);
    void    (*fn_send_ready_for_query)(CommandDest dest);
    int	    (*fn_read_command)(StringInfo inBuf);
    void    (*fn_end_command)(const char *completionTag);
    DestReceiver*   (*fn_printtup_create_DR)(CommandDest dest);
    void    (*fn_set_DR_params)(DestReceiver* self, List* target_list);
    int     (*fn_process_command)(StringInfo inBuf);
    void    (*fn_report_param_status)(const char *name, char *val);
} ProtocolExtensionConfig;

/*
 * This is used by the postmaster in its communication with frontends.	It
 * contains all state information needed during this communication before the
 * backend is run.	The Port structure is kept in malloc'd memory and is
 * still available when a backend is running (see u_sess->proc_cxt.MyProcPort).	The data
 * it points to must also be malloc'd, or else palloc'd in TopMemoryContext,
 * so that it survives into PostgresMain execution!
 */

typedef struct Port {
    volatile pgsocket sock;     /* File descriptor */
    bool noblock;               /* is the socket in non-blocking mode? */
    ProtocolVersion proto;      /* FE/BE protocol version */
    SockAddr laddr;             /* local addr (postmaster) */
    SockAddr raddr;             /* remote addr (client) */
    char* remote_host;          /* name (or ip addr) of remote host */
    char* remote_hostname;      /* name (not ip addr) of remote host, if
                                 * available */
    int remote_hostname_resolv; /* +1 = remote_hostname is known to
                                 * resolve to client's IP address; -1
                                 * = remote_hostname is known NOT to
                                 * resolve to client's IP address; 0 =
                                 * we have not done the forward DNS
                                 * lookup yet */
    char* remote_port;          /* text rep of remote port */

    // Stream Commucation. Use only between DN.
    //
    libcommaddrinfo* libcomm_addrinfo;

    gsocket gs_sock;

    CAC_state canAcceptConnections; /* postmaster connection status */

    ProtocolExtensionConfig *protocol_config;	/* wire protocol functions */

    /*
     * Information that needs to be saved from the startup packet and passed
     * into backend execution.	"char *" fields are NULL if not set.
     * guc_options points to a List of alternating option names and values.
     */
    char* database_name;
    char* user_name;
    char* cmdline_options;
    List* guc_options;

    /*
     * Information that needs to be held during the authentication cycle.
     */
    HbaLine* hba;
    char md5Salt[4]; /* Password salt */

    /*
     * Information that really has no business at all being in struct Port,
     * but since it gets used by elog.c in the same way as database_name and
     * other members of this struct, we may as well keep it here.
     */
    TimestampTz SessionStartTime; /* backend start time */

    /*
     * Backend version number of the remote end, if available.
     *
     * For local backends, they should be launched with the same version
     * number as the remote end for compatibility.
     */
    uint32 SessionVersionNum;

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
    int default_tcp_user_timeout;
    int keepalives_idle;
    int keepalives_interval;
    int keepalives_count;
    int tcp_user_timeout;

#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)

    /*
     * If GSSAPI is supported, store GSSAPI information. Otherwise, store a
     * NULL pointer to make sure offsets in the struct remain the same.
     */
    pg_gssinfo* gss;
#else
    void* gss;
#endif

    /*
     * SSL structures (keep these last so that USE_SSL doesn't affect
     * locations of other fields)
     */
#ifdef USE_SSL
    SSL* ssl;
    X509* peer;
    char* peer_cn;
    unsigned long count;

#endif
    /* begin RFC 5802 */
    char token[TOKEN_LENGTH * 2 + 1];
    /* end RFC 5802 */
    bool is_logic_conn;
    MessageCommLog *msgLog;

    /* Support kerberos authentication for gtm server */
#ifdef ENABLE_GSS
    char* krbsrvname;           /* Kerberos service name */
    gss_ctx_id_t gss_ctx;       /* GSS context */
    gss_cred_id_t gss_cred;     /* GSS credential */
    gss_name_t gss_name;        /* GSS target name */
    gss_buffer_desc gss_outbuf; /* GSS output token buffer */
#endif
} Port;

#define STREAM_BUFFER_SIZE 8192
#define STREAM_BUFFER_SIZE_KB 8

typedef struct StreamBuffer {
    char PqSendBuffer[STREAM_BUFFER_SIZE];
    int PqSendBufferSize;
    int PqSendPointer; /* Next index to store a byte in PqSendBuffer */
    int PqSendStart;
    bool PqCommBusy;
} StreamBuffer;

extern char* tcp_link_addr;
extern THR_LOCAL ProtocolVersion FrontendProtocol;

/* TCP keepalives configuration. These are no-ops on an AF_UNIX socket. */

extern int pq_getkeepalivesidle(Port* port);
extern int pq_getkeepalivesinterval(Port* port);
extern int pq_getkeepalivescount(Port* port);
extern int pq_gettcpusertimeout(Port *port);

extern int pq_setkeepalivesidle(int idle, Port* port);
extern int pq_setkeepalivesinterval(int interval, Port* port);
extern int pq_setkeepalivescount(int count, Port* port);
extern int pq_settcpusertimeout(int timeout, Port *port);

extern CAC_state canAcceptConnections(bool isSession);

#endif /* LIBPQ_BE_H */
