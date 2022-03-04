/* -------------------------------------------------------------------------
 *
 * auth.h
 *	  Definitions for network authentication routines
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/auth.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef AUTH_H
#define AUTH_H

#include "libpq/libpq-be.h"
#ifdef ENABLE_GSS
#include "gssapi/gssapi.h"
#include "gssapi/gssapi_krb5.h"
#endif
#define INITIAL_USER_ID 10
#define POSTFIX_LENGTH 8

/* The struct for gss kerberos authentication. */
typedef struct GssConn {
    int sock;
#ifdef ENABLE_GSS
    gss_ctx_id_t gctx;       /* GSS context */
    gss_name_t gtarg_nam;    /* GSS target name */
    gss_buffer_desc ginbuf;  /* GSS input token */
    gss_buffer_desc goutbuf; /* GSS output token */
#endif    
} GssConn;

extern char* pg_krb_server_hostname;
extern char* pg_krb_realm;

extern void ClientAuthentication(Port* port);

/* Main function for gss kerberos client/server authentication. */
extern int GssServerAuth(int socket, const char* krb_keyfile);
extern int GssClientAuth(int socket, char* server_host);

/* Hook for plugins to get control in ClientAuthentication() */
typedef void (*ClientAuthentication_hook_type)(Port*, int);
extern THR_LOCAL PGDLLIMPORT ClientAuthentication_hook_type ClientAuthentication_hook;
extern bool IsDSorHaWalSender();

#endif /* AUTH_H */
