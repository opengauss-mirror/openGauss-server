/* ---------------------------------------------------------------------------------------
 * 
 * pqcomm.h
 *        Definitions common to frontends and backends.
 *
 * NOTE: for historical reasons, this does not correspond to pqcomm.c.
 * pqcomm.c's routines are declared in libpq.h.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/pqcomm.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CM_PQCOMM_H
#define CM_PQCOMM_H

#include <sys/socket.h>
#include <netdb.h>
#ifdef HAVE_SYS_UN_H
#include <sys/un.h>
#endif
#include <netinet/in.h>

typedef struct {
    struct sockaddr_storage addr;
    size_t salen;
} CMSockAddr;

/*
 * This is the default directory in which AF_UNIX socket files are
 * placed.	Caution: changing this risks breaking your existing client
 * applications, which are likely to continue to look in the old
 * directory.  But if you just hate the idea of sockets in /tmp,
 * here's where to twiddle it.  You can also override this at runtime
 * with the postmaster's -k switch.
 */
#define DEFAULT_PGSOCKET_DIR "/tmp"

/*
 * In protocol 3.0 and later, the startup packet length is not fixed, but
 * we set an arbitrary limit on it anyway.	This is just to prevent simple
 * denial-of-service attacks via sending enough data to run the server
 * out of memory.
 */
#define MAX_STARTUP_PACKET_LENGTH 10000

/* These are the authentication request codes sent by gtm server. */
#define CM_AUTH_REQ_OK 0       /* User is authenticated */
#define CM_AUTH_REQ_GSS 1      /* GSSAPI without wrap() */
#define CM_AUTH_REQ_GSS_CONT 2 /* Continue GSS exchange */

#endif /* PQCOMM_H */
