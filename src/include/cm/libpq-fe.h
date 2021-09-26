/* ---------------------------------------------------------------------------------------
 * 
 * libpq-fe.h
 *	  This file contains definitions for structures and
 *	  externs for functions used by frontend openGauss applications.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/libpq-fe.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CM_LIBPQ_FE_H
#define CM_LIBPQ_FE_H

#include <stdio.h>
#include "cm_c.h"

#ifdef __cplusplus
extern "C" {
#endif

#define libpq_gettext(x) x

/*
 * Option flags for PQcopyResult
 */
#define PG_COPYRES_ATTRS 0x01
#define PG_COPYRES_TUPLES 0x02 /* Implies PG_COPYRES_ATTRS */
#define PG_COPYRES_EVENTS 0x04
#define PG_COPYRES_NOTICEHOOKS 0x08

/* Application-visible enum types */

/*
 * Although it is okay to add to these lists, values which become unused
 * should never be removed, nor should constants be redefined - that would
 * break compatibility with existing code.
 */

typedef enum {
    CONNECTION_OK,
    CONNECTION_BAD,
    /* Non-blocking mode only below here */

    /*
     * The existence of these should never be relied upon - they should only
     * be used for user feedback or similar purposes.
     */
    CONNECTION_STARTED,           /* Waiting for connection to be made.  */
    CONNECTION_MADE,              /* Connection OK; waiting to send.	   */
    CONNECTION_AWAITING_RESPONSE, /* Waiting for a response from the
                                   * postmaster.		  */
    CONNECTION_AUTH_OK,           /* Received authentication; waiting for
                                   * backend startup. */
    CONNECTION_SETENV,            /* Negotiating environment. */
    CONNECTION_SSL_STARTUP,       /* Negotiating SSL. */
    CONNECTION_NEEDED             /* Internal state: connect() needed */
} CMConnStatusType;

typedef enum {
    PGRES_POLLING_FAILED = 0,
    PGRES_POLLING_READING, /* These two indicate that one may	  */
    PGRES_POLLING_WRITING, /* use select before polling again.   */
    PGRES_POLLING_OK,
    PGRES_POLLING_ACTIVE /* unused; keep for awhile for backwards
                          * compatibility */
} CMPostgresPollingStatusType;

/* ----------------
 * Structure for the conninfo parameter definitions returned by PQconndefaults
 * or CMPQconninfoParse.
 *
 * All fields except "val" point at static strings which must not be altered.
 * "val" is either NULL or a malloc'd current-value string.  CMPQconninfoFree()
 * will release both the val strings and the CMPQconninfoOption array itself.
 * ----------------
 */
typedef struct _CMPQconninfoOption {
    char* keyword; /* The keyword of the option			*/
    char* val;     /* Option's current value, or NULL		 */
} CMPQconninfoOption;

typedef struct cm_conn CM_Conn;

/* ----------------
 * Exported functions of libpq
 * ----------------
 */

/* ===	in fe-connect.c === */

/* make a new client connection to the backend */
/* Asynchronous (non-blocking) */
extern CM_Conn* PQconnectCMStart(const char* conninfo);
extern CMPostgresPollingStatusType CMPQconnectPoll(CM_Conn* conn);

/* Synchronous (blocking) */
extern CM_Conn* PQconnectCM(const char* conninfo);

/* close the current connection and free the CM_Conn data structure */
extern void CMPQfinish(CM_Conn* conn);

/* free the data structure returned by PQconndefaults() or CMPQconninfoParse() */
extern void CMPQconninfoFree(CMPQconninfoOption* connOptions);

extern CMConnStatusType CMPQstatus(const CM_Conn* conn);
extern char* CMPQerrorMessage(const CM_Conn* conn);

/* Force the write buffer to be written (or at least try) */
extern int CMPQflush(CM_Conn* conn);

extern int CMPQPacketSend(CM_Conn* conn, char packet_type, const void* buf, size_t buf_len);
extern char* gs_getenv_with_check(const char* envKey);

#ifdef __cplusplus
}
#endif

#endif /* CM_LIBPQ_FE_H */
