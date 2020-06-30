/* ---------------------------------------------------------------------------------------
 * 
 * libpq-fe.h
 *	  This file contains definitions for structures and
 *	  externs for functions used by frontend postgres applications.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/heartbeat/libpq/libpq-fe.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CM_LIBPQ_FE_H
#define CM_LIBPQ_FE_H

#include "replication/heartbeat/libpq/libpq-be.h"

namespace PureLibpq {
/* Synchronous (blocking) */
extern Port* PQconnect(const char* conninfo);
}  /* namespace PureLibpq */

#endif /* CM_LIBPQ_FE_H */

