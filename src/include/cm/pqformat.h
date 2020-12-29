/* ---------------------------------------------------------------------------------------
 * 
 * pqformat.h
 *        Definitions for formatting and parsing frontend/backend messages
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * IDENTIFICATION
 *        src/include/cm/pqformat.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PQFORMAT_H
#define PQFORMAT_H

#include "cm/stringinfo.h"
#include "cm/cm_msg.h"

extern const char* pq_get_msg_type(CM_StringInfo msg, int datalen);
extern const char* pq_getmsgbytes(CM_StringInfo msg, int datalen);
extern const char* pq_getmsgbytes(CM_Result* msg, int datalen);
#endif
