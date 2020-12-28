/* -------------------------------------------------------------------------
 *
 * fe-auth.h
 *
 *	  Definitions for network authentication routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/common/interfaces/libpq/fe-auth.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef FE_AUTH_H
#define FE_AUTH_H

#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"

extern int pg_fe_sendauth(AuthRequest areq, PGconn* conn);
extern char* pg_fe_getauthname(PQExpBuffer errorMessage);
extern const char* check_client_env(const char* input_env_value);

#endif /* FE_AUTH_H */
