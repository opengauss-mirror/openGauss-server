/* ---------------------------------------------------------------------------------------
 * 
 * ip.h
 *   Definitions for IPv6-aware network access.
 *
 * These definitions are used by both frontend and backend code.  Be careful
 * what you include here!
 *
 * Copyright (c) 2003-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/ip.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef IP_H
#define IP_H

#include "cm/pqcomm.h"

extern int cmpg_getaddrinfo_all(
    const char* hostname, const char* servname, const struct addrinfo* hintp, struct addrinfo** result);
extern void cmpg_freeaddrinfo_all(struct addrinfo* ai);

#ifdef HAVE_UNIX_SOCKETS
#define IS_AF_UNIX(fam) ((fam) == AF_UNIX)
#else
#define IS_AF_UNIX(fam) (0)
#endif

#endif /* IP_H */
