/* -------------------------------------------------------------------------
 *
 * ip.c
 *	  IPv6-aware network access.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/libpq/ip.c,v 1.43 2009/01/01 17:23:42 momjian Exp $
 *
 * This file and the IPV6 implementation were initially provided by
 * Nigel Kukard <nkukard@lbsd.net>, Linux Based Systems Design
 * http://www.lbsd.net.
 *
 * -------------------------------------------------------------------------
 */

/* This is intended to be used in both frontend and backend, so use c.h */

#include <unistd.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif

#include "cm/cm_ip.h"

/*
 * cm_getaddrinfo_all - get address info for Unix, IPv4 and IPv6 sockets
 */
int cm_getaddrinfo_all(
    const char* hostname, const char* servname, const struct addrinfo* hintp, struct addrinfo** result)
{
    int rc;

    /* not all versions of getaddrinfo() zero *result on failure */
    *result = NULL;

    /* NULL has special meaning to getaddrinfo(). */
    rc = getaddrinfo(((hostname == NULL) || hostname[0] == '\0') ? NULL : hostname, servname, hintp, result);

    return rc;
}

/*
 * cm_freeaddrinfo_all - free addrinfo structures for IPv4, IPv6, or Unix
 *
 * Note: the ai_family field of the original hint structure must be passed
 * so that we can tell whether the addrinfo struct was built by the system's
 * getaddrinfo() routine or our own getaddrinfo_unix() routine.  Some versions
 * of getaddrinfo() might be willing to return AF_UNIX addresses, so it's
 * not safe to look at ai_family in the addrinfo itself.
 */
void cm_freeaddrinfo_all(int hint_ai_family, struct addrinfo* ai)
{
    {
        /* struct was built by getaddrinfo() */
        if (ai != NULL)
            freeaddrinfo(ai);
    }
}
