/* -------------------------------------------------------------------------
 *
 * streamutil.c - utility functions for pg_basebackup and pg_receivelog
 *
 * Author: Magnus Hagander <magnus@hagander.net>
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/streamutil.c
 * -------------------------------------------------------------------------
 */

/*
 * We have to use postgres.h not postgres_fe.h here, because there's so much
 * backend-only stuff in the XLOG include files we need.  But we need a
 * frontend-ish environment otherwise.	Hence this ugly hack.
 */
#define FRONTEND 1
#include "postgres.h"
#include "knl/knl_variable.h"
#include "streamutil.h"

#include <stdio.h>
#include <string.h>

#include "bin/elog.h"
#include "logging.h"

PGconn* streamConn = NULL;
char* replication_slot = NULL;
/*
 * strdup() and malloc() replacements that prints an error and exits
 * if something goes wrong. Can never return NULL.
 */
char* xstrdup(const char* s)
{
    char* result = NULL;

    result = strdup(s);
    if (result == NULL) {
        pg_log(PG_PRINT, _("%s: out of memory\n"), progname);
        exit(1);
    }
    return result;
}

void* xmalloc0(int size)
{
    void* result = NULL;

    /* Avoid unportable behavior of malloc(0) */
    if (size == 0) {
        pg_log(PG_PRINT, _("%s: malloc 0\n"), progname);
        exit(1);
    }
    result = malloc(size);
    if (result == NULL) {
        pg_log(PG_PRINT, _("%s: out of memory\n"), progname);
        exit(1);
    }
    errno_t errorno = memset_s(result, (size_t)size, 0, (size_t)size);
    securec_check_c(errorno, "", "");
    return result;
}
