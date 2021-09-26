/* -----------------------------------------------------------------------
 *
 * openGauss locale utilities
 *
 * Portions Copyright (c) 2002-2012, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/pg_lsn.c
 *
 * -----------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/hash.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

#define MAXPG_LSNCOMPONENT 8

/*----------------------------------------------------------
 * Formatting and conversion routines.
 *---------------------------------------------------------*/
Datum pg_lsn_in(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);
    Size len1, len2;
    uint32 id, off;
    XLogRecPtr result;

    /* Sanity check input format. */
    len1 = strspn(str, "0123456789abcdefABCDEF");
    if (len1 < 1 || len1 > MAXPG_LSNCOMPONENT || str[len1] != '/')
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type %s: \"%s\"", "pg_lsn", str)));
    len2 = strspn(str + len1 + 1, "0123456789abcdefABCDEF");
    if (len2 < 1 || len2 > MAXPG_LSNCOMPONENT || str[len1 + 1 + len2] != '\0')
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type %s: \"%s\"", "pg_lsn", str)));

    /* Decode result. */
    id = (uint32)strtoul(str, NULL, 16);
    off = (uint32)strtoul(str + len1 + 1, NULL, 16);
    result = ((uint64)id << 32) | off;

    PG_RETURN_LSN(result);
}

