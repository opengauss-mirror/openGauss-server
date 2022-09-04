/*-------------------------------------------------------------------------
 *
 * compat.cpp
 *		Reimplementations of various backend functions.
 *
 * Portions Copyright (c) 2013-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/pg_xlogdump/compat.cpp
 *
 * This file contains client-side implementations for various backend
 * functions that the rm_desc functions in *desc.c files rely on.
 *
 *-------------------------------------------------------------------------
 */

/* ugly hack, same as in e.g pg_controldata */
#define FRONTEND 1
#include "postgres.h"
#include "knl/knl_variable.h"

#include <time.h>

#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "lib/stringinfo.h"
#include "storage/backendid.h"
#include "storage/custorage.h"
#include "utils/datetime.h"

/* We didn't know the exact pgxc nodename, use a const string instead. */
const char PGXCNodeName[] = "PGXC_NODENAME";

/*
 * Lookup table of fork name by fork number.
 *
 * If you add a new entry, remember to update the errhint in
 * forkname_to_number() below, and update the SGML documentation for
 * pg_relation_size().
 */
const char* forkNames[] = {
    "main", /* MAIN_FORKNUM */
    "fsm",  /* FSM_FORKNUM */
    "vm",   /* VISIBILITYMAP_FORKNUM */
    "bcm",  /* BCM_FORKNUM */
    "init"  /* INIT_FORKNUM */
};

/* copied from timestamp.c */
pg_time_t timestamptz_to_time_t(TimestampTz t)
{
    pg_time_t result;

#ifdef HAVE_INT64_TIMESTAMP
    result = (pg_time_t)(t / USECS_PER_SEC + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#else
    result = (pg_time_t)(t + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#endif
    return result;
}

/*
 * Stopgap implementation of timestamptz_to_str that doesn't depend on backend
 * infrastructure.  This will work for timestamps that are within the range
 * of the platform time_t type.  (pg_time_t is compatible except for possibly
 * being wider.)
 *
 * XXX the return value points to a static buffer, so beware of using more
 * than one result value concurrently.
 *
 * XXX: The backend timestamp infrastructure should instead be split out and
 * moved into src/common.  That's a large project though.
 */
const char* timestamptz_to_str(TimestampTz dt)
{
    static char buf[2 * MAXDATELEN + 14];
    char ts[MAXDATELEN + 1];
    char zone[MAXDATELEN + 1];
    time_t result = (time_t)timestamptz_to_time_t(dt);
    struct tm* ltime = localtime(&result);
    errno_t rc = EOK;

    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", ltime);
    strftime(zone, sizeof(zone), "%Z", ltime);

#ifdef HAVE_INT64_TIMESTAMP
    rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%s.%06d %s", ts, (int)(dt % USECS_PER_SEC), zone);
    securec_check_ss_c(rc, "\0", "\0");
#else
    rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%s.%.6f %s", ts, fabs(dt - floor(dt)), zone);
    securec_check_ss_c(rc, "\0", "\0");
#endif

    return buf;
}

/*
 * Provide a hacked up compat layer for StringInfos so xlog desc functions can
 * be linked/called.
 */
void appendStringInfo(StringInfo str, const char* fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
}

void appendStringInfoString(StringInfo str, const char* string)
{
    appendStringInfo(str, "%s", string);
}

void appendStringInfoChar(StringInfo str, char ch)
{
    appendStringInfo(str, "%c", ch);
}

/*
 * forkname_to_number - look up fork number by name
 *
 * In backend, we throw an error for no match; in frontend, we just
 * return InvalidForkNumber.
 */
ForkNumber forkname_to_number(const char* forkName)
{
    ForkNumber forkNum;

    for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
        if (strcmp(forkNames[forkNum], forkName) == 0)
            return forkNum;

#ifndef FRONTEND
    ereport(ERROR,
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid fork name"),
            errhint("Valid fork names are \"main\", \"fsm\", "
                    "\"vm\", and \"init\".")));
#endif

    return InvalidForkNumber;
}

/*
 * relpathbackend - construct path to a relation's file
 */
char *relpathbackend(RelFileNode rnode, BackendId backend, ForkNumber forknum)
{
    int pathlen;
    char *path = NULL;
    errno_t rc = EOK;

    /* Column store file path, e.g: 16384_C1.0, 16384_C1_bcm */
    if (forknum > MAX_FORKNUM) {
        char attr_name[32];
        int attid = forknum - MAX_FORKNUM;

        path = (char *)calloc(MAXPGPATH, sizeof(char));
        rc = snprintf_s(attr_name, sizeof(attr_name), sizeof(attr_name) - 1, "C%d", attid);
        securec_check_ss_c(rc, "\0", "\0");

        if (rnode.spcNode == GLOBALTABLESPACE_OID) {
            /* Shared system relations live in {datadir}/global */
            Assert(rnode.dbNode == 0);
            pathlen = strlen("global") + 1 + OIDCHARS + 1 + strlen(attr_name) + 1;
            rc = snprintf_s(path, pathlen, pathlen - 1, "global/%u_%s", rnode.relNode, attr_name);
        } else if (rnode.spcNode == DEFAULTTABLESPACE_OID) {
            /* The default tablespace is {datadir}/base */
            pathlen = strlen("base") + 1 + OIDCHARS + 1 + OIDCHARS + 1 + strlen(attr_name) + 1;
            rc = snprintf_s(path, pathlen, pathlen - 1, "base/%u/%u_%s", rnode.dbNode, rnode.relNode, attr_name);
        } else {
            /* All other tablespaces are accessed via symlinks */
            pathlen = 9 + 1 + OIDCHARS + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1 + strlen(PGXCNodeName) + 1 +
                OIDCHARS + 1 + OIDCHARS + 1 + strlen(attr_name) + 1;
            rc = snprintf_s(path, pathlen, pathlen - 1, "pg_tblspc/%u/%s_%s/%u/%u_%s", rnode.spcNode,
                TABLESPACE_VERSION_DIRECTORY, PGXCNodeName, rnode.dbNode, rnode.relNode, attr_name);
        }
    } else {
        if (rnode.spcNode == GLOBALTABLESPACE_OID) {
            /* Shared system relations live in {datadir}/global */
            Assert(rnode.dbNode == 0);
            Assert(backend == InvalidBackendId);
            pathlen = 7 + OIDCHARS + 1 + FORKNAMECHARS + 1;
            path = (char *)malloc(pathlen);
            if (forknum != MAIN_FORKNUM)
                rc = snprintf_s(path, pathlen, pathlen - 1, "global/%u_%s", rnode.relNode, forkNames[forknum]);
            else
                rc = snprintf_s(path, pathlen, pathlen - 1, "global/%u", rnode.relNode);
        } else if (rnode.spcNode == DEFAULTTABLESPACE_OID) {
            /* The default tablespace is {datadir}/base */
            if (backend == InvalidBackendId) {
                pathlen = 5 + OIDCHARS + 1 + OIDCHARS + 1 + FORKNAMECHARS + 1;
                path = (char *)malloc(pathlen);
                if (forknum != MAIN_FORKNUM)
                    rc = snprintf_s(path, pathlen, pathlen - 1, "base/%u/%u_%s", rnode.dbNode, rnode.relNode,
                        forkNames[forknum]);
                else
                    rc = snprintf_s(path, pathlen, pathlen - 1, "base/%u/%u", rnode.dbNode, rnode.relNode);
            } else {
                /* OIDCHARS will suffice for an integer, too */
                pathlen = 5 + OIDCHARS + 2 + OIDCHARS + 1 + OIDCHARS + 1 + FORKNAMECHARS + 1;
                path = (char *)malloc(pathlen);
                if (forknum != MAIN_FORKNUM)
                    rc = snprintf_s(path, pathlen, pathlen - 1, "base/%u/t%d_%u_%s", rnode.dbNode, backend,
                        rnode.relNode, forkNames[forknum]);
                else
                    rc = snprintf_s(path, pathlen, pathlen - 1, "base/%u/t%d_%u", rnode.dbNode, backend,
                        rnode.relNode);
            }
        } else {
            /* All other tablespaces are accessed via symlinks */
            if (backend == InvalidBackendId) {
                pathlen = 9 + 1 + OIDCHARS + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1 + OIDCHARS +
                    1
#ifdef PGXC
                    /* Postgres-XC tablespaces include node name */
                    + strlen(PGXCNodeName) + 1
#endif
                    + OIDCHARS + 1 + FORKNAMECHARS + 1;
                path = (char *)malloc(pathlen);
#ifdef PGXC
                if (forknum != MAIN_FORKNUM)
                    rc = snprintf_s(path, pathlen, pathlen - 1, "pg_tblspc/%u/%s_%s/%u/%u_%s", rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY, PGXCNodeName, rnode.dbNode, rnode.relNode, forkNames[forknum]);
                else
                    rc = snprintf_s(path, pathlen, pathlen - 1, "pg_tblspc/%u/%s_%s/%u/%u", rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY, PGXCNodeName, rnode.dbNode, rnode.relNode);
#else
                if (forknum != MAIN_FORKNUM)
                    rc = snprintf_s(path, pathlen, pathlen - 1, "pg_tblspc/%u/%s/%u/%u_%s", rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY, rnode.dbNode, rnode.relNode, forkNames[forknum]);
                else
                    rc = snprintf_s(path, pathlen, pathlen - 1, "pg_tblspc/%u/%s/%u/%u", rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY, rnode.dbNode, rnode.relNode);
#endif
            } else {
                /* OIDCHARS will suffice for an integer, too */
                pathlen = 9 + 1 + OIDCHARS + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1 + OIDCHARS + 2
#ifdef PGXC
                    + strlen(PGXCNodeName) + 1
#endif
                    + OIDCHARS + 1 + OIDCHARS + 1 + FORKNAMECHARS + 1;
                path = (char *)malloc(pathlen);
#ifdef PGXC
                if (forknum != MAIN_FORKNUM)
                    rc = snprintf_s(path, pathlen, pathlen - 1, "pg_tblspc/%u/%s_%s/%u/t%d_%u_%s", rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY, PGXCNodeName, rnode.dbNode, backend, rnode.relNode,
                        forkNames[forknum]);
                else
                    rc = snprintf_s(path, pathlen, pathlen - 1, "pg_tblspc/%u/%s_%s/%u/t%d_%u", rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY, PGXCNodeName, rnode.dbNode, backend, rnode.relNode);
#else
                if (forknum != MAIN_FORKNUM)
                    rc = snprintf_s(path, pathlen, pathlen - 1, "pg_tblspc/%u/%s/%u/t%d_%u_%s", rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY, rnode.dbNode, backend, rnode.relNode, forkNames[forknum]);
                else
                    rc = snprintf_s(path, pathlen, pathlen - 1, "pg_tblspc/%u/%s/%u/t%d_%u", rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY, rnode.dbNode, backend, rnode.relNode);
#endif
            }
        }
    }
    securec_check_ss_c(rc, "\0", "\0");
    return path;
}
