/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * dblink_query.cpp
 *
 * Functions returning results from a remote database
 * 
 * IDENTIFICATION
 *	  src/gausskernel/cbb/instruments/wdr/dblink_query.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "securec.h"
#include "funcapi.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "access/heapam.h"

#include "libpq/libpq-int.h"
#include "libpq/libpq-fe.h"
#include "instruments/dblink_query.h"

typedef struct storeInfo {
    FunctionCallInfo fcinfo;
    Tuplestorestate* tuplestore;
    AttInMetadata* attinmeta;
    MemoryContext tmpcontext;
    char** cstrs;
    /* temp storage for results to avoid leaks on exception */
    PGresult* last_res;
    PGresult* cur_res;
} storeInfo;

/*
 * Internal declarations
 */
namespace DBlink {
void prepTuplestoreResult(FunctionCallInfo fcinfo);
void procResultSuccess(ReturnSetInfo* rsinfo, PGresult* res);
void materializeQueryResult(FunctionCallInfo fcinfo, PGconn* conn, const char* sql);
PGresult* storeQueryResult(storeInfo* sinfo, PGconn* conn, const char* sql);
TupleDesc dealTupDesc(storeInfo* sinfo, int nfields);
void storeRow(storeInfo* sinfo, PGresult* res, bool first);
void dblinkResError(PGconn* conn, PGresult* res, const char* context_msg);
int applyRemoteGucs(PGconn* conn);
void restoreLocalGucs(int nestlevel);
char** checkOptLines(void);
void getDblinkConnect(const char* xdbname);
void remoteDatabaseConnect(const char* xdbname);
char* writeBuffer(int fd, struct stat* statbuf, int* nlines, int* length);
char** readFile(const char* path);
void freeFile(char** lines);
}  // namespace DBlink

const int MAX_INFO_LEN = 1024;

#define xpstrdup(var_c, var_)      \
    do {                           \
        if ((var_) != NULL) {      \
            var_c = pstrdup(var_); \
        } else {                   \
            var_c = NULL;          \
        }                          \
    } while (0)

char* DBlink::writeBuffer(int fd, struct stat* statbuf, int* nlines, int* length)
{
    int i;
    ssize_t len;
    int lines;
    char* buffer = NULL;

    buffer = (char*)palloc0_noexcept(statbuf->st_size + 1);
    if (buffer == NULL) {
        (void)close(fd);
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory of current node.")));
    }
    len = read(fd, buffer, statbuf->st_size + 1);
    (void)close(fd);
    if (len != statbuf->st_size) {
        /* oops, the file size changed between fstat and read */
        pfree(buffer);
        return NULL;
    }

    /*
     * Count newlines. We expect there to be a newline after each full line,
     * including one at the end of file. If there isn't a newline at the end,
     * any characters after the last newline will be ignored.
     */
    lines = 0;
    for (i = 0; i < len; i++) {
        if (buffer[i] == '\n') {
            lines++;
        }
    }
    *nlines = lines;
    *length = len;
    return buffer;
}

char** DBlink::readFile(const char* path)
{
    int len;
    int nlines;
    char** result = NULL;
    char* buffer = NULL;
    char* linebegin = NULL;
    struct stat statbuf;
    char relPath[PATH_MAX + 1] = {0};

    /*
     * Slurp the file into memory.
     *
     * The file can change concurrently, so we read the whole file into memory
     * with a single read() call. That's not guaranteed to get an atomic
     * snapshot, but in practice, for a small file, it's close enough for the
     * current use.
     */
    if (realpath(path, relPath) == NULL) {
        return NULL;
    }
    int fd = open(relPath, O_RDONLY | PG_BINARY, 0);
    if (fd < 0) {
        return NULL;
    }
    if (fstat(fd, &statbuf) < 0) {
        (void)close(fd);
        return NULL;
    }
    if (statbuf.st_size == 0) {
        /* empty file */
        (void)close(fd);
        result = (char**)palloc(sizeof(char*));
        *result = NULL;
        return result;
    }
    buffer = DBlink::writeBuffer(fd, &statbuf, &nlines, &len);
    if (buffer == NULL) {
        return NULL;
    }

    /* set up the result buffer */
    result = (char**)palloc(((unsigned int)nlines + 1) * sizeof(char*));

    /* now split the buffer into lines */
    linebegin = buffer;
    int n = 0;
    for (int i = 0; i < len; i++) {
        if (buffer[i] == '\n') {
            size_t slen = &buffer[i] - linebegin + 1;
            char* linebuf = (char*)palloc(slen + 1);
            errno_t rc = memcpy_s(linebuf, slen + 1, linebegin, slen);
            securec_check_c(rc, linebuf, "\0");
            linebuf[slen] = '\0';
            result[n++] = linebuf;
            linebegin = &buffer[i + 1];
        }
    }
    result[n] = NULL;

    pfree(buffer);

    return result;
}

void DBlink::freeFile(char** lines)
{
    char** line = NULL;
    if (lines == NULL) {
        return;
    }
    line = lines;
    while (*line != NULL) {
        pfree(*line);
        line++;
    }
    pfree(lines);
}

char** DBlink::checkOptLines(void)
{
    char** optlines = NULL;
    char path_file[MAX_INFO_LEN];

    errno_t rc = memset_s(path_file, MAX_INFO_LEN, 0, MAX_INFO_LEN);
    securec_check(rc, "\0", "\0");
    int rc1 = snprintf_s(path_file, sizeof(path_file), MAX_INFO_LEN - 1, "%s/postmaster.pid", t_thrd.proc_cxt.DataDir);
    securec_check_ss(rc1, "\0", "\0");
    /* Try to read the postmaster.pid file */
    if ((optlines = DBlink::readFile(path_file)) == NULL ||
        optlines[0] == NULL || optlines[1] == NULL || optlines[2] == NULL ||
        optlines[3] == NULL ||  /* File is exactly three lines, must be pre-9.1 */
        optlines[4] == NULL || optlines[5] == NULL) {
        DBlink::freeFile(optlines);
        optlines = NULL;
        return NULL;
    }

    return optlines;
}

void DBlink::getDblinkConnect(const char* xdbname)
{
    long pmpid;
    char** optlines = NULL;
    char* endStr = NULL;
    int base = 10;

    optlines = DBlink::checkOptLines();
    if (optlines == NULL) {
        return;
    }

    /* File is complete enough for us, parse it */
    pmpid = strtol(optlines[LOCK_FILE_LINE_PID - 1], &endStr, base);
    if (pmpid > 0) {
        /*
         * OK, seems to be a valid pidfile from our child.
         */
        int rc;
        int portnum;
        char* cptr = NULL;
        char* sockdir = NULL;
        char* hostaddr = NULL;
        char host_str[MAX_INFO_LEN] = {0};
        char local_conninfo[MAX_INFO_LEN] = {0};

        /*
         * Extract port number and host string to use. Prefer
         * using Unix socket if available.
         */
        portnum = (int)strtol(optlines[LOCK_FILE_LINE_PORT - 1], &endStr, base);
        sockdir = optlines[LOCK_FILE_LINE_SOCKET_DIR - 1];
        hostaddr = optlines[LOCK_FILE_LINE_LISTEN_ADDR - 1];

        if (hostaddr != NULL && hostaddr[0] != '\0' && hostaddr[0] != '\n') {
            rc = strncpy_s(host_str, sizeof(host_str), hostaddr, sizeof(host_str) - 1);
            securec_check(rc, "\0", "\0");
        } else if (sockdir[0] == '/') {
            rc = strncpy_s(host_str, sizeof(host_str), sockdir, sizeof(host_str) - 1);
            securec_check(rc, "\0", "\0");
        }

        /* remove trailing newline */
        cptr = strchr(host_str, '\n');
        if (cptr != NULL) {
            *cptr = '\0';
        }

        /* Fail if couldn't get either sockdir or host addr */
        if (host_str[0] == '\0') {
            DBlink::freeFile(optlines);
            optlines = NULL;
            return;
        }

        /* If postmaster is listening on "*", use localhost */
        if (strcmp(host_str, "*") == 0) {
            rc = strncpy_s(host_str, sizeof(host_str), "localhost", sizeof("localhost"));
            securec_check(rc, "\0", "\0");
        }

        rc = snprintf_s(local_conninfo,
            sizeof(local_conninfo),
            sizeof(local_conninfo) - 1,
            "%s port=%d host='%s' connect_timeout=10 rw_timeout=%d application_name=%s",
            xdbname,
            portnum,
            host_str,
            u_sess->attr.attr_common.wdr_snapshot_query_timeout,
            "WDRXdb");
        securec_check_ss(rc, "\0", "\0");

        CHECK_FOR_INTERRUPTS();
        t_thrd.perf_snap_cxt.connect = PQconnectdb(local_conninfo);
    }

    DBlink::freeFile(optlines);
    optlines = NULL;
}

void DBlink::remoteDatabaseConnect(const char* xdbname)
{
    char* msg = NULL;

    DBlink::getDblinkConnect(xdbname);
    if (PQstatus(t_thrd.perf_snap_cxt.connect) == CONNECTION_BAD) {
        msg = pstrdup(PQerrorMessage(t_thrd.perf_snap_cxt.connect));
        dblinkCloseConn();
        ereport(ERROR,
            (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
                errmsg("could not establish connection"),
                errdetail_internal("%s", msg)));
    }
    (void)PQsetClientEncoding(t_thrd.perf_snap_cxt.connect, GetDatabaseEncodingName());
}

Datum wdr_xdb_query(PG_FUNCTION_ARGS)
{
    const int NUM_ARGS = 2;
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Superuser privilege is need to operate wdr_xdb_query")));
    }
    DBlink::prepTuplestoreResult(fcinfo);

    PG_TRY();
    {
        char* sql = NULL;
        char* conname = NULL;

        if (PG_NARGS() == NUM_ARGS) {
            conname = text_to_cstring(PG_GETARG_TEXT_PP(0));
            DBlink::remoteDatabaseConnect(conname);
            sql = text_to_cstring(PG_GETARG_TEXT_PP(1));
        } else {
            /* shouldn't happen */
            ereport(ERROR, (errcode(ERRCODE_TOO_MANY_ARGUMENTS), errmsg("wrong number of arguments")));
        }

        if (t_thrd.perf_snap_cxt.connect == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST), errmsg("connection \"%s\" not available", conname)));
        }

        /* synchronous query, use efficient tuple collection method */
        DBlink::materializeQueryResult(fcinfo, t_thrd.perf_snap_cxt.connect, sql);
    }
    PG_CATCH();
    {
        /* if needed, close the connection to the database */
        dblinkCloseConn();
        PG_RE_THROW();
    }
    PG_END_TRY();

    /* if needed, close the connection to the database */
    dblinkCloseConn();

    return (Datum)0;
}

/*
 * in order to solve concurrency, the cancel_request vlaue is following:
 * set to 1 when close conn
 * set to 2 when cancel request
 */
void dblinkCloseConn(void)
{
    const int defaultVal = 0;
    const int closeVal = 1;

    if ((t_thrd.perf_snap_cxt.cancel_request == defaultVal) &&
        gs_compare_and_swap_32(&t_thrd.perf_snap_cxt.cancel_request, defaultVal, closeVal)) {
        if (t_thrd.perf_snap_cxt.connect != NULL) {
            PQfinish(t_thrd.perf_snap_cxt.connect);
            t_thrd.perf_snap_cxt.connect = NULL;
        }
        if (t_thrd.perf_snap_cxt.res != NULL) {
            PQclear(t_thrd.perf_snap_cxt.res);
            t_thrd.perf_snap_cxt.res = NULL;
        }
        gs_compare_and_swap_32(&t_thrd.perf_snap_cxt.cancel_request, closeVal, defaultVal);
    }
}

void dblinkRequestCancel(void)
{
    const int defaultVal = 0;
    const int cancelVal = 2;
    const int bufSize = 256;
    PGcancel* requestCancel = NULL;
    char errBuf[bufSize] = {0};

    if ((t_thrd.perf_snap_cxt.cancel_request == defaultVal) &&
        gs_compare_and_swap_32(&t_thrd.perf_snap_cxt.cancel_request, defaultVal, cancelVal)) {
        if ((requestCancel = PQgetCancel(t_thrd.perf_snap_cxt.connect)) != NULL) {
            (void)PQcancel(requestCancel, errBuf, sizeof(errBuf));
            free(requestCancel);
        }
        (void)gs_compare_and_swap_32(&t_thrd.perf_snap_cxt.cancel_request, cancelVal, defaultVal);
    }
}

/*
 * Verify function caller can handle a tuplestore result, and set up for that.
 *
 * Note: if the caller returns without actually creating a tuplestore, the
 * executor will treat the function result as an empty set.
 */
void DBlink::prepTuplestoreResult(FunctionCallInfo fcinfo)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;

    /* check to see if query supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot accept a set")));
    }
    if (!((uint32)rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not allowed in this context")));
    }

    /* let the executor know we're sending back a tuplestore */
    rsinfo->returnMode = SFRM_Materialize;

    /* caller must fill these to return a non-empty result */
    rsinfo->setResult = NULL;
    rsinfo->setDesc = NULL;
}

void DBlink::procResultSuccess(ReturnSetInfo* rsinfo, PGresult* res)
{
    /*
     * storeRow didn't get called, so we need to convert the command
     * status string to a tuple manually
     */
    TupleDesc tupdesc = NULL;
    AttInMetadata* attinmeta = NULL;
    Tuplestorestate* tupstore = NULL;
    HeapTuple tuple = NULL;
    char* values[1];
    MemoryContext oldcontext = NULL;

    /*
     * need a tuple descriptor representing one TEXT column to return
     * the command status string as our result tuple
     */
    tupdesc = CreateTemplateTupleDesc(1, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "status", TEXTOID, -1, 0);
    attinmeta = TupleDescGetAttInMetadata(tupdesc);

    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    (void)MemoryContextSwitchTo(oldcontext);

    values[0] = PQcmdStatus(res);

    /* build the tuple and put it into the tuplestore. */
    tuple = BuildTupleFromCStrings(attinmeta, values);
    tuplestore_puttuple(tupstore, tuple);
}
/*
 * Execute the given SQL command and store its results into a tuplestore
 * to be returned as the result of the current function.
 *
 * This is equivalent to PQexec followed by materializeResult, but we make
 * use of libpq's single-row mode to avoid accumulating the whole result
 * inside libpq before it gets transferred to the tuplestore.
 */
void DBlink::materializeQueryResult(FunctionCallInfo fcinfo, PGconn* conn, const char* sql)
{
    storeInfo sinfo;
    PGresult* volatile res = NULL;
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;

    /* prepTuplestoreResult must have been called previously */
    Assert(rsinfo->returnMode == SFRM_Materialize);

    /* initialize storeInfo to empty */
    errno_t rc = memset_s(&sinfo, sizeof(sinfo), 0, sizeof(sinfo));
    securec_check(rc, "\0", "\0");
    sinfo.fcinfo = fcinfo;

    PG_TRY();
    {
        /* execute query, collecting any tuples into the tuplestore */
        res = DBlink::storeQueryResult(&sinfo, conn, sql);
        if (res == NULL || (PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK)) {
            /*
             * dblinkResError will clear the passed PGresult, so we need
             * this ugly dance to avoid doing so twice during error exit
             */
            PGresult* res1 = res;
            res = NULL;
            DBlink::dblinkResError(conn, res1, "could not execute query");
            /* if fail isn't set, we'll return an empty query result */
        } else if (PQresultStatus(res) == PGRES_COMMAND_OK) {
            DBlink::procResultSuccess(rsinfo, res);
            PQclear(res);
            res = NULL;
        } else {
            Assert(PQresultStatus(res) == PGRES_TUPLES_OK);
            /* storeRow should have created a tuplestore */
            Assert(rsinfo->setResult != NULL);

            PQclear(res);
            res = NULL;
        }
        t_thrd.perf_snap_cxt.res = NULL;
        PQclear(sinfo.last_res);
        sinfo.last_res = NULL;
        PQclear(sinfo.cur_res);
        sinfo.cur_res = NULL;
    }
    PG_CATCH();
    {
        /* be sure to release any libpq result we collected */
        t_thrd.perf_snap_cxt.res = NULL;
        PQclear(res);
        PQclear(sinfo.last_res);
        PQclear(sinfo.cur_res);
        /* and clear out any pending data in libpq */
        while ((res = PQgetResult(conn)) != NULL)
            PQclear(res);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/*
 * Execute query, and send any result rows to sinfo->tuplestore.
 */
PGresult* DBlink::storeQueryResult(storeInfo* sinfo, PGconn* conn, const char* sql)
{
    bool first = true;
    int nestlevel = -1;
    PGresult* res = NULL;

    if (!PQsendQuery(conn, sql)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPTION), errmsg("could not send query: %s", PQerrorMessage(conn))));
    }

    if (!PQsetSingleRowMode(conn)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPTION), errmsg("failed to set single-row mode for dblink query")));
    }

    for (;;) {
        CHECK_FOR_INTERRUPTS();

        sinfo->cur_res = PQgetResult(conn);
        if (sinfo->cur_res == NULL) {
            break;
        }

        if (PQresultStatus(sinfo->cur_res) == PGRES_SINGLE_TUPLE) {
            /* got one row from possibly-bigger resultset */
            /*
             * Set GUCs to ensure we read GUC-sensitive data types correctly.
             * We shouldn't do this until we have a row in hand, to ensure
             * libpq has seen any earlier ParameterStatus protocol messages.
             */
            if (first && nestlevel < 0) {
                nestlevel = DBlink::applyRemoteGucs(conn);
            }

            DBlink::storeRow(sinfo, sinfo->cur_res, first);
            PQclear(sinfo->cur_res);
            sinfo->cur_res = NULL;
            first = false;
        } else {
            /* if empty resultset, fill tuplestore header */
            if (first && PQresultStatus(sinfo->cur_res) == PGRES_TUPLES_OK) {
                DBlink::storeRow(sinfo, sinfo->cur_res, first);
            }

            /* store completed result at last_res */
            PQclear(sinfo->last_res);
            sinfo->last_res = sinfo->cur_res;
            t_thrd.perf_snap_cxt.res = sinfo->cur_res;
            sinfo->cur_res = NULL;
            first = true;
        }
    }

    /* clean up GUC settings, if we changed any */
    DBlink::restoreLocalGucs(nestlevel);

    /* return last_res */
    res = sinfo->last_res;
    sinfo->last_res = NULL;
    return res;
}

TupleDesc DBlink::dealTupDesc(storeInfo* sinfo, int nfields)
{
    TupleDesc tupdesc = NULL;

    /*
     * It's possible to get more than one result set if the query string
     * contained multiple SQL commands.  In that case, we follow PQexec's
     * traditional behavior of throwing away all but the last result.
     */
    if (sinfo->tuplestore != NULL) {
        tuplestore_end(sinfo->tuplestore);
    }
    sinfo->tuplestore = NULL;

    /* get a tuple descriptor for our result type */
    switch (get_call_result_type(sinfo->fcinfo, NULL, &tupdesc)) {
        case TYPEFUNC_COMPOSITE:
            /* success */
            break;
        case TYPEFUNC_RECORD:
            /* failed to determine actual type of RECORD */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("function returning record called in context that cannot accept type record")));
            break;
        default:
            /* result type isn't composite */
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("return type must be a row type")));
            break;
    }

    /* make sure we have a persistent copy of the tupdesc */
    tupdesc = CreateTupleDescCopy(tupdesc);
    /* check result and tuple descriptor have the same number of columns */
    if (nfields != tupdesc->natts) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("remote query result rowtype does not match the specified FROM clause rowtype")));
    }

    return tupdesc;
}

/*
 * storeRow Send single row to sinfo->tuplestore.
 *
 * If "first" is true, create the tuplestore using PGresult's metadata
 * (in this case the PGresult might contain either zero or one row).
 */
void DBlink::storeRow(storeInfo* sinfo, PGresult* res, bool first)
{
    int i;
    HeapTuple tuple = NULL;
    MemoryContext oldcontext = NULL;
    int nfields = PQnfields(res);

    if (first) {
        /* Prepare for new result set */
        ReturnSetInfo* rsinfo = (ReturnSetInfo*)sinfo->fcinfo->resultinfo;
        TupleDesc tupdesc = DBlink::dealTupDesc(sinfo, nfields);
        /* Prepare attinmeta for later data conversions */
        if (tupdesc != NULL) {
            sinfo->attinmeta = TupleDescGetAttInMetadata(tupdesc);
        }

        /* Create a new, empty tuplestore */
        oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
        sinfo->tuplestore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
        rsinfo->setResult = sinfo->tuplestore;
        rsinfo->setDesc = tupdesc;
        (void)MemoryContextSwitchTo(oldcontext);

        /* Done if empty resultset */
        if (PQntuples(res) == 0) {
            return;
        }

        /*
         * Set up sufficiently-wide string pointers array; this won't change
         * in size so it's easy to preallocate.
         */
        if (sinfo->cstrs != NULL) {
            pfree(sinfo->cstrs);
        }
        sinfo->cstrs = (char**)palloc((unsigned int)nfields * sizeof(char*));

        /* Create short-lived memory context for data conversions */
        if (!sinfo->tmpcontext) {
            sinfo->tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
                "dblink temporary context",
                ALLOCSET_DEFAULT_MINSIZE,
                ALLOCSET_DEFAULT_INITSIZE,
                ALLOCSET_DEFAULT_MAXSIZE);
        }
    }

    /* Should have a single-row result if we get here */
    Assert(PQntuples(res) == 1);

    /*
     * Do the following work in a temp context that we reset after each tuple.
     * This cleans up not only the data we have direct access to, but any
     * cruft the I/O functions might leak.
     */
    oldcontext = MemoryContextSwitchTo(sinfo->tmpcontext);

    /*
     * Fill cstrs with null-terminated strings of column values.
     */
    for (i = 0; i < nfields; i++) {
        if (PQgetisnull(res, 0, i)) {
            sinfo->cstrs[i] = NULL;
        } else {
            sinfo->cstrs[i] = PQgetvalue(res, 0, i);
        }
    }

    /* Convert row to a tuple, and add it to the tuplestore */
    tuple = BuildTupleFromCStrings(sinfo->attinmeta, sinfo->cstrs);

    tuplestore_puttuple(sinfo->tuplestore, tuple);

    /* Clean up */
    (void)MemoryContextSwitchTo(oldcontext);
    MemoryContextReset(sinfo->tmpcontext);
}

void DBlink::dblinkResError(PGconn* conn, PGresult* res, const char* context_msg)
{
    int sqlstate;
    char* message_primary = NULL;
    char* message_detail = NULL;
    char* message_hint = NULL;
    char* message_context = NULL;
    char* pg_diag_sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
    char* pg_diag_message_primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
    char* pg_diag_message_detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
    char* pg_diag_message_hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
    char* pg_diag_context = PQresultErrorField(res, PG_DIAG_CONTEXT);

    if (pg_diag_sqlstate != NULL) {
        sqlstate = MAKE_SQLSTATE(
            pg_diag_sqlstate[0], pg_diag_sqlstate[1], pg_diag_sqlstate[2], pg_diag_sqlstate[3], pg_diag_sqlstate[4]);
    } else {
        sqlstate = ERRCODE_CONNECTION_FAILURE;
    }

    xpstrdup(message_primary, pg_diag_message_primary);
    xpstrdup(message_detail, pg_diag_message_detail);
    xpstrdup(message_hint, pg_diag_message_hint);
    xpstrdup(message_context, pg_diag_context);

    if (res != NULL) {
        PQclear(res);
    }

    /* we should send cancel request to cancel backend query before close conn
       to ensure only one active backend session during two snapshot gap. */
    dblinkRequestCancel();

    ereport(ERROR,
        (errcode(sqlstate),
            message_primary ? errmsg_internal("%s", message_primary)
                            : errmsg("WDR snapshot xdb query failed: %s", trim(conn->errorMessage.data)),
            message_detail ? errdetail_internal("%s", message_detail) : 0,
            message_hint ? errhint("%s", message_hint) : 0,
            message_context ? errcontext("%s", message_context) : 0,
            errcontext("Error occurred on xdb connection named \"dbname=%s\": %s.", conn->dbName, context_msg)));
}

/*
 * Copy the remote session's values of GUCs that affect datatype I/O
 * and apply them locally in a new GUC nesting level.  Returns the new
 * nestlevel (which is needed by restoreLocalGucs to undo the settings),
 * or -1 if no new nestlevel was needed.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls restoreLocalGucs.  If an error is
 * thrown in between, guc.c will take care of undoing the settings.
 */
int DBlink::applyRemoteGucs(PGconn* conn)
{
    static const char* const GUCsAffectingIO[] = {"DateStyle", "IntervalStyle"};

    int nestlevel = -1;
    unsigned int i;

    for (i = 0; i < lengthof(GUCsAffectingIO); i++) {
        const char* gucName = GUCsAffectingIO[i];
        const char* remoteVal = PQparameterStatus(conn, gucName);
        const char* localVal = NULL;

        /*
         * If the remote server is pre-8.4, it won't have IntervalStyle, but
         * that's okay because its output format won't be ambiguous.  So just
         * skip the GUC if we don't get a value for it.  (We might eventually
         * need more complicated logic with remote-version checks here.)
         */
        if (remoteVal == NULL) {
            continue;
        }

        /*
         * Avoid GUC-setting overhead if the remote and local GUCs already
         * have the same value.
         */
        localVal = GetConfigOption(gucName, false, false);
        Assert(localVal != NULL);

        if (strcmp(remoteVal, localVal) == 0) {
            continue;
        }

        /* Create new GUC nest level if we didn't already */
        if (nestlevel < 0) {
            nestlevel = NewGUCNestLevel();
        }

        /* Apply the option (this will throw error on failure) */
        (void)set_config_option(gucName, remoteVal, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0);
    }

    return nestlevel;
}

/*
 * Restore local GUCs after they have been overlaid with remote settings.
 */
void DBlink::restoreLocalGucs(int nestlevel)
{
    /* Do nothing if no new nestlevel was created */
    if (nestlevel > 0) {
        AtEOXact_GUC(true, nestlevel);
    }
}
