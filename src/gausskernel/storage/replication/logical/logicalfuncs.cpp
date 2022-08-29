/* -------------------------------------------------------------------------
 *
 * logicalfuncs.cpp
 *
 *	 Support functions for using logical decoding and management of
 *	 logical replication slots via SQL.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/gausskernel/storage/replication/logical/logicalfuncs.cpp
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"

#include "nodes/makefuncs.h"

#include "mb/pg_wchar.h"

#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/logicalfuncs.h"
#include "replication/walsender_private.h"
#include "access/xlog_internal.h"

#include "storage/smgr/fd.h"
#define MAXPG_LSNCOMPONENT 8

#define str_lsn_len 128
/* private date for writing out data */
typedef struct DecodingOutputState {
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    bool binary_output;
    int64 returned_rows;
} DecodingOutputState;
extern void shutdown_cb_wrapper(LogicalDecodingContext *ctx);


/*
 * Prepare for a output plugin write.
 */
static void LogicalOutputPrepareWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid, bool last_write)
{
    resetStringInfo(ctx->out);
}

/*
 * Perform output plugin write into tuplestore.
 */
static void LogicalOutputWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid, bool last_write)
{
    Datum values[3];
    bool nulls[3];
    DecodingOutputState *p = NULL;
    char *str_lsn_temp = NULL;
    int rc = 0;
    /* SQL Datums can only be of a limited length... */
    if ((uint32)ctx->out->len > MaxAllocSize - VARHDRSZ)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("too much output for sql interface")));

    p = (DecodingOutputState *)ctx->output_writer_private;

    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "", "");

    str_lsn_temp = (char *)palloc0(str_lsn_len);
    rc = sprintf_s(str_lsn_temp, str_lsn_len, "%X/%X", uint32(lsn >> 32), uint32(lsn));
    securec_check_ss(rc, "", "");
    values[0] = CStringGetTextDatum(str_lsn_temp);
    values[1] = TransactionIdGetDatum(xid);

    /*
     * Assert ctx->out is in database encoding when we're writing textual
     * output.
     */
    if (!p->binary_output) {
        Assert(pg_verify_mbstr(GetDatabaseEncoding(), ctx->out->data, ctx->out->len, false));
    }

    /* ick, but cstring_to_text_with_len works for bytea perfectly fine */
    values[2] = PointerGetDatum(cstring_to_text_with_len(ctx->out->data, (size_t)(uint)(ctx->out->len)));

    tuplestore_putvalues(p->tupstore, p->tupdesc, values, nulls);
    p->returned_rows++;
}

void check_permissions(bool for_backup)
{
    if (!superuser() && !has_rolreplication(GetUserId()) &&
        !is_member_of_role(GetUserId(), DEFAULT_ROLE_REPLICATION) &&
        !(for_backup && isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("must be system admin or replication role or a member of the gs_role_replication role "
                "to use replication slots"))));
    }
}

void CheckLogicalPremissions(Oid userId)
{
    if (g_instance.attr.attr_security.enablePrivilegesSeparate &&
        !superuser_arg(userId) && !has_rolreplication(userId)&&
        !is_member_of_role(userId, DEFAULT_ROLE_REPLICATION)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("must be replication role or a member of the gs_role_replication role "
                "to use replication slots when separation of privileges is used"))));
    } else if (!g_instance.attr.attr_security.enablePrivilegesSeparate &&
        !superuser_arg(userId) && !systemDBA_arg(userId) && !has_rolreplication(userId) &&
        !is_member_of_role(userId, DEFAULT_ROLE_REPLICATION)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("must be system admin or replication role or a member of the gs_role_replication role "
                "to use replication slots"))));
    }
}

/*
 * This is duplicate code with pg_xlogdump, similar to walsender.c, but
 * we currently don't have the infrastructure (elog!) to share it.
 */
static void XLogRead(char *buf, TimeLineID tli, XLogRecPtr startptr, Size count, char* xlog_path)
{
    char *p = NULL;
    XLogRecPtr recptr = InvalidXLogRecPtr;
    Size nbytes = 0;

    p = buf;
    recptr = startptr;
    nbytes = count;

    while (nbytes > 0) {
        uint32 startoff;
        int segbytes;
        int readbytes;

        startoff = uint32(recptr) % XLogSegSize;

        if (t_thrd.logical_cxt.sendFd < 0 || !(((recptr) / XLogSegSize) == t_thrd.logical_cxt.sendSegNo)) {
            char path[MAXPGPATH];
            errno_t rc = memset_s(path, MAXPGPATH, 0, sizeof(path));
            securec_check(rc, "", "");

            /* Switch to another logfile segment */
            if (t_thrd.logical_cxt.sendFd >= 0) {
                (void)close(t_thrd.logical_cxt.sendFd);
            }

            t_thrd.logical_cxt.sendSegNo = (recptr) / XLogSegSize;
            int ret = 0;
            if (xlog_path == NULL) {
                ret = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X", tli,
                                  (uint32)((t_thrd.logical_cxt.sendSegNo) / XLogSegmentsPerXLogId),
                                  (uint32)((t_thrd.logical_cxt.sendSegNo) % XLogSegmentsPerXLogId));
            } else {
                ret = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X%08X", xlog_path, tli,
                                    (uint32)((t_thrd.logical_cxt.sendSegNo) / XLogSegmentsPerXLogId),
                                    (uint32)((t_thrd.logical_cxt.sendSegNo) % XLogSegmentsPerXLogId));
            }
            securec_check_ss(ret, "", "");

            t_thrd.logical_cxt.sendFd = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);

            if (t_thrd.logical_cxt.sendFd < 0) {
                if (errno == ENOENT)
                    ereport(ERROR, (errcode_for_file_access(),
                                    errmsg("requested WAL segment %s has already been removed", path)));
                else
                    ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
            }
            t_thrd.logical_cxt.sendOff = 0;
        }

        /* Need to seek in the file? */
        if (t_thrd.logical_cxt.sendOff != startoff) {
            if (lseek(t_thrd.logical_cxt.sendFd, (off_t)startoff, SEEK_SET) < 0) {
                char path[MAXPGPATH] = "\0";

                int nRet = 0;
                if (xlog_path == NULL) {
                    nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X", tli,
                                    (uint32)((t_thrd.logical_cxt.sendSegNo) / XLogSegmentsPerXLogId),
                                    (uint32)((t_thrd.logical_cxt.sendSegNo) % XLogSegmentsPerXLogId));
                } else {
                    nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X%08X", xlog_path, tli,
                                    (uint32)((t_thrd.logical_cxt.sendSegNo) / XLogSegmentsPerXLogId),
                                    (uint32)((t_thrd.logical_cxt.sendSegNo) % XLogSegmentsPerXLogId));
                }
                securec_check_ss(nRet, "", "");

                ereport(ERROR, (errcode_for_file_access(),
                                errmsg("could not seek in log segment %s to offset %u: %m", path, startoff)));
            }
            t_thrd.logical_cxt.sendOff = startoff;
        }

        /* How many bytes are within this segment? */
        if (nbytes > (XLogSegSize - startoff))
            segbytes = XLogSegSize - startoff;
        else {
            segbytes = (int)(uint)nbytes;
        }

        readbytes = read(t_thrd.logical_cxt.sendFd, p, segbytes);
        if (readbytes <= 0) {
            int rc = 0;
            char path[MAXPGPATH] = "\0";
            if (xlog_path == NULL) {
                rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X", tli,
                                    (uint32)((t_thrd.logical_cxt.sendSegNo) / XLogSegmentsPerXLogId),
                                    (uint32)((t_thrd.logical_cxt.sendSegNo) % XLogSegmentsPerXLogId));
            } else {
                rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X%08X", xlog_path, tli,
                                    (uint32)((t_thrd.logical_cxt.sendSegNo) / XLogSegmentsPerXLogId),
                                    (uint32)((t_thrd.logical_cxt.sendSegNo) % XLogSegmentsPerXLogId));
            }
            securec_check_ss(rc, "", "");

            ereport(ERROR,
                    (errcode_for_file_access(), errmsg("could not read from log segment %s, offset %u, length %lu: %m",
                                                       path, t_thrd.logical_cxt.sendOff, INT2ULONG(segbytes))));
        }

        /* Update state for read */
        recptr += readbytes;

        t_thrd.logical_cxt.sendOff += readbytes;
        nbytes -= readbytes;
        p += readbytes;
    }
}

/*
 * read_page callback for logical decoding contexts.
 *
 * Public because it would likely be very helpful for someone writing another
 * output method outside walsender, e.g. in a bgworker.
 *
 * description: The walsender has it's own version of this, but it relies on the
 * walsender's latch being set whenever WAL is flushed. No such infrastructure
 * exists for normal backends, so we have to do a check/sleep/repeat style of
 * loop for now.
 */
int logical_read_local_xlog_page(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr,
                                 char *cur_page, TimeLineID *pageTLI, char* xlog_path)
{
    XLogRecPtr flushptr, loc;
    int count;

    loc = targetPagePtr + reqLen;

    while (1) {
        /*
         * description: we're going to have to do something more intelligent about
         * timelines on standbys. Use readTimeLineHistory() and
         * tliOfPointInHistory() to get the proper LSN? For now we'll catch
         * that case earlier, but the code and description is left in here for when
         * that changes.
         */
        if (!RecoveryInProgress()) {
            *pageTLI = t_thrd.xlog_cxt.ThisTimeLineID;
            flushptr = GetFlushRecPtr();
        } else
            flushptr = GetXLogReplayRecPtr(pageTLI);

        if (XLByteLE(loc, flushptr))
            break;

        CHECK_FOR_INTERRUPTS();
        pg_usleep(1000L);
    }

    /* more than one block available */
    if ((targetPagePtr) + XLOG_BLCKSZ <= (flushptr))
        count = XLOG_BLCKSZ;
    /* not enough data there */
    else if ((targetPagePtr) + reqLen > (flushptr))
        return -1;
    /* part of the page available */
    else
        count = (flushptr) - (targetPagePtr);

    XLogRead(cur_page, *pageTLI, targetPagePtr, XLOG_BLCKSZ, xlog_path);

    return count;
}

bool AssignLsn(XLogRecPtr *lsn_ptr, const char *input)
{
    int len1 = 0;
    int len2 = 0;
    bool res = false;
    uint32 xlogid = 0;
    uint32 xrecoff = 0;

    Assert(input != NULL && lsn_ptr != NULL);

    /* Sanity check input format. */
    len1 = strspn(input, "0123456789abcdefABCDEF");
    if (len1 < 1 || len1 > MAXPG_LSNCOMPONENT || input[len1] != '/') {
        goto exit;
    }
    len2 = strspn(input + len1 + 1, "0123456789abcdefABCDEF");
    if (len2 < 1 || len2 > MAXPG_LSNCOMPONENT || input[len1 + 1 + len2] != '\0') {
        goto exit;
    }
    /* Decode result. */
    xlogid = (uint32)strtoul(input, NULL, 16);
    xrecoff = (uint32)strtoul(input + len1 + 1, NULL, 16);
    *lsn_ptr = (((uint64)xlogid) << 32) + xrecoff;
    res = true;

exit:
    return res;
}

/*
 * Helper function for the various SQL callable logical decoding functions.
 */
static Datum pg_logical_slot_get_changes_guts(FunctionCallInfo fcinfo, bool confirm, bool binary)
{
    Name name = PG_GETARG_NAME(0);
    XLogRecPtr upto_lsn = InvalidXLogRecPtr;
    int64 upto_nchanges = 0;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    MemoryContext per_query_ctx = NULL;
    MemoryContext oldcontext = NULL;
    XLogRecPtr end_of_wal = InvalidXLogRecPtr;
    XLogRecPtr startptr = InvalidXLogRecPtr;
    LogicalDecodingContext *ctx = NULL;
    ResourceOwner old_resowner = t_thrd.utils_cxt.CurrentResourceOwner;
    ArrayType *arr = NULL;
    Size ndim = 0;
    List *options = NIL;
    DecodingOutputState *p = NULL;
    int rc = 0;
    char path[MAXPGPATH];
    struct stat st;
    t_thrd.logical_cxt.IsAreaDecode = false;

    Oid userId = GetUserId();
    CheckLogicalPremissions(userId);
    ValidateName(NameStr(*name));
    if (RecoveryInProgress() && confirm)
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("couldn't advance in recovery")));

    if (PG_ARGISNULL(1))
        upto_lsn = InvalidXLogRecPtr;
    else {
        const char *str_upto_lsn = TextDatumGetCString(PG_GETARG_DATUM(1));
        ValidateName(str_upto_lsn);
        if (!AssignLsn(&upto_lsn, str_upto_lsn)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for type lsn: \"%s\" "
                                                                          "of start_lsn",
                                                                          str_upto_lsn)));
        }
    }

    if (PG_ARGISNULL(2))
        upto_nchanges = 0;
    else {
        upto_nchanges = PG_GETARG_INT32(2);
    }

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("materialize mode required, but it is not allowed in this context")));

    /* state to write output to */
    p = (DecodingOutputState *)palloc0(sizeof(DecodingOutputState));

    p->binary_output = binary;

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &p->tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("return type must be a row type")));

    CheckLogicalDecodingRequirements(u_sess->proc_cxt.MyDatabaseId);

    arr = PG_GETARG_ARRAYTYPE_P(3);
    ndim = (Size)(uint)(ARR_NDIM(arr));

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    if (ndim > 1) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("array must be one-dimensional")));
    } else if (array_contains_nulls(arr)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("array must not contain nulls")));
    } else if (ndim == 1) {
        int nelems = 0;
        Datum *datum_opts = NULL;
        int i = 0;

        if (ARR_ELEMTYPE(arr) != TEXTOID) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("array must be TEXTOID")));
        }

        deconstruct_array(arr, TEXTOID, -1, false, 'i', &datum_opts, NULL, &nelems);

        if (nelems % 2 != 0)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("array must have even number of elements")));

        for (i = 0; i < nelems; i += 2) {
            char *dname = TextDatumGetCString(datum_opts[i]);
            char *opt = TextDatumGetCString(datum_opts[i + 1]);
            ValidateName(dname);
            options = lappend(options, makeDefElem(dname, (Node *)makeString(opt)));
        }
    }

    p->tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = p->tupstore;
    rsinfo->setDesc = p->tupdesc;

    /* compute the current end-of-wal */
    if (!RecoveryInProgress())
        end_of_wal = GetFlushRecPtr();
    else
        end_of_wal = GetXLogReplayRecPtr(NULL);

    CheckLogicalDecodingRequirements(u_sess->proc_cxt.MyDatabaseId);
    ReplicationSlotAcquire(NameStr(*name), false);
    rc = sprintf_s(path, sizeof(path), "pg_replslot/%s/snap", NameStr(t_thrd.slot_cxt.MyReplicationSlot->data.name));
    securec_check_ss(rc, "", "");
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
        if (!rmtree(path, true))
            ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR), errmsg("could not remove directory \"%s\"", path)));
    }
    if (mkdir(path, S_IRWXU) < 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", path)));
    }

    PG_TRY();
    {
        ctx = CreateDecodingContext(InvalidXLogRecPtr, options, false, logical_read_local_xlog_page,
                                    LogicalOutputPrepareWrite, LogicalOutputWrite);

        (void)MemoryContextSwitchTo(oldcontext);

        /*
         * Check whether the output pluggin writes textual output if that's
         * what we need.
         */
        if (!binary && ctx->options.output_type != OUTPUT_PLUGIN_TEXTUAL_OUTPUT)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("output plugin cannot produce binary output")));

        ctx->output_writer_private = p;

        startptr = t_thrd.slot_cxt.MyReplicationSlot->data.restart_lsn;

        t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner,
            "logical decoding", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

        /* invalidate non-timetravel entries */
        InvalidateSystemCaches();

        while ((!XLByteEQ(startptr, InvalidXLogRecPtr) && XLByteLT(startptr, end_of_wal)) ||
               (!XLByteEQ(ctx->reader->EndRecPtr, InvalidXLogRecPtr) && XLByteLT(ctx->reader->EndRecPtr, end_of_wal))) {
            XLogRecord *record = NULL;
            char *errm = NULL;

            record = XLogReadRecord(ctx->reader, startptr, &errm);
            if (errm != NULL)
                ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR),
                                errmsg("Stopped to parse any valid XLog Record at %X/%X: %s.",
                                       (uint32)(ctx->reader->EndRecPtr >> 32), (uint32)ctx->reader->EndRecPtr, errm)));

            startptr = InvalidXLogRecPtr;

            /*
             * The {begin_txn,change,commit_txn}_wrapper callbacks above will
             * store the description into our tuplestore.
             */
            if (record != NULL)
                LogicalDecodingProcessRecord(ctx, ctx->reader);

            /* check limits */
            if (!XLByteEQ(upto_lsn, InvalidXLogRecPtr) && XLByteLE(upto_lsn, ctx->reader->EndRecPtr))
                break;
            if (upto_nchanges != 0 && upto_nchanges <= p->returned_rows)
                break;
            CHECK_FOR_INTERRUPTS();
        }
    }
    PG_CATCH();
    {
        /* clear all timetravel entries */
        InvalidateSystemCaches();

        PG_RE_THROW();
    }
    PG_END_TRY();

    tuplestore_donestoring(tupstore);

    t_thrd.utils_cxt.CurrentResourceOwner = old_resowner;

    /*
     * Next time, start where we left off. (Hunting things, the family
     * business..)
     */
    if (!XLByteEQ(ctx->reader->EndRecPtr, InvalidXLogRecPtr) && confirm) {
        LogicalConfirmReceivedLocation(ctx->reader->EndRecPtr);
        log_slot_advance(&t_thrd.slot_cxt.MyReplicationSlot->data);
    }

    if (t_thrd.logical_cxt.sendFd >= 0) {
        (void)close(t_thrd.logical_cxt.sendFd);
        t_thrd.logical_cxt.sendFd = -1;
    }

    /* free context, call shutdown callback */
    FreeDecodingContext(ctx);

    ReplicationSlotRelease();
    InvalidateSystemCaches();

    return (Datum)0;
}

static XLogRecPtr getStartLsn(FunctionCallInfo fcinfo)
{
    XLogRecPtr start_lsn = InvalidXLogRecPtr;
    if (PG_ARGISNULL(0))
        start_lsn = InvalidXLogRecPtr;
    else {
        const char *str_start_lsn = TextDatumGetCString(PG_GETARG_DATUM(0));
        ValidateName(str_start_lsn);
        if (!AssignLsn(&start_lsn, str_start_lsn)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for type lsn: \"%s\" "
                                                                          "of start_lsn",
                                                                          str_start_lsn)));
        }
    }
    return start_lsn;
}

static XLogRecPtr getUpToLsn(FunctionCallInfo fcinfo)
{
    XLogRecPtr upto_lsn = InvalidXLogRecPtr;
    if (PG_ARGISNULL(1))
        upto_lsn = InvalidXLogRecPtr;
    else {
        const char *str_upto_lsn = TextDatumGetCString(PG_GETARG_DATUM(1));
        ValidateName(str_upto_lsn);
        if (!AssignLsn(&upto_lsn, str_upto_lsn)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for type lsn: \"%s\" "
                                                                          "of upto_lsn",
                                                                          str_upto_lsn)));
        }
    }
    return upto_lsn;
}

static int64 getUpToNChanges(FunctionCallInfo fcinfo)
{
    if (PG_ARGISNULL(2))
        return 0;
    else {
        return PG_GETARG_INT32(2);
    }
}

static void CheckSupportTupleStore(FunctionCallInfo fcinfo)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("materialize mode required, but it is not allowed in this context")));
}

char* getXlogDirUpdateLsn(FunctionCallInfo fcinfo, XLogRecPtr *start_lsn, XLogRecPtr *upto_lsn) {
    char* xlog_path = TextDatumGetCString(PG_GETARG_DATUM(4));
    char* xlog_name =  basename(xlog_path);
    char* xlog_dir = dirname(xlog_path);
    uint32 log;
    uint32 seg;
    uint32 tli;
    errno_t ret = sscanf_s(xlog_name, "%08X%08X%08X", &tli, &log, &seg);
    char *str_lsn = NULL;
    str_lsn = (char*)palloc(MAXPGPATH);
    ret = memset_s(str_lsn, MAXPGPATH, '\0', MAXPGPATH);
    securec_check(ret, "", "");
    sprintf_s(str_lsn, MAXPGPATH, "%X/%X000000", log, seg);
    ValidateName(str_lsn);
    if (!AssignLsn(start_lsn, str_lsn)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid lsn: \"%s\" "
                                                                      "of start_lsn",
                                                                      str_lsn)));
    }
    ret = memset_s(str_lsn, MAXPGPATH, '\0', MAXPGPATH);
    securec_check(ret, "", "");
    sprintf_s(str_lsn, MAXPGPATH, "%X/%XFFFFFF", log, seg);
    ValidateName(str_lsn);
    if (!AssignLsn(upto_lsn, str_lsn)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid lsn: \"%s\" "
                                                                      "of upto_lsn",
                                                                      str_lsn)));
    }
    return xlog_dir;
}

void CheckAreaDecodeOption(FunctionCallInfo fcinfo)
{
    ArrayType *arr = PG_GETARG_ARRAYTYPE_P(5);
    Size ndim = (Size)(uint)(ARR_NDIM(arr));

    if (ndim > 1) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("array must be one-dimensional")));
    } else if (array_contains_nulls(arr)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("array must not contain nulls")));
    } else if (ndim == 1 && ARR_ELEMTYPE(arr) != TEXTOID) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("array must be TEXTOID")));
    }

}

/*
 * Decode and ouput changes from start lsn to upto lsn.
 */
static Datum pg_logical_get_area_changes_guts(FunctionCallInfo fcinfo)
{
    XLogRecPtr start_lsn = InvalidXLogRecPtr;
    XLogRecPtr upto_lsn = InvalidXLogRecPtr;
    int64 upto_nchanges = 0;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    MemoryContext per_query_ctx = NULL;
    MemoryContext oldcontext = NULL;
    XLogRecPtr startptr = InvalidXLogRecPtr;
    XLogRecPtr end_of_wal = InvalidXLogRecPtr;
    LogicalDecodingContext *ctx = NULL;
    ResourceOwner old_resowner = t_thrd.utils_cxt.CurrentResourceOwner;
    ArrayType *arr = NULL;
    Size ndim = 0;
    List *options = NIL;
    DecodingOutputState *p = NULL;
    char* xlog_dir = NULL;
    t_thrd.logical_cxt.IsAreaDecode = true;

    Oid userId = GetUserId();
    CheckLogicalPremissions(userId);
    if (PG_ARGISNULL(0) && PG_ARGISNULL(4)) {
        ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR),
            errmsg("start_lsn and xlog_dir cannot be null at the same time.")));
    }

    /* arg1 get start lsn    start_lsn */
    start_lsn = getStartLsn(fcinfo);
    /* arg2 get end lsn    upto_lsn */
    upto_lsn = getUpToLsn(fcinfo);
    /* arg3 how many statements to output    upto_nchanges */
    upto_nchanges = getUpToNChanges(fcinfo);
    /* arg4 output format plugin */
    Name plugin = PG_GETARG_NAME(3);
    ValidateName(NameStr(*plugin));

    /* check to see if caller supports us returning a tuplestore */
    CheckSupportTupleStore(fcinfo);

    /* state to write output to */
    p = (DecodingOutputState *)palloc0(sizeof(DecodingOutputState));

    p->binary_output = false;

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &p->tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("return type must be a row type")));
    /*
     * arg5 which xlog file to decode. If it is NULL, decode the origin xlog.
     * If xlog_dir is not null, get the xlog dir and update the lsn.
     */
    if (PG_ARGISNULL(4)) {
        xlog_dir = NULL;
    } else {
        xlog_dir = getXlogDirUpdateLsn(fcinfo, &start_lsn, &upto_lsn);
    }
    /* The memory is controlled. The number of decoded files cannot exceed 10. */
    XLogRecPtr max_lsn_distance = XLOG_SEG_SIZE * 10;
    if (upto_lsn < start_lsn) {
        ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR), errmsg("upto_lsn can not be smaller than start_lsn.")));
    }
    if (upto_lsn == InvalidXLogRecPtr || ((upto_lsn - start_lsn) > max_lsn_distance)) {
        upto_lsn = start_lsn + max_lsn_distance;
    }

    arr = PG_GETARG_ARRAYTYPE_P(5);
    ndim = (Size)(uint)(ARR_NDIM(arr));

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    CheckAreaDecodeOption(fcinfo);
 
    if (ndim == 1) {
        int i = 0;
        Datum *datum_opts = NULL;
        int nelems = 0;

        deconstruct_array(arr, TEXTOID, -1, false, 'i', &datum_opts, NULL, &nelems);

        if (nelems % 2 != 0)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("array must have even number of elements")));

        for (i = 0; i < nelems; i = i + 2) {
            char *dname = TextDatumGetCString(datum_opts[i]);
            char *opt = TextDatumGetCString(datum_opts[i + 1]);
            ValidateName(dname);
            options = lappend(options, makeDefElem(dname, (Node *)makeString(opt)));
        }
    }

    p->tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = p->tupdesc;
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = p->tupstore;

    /* compute the current end-of-wal */
    if (!RecoveryInProgress())
        end_of_wal = GetFlushRecPtr();
    else
        end_of_wal = GetXLogReplayRecPtr(NULL);

    CheckLogicalDecodingRequirements(u_sess->proc_cxt.MyDatabaseId);

    PG_TRY();
    {
        ctx = CreateDecodingContextForArea(InvalidXLogRecPtr, NameStr(*plugin), options, false, logical_read_local_xlog_page,
                                    LogicalOutputPrepareWrite, LogicalOutputWrite);

        (void)MemoryContextSwitchTo(oldcontext);

        /*
         * Check whether the output pluggin writes textual output if that's
         * what we need.
         */
        if (ctx->options.output_type != OUTPUT_PLUGIN_TEXTUAL_OUTPUT)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("output plugin cannot produce binary output")));

        ctx->output_writer_private = p;

        /* find a valid recptr to start from */
        startptr = XLogFindNextRecord(ctx->reader, start_lsn, NULL, xlog_dir);

        t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner,
            "logical decoding", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

        /* invalidate non-timetravel entries */
        InvalidateSystemCaches();


        while ((!XLByteEQ(startptr, InvalidXLogRecPtr) && XLByteLT(startptr, end_of_wal)) ||
               (!XLByteEQ(ctx->reader->EndRecPtr, InvalidXLogRecPtr) && XLByteLT(ctx->reader->EndRecPtr, end_of_wal))) {
            XLogRecord *record = NULL;
            char *errm = NULL;

            record = XLogReadRecord(ctx->reader, startptr, &errm, true, xlog_dir);
            if (errm != NULL) {
                ereport(WARNING, (errcode(ERRCODE_LOGICAL_DECODE_ERROR),
                                errmsg("Stopped to parse any valid XLog Record at %X/%X: %s.",
                                       (uint32)(ctx->reader->EndRecPtr >> 32), (uint32)ctx->reader->EndRecPtr, errm)));
                break;
            }

            startptr = InvalidXLogRecPtr;

            /*
             * The {begin_txn,change,commit_txn}_wrapper callbacks above will
             * store the description into our tuplestore.
             */
            if (record != NULL)
                AreaLogicalDecodingProcessRecord(ctx, ctx->reader);

            /* check limits */
            if (!XLByteEQ(upto_lsn, InvalidXLogRecPtr) && XLByteLE(upto_lsn, ctx->reader->EndRecPtr))
                break;
            if (upto_nchanges != 0 && upto_nchanges <= p->returned_rows)
                break;
            CHECK_FOR_INTERRUPTS();
        }
    }
    PG_CATCH();
    {
        /* clear all timetravel entries */
        InvalidateSystemCaches();

        PG_RE_THROW();
    }
    PG_END_TRY();
    tuplestore_donestoring(tupstore);
    t_thrd.utils_cxt.CurrentResourceOwner = old_resowner;

    if (t_thrd.logical_cxt.sendFd >= 0) {
        (void)close(t_thrd.logical_cxt.sendFd);
        t_thrd.logical_cxt.sendFd = -1;
    }

    /* free context, call shutdown callback */
    if (ctx->callbacks.shutdown_cb != NULL)
        shutdown_cb_wrapper(ctx);

    ReorderBufferFree(ctx->reorder);
    XLogReaderFree(ctx->reader);
    MemoryContextDelete(ctx->context);
    InvalidateSystemCaches();

    return (Datum)0;

}

/*
 * SQL function returning the changestream as text, consuming the data.
 */
Datum pg_logical_slot_get_changes(PG_FUNCTION_ARGS)
{
    Datum ret = pg_logical_slot_get_changes_guts(fcinfo, true, false);
    return ret;
}

/*
 * SQL function returning the changestream as text, only peeking ahead.
 */
Datum pg_logical_slot_peek_changes(PG_FUNCTION_ARGS)
{
    Datum ret = pg_logical_slot_get_changes_guts(fcinfo, false, false);
    return ret;
}

/*
 * SQL function returning the changestream in binary, consuming the data.
 */
Datum pg_logical_slot_get_binary_changes(PG_FUNCTION_ARGS)
{
    Datum ret = pg_logical_slot_get_changes_guts(fcinfo, true, true);
    return ret;
}

/*
 * SQL function returning the changestream in binary, only peeking ahead.
 */
Datum pg_logical_slot_peek_binary_changes(PG_FUNCTION_ARGS)
{
    Datum ret = pg_logical_slot_get_changes_guts(fcinfo, false, true);
    return ret;
}

Datum pg_logical_get_area_changes(PG_FUNCTION_ARGS)
{
    Datum ret = pg_logical_get_area_changes_guts(fcinfo);
    return ret;
}


Datum gs_write_term_log(PG_FUNCTION_ARGS)
{
    if (RecoveryInProgress()) {
        PG_RETURN_BOOL(false);
    }

    /* we are about to start streaming switch over, stop any xlog insert. */
    if (t_thrd.xlog_cxt.LocalXLogInsertAllowed == 0 && g_instance.streaming_dr_cxt.isInSwitchover == true) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("cannot write term log during streaming disaster recovery")));
    }

    uint32 term_cur = Max(g_instance.comm_cxt.localinfo_cxt.term_from_file,
        g_instance.comm_cxt.localinfo_cxt.term_from_xlog);
    write_term_log(term_cur);

    PG_RETURN_BOOL(true);
}
