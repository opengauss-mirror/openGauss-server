/* -------------------------------------------------------------------------
 * txid.c
 *
 *	Export internal transaction IDs to user level.
 *
 * Note that only top-level transaction IDs are ever converted to TXID.
 * This is important because TXIDs frequently persist beyond the global
 * xmin horizon, or may even be shipped to other machines, so we cannot
 * rely on being able to correlate subtransaction IDs with their parents
 * via functions such as SubTransGetTopmostTransaction().
 *
 *
 *	Copyright (c) 2003-2012, PostgreSQL Global Development Group
 *	64-bit txids: Marko Kreen, Skype Technologies
 *
 *	src/backend/utils/adt/txid.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/gtm.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "libpq/pqformat.h"
#include "storage/standby.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

/* txid will be signed int8 in database, so must limit to 63 bits */
#define MAX_TXID UINT64CONST(0x7FFFFFFFFFFFFFFF)

/*
 * If defined, use bsearch() function for searching for txids in snapshots
 * that have more than the specified number of values.
 */
#define USE_BSEARCH_IF_NXIP_GREATER 30

/*
 * Snapshot containing 8byte txids.
 */
typedef struct {
    /*
     * 4-byte length hdr, should not be touched directly.
     *
     * Explicit embedding is ok as we want always correct alignment anyway.
     */
    int32 __varsz;

    uint32 nxip; /* number of txids in xip array */
    TransactionId xmin;
    TransactionId xmax;
    TransactionId csn;
    TransactionId xip[1]; /* in-progress txids, xmin <= xip[i] < xmax */
} TxidSnapshot;

#define TXID_SNAPSHOT_SIZE(nxip) (offsetof(TxidSnapshot, xip) + sizeof(TransactionId) * (nxip))
#define TXID_SNAPSHOT_MAX_NXIP ((MaxAllocSize - offsetof(TxidSnapshot, xip)) / sizeof(TransactionId))

/*
 * txid comparator for qsort/bsearch
 */
static int cmp_txid(const void* aa, const void* bb)
{
    TransactionId a = *(const TransactionId*)aa;
    TransactionId b = *(const TransactionId*)bb;

    if (a < b)
        return -1;
    if (a > b)
        return 1;
    return 0;
}

/*
 * check txid visibility.
 */
static bool is_visible_txid(TransactionId value, const TxidSnapshot* snap)
{
    if (value < snap->xmin)
        return true;
    else if (value >= snap->xmax)
        return false;
#ifdef USE_BSEARCH_IF_NXIP_GREATER
    else if (snap->nxip > USE_BSEARCH_IF_NXIP_GREATER) {
        void* res = NULL;

        res = bsearch(&value, snap->xip, snap->nxip, sizeof(TransactionId), cmp_txid);
        /* if found, transaction is still in progress */
        return (res) ? false : true;
    }
#endif
    else {
        uint32 i;

        for (i = 0; i < snap->nxip; i++) {
            if (value == snap->xip[i])
                return false;
        }
        return true;
    }
}

/*
 * helper functions to use StringInfo for TxidSnapshot creation.
 */

static StringInfo buf_init(TransactionId xmin, TransactionId xmax)
{
    TxidSnapshot snap;
    StringInfo buf;

    snap.xmin = xmin;
    snap.xmax = xmax;
    snap.nxip = 0;

    buf = makeStringInfo();
    appendBinaryStringInfo(buf, (char*)&snap, TXID_SNAPSHOT_SIZE(0));
    return buf;
}

static void buf_add_txid(StringInfo buf, TransactionId xid)
{
    TxidSnapshot* snap = (TxidSnapshot*)buf->data;

    /* do this before possible realloc */
    snap->nxip++;

    appendBinaryStringInfo(buf, (char*)&xid, sizeof(xid));
}

static TxidSnapshot* buf_finalize(StringInfo buf)
{
    TxidSnapshot* snap = (TxidSnapshot*)buf->data;

    SET_VARSIZE(snap, buf->len);

    /* buf is not needed anymore */
    buf->data = NULL;
    pfree_ext(buf);

    return snap;
}

/*
 * simple number parser.
 *
 * We return 0 on error, which is invalid value for txid.
 */
static TransactionId str2txid(const char* s, const char** endp)
{
    TransactionId val = 0;
    TransactionId cutoff = MAX_TXID / 10;
    TransactionId cutlim = MAX_TXID % 10;

    for (; *s; s++) {
        unsigned d;

        if (*s < '0' || *s > '9')
            break;
        d = *s - '0';

        /*
         * check for overflow
         */
        if (val > cutoff || (val == cutoff && d > cutlim)) {
            val = 0;
            break;
        }

        val = val * 10 + d;
    }
    if (endp != NULL)
        *endp = s;
    return val;
}

/*
 * parse snapshot from cstring
 */
static TxidSnapshot* parse_snapshot(const char* str)
{
    TransactionId xmin;
    TransactionId xmax;
    TransactionId last_val = 0, val;
    const char* str_start = str;
    const char* endp = NULL;
    StringInfo buf;

    xmin = str2txid(str, &endp);
    if (*endp != ':')
        goto bad_format;
    str = endp + 1;

    xmax = str2txid(str, &endp);
    if (*endp != ':')
        goto bad_format;
    str = endp + 1;

    /* it should look sane */
    if (xmin == 0 || xmax == 0 || xmin > xmax)
        goto bad_format;

    /* allocate buffer */
    buf = buf_init(xmin, xmax);

    /* loop over values */
    while (*str != '\0') {
        /* read next value */
        val = str2txid(str, &endp);
        str = endp;

        /* require the input to be in order */
        if (val < xmin || val >= xmax || val <= last_val)
            goto bad_format;

        buf_add_txid(buf, val);
        last_val = val;

        if (*str == ',')
            str++;
        else if (*str != '\0')
            goto bad_format;
    }

    return buf_finalize(buf);

bad_format:
    ereport(
        ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("invalid input for txid_snapshot: \"%s\"", str_start)));
    return NULL;
}

/*
 * Public functions.
 *
 * txid_current() and txid_current_snapshot() are the only ones that
 * communicate with core xid machinery.  All the others work on data
 * returned by them.
 */

/*
 * txid_current() returns int8
 *
 *	Return the current toplevel transaction ID
 */
Datum txid_current(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(GetTopTransactionId());
}

Datum pgxc_snapshot_status(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));
    SRF_RETURN_DONE(funcctx);
#else
    if (!GTM_MODE) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("unsupported function or view in %s mode.", GTM_LITE_MODE ? "GTM-Lite" : "GTM-Free")));
    }
    FuncCallContext* funcctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(10, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "xmin", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "xmax", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "xcnt", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "oldestxmin", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "next_xid", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "timeline", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "active_thread_num", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "max_active_thread_num", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "snapshot_num", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)10, "snapshot_totalsize", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = palloc0(sizeof(int));
        funcctx->max_calls = 1;

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for each row */
        Datum values[10];
        bool nulls[10];
        HeapTuple tuple;
        GTM_SnapshotStatus sn_stat;
        errno_t ss_rc = 0;

        ss_rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(ss_rc, "\0", "\0");

        sn_stat = GetGTMSnapshotStatus(GetCurrentTransactionKey());
        if (!sn_stat) {
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg(
                        "GTM error, could not obtain snapshot_status, please check GTM is running or failovering.")));
        }
        /* log important result of transaction information */
        elog(LOG,
            "GTM next_xid:%lu, GTM oldestxmin:%lu, GTM gtmtimeline:%u",
            sn_stat->next_xid,
            sn_stat->recent_global_xmin,
            sn_stat->timeline);
        /* simple check: check warplimit */
        TransactionId xid = sn_stat->next_xid;
        /*
         * Check oldestxmin and next_xid in case that on-fly scence.
         * Notice:
         * This is a simple check: Here we just check and log GTM oldestxmin
         * and GTM next_xid without holding any locks. If GTM crash now, GTM
         * will lost all information, and DN will get next_xid as oldestxmin
         * to do vacuum and prune. It's very dangerous.
         * diff_num_threadhold, which as checking threadhold, is always same
         * as vacuum_defer_cleanup_age.
         */
        int diff_num_threadhold = (u_sess->attr.attr_storage.vacuum_defer_cleanup_age > 2000)
                                      ? u_sess->attr.attr_storage.vacuum_defer_cleanup_age
                                      : 2000;
        xid = sn_stat->recent_global_xmin + diff_num_threadhold;
        if (TransactionIdFollowsOrEquals(sn_stat->next_xid, xid)) {
            elog(WARNING,
                "A very old transaction is running, recent_global_xmin: %lu, next_xid: %lu.",
                sn_stat->recent_global_xmin,
                sn_stat->next_xid);
        }
        /* Values available to all callers */
        values[0] = TransactionIdGetDatum(sn_stat->xmin);
        values[1] = TransactionIdGetDatum(sn_stat->xmax);
        values[2] = Int32GetDatum(0);
        values[3] = TransactionIdGetDatum(sn_stat->recent_global_xmin);
        values[4] = TransactionIdGetDatum(sn_stat->next_xid);
        values[5] = Int32GetDatum(sn_stat->timeline);
        values[6] = Int32GetDatum(sn_stat->active_thread_num);
        values[7] = Int32GetDatum(sn_stat->max_thread_num);
        values[8] = Int32GetDatum(sn_stat->snapshot_num);
        values[9] = Int64GetDatum(sn_stat->snapshot_totalsize);
        /* notice: we free memory using memory contxt, so don't have memory leak here.*/
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(funcctx);
    }
#endif
}

/*
 * txid_current_snapshot() returns txid_snapshot
 *
 *		Return current snapshot in TXID format
 *
 * Note that only top-transaction XIDs are included in the snapshot.
 */
Datum txid_current_snapshot(PG_FUNCTION_ARGS)
{
    TxidSnapshot* snap = NULL;
    uint32 size;
    Snapshot cur;

    cur = GetActiveSnapshot();
    if (cur == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("no active snapshot set")));

    /* allocate */
    size = TXID_SNAPSHOT_SIZE(0);
    snap = (TxidSnapshot*)palloc(size);
    SET_VARSIZE(snap, size);

    /* fill */
    snap->xmin = cur->xmin;
    snap->xmax = cur->xmax;
    snap->csn = cur->snapshotcsn;
    snap->nxip = 0;

    PG_RETURN_POINTER(snap);
}

/*
 * txid_snapshot_in(cstring) returns txid_snapshot
 *
 *		input function for type txid_snapshot
 */
Datum txid_snapshot_in(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);
    TxidSnapshot* snap = NULL;

    snap = parse_snapshot(str);

    PG_RETURN_POINTER(snap);
}

/*
 * txid_snapshot_out(txid_snapshot) returns cstring
 *
 *		output function for type txid_snapshot
 */
Datum txid_snapshot_out(PG_FUNCTION_ARGS)
{
    TxidSnapshot* snap = (TxidSnapshot*)PG_GETARG_VARLENA_P(0);
    StringInfoData str;
    uint32 i;

    initStringInfo(&str);

    appendStringInfo(&str, XID_FMT ":", snap->xmin);
    appendStringInfo(&str, XID_FMT ":", snap->xmax);

    for (i = 0; i < snap->nxip; i++) {
        if (i > 0)
            appendStringInfoChar(&str, ',');
        appendStringInfo(&str, XID_FMT, snap->xip[i]);
    }

    /* free memory if allocated by the toaster */
    PG_FREE_IF_COPY(snap, 0);

    PG_RETURN_CSTRING(str.data);
}

/*
 * txid_snapshot_recv(internal) returns txid_snapshot
 *
 *		binary input function for type txid_snapshot
 *
 *		format: int4 nxip, int8 xmin, int8 xmax, int8 xip
 */
Datum txid_snapshot_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    TxidSnapshot* snap = NULL;
    TransactionId last = 0;
    int nxip;
    int i;
    TransactionId xmin, xmax, csn;

    /* load and validate nxip */
    nxip = pq_getmsgint(buf, 4);
    if (nxip < 0 || nxip > (int)TXID_SNAPSHOT_MAX_NXIP)
        goto bad_format;

    xmin = pq_getmsgint64(buf);
    xmax = pq_getmsgint64(buf);
    csn = pq_getmsgint64(buf);
    if (xmin == 0 || xmax == 0 || xmin > xmax || xmax > MAX_TXID)
        goto bad_format;

    snap = (TxidSnapshot*)palloc(TXID_SNAPSHOT_SIZE(nxip));
    snap->xmin = xmin;
    snap->xmax = xmax;
    snap->csn = csn;
    snap->nxip = nxip;
    SET_VARSIZE(snap, TXID_SNAPSHOT_SIZE(nxip));

    for (i = 0; i < nxip; i++) {
        TransactionId cur = pq_getmsgint64(buf);

        if (cur <= last || cur < xmin || cur >= xmax)
            goto bad_format;
        snap->xip[i] = cur;
        last = cur;
    }
    PG_RETURN_POINTER(snap);

bad_format:
    ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("invalid snapshot data")));
    return (Datum)NULL;
}

/*
 * txid_snapshot_send(txid_snapshot) returns bytea
 *
 *		binary output function for type txid_snapshot
 *
 *		format: int4 nxip, int8 xmin, int8 xmax, int8 xip
 */
Datum txid_snapshot_send(PG_FUNCTION_ARGS)
{
    TxidSnapshot* snap = (TxidSnapshot*)PG_GETARG_VARLENA_P(0);
    StringInfoData buf;
    uint32 i;

    pq_begintypsend(&buf);
    pq_sendint32(&buf, snap->nxip);
    pq_sendint64(&buf, snap->xmin);
    pq_sendint64(&buf, snap->xmax);
    pq_sendint64(&buf, snap->csn);
    for (i = 0; i < snap->nxip; i++)
        pq_sendint64(&buf, snap->xip[i]);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * txid_visible_in_snapshot(int8, txid_snapshot) returns bool
 *
 *		is txid visible in snapshot ?
 */
Datum txid_visible_in_snapshot(PG_FUNCTION_ARGS)
{
    TransactionId value = PG_GETARG_INT64(0);
    TxidSnapshot* snap = (TxidSnapshot*)PG_GETARG_VARLENA_P(1);

    PG_RETURN_BOOL(is_visible_txid(value, snap));
}

/*
 * txid_snapshot_xmin(txid_snapshot) returns int8
 *
 *		return snapshot's xmin
 */
Datum txid_snapshot_xmin(PG_FUNCTION_ARGS)
{
    TxidSnapshot* snap = (TxidSnapshot*)PG_GETARG_VARLENA_P(0);

    PG_RETURN_INT64(snap->xmin);
}

/*
 * txid_snapshot_xmax(txid_snapshot) returns int8
 *
 *		return snapshot's xmax
 */
Datum txid_snapshot_xmax(PG_FUNCTION_ARGS)
{
    TxidSnapshot* snap = (TxidSnapshot*)PG_GETARG_VARLENA_P(0);

    PG_RETURN_INT64(snap->xmax);
}

/*
 * txid_snapshot_xip(txid_snapshot) returns setof int8
 *
 *		return in-progress TXIDs in snapshot.
 */
Datum txid_snapshot_xip(PG_FUNCTION_ARGS)
{
    FuncCallContext* fctx = NULL;
    TxidSnapshot* snap = NULL;
    TransactionId value;
    errno_t rc = EOK;

    /* on first call initialize snap_state and get copy of snapshot */
    if (SRF_IS_FIRSTCALL()) {
        TxidSnapshot* arg = (TxidSnapshot*)PG_GETARG_VARLENA_P(0);

        fctx = SRF_FIRSTCALL_INIT();

        /* make a copy of user snapshot */
        snap = (TxidSnapshot*)MemoryContextAlloc(fctx->multi_call_memory_ctx, VARSIZE(arg));

        if (VARSIZE(arg) > 0) {
            rc = memcpy_s(snap, VARSIZE(arg), arg, VARSIZE(arg));
            securec_check(rc, "\0", "\0");
        }
        fctx->user_fctx = snap;
    }

    /* return values one-by-one */
    fctx = SRF_PERCALL_SETUP();
    snap = (TxidSnapshot*)fctx->user_fctx;
    if (fctx->call_cntr < snap->nxip) {
        value = snap->xip[fctx->call_cntr];
        SRF_RETURN_NEXT(fctx, Int64GetDatum(value));
    } else {
        SRF_RETURN_DONE(fctx);
    }
}
