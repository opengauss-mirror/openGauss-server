/* -------------------------------------------------------------------------
 *
 * xid.c
 *	  openGauss transaction identifier and command identifier datatypes.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/xid.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <limits.h>

#include "access/transam.h"
#include "access/xact.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"

#define PG_GETARG_COMMANDID(n) DatumGetCommandId(PG_GETARG_DATUM(n))
#define PG_RETURN_COMMANDID(x) return CommandIdGetDatum(x)

Datum xidin4(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);

    PG_RETURN_SHORTTRANSACTIONID((ShortTransactionId)strtoul(str, NULL, 0));
}

Datum xidout4(PG_FUNCTION_ARGS)
{
    ShortTransactionId transactionId = PG_GETARG_SHORTTRANSACTIONID(0);

    /* maximum 32 bit unsigned integer representation takes 10 chars */
    const size_t buffer_len = 11;
    char* str = (char*)palloc(buffer_len);
    errno_t ss_rc = 0;

    ss_rc = snprintf_s(str, buffer_len, buffer_len - 1, "%lu", (unsigned long)transactionId);
    securec_check_ss(ss_rc, "\0", "\0");

    PG_RETURN_CSTRING(str);
}

/*
 *		xidrecv			- converts external binary format to xid
 */
Datum xidrecv4(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);

    PG_RETURN_SHORTTRANSACTIONID((ShortTransactionId)pq_getmsgint(buf, sizeof(ShortTransactionId)));
}

/*
 *		xidsend			- converts xid to binary format
 */
Datum xidsend4(PG_FUNCTION_ARGS)
{
    ShortTransactionId arg1 = PG_GETARG_SHORTTRANSACTIONID(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint32(&buf, arg1);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 *		xideq			- are two xids equal?
 */
Datum xideq4(PG_FUNCTION_ARGS)
{
    ShortTransactionId xid1 = PG_GETARG_SHORTTRANSACTIONID(0);
    ShortTransactionId xid2 = PG_GETARG_SHORTTRANSACTIONID(1);

    PG_RETURN_BOOL(TransactionIdEquals(xid1, xid2));
}

/*
 *		xidlt			- xid1 < xid2 ?
 */
Datum xidlt4(PG_FUNCTION_ARGS)
{
    ShortTransactionId xid1 = PG_GETARG_SHORTTRANSACTIONID(0);
    ShortTransactionId xid2 = PG_GETARG_SHORTTRANSACTIONID(1);

    PG_RETURN_BOOL(TransactionIdPrecedes(xid1, xid2));
}

Datum xidin(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);

    PG_RETURN_TRANSACTIONID((TransactionId)pg_strtouint64(str, NULL, 0));
}

Datum xidout(PG_FUNCTION_ARGS)
{
    TransactionId transactionId = PG_GETARG_TRANSACTIONID(0);

    /* maximum 32 bit unsigned integer representation takes 10 chars */
    const size_t buffer_len = 32;
    char* str = (char*)palloc(buffer_len);
    errno_t ss_rc = 0;

    ss_rc = snprintf_s(str, buffer_len, buffer_len - 1, XID_FMT, transactionId);
    securec_check_ss(ss_rc, "\0", "\0");

    PG_RETURN_CSTRING(str);
}

/*
 *		xidrecv			- converts external binary format to xid
 */
Datum xidrecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    uint32 lo, hi;

    lo = (uint32)pq_getmsgint(buf, sizeof(TransactionId));
    hi = (uint32)pq_getmsgint(buf, sizeof(TransactionId));

    PG_RETURN_TRANSACTIONID((uint64)lo + ((uint64)hi << 32));
}

/*
 *		xidsend			- converts xid to binary format
 */
Datum xidsend(PG_FUNCTION_ARGS)
{
    TransactionId arg1 = PG_GETARG_TRANSACTIONID(0);
    StringInfoData buf;
    uint32 lo, hi;

    lo = (uint32)(arg1 & 0xFFFFFFFF);
    hi = (uint32)(arg1 >> 32);

    pq_begintypsend(&buf);
    pq_sendint32(&buf, lo);
    pq_sendint32(&buf, hi);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 *		xideq			- are two xids equal?
 */
Datum xideq(PG_FUNCTION_ARGS)
{
    TransactionId xid1 = PG_GETARG_TRANSACTIONID(0);
    TransactionId xid2 = PG_GETARG_TRANSACTIONID(1);

    PG_RETURN_BOOL(TransactionIdEquals(xid1, xid2));
}

/*
 *		xidlt			- xid1 < xid2 ?
 */
Datum xidlt(PG_FUNCTION_ARGS)
{
    TransactionId xid1 = PG_GETARG_TRANSACTIONID(0);
    TransactionId xid2 = PG_GETARG_TRANSACTIONID(1);

    PG_RETURN_BOOL(TransactionIdPrecedes(xid1, xid2));
}

/*
 *		xid_age			- compute age of an XID (relative to latest stable xid)
 */
Datum xid_age(PG_FUNCTION_ARGS)
{
    TransactionId xid = PG_GETARG_TRANSACTIONID(0);
    TransactionId now = GetStableLatestTransactionId();

    /* Permanent XIDs are always infinitely old */
    if (!TransactionIdIsNormal(xid))
        PG_RETURN_INT64(INT64_MAX);

    PG_RETURN_INT64((int64)(now - xid));
}

/*
 * xidComparator
 *		qsort comparison function for XIDs
 */
int xidComparator(const void* arg1, const void* arg2)
{
    TransactionId xid1 = *(const TransactionId*)arg1;
    TransactionId xid2 = *(const TransactionId*)arg2;

    if (xid1 > xid2)
        return 1;
    if (xid1 < xid2)
        return -1;
    return 0;
}

// return the number after removing repetitive xids.
int RemoveRepetitiveXids(TransactionId* xids, int nxid)
{
    if (nxid <= 1)
        return nxid;

    // step1:sort, and make the same xids continual
    qsort(xids, nxid, sizeof(TransactionId), xidComparator);

    // step2: find the first same pair.
    int i = 1;
    while (i < nxid && xids[i] != xids[i - 1])
        ++i;

    // it's known that xids[i] is equal to xids[i-1], so that:
    // 1. xids[i-1] is used to be compared with the other xid.
    // 2. xids[i] is the place to remember the new xid;
    // 3. start from (i+1) to compare.
    int cmp = i - 1;
    ++i;

    // step3: remove the continual same values.
    for (; i < nxid; ++i) {
        if (xids[i] == xids[cmp])
            continue;

        xids[++cmp] = xids[i];
    }

    Assert(cmp < nxid);
    return cmp + 1;
}

/*****************************************************************************
 *	 COMMAND IDENTIFIER ROUTINES											 *
 *****************************************************************************/

/*
 *		cidin	- converts CommandId to internal representation.
 */
Datum cidin(PG_FUNCTION_ARGS)
{
    char* s = PG_GETARG_CSTRING(0);
    CommandId c;

    c = atoi(s);

    PG_RETURN_COMMANDID(c);
}

/*
 *		cidout	- converts a cid to external representation.
 */
Datum cidout(PG_FUNCTION_ARGS)
{
    CommandId c = PG_GETARG_COMMANDID(0);
    const size_t buffer_len = 16;
    char* result = (char*)palloc(buffer_len);
    errno_t ss_rc = 0;

    ss_rc = snprintf_s(result, buffer_len, buffer_len - 1, "%u", (unsigned int)c);
    securec_check_ss(ss_rc, "\0", "\0");

    PG_RETURN_CSTRING(result);
}

/*
 *		cidrecv			- converts external binary format to cid
 */
Datum cidrecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);

    PG_RETURN_COMMANDID((CommandId)pq_getmsgint(buf, sizeof(CommandId)));
}

/*
 *		cidsend			- converts cid to binary format
 */
Datum cidsend(PG_FUNCTION_ARGS)
{
    CommandId arg1 = PG_GETARG_COMMANDID(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint32(&buf, arg1);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum cideq(PG_FUNCTION_ARGS)
{
    CommandId arg1 = PG_GETARG_COMMANDID(0);
    CommandId arg2 = PG_GETARG_COMMANDID(1);

    PG_RETURN_BOOL(arg1 == arg2);
}
