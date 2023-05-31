/*
* Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
* ---------------------------------------------------------------------------------------
 *
 * set.cpp
 *	  I/O functions, operators, aggregates etc for set types
 *
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/set.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/hash.h"
#include "catalog/indexing.h"
#include "catalog/pg_set.h"
#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/varbit.h"
#include "utils/int8.h"
#include "utils/sortsupport.h"
#include "fmgr.h"
#include "catalog/gs_collation.h"

#define KEY_NUM (2)

static VarBit* get_set_in_result(Oid settypoid, char *setlabels, Oid collation);

/* Basic I/O support */

Datum set_in(PG_FUNCTION_ARGS)
{
    char *setlabels = PG_GETARG_CSTRING(0);
    Oid settypoid = PG_GETARG_OID(1);

    PG_RETURN_VARBIT_P(get_set_in_result(settypoid, setlabels, PG_GET_COLLATION()));
}

Datum set_out(PG_FUNCTION_ARGS)
{
    bool needdelimt = false;
    Oid settypid = InvalidOid;
    VarBit *data = PG_GETARG_VARBIT_P(0);
    char *result = pstrdup("");
    Relation pg_set = NULL;
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;
    bool isnull = true;

    int typmod = VARBITLEN(data);
    if (typmod <= int(sizeof(Oid) * BITS_PER_BYTE)) {
        PG_RETURN_CSTRING(result);
    }

    StringInfoData buf;
    initStringInfo(&buf);

    settypid = *(Oid *)VARBITS(data);
    bits8 *base = (bits8*)VARBITS(data) + sizeof(Oid);

    pg_set = heap_open(SetRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_set_settypid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(settypid));

    int1 bitlen = typmod - sizeof(Oid) * BITS_PER_BYTE;
    for (int1 order = 0; order < bitlen; order++) {
        bits8 *r = base + order / BITS_PER_BYTE;
        bool bitset = (*r) & (1 << (order % BITS_PER_BYTE));

        if (bitset) {
            ScanKeyInit(&key[1], Anum_pg_set_setsortorder, BTEqualStrategyNumber, F_INT1EQ, UInt8GetDatum(order));
            scan = systable_beginscan(pg_set, SetTypIdOrderIndexId, true, NULL, KEY_NUM, key);
            tup = systable_getnext(scan);
            if (!HeapTupleIsValid(tup)) {
                systable_endscan(scan);
                heap_close(pg_set, AccessShareLock);
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                    errmsg("invalid internal value for set order: %d", order)));
            }

            text* setlabel = (text*)heap_getattr(tup, Anum_pg_set_setlabel, pg_set->rd_att, &isnull);
            char* label = text_to_cstring(setlabel);
            if (needdelimt) {
                appendStringInfoString(&buf, SETLABELDELIMIT);
            }
            appendStringInfoString(&buf, label);
            needdelimt = true;
            pfree(label);
            systable_endscan(scan);
        }
    }

    heap_close(pg_set, AccessShareLock);

    if (buf.len > 0) {
        result = (char *)palloc(buf.len + 1);
        errno_t rc = memcpy_s(result, buf.len + 1, buf.data, buf.len);
        securec_check(rc, "\0", "\0");
        result[buf.len] = '\0';
    }

    FreeStringInfo(&buf);
    PG_RETURN_CSTRING(result);
}

/* Binary I/O support */
Datum set_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    Oid settypoid = PG_GETARG_OID(1);

    if (get_fn_expr_argtype(fcinfo->flinfo, 0) == CSTRINGOID)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid argument for set_recv")));

    int nbytes;
    char* setlabels = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);

    PG_RETURN_VARBIT_P(get_set_in_result(settypoid, setlabels, PG_GET_COLLATION()));
}

Datum set_send(PG_FUNCTION_ARGS)
{
    Oid settypid = InvalidOid;
    VarBit *data = PG_GETARG_VARBIT_P(0);
    StringInfoData buf;
    bool needdelimt = false;
    Relation pg_set = NULL;
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;
    bool isnull = true;

    pq_begintypsend(&buf);

    int typmod = VARBITLEN(data);
    if (typmod <= int(sizeof(Oid) * BITS_PER_BYTE)) {
        PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
    }

    settypid = *(Oid *)VARBITS(data);
    bits8 *base = (bits8*)VARBITS(data) + sizeof(Oid);

    pg_set = heap_open(SetRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_set_settypid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(settypid));

    int1 bitlen = typmod - sizeof(Oid) * BITS_PER_BYTE;
    for (int1 order = 0; order < bitlen; order++) {
        bits8 *r = base + order / BITS_PER_BYTE;
        bool bitset = (*r) & (1 << (order % BITS_PER_BYTE));

        if (bitset) {
            ScanKeyInit(&key[1], Anum_pg_set_setsortorder, BTEqualStrategyNumber, F_INT1EQ, UInt8GetDatum(order));
            scan = systable_beginscan(pg_set, SetTypIdOrderIndexId, true, NULL, KEY_NUM, key);
            tup = systable_getnext(scan);
            if (!HeapTupleIsValid(tup)) {
                systable_endscan(scan);
                heap_close(pg_set, AccessShareLock);
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                    errmsg("invalid internal value for set order: %d", order)));
            }

            text* setlabel = (text*)heap_getattr(tup, Anum_pg_set_setlabel, pg_set->rd_att, &isnull);
            char* label = text_to_cstring(setlabel);
            if (needdelimt) {
                pq_sendtext(&buf, SETLABELDELIMIT, 1);
            }

            needdelimt = true;
            pq_sendtext(&buf, label, strlen(label));
            pfree(label);
            systable_endscan(scan);
        }
    }

    heap_close(pg_set, AccessShareLock);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

static int64 settoint64(VarBit *bitmap)
{
    int64 result = 0;
    int typmod = VARBITLEN(bitmap);

    bits8 *base = (bits8*)VARBITS(bitmap) + sizeof(Oid);
    int1 bitlen = typmod - sizeof(Oid) * BITS_PER_BYTE;

    /* bitlen can up to max 64 */
    for (int1 order = 0; order < bitlen; order++) {
        bits8 *r = base + order / BITS_PER_BYTE;
        bool bitset = (*r) & (1 << (order % BITS_PER_BYTE));
        if (bitset) {
            result |= (1UL << order);
        }
    }

    return result;
}

static Datum int64toset(int64 val, Oid typid)
{
    int bitlen = 0;
    Relation pg_set = NULL;
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;

    if (val != 0) {
        pg_set = heap_open(SetRelationId, AccessShareLock);
        ScanKeyInit(&key[0], Anum_pg_set_settypid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(typid));
        ScanKeyInit(&key[1], Anum_pg_set_setsortorder, BTEqualStrategyNumber, F_INT1EQ, UInt8GetDatum(0));

        scan = systable_beginscan(pg_set, SetTypIdOrderIndexId, true, NULL, KEY_NUM, key);
        tup = systable_getnext(scan);
        if (!HeapTupleIsValid(tup)) {
            systable_endscan(scan);
            heap_close(pg_set, AccessShareLock);
            /* can not assign valid value for empty set  */
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                errmsg("invalid input value for set %s: %ld", format_type_be(typid), val)));
        }

        Form_pg_set settup = (Form_pg_set)GETSTRUCT(tup);
        bitlen = settup->setnum;

        systable_endscan(scan);
        heap_close(pg_set, AccessShareLock);

        uint64 mask = (bitlen < SETLABELNUM) ? ((1UL << bitlen) - 1) : PG_UINT64_MAX;
        if (val & (~mask)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                errmsg("invalid input value for set %s: %ld", format_type_be(typid), val)));
        }
    }

    int typmod = sizeof(Oid) * BITS_PER_BYTE + bitlen;
    int len = VARBITTOTALLEN(typmod);
    VarBit *result = (VarBit*)palloc0(len);
    *(Oid *)VARBITS(result) = typid;
    bits8 *base = (bits8*)VARBITS(result) + sizeof(Oid);

    for (int1 order = 0; order < bitlen; order++) {
        bits8 *r = base + order / BITS_PER_BYTE;
        if (val & (1UL << order)) {
            (*r) |= (1 << (order % BITS_PER_BYTE));
        }
    }

    SET_VARSIZE(result, len);
    VARBITLEN(result) = typmod;
    PG_RETURN_VARBIT_P(result);
}

Datum hashsetint(PG_FUNCTION_ARGS)
{
    int64 val = settoint64(PG_GETARG_VARBIT_P(0));
    return DirectFunctionCall1(hashint8, Int64GetDatum(val));
}

Datum hashsettext(PG_FUNCTION_ARGS)
{
    char *setlabels = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
    if (is_b_format_collation(PG_GET_COLLATION())) {
        Datum result = hash_text_by_builtin_collations((GS_UCHAR*)setlabels, strlen(setlabels), PG_GET_COLLATION());
        pfree_ext(setlabels);
        return result;
    }

    return DirectFunctionCall1(hashtext, PointerGetDatum(cstring_to_text(setlabels)));
}

Datum btsetcmp(PG_FUNCTION_ARGS)
{
    VarBit *d1 = PG_GETARG_VARBIT_P(0);
    Oid settypid1 = *(Oid *)VARBITS(d1);

    VarBit *d2 = PG_GETARG_VARBIT_P(1);
    Oid settypid2 = *(Oid *)VARBITS(d2);
    /* compare by its value if arguments are the same set, otherwise by character order */
    if (settypid1 == settypid2) {
        int64 a = settoint64(d1);
        int64 b = settoint64(d2);
        return DirectFunctionCall2(btint8cmp, Int64GetDatum(a), Int64GetDatum(b));
    } else {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(1)));

        int result = strcmp(a, b);
        PG_RETURN_INT32(result);
    }
}

Datum btint4setcmp(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT32(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    return DirectFunctionCall2(btint8cmp, Int64GetDatum(a), Int64GetDatum(b));
}

Datum btsetint4cmp(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT32(1);
    return DirectFunctionCall2(btint8cmp, Int64GetDatum(a), Int64GetDatum(b));
}

Datum btint2setcmp(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT16(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    return DirectFunctionCall2(btint8cmp, Int64GetDatum(a), Int64GetDatum(b));
}

Datum btsetint2cmp(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT16(1);
    return DirectFunctionCall2(btint8cmp, Int64GetDatum(a), Int64GetDatum(b));
}

Datum btint8setcmp(PG_FUNCTION_ARGS)
{
    int64 a = PG_GETARG_INT64(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    return DirectFunctionCall2(btint8cmp, Int64GetDatum(a), Int64GetDatum(b));
}

Datum btsetint8cmp(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = PG_GETARG_INT64(1);
    return DirectFunctionCall2(btint8cmp, Int64GetDatum(a), Int64GetDatum(b));
}

/* compare (>,>=,<,<=,=,!=) functions */
Datum seteq(PG_FUNCTION_ARGS)
{
    if (is_b_format_collation(PG_GET_COLLATION())) {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(1)));
        bool result = (varstr_cmp_by_builtin_collations(a, strlen(a), b, strlen(b), PG_GET_COLLATION()) == 0);
        PG_RETURN_BOOL(result);
    }

    VarBit *d1 = PG_GETARG_VARBIT_P(0);
    Oid settypid1 = *(Oid *)VARBITS(d1);
    VarBit *d2 = PG_GETARG_VARBIT_P(1);
    Oid settypid2 = *(Oid *)VARBITS(d2);

    if (settypid1 == settypid2) {
        int64 a = settoint64(d1);
        int64 b = settoint64(d2);
        PG_RETURN_BOOL(a == b);
    } else {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(1)));

        int result = strcmp(a, b);
        if (result == 0)
            PG_RETURN_BOOL(true);
        else
            PG_RETURN_BOOL(false);
    }
}

Datum setne(PG_FUNCTION_ARGS)
{
    if (is_b_format_collation(PG_GET_COLLATION())) {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(1)));
        bool result = (varstr_cmp_by_builtin_collations(a, strlen(a), b, strlen(b), PG_GET_COLLATION()) != 0);
        PG_RETURN_BOOL(result);
    }

    VarBit *d1 = PG_GETARG_VARBIT_P(0);
    Oid settypid1 = *(Oid *)VARBITS(d1);
    VarBit *d2 = PG_GETARG_VARBIT_P(1);
    Oid settypid2 = *(Oid *)VARBITS(d2);

    if (settypid1 == settypid2) {
        int64 a = settoint64(d1);
        int64 b = settoint64(d2);
        PG_RETURN_BOOL(a != b);
    } else {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(1)));

        int result = strcmp(a, b);
        if (result != 0)
            PG_RETURN_BOOL(true);
        else
            PG_RETURN_BOOL(false);
    }
}

Datum setge(PG_FUNCTION_ARGS)
{
    if (is_b_format_collation(PG_GET_COLLATION())) {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(1)));
        bool result = (varstr_cmp_by_builtin_collations(a, strlen(a), b, strlen(b), PG_GET_COLLATION()) >= 0);
        PG_RETURN_BOOL(result);
    }

    VarBit *d1 = PG_GETARG_VARBIT_P(0);
    Oid settypid1 = *(Oid *)VARBITS(d1);
    VarBit *d2 = PG_GETARG_VARBIT_P(1);
    Oid settypid2 = *(Oid *)VARBITS(d2);

    if (settypid1 == settypid2) {
        int64 a = settoint64(d1);
        int64 b = settoint64(d2);
        PG_RETURN_BOOL(a >= b);
    } else {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(1)));

        int result = strcmp(a, b);
        if (result >= 0)
            PG_RETURN_BOOL(true);
        else
            PG_RETURN_BOOL(false);
    }
}

Datum setgt(PG_FUNCTION_ARGS)
{
    if (is_b_format_collation(PG_GET_COLLATION())) {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(1)));
        bool result = (varstr_cmp_by_builtin_collations(a, strlen(a), b, strlen(b), PG_GET_COLLATION()) > 0);
        PG_RETURN_BOOL(result);
    }

    VarBit *d1 = PG_GETARG_VARBIT_P(0);
    Oid settypid1 = *(Oid *)VARBITS(d1);

    VarBit *d2 = PG_GETARG_VARBIT_P(1);
    Oid settypid2 = *(Oid *)VARBITS(d2);

    if (settypid1 == settypid2) {
        int64 a = settoint64(d1);
        int64 b = settoint64(d2);
        PG_RETURN_BOOL(a > b);
    } else {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(1)));

        int result = strcmp(a, b);
        if (result > 0)
            PG_RETURN_BOOL(true);
        else
            PG_RETURN_BOOL(false);
    }
}

Datum setlt(PG_FUNCTION_ARGS)
{
    if (is_b_format_collation(PG_GET_COLLATION())) {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(1)));
        bool result = (varstr_cmp_by_builtin_collations(a, strlen(a), b, strlen(b), PG_GET_COLLATION()) < 0);
        PG_RETURN_BOOL(result);
    }

    VarBit *d1 = PG_GETARG_VARBIT_P(0);
    Oid settypid1 = *(Oid *)VARBITS(d1);
    VarBit *d2 = PG_GETARG_VARBIT_P(1);
    Oid settypid2 = *(Oid *)VARBITS(d2);

    if (settypid1 == settypid2) {
        int64 a = settoint64(d1);
        int64 b = settoint64(d2);
        PG_RETURN_BOOL(a < b);
    } else {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(1)));

        int result = strcmp(a, b);
        if (result < 0)
            PG_RETURN_BOOL(true);
        else
            PG_RETURN_BOOL(false);
    }
}

Datum setle(PG_FUNCTION_ARGS)
{
    if (is_b_format_collation(PG_GET_COLLATION())) {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(1)));
        bool result = (varstr_cmp_by_builtin_collations(a, strlen(a), b, strlen(b), PG_GET_COLLATION()) <= 0);
        PG_RETURN_BOOL(result);
    }

    VarBit *d1 = PG_GETARG_VARBIT_P(0);
    Oid settypid1 = *(Oid *)VARBITS(d1);
    VarBit *d2 = PG_GETARG_VARBIT_P(1);
    Oid settypid2 = *(Oid *)VARBITS(d2);

    if (settypid1 == settypid2) {
        int64 a = settoint64(d1);
        int64 b = settoint64(d2);
        PG_RETURN_BOOL(a <= b);
    } else {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(0)));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, PG_GETARG_DATUM(1)));

        int result = strcmp(a, b);
        if (result <= 0)
            PG_RETURN_BOOL(true);
        else
            PG_RETURN_BOOL(false);
    }
}

/* int4 compare with set */
Datum int4seteq(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT32(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a == b);
}

Datum int4setne(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT32(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a != b);
}

Datum int4setge(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT32(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a >= b);
}

Datum int4setgt(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT32(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a > b);
}

Datum int4setlt(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT32(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a < b);
}

Datum int4setle(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT32(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a <= b);
}

/* set compare with int4 */
Datum setint4eq(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a == b);
}

Datum setint4ne(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a != b);
}

Datum setint4ge(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a >= b);
}

Datum setint4gt(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a > b);
}

Datum setint4lt(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a < b);
}

Datum setint4le(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT32(1);
    PG_RETURN_BOOL(a <= b);
}

/* int2 compare with set */
Datum int2seteq(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT16(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a == b);
}

Datum int2setne(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT16(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a != b);
}

Datum int2setge(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT16(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a >= b);
}

Datum int2setgt(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT16(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a > b);
}

Datum int2setlt(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT16(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a < b);
}

Datum int2setle(PG_FUNCTION_ARGS)
{
    int64 a = (int64)PG_GETARG_INT16(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a <= b);
}

/* set compare with int2 */
Datum setint2eq(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT16(1);
    PG_RETURN_BOOL(a == b);
}

Datum setint2ne(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT16(1);
    PG_RETURN_BOOL(a != b);
}

Datum setint2ge(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT16(1);
    PG_RETURN_BOOL(a >= b);
}

Datum setint2gt(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT16(1);
    PG_RETURN_BOOL(a > b);
}

Datum setint2lt(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT16(1);
    PG_RETURN_BOOL(a < b);
}

Datum setint2le(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = (int64)PG_GETARG_INT16(1);
    PG_RETURN_BOOL(a <= b);
}

/* int8 compare with set */
Datum int8seteq(PG_FUNCTION_ARGS)
{
    int64 a = PG_GETARG_INT64(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a == b);
}

Datum int8setne(PG_FUNCTION_ARGS)
{
    int64 a = PG_GETARG_INT64(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a != b);
}

Datum int8setge(PG_FUNCTION_ARGS)
{
    int64 a = PG_GETARG_INT64(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a >= b);
}

Datum int8setgt(PG_FUNCTION_ARGS)
{
    int64 a = PG_GETARG_INT64(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a > b);
}

Datum int8setlt(PG_FUNCTION_ARGS)
{
    int64 a = PG_GETARG_INT64(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a < b);
}

Datum int8setle(PG_FUNCTION_ARGS)
{
    int64 a = PG_GETARG_INT64(0);
    int64 b = settoint64(PG_GETARG_VARBIT_P(1));
    PG_RETURN_BOOL(a <= b);
}

/* set compare with int8 */
Datum setint8eq(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = PG_GETARG_INT64(1);
    PG_RETURN_BOOL(a == b);
}

Datum setint8ne(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = PG_GETARG_INT64(1);
    PG_RETURN_BOOL(a != b);
}

Datum setint8ge(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = PG_GETARG_INT64(1);
    PG_RETURN_BOOL(a >= b);
}

Datum setint8gt(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = PG_GETARG_INT64(1);
    PG_RETURN_BOOL(a > b);
}

Datum setint8lt(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = PG_GETARG_INT64(1);
    PG_RETURN_BOOL(a < b);
}

Datum setint8le(PG_FUNCTION_ARGS)
{
    int64 a = settoint64(PG_GETARG_VARBIT_P(0));
    int64 b = PG_GETARG_INT64(1);
    PG_RETURN_BOOL(a <= b);
}

/* set compare with text */
Datum settexteq(PG_FUNCTION_ARGS)
{
    Datum set = DirectFunctionCall1(settotext, PG_GETARG_DATUM(0));
    return DirectFunctionCall2Coll(texteq, PG_GET_COLLATION(), PointerGetDatum(set), PG_GETARG_DATUM(1));
}

Datum settextne(PG_FUNCTION_ARGS)
{
    Datum set = DirectFunctionCall1(settotext, PG_GETARG_DATUM(0));
    return DirectFunctionCall2Coll(textne, PG_GET_COLLATION(), PointerGetDatum(set), PG_GETARG_DATUM(1));
}

Datum settextgt(PG_FUNCTION_ARGS)
{
    Datum set = DirectFunctionCall1(settotext, PG_GETARG_DATUM(0));
    return DirectFunctionCall2Coll(text_gt, PG_GET_COLLATION(), PointerGetDatum(set), PG_GETARG_DATUM(1));
}

Datum settextge(PG_FUNCTION_ARGS)
{
    Datum set = DirectFunctionCall1(settotext, PG_GETARG_DATUM(0));
    return DirectFunctionCall2Coll(text_ge, PG_GET_COLLATION(), PointerGetDatum(set), PG_GETARG_DATUM(1));
}

Datum settextlt(PG_FUNCTION_ARGS)
{
    Datum set = DirectFunctionCall1(settotext, PG_GETARG_DATUM(0));\
    return DirectFunctionCall2Coll(text_lt, PG_GET_COLLATION(), PointerGetDatum(set), PG_GETARG_DATUM(1));
}

Datum settextle(PG_FUNCTION_ARGS)
{
    Datum set = DirectFunctionCall1(settotext, PG_GETARG_DATUM(0));
    return DirectFunctionCall2Coll(text_le, PG_GET_COLLATION(), PointerGetDatum(set), PG_GETARG_DATUM(1));
}

/* text compare with set */
Datum textseteq(PG_FUNCTION_ARGS)
{
    Datum set = DirectFunctionCall1(settotext, PG_GETARG_DATUM(1));
    return DirectFunctionCall2Coll(texteq, PG_GET_COLLATION(), PG_GETARG_DATUM(0), PointerGetDatum(set));
}

Datum textsetne(PG_FUNCTION_ARGS)
{
    Datum set = DirectFunctionCall1(settotext, PG_GETARG_DATUM(1));
    return DirectFunctionCall2Coll(textne, PG_GET_COLLATION(), PG_GETARG_DATUM(0), PointerGetDatum(set));
}

Datum textsetgt(PG_FUNCTION_ARGS)
{
    Datum set = DirectFunctionCall1(settotext, PG_GETARG_DATUM(1));
    return DirectFunctionCall2Coll(text_gt, PG_GET_COLLATION(), PG_GETARG_DATUM(0), PointerGetDatum(set));
}

Datum textsetge(PG_FUNCTION_ARGS)
{
    Datum set = DirectFunctionCall1(settotext, PG_GETARG_DATUM(1));
    return DirectFunctionCall2Coll(text_ge, PG_GET_COLLATION(), PG_GETARG_DATUM(0), PointerGetDatum(set));
}

Datum textsetlt(PG_FUNCTION_ARGS)
{
    Datum set = DirectFunctionCall1(settotext, PG_GETARG_DATUM(1));
    return DirectFunctionCall2Coll(text_lt, PG_GET_COLLATION(), PG_GETARG_DATUM(0), PointerGetDatum(set));
}

Datum textsetle(PG_FUNCTION_ARGS)
{
    Datum set = DirectFunctionCall1(settotext, PG_GETARG_DATUM(1));
    return DirectFunctionCall2Coll(text_le, PG_GET_COLLATION(), PG_GETARG_DATUM(0), PointerGetDatum(set));
}

/* type coerce functions */
Datum setint2(PG_FUNCTION_ARGS)
{
    int64 val = settoint64(PG_GETARG_VARBIT_P(0));
    return DirectFunctionCall1(int82, Int64GetDatum(val));
}

Datum setint4(PG_FUNCTION_ARGS)
{
    int64 val = settoint64(PG_GETARG_VARBIT_P(0));
    return DirectFunctionCall1(int84, Int64GetDatum(val));
}

Datum setint8(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(settoint64(PG_GETARG_VARBIT_P(0)));
}

Datum settof(PG_FUNCTION_ARGS)
{
    int64 val = settoint64(PG_GETARG_VARBIT_P(0));
    return DirectFunctionCall1(i8tof, Int64GetDatum(val));
}

Datum settod(PG_FUNCTION_ARGS)
{
    int64 val = settoint64(PG_GETARG_VARBIT_P(0));
    return DirectFunctionCall1(i8tod, Int64GetDatum(val));
}

Datum settonumber(PG_FUNCTION_ARGS)
{
    int64 val = settoint64(PG_GETARG_VARBIT_P(0));
    return DirectFunctionCall1(int8_numeric, Int64GetDatum(val));
}

Datum settobpchar(PG_FUNCTION_ARGS)
{
    Datum setlabels = DirectFunctionCall1(set_out, PG_GETARG_DATUM(0));
    int32 atttypmod = PG_GETARG_INT32(1);
    return DirectFunctionCall3(bpcharin, PointerGetDatum(setlabels),
        ObjectIdGetDatum(BPCHAROID), Int32GetDatum(atttypmod));
}

Datum settovarchar(PG_FUNCTION_ARGS)
{
    Datum setlabels = DirectFunctionCall1(set_out, PG_GETARG_DATUM(0));
    int32 atttypmod = PG_GETARG_INT32(1);
    return DirectFunctionCall3(varcharin, PointerGetDatum(setlabels),
        ObjectIdGetDatum(VARCHAROID), Int32GetDatum(atttypmod));
}

Datum settotext(PG_FUNCTION_ARGS)
{
    Datum setlabels = DirectFunctionCall1(set_out, PG_GETARG_DATUM(0));
    return DirectFunctionCall1(textin, PointerGetDatum(setlabels));
}

Datum settonvarchar2(PG_FUNCTION_ARGS)
{
    Datum setlabels = DirectFunctionCall1(set_out, PG_GETARG_DATUM(0));
    int32 atttypmod = PG_GETARG_INT32(1);
    return DirectFunctionCall3(nvarchar2in, PointerGetDatum(setlabels),
        ObjectIdGetDatum(NVARCHAR2OID), Int32GetDatum(atttypmod));
}

Datum i8toset(PG_FUNCTION_ARGS)
{
    return int64toset(PG_GETARG_INT64(0), PG_GETARG_OID(1));
}

Datum i4toset(PG_FUNCTION_ARGS)
{
    return int64toset(PG_GETARG_INT32(0), PG_GETARG_OID(1));
}

Datum i2toset(PG_FUNCTION_ARGS)
{
    return int64toset(PG_GETARG_INT16(0), PG_GETARG_OID(1));
}

Datum ftoset(PG_FUNCTION_ARGS)
{
    float4 val = PG_GETARG_FLOAT4(0);
    return int64toset(DirectFunctionCall1(ftoi8, Float4GetDatum(val)), PG_GETARG_OID(1));
}

Datum dtoset(PG_FUNCTION_ARGS)
{
    float8 val = PG_GETARG_FLOAT8(0);
    return int64toset(DirectFunctionCall1(dtoi8, Float8GetDatum(val)), PG_GETARG_OID(1));
}

Datum numbertoset(PG_FUNCTION_ARGS)
{
    Numeric val = PG_GETARG_NUMERIC(0);
    return int64toset(DirectFunctionCall1(numeric_int8, NumericGetDatum(val)), PG_GETARG_OID(1));
}

Datum bpchartoset(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    text* trimtxt = (text*)DirectFunctionCall1(rtrim1, txt);
    char* setlabels = TextDatumGetCString(trimtxt);
    Datum result = (Datum)get_set_in_result(PG_GETARG_OID(1), setlabels, PG_GET_COLLATION());
    pfree_ext(trimtxt);
    pfree_ext(setlabels);
    PG_RETURN_VARBIT_P(result);
}

Datum varchartoset(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* setlabels = DatumGetCString(DirectFunctionCall1(varcharout, txt));

    Datum result = (Datum)get_set_in_result(PG_GETARG_OID(1), setlabels, PG_GET_COLLATION());
    pfree_ext(setlabels);
    PG_RETURN_VARBIT_P(result);
}

Datum texttoset(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char *setlabels = DatumGetCString(DirectFunctionCall1(textout, txt));

    Datum result = (Datum)get_set_in_result(PG_GETARG_OID(1), setlabels, PG_GET_COLLATION());
    pfree_ext(setlabels);
    PG_RETURN_VARBIT_P(result);
}

Datum nvarchar2toset(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char *setlabels = DatumGetCString(DirectFunctionCall1(nvarchar2out, txt));

    Datum result = (Datum)get_set_in_result(PG_GETARG_OID(1), setlabels, PG_GET_COLLATION());
    pfree_ext(setlabels);
    PG_RETURN_VARBIT_P(result);
}

static int btsetfastcmp(Datum x, Datum y, SortSupport ssup)
{
    VarBit *d1 = DatumGetVarBitP(x);
    Oid settypid1 = *(Oid *)VARBITS(d1);

    VarBit *d2 = DatumGetVarBitP(y);
    Oid settypid2 = *(Oid *)VARBITS(d2);

    if (is_b_format_collation(ssup->ssup_collation)) {
        int result;
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, x));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, y));
        result = varstr_cmp_by_builtin_collations((char*)a, strlen(a), (char*)b, strlen(b), ssup->ssup_collation);
        return result;
    }

    /* compare by its value if arguments are the same set, otherwise by character order */
    if (settypid1 == settypid2) {
        int64 a = settoint64(d1);
        int64 b = settoint64(d2);
        if (a > b)
            return 1;
        else if (a == b)
            return 0;
        else
            return -1;
    } else {
        const char *a = DatumGetCString(DirectFunctionCall1(set_out, x));
        const char *b = DatumGetCString(DirectFunctionCall1(set_out, y));
        return strcmp(a, b);
    }
}

Datum btsetsortsupport(PG_FUNCTION_ARGS)
{
    SortSupport ssup = (SortSupport)PG_GETARG_POINTER(0);

    ssup->comparator = btsetfastcmp;
    PG_RETURN_VOID();
}

Datum findinset(PG_FUNCTION_ARGS)
{
    int count = 0;
    bool isnull = true;
    text *txt = PG_GETARG_TEXT_PP(0);
    FUNC_CHECK_HUGE_POINTER(PG_ARGISNULL(0), txt, "findinset()");

    char *label = text_to_cstring(txt);
    if (strstr(label, ",")) {
        /* invalid set label value */
        PG_RETURN_INT32(0);
    }

    VarBit *data = PG_GETARG_VARBIT_P(1);
    int typmod = VARBITLEN(data);
    if (typmod <= int(sizeof(Oid) * BITS_PER_BYTE)) {
        PG_RETURN_INT32(0);
    }

    Oid settypid = *(Oid *)VARBITS(data);
    if (settypid == InvalidOid) {
        PG_RETURN_INT32(0);
    }

    Relation pg_set = NULL;
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;

    pg_set = heap_open(SetRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_set_settypid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(settypid));

    bits8 *base = (bits8*)VARBITS(data) + sizeof(Oid);
    int1 bitlen = typmod - sizeof(Oid) * BITS_PER_BYTE;
    for (int1 order = 0; order < bitlen; order++) {
        bits8 *r = base + order / BITS_PER_BYTE;
        bool bitset = (*r) & (1 << (order % BITS_PER_BYTE));

        if (bitset) {
            ScanKeyInit(&key[1], Anum_pg_set_setsortorder, BTEqualStrategyNumber, F_INT1EQ, UInt8GetDatum(order));
            scan = systable_beginscan(pg_set, SetTypIdOrderIndexId, true, NULL, KEY_NUM, key);
            tup = systable_getnext(scan);
            if (!HeapTupleIsValid(tup)) {
                systable_endscan(scan);
                heap_close(pg_set, AccessShareLock);
                PG_RETURN_INT32(0);
            }

            count++;
            text* set = (text*)heap_getattr(tup, Anum_pg_set_setlabel, pg_set->rd_att, &isnull);
            char* setlabel = text_to_cstring(set);
            if (strcmp(label, setlabel) == 0) {
                pfree(setlabel);
                systable_endscan(scan);
                heap_close(pg_set, AccessShareLock);
                PG_RETURN_INT32(count);
            }

            pfree(setlabel);
            systable_endscan(scan);
        }
    }

    heap_close(pg_set, AccessShareLock);
    PG_RETURN_INT32(0);
}

static void get_set_result(Oid settypoid, VarBit **result, Form_pg_set settup)
{
    Oid *oid = NULL;

    if (*result == NULL) {
        int typmod = settup->setnum + sizeof(Oid) * BITS_PER_BYTE;
        int len = VARBITTOTALLEN(typmod);
        *result = (VarBit*)palloc0(len);
        SET_VARSIZE(*result, len);
        VARBITLEN(*result) = typmod;
    }

    /* place the typoid at the first 4 bytes */
    oid = (Oid *)VARBITS(*result);
    *oid = settypoid;
    bits8 *bitmap = (bits8 *)((char*)oid + sizeof(Oid) + (settup->setsortorder / BITS_PER_BYTE));
    (*bitmap) |= (1 << (settup->setsortorder % BITS_PER_BYTE));

}

static void process_set_label_by_collation(char *label, Oid settypoid, VarBit **result, Oid collation)
{
    Relation pg_set = NULL;
    HeapTuple tup = NULL;

    ScanKeyData key[1];
    SysScanDesc scan = NULL;
    int res = 0;
    bool find = false;
    bool isnull = true;

    pg_set = heap_open(SetRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_set_settypid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(settypoid));

    scan = systable_beginscan(pg_set, SetTypIdLabelIndexId, true, NULL, 1, key);
    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_set settup = (Form_pg_set)GETSTRUCT(tup);
        text* setlabel = (text*)heap_getattr(tup, Anum_pg_set_setlabel, pg_set->rd_att, &isnull);

        res = varstr_cmp_by_builtin_collations((char*)VARDATA_ANY(setlabel), VARSIZE_ANY_EXHDR(setlabel),
                                                label, strlen(label), collation);
        if (res == 0) {
            get_set_result(settypoid, result, settup);

            find = true;
            break;
        }
    }
    systable_endscan(scan);
    heap_close(pg_set, AccessShareLock);
    if (!find) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input value for set %s: '%s'", format_type_be(settypoid), label)));
    }
}

static void process_set_label(char *label, Oid settypoid, VarBit **result, Oid collation)
{
    Relation pg_set = NULL;
    HeapTuple tup = NULL;
    text *s = cstring_to_text(label);

    if (is_b_format_collation(collation)) {
        process_set_label_by_collation(label, settypoid, result, collation);
    } else {
        if (text_length(PointerGetDatum(s)) > SETNAMELEN) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input value for set %s: \"%s\"", format_type_be(settypoid), label)));
        }
        ScanKeyData key[2];
        SysScanDesc scan = NULL;
        pg_set = heap_open(SetRelationId, AccessShareLock);
        ScanKeyInit(&key[0], Anum_pg_set_settypid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(settypoid));
        ScanKeyInit(&key[1], Anum_pg_set_setlabel, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(s));

        scan = systable_beginscan(pg_set, SetTypIdLabelIndexId, true, NULL, KEY_NUM, key);

        tup = systable_getnext(scan);
        pfree_ext(s);

        if (!HeapTupleIsValid(tup)) {
            systable_endscan(scan);
            heap_close(pg_set, AccessShareLock);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input value for set %s: '%s'", format_type_be(settypoid), label)));
        }
        
        Form_pg_set settup = (Form_pg_set)GETSTRUCT(tup);
        get_set_result(settypoid, result, settup);

        systable_endscan(scan);
        heap_close(pg_set, AccessShareLock);
    }
}

static void preprocess_set_value(char *value, Oid settypoid, VarBit **result, Oid collation)
{
    bool hasEmpty = false;
    
    if (*value == '\0') {
        return;
    } else {
        int len = strlen(value);
        if (*value == ',' || *(value + len - 1) == ',') {
            hasEmpty = true;
        }

        if (strstr(value, ",,")) {
            hasEmpty = true;
        }
    }

    if (hasEmpty) {
        process_set_label("", settypoid, result, collation);
    }
}

static VarBit* get_set_in_result(Oid settypoid, char *setlabels, Oid collation)
{
    VarBit *result = NULL;
    char* next_token = NULL;
    char* labels = pstrdup(setlabels);

    preprocess_set_value(setlabels, settypoid, &result, collation);

    char* token = strtok_s(labels, SETLABELDELIMIT, &next_token);
    while (token != NULL) {
        process_set_label(token, settypoid, &result, collation);
        token = strtok_s(NULL, SETLABELDELIMIT, &next_token);
    }

    if (result == NULL) {
        int typmod = sizeof(Oid) * BITS_PER_BYTE;
        int len = VARBITTOTALLEN(typmod);
        result = (VarBit*)palloc(len);
        SET_VARSIZE(result, len);
        VARBITLEN(result) = typmod;
        *(Oid *)VARBITS(result) = settypoid;
    }

    pfree_ext(labels);
    return result;
}