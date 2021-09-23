/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * hll_function.cpp
 *    Realize the upper-layer functions of HLL algorithm.
 *
 * IDENTIFICATION
 * src/gausskernel/cbb/utils/hll/hll_function.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <postgres.h>

#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/hll.h"
#include "utils/hll_function.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"

static Datum hll_emptyn(FunctionCallInfo fcinfo);
static int32_t *ArrayGetTypmods(ArrayType *arr, int *n);

PG_FUNCTION_INFO_V1(hll);
PG_FUNCTION_INFO_V1(hll_in);
PG_FUNCTION_INFO_V1(hll_out);
PG_FUNCTION_INFO_V1(hll_recv);
PG_FUNCTION_INFO_V1(hll_send);
PG_FUNCTION_INFO_V1(hll_typmod_in);
PG_FUNCTION_INFO_V1(hll_typmod_out);
PG_FUNCTION_INFO_V1(hll_hashval_in);
PG_FUNCTION_INFO_V1(hll_hashval_out);
PG_FUNCTION_INFO_V1(hll_hashval);
PG_FUNCTION_INFO_V1(hll_hashval_int4);
PG_FUNCTION_INFO_V1(hll_hashval_eq);
PG_FUNCTION_INFO_V1(hll_hashval_ne);
PG_FUNCTION_INFO_V1(hll_hash_1byte);
PG_FUNCTION_INFO_V1(hll_hash_1bytes);
PG_FUNCTION_INFO_V1(hll_hash_2byte);
PG_FUNCTION_INFO_V1(hll_hash_2bytes);
PG_FUNCTION_INFO_V1(hll_hash_4byte);
PG_FUNCTION_INFO_V1(hll_hash_4bytes);
PG_FUNCTION_INFO_V1(hll_hash_8byte);
PG_FUNCTION_INFO_V1(hll_hash_8bytes);
PG_FUNCTION_INFO_V1(hll_hash_varlena);
PG_FUNCTION_INFO_V1(hll_hash_varlenas);
PG_FUNCTION_INFO_V1(hll_hash_any);
PG_FUNCTION_INFO_V1(hll_hash_anys);
PG_FUNCTION_INFO_V1(hll_eq);
PG_FUNCTION_INFO_V1(hll_ne);
PG_FUNCTION_INFO_V1(hll_add);
PG_FUNCTION_INFO_V1(hll_add_rev);
PG_FUNCTION_INFO_V1(hll_empty0);
PG_FUNCTION_INFO_V1(hll_empty1);
PG_FUNCTION_INFO_V1(hll_empty2);
PG_FUNCTION_INFO_V1(hll_empty3);
PG_FUNCTION_INFO_V1(hll_empty4);
PG_FUNCTION_INFO_V1(hll_union);
PG_FUNCTION_INFO_V1(hll_cardinality);
PG_FUNCTION_INFO_V1(hll_print);
PG_FUNCTION_INFO_V1(hll_type);
PG_FUNCTION_INFO_V1(hll_log2m);
PG_FUNCTION_INFO_V1(hll_log2explicit);
PG_FUNCTION_INFO_V1(hll_log2sparse);
PG_FUNCTION_INFO_V1(hll_duplicatecheck);

/* these functions are not supported again */
PG_FUNCTION_INFO_V1(hll_expthresh);
PG_FUNCTION_INFO_V1(hll_regwidth);
PG_FUNCTION_INFO_V1(hll_schema_version);
PG_FUNCTION_INFO_V1(hll_sparseon);

Datum hll(PG_FUNCTION_ARGS)
{
    MemoryContext ctx = CurrentMemoryContext;
    Hll hll(ctx);
    hll.HllInit();

    Datum hlldatum = PG_GETARG_DATUM(0);
    bytea *byteval = DatumGetByteaP(hlldatum);
    size_t hllsize = BYTEVAL_LENGTH(byteval);
    hll.HllObjectUnpack((uint8_t *)VARDATA(byteval), hllsize);

    int32_t typmod = PG_GETARG_INT32(1);
    HllPara hllpara;
    HllParaUnpack(typmod, &hllpara);
    HllCheckParaequal(*hll.HllGetPara(), hllpara);

    hll.HllFree();

    return hlldatum;
}

Datum hll_in(PG_FUNCTION_ARGS)
{
    MemoryContext ctx = CurrentMemoryContext;
    Hll hll(ctx);
    hll.HllInit();

    Datum hlldatum = DirectFunctionCall1(byteain, PG_GETARG_DATUM(0));
    bytea *byteval = DatumGetByteaP(hlldatum);
    size_t hllsize = BYTEVAL_LENGTH(byteval);
    hll.HllObjectUnpack((uint8_t *)VARDATA(byteval), hllsize);

    int32_t typmod = PG_GETARG_INT32(2);
    if (typmod != -1) {
        HllPara hllpara;
        HllParaUnpack(typmod, &hllpara);
        HllCheckParaequal(*hll.HllGetPara(), hllpara);
    }

    hll.HllFree();

    return hlldatum;
}

Datum hll_out(PG_FUNCTION_ARGS)
{
    return DirectFunctionCall1(byteaout, PG_GETARG_DATUM(0));
}

Datum hll_recv(PG_FUNCTION_ARGS)
{
    return DirectFunctionCall1(bytearecv, PG_GETARG_DATUM(0));
}

Datum hll_send(PG_FUNCTION_ARGS)
{
    bytea *byteval = PG_GETARG_BYTEA_P(0);
    StringInfoData buf;
    pq_begintypsend(&buf);
    pq_sendbytes(&buf, VARDATA(byteval), BYTEVAL_LENGTH(byteval));
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum hll_typmod_in(PG_FUNCTION_ARGS)
{
    ArrayType *ta = PG_GETARG_ARRAYTYPE_P(0);
    int n;
    int32_t *tl = ArrayGetTypmods(ta, &n);

    int32_t log2Registers = u_sess->attr.attr_sql.hll_default_log2m;
    int32_t log2Explicitsize = u_sess->attr.attr_sql.hll_default_log2explicit;
    int32_t log2Sparsesize = u_sess->attr.attr_sql.hll_default_log2sparse;
    int32_t duplicateCheck = u_sess->attr.attr_sql.hll_duplicate_check;
    switch (n) {
        case HLL_PARAMETER4:
            if (tl[3] != -1) {
                duplicateCheck = tl[3];
            }
        case HLL_PARAMETER3:
            if (tl[2] != -1) {
                log2Sparsesize = tl[2];
            }
        case HLL_PARAMETER2:
            if (tl[1] != -1) {
                log2Explicitsize = tl[1];
            }
        case HLL_PARAMETER1:
            if (tl[0] != -1) {
                log2Registers = tl[0];
            }
            break;
        default: /* can never reach here */
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid number of type modifiers")));
    }
    HllCheckPararange(log2Registers, log2Explicitsize, log2Sparsesize, duplicateCheck);

    HllPara hllpara;
    hllpara.log2Registers = (uint8_t)log2Registers;
    hllpara.log2Explicitsize = (uint8_t)log2Explicitsize;
    hllpara.log2Sparsesize = (uint8_t)log2Sparsesize;
    hllpara.duplicateCheck = (uint8_t)duplicateCheck;
    int32_t typmod = HllParaPack(hllpara);

    PG_RETURN_INT32(typmod);
}

Datum hll_typmod_out(PG_FUNCTION_ARGS)
{
    int32_t typmod = PG_GETARG_INT32(0);
    HllPara hllpara;
    HllParaUnpack(typmod, &hllpara);

    size_t len = 1024;
    char *typmodstr = (char *)palloc0(len);
    errno_t rc;

    rc = memset_s(typmodstr, len, '\0', len);
    securec_check(rc, "\0", "\0");
    rc = snprintf_s(typmodstr, len, len - 1, "(%d,%d,%d,%d)", hllpara.log2Registers, hllpara.log2Explicitsize,
        hllpara.log2Sparsesize, hllpara.duplicateCheck);
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_CSTRING(typmodstr);
}

Datum hll_hashval_in(PG_FUNCTION_ARGS)
{
    return DirectFunctionCall1(int8in, PG_GETARG_DATUM(0));
}

Datum hll_hashval_out(PG_FUNCTION_ARGS)
{
    return DirectFunctionCall1(int8out, PG_GETARG_DATUM(0));
}

Datum hll_hashval(PG_FUNCTION_ARGS)
{
    uint64_t hashval = PG_GETARG_INT64(0);
    PG_RETURN_INT64(hashval);
}

Datum hll_hashval_int4(PG_FUNCTION_ARGS)
{
    uint64_t hashval = (uint64_t)PG_GETARG_INT32(0);
    PG_RETURN_INT64(hashval);
}

Datum hll_hashval_eq(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(PG_GETARG_INT64(0) == PG_GETARG_INT64(1));
}

Datum hll_hashval_ne(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(PG_GETARG_INT64(0) != PG_GETARG_INT64(1));
}

/* hll hash function for char, seed is 0 */
Datum hll_hash_1byte(PG_FUNCTION_ARGS)
{
    int8_t value = PG_GETARG_INT8(0);

    PG_RETURN_INT64(HllHash(&value, sizeof(value), 0));
}

/* hll hash function for char, seed is set by input */
Datum hll_hash_1bytes(PG_FUNCTION_ARGS)
{
    int8_t value = PG_GETARG_INT8(0);
    int32_t seed = PG_GETARG_INT32(1);

    PG_RETURN_INT64(HllHash(&value, sizeof(value), seed));
}

/* hll hash function for short int, seed is 0 */
Datum hll_hash_2byte(PG_FUNCTION_ARGS)
{
    int16_t value = PG_GETARG_INT16(0);

    PG_RETURN_INT64(HllHash(&value, sizeof(value), 0));
}

/* hll hash function for short int, seed is set by input */
Datum hll_hash_2bytes(PG_FUNCTION_ARGS)
{
    int16_t value = PG_GETARG_INT16(0);
    int32_t seed = PG_GETARG_INT32(1);

    PG_RETURN_INT64(HllHash(&value, sizeof(value), seed));
}

/* hll hash function for int, seed is 0 */
Datum hll_hash_4byte(PG_FUNCTION_ARGS)
{
    int32_t value = PG_GETARG_INT32(0);

    PG_RETURN_INT64(HllHash(&value, sizeof(value), 0));
}

/* hll hash function for int, seed is set by input */
Datum hll_hash_4bytes(PG_FUNCTION_ARGS)
{
    int32_t value = PG_GETARG_INT32(0);
    int32_t seed = PG_GETARG_INT32(1);

    PG_RETURN_INT64(HllHash(&value, sizeof(value), seed));
}

/* hll hash function for long int, seed is 0 */
Datum hll_hash_8byte(PG_FUNCTION_ARGS)
{
    int64_t value = PG_GETARG_INT64(0);

    PG_RETURN_INT64(HllHash(&value, sizeof(value), 0));
}

/* hll hash function for long int, seed is set by input */
Datum hll_hash_8bytes(PG_FUNCTION_ARGS)
{
    int64_t value = PG_GETARG_INT64(0);
    int32_t seed = PG_GETARG_INT32(1);

    PG_RETURN_INT64(HllHash(&value, sizeof(value), seed));
}

/* hll hash function for text, seed is 0 */
Datum hll_hash_varlena(PG_FUNCTION_ARGS)
{
    varlena *vlap = PG_GETARG_VARLENA_PP(0);

    void *key = VARDATA_ANY(vlap);
    int len = VARSIZE_ANY_EXHDR(vlap);
    uint64_t hashval = HllHash(key, len, 0);

    PG_FREE_IF_COPY(vlap, 0);
    PG_RETURN_INT64(hashval);
}

/* hll hash function for text, seed is set by input */
Datum hll_hash_varlenas(PG_FUNCTION_ARGS)
{
    varlena *vlap = PG_GETARG_VARLENA_PP(0);
    int32_t seed = PG_GETARG_INT32(1);

    void *key = VARDATA_ANY(vlap);
    int len = VARSIZE_ANY_EXHDR(vlap);
    uint64_t hashval = HllHash(key, len, seed);

    PG_FREE_IF_COPY(vlap, 0);
    PG_RETURN_INT64(hashval);
}

/* hll hash function for any type, seed is 0 */
Datum hll_hash_any(PG_FUNCTION_ARGS)
{
    Datum inputDatum = PG_GETARG_DATUM(0);

    /* first we get the type */
    Oid typeoid = get_fn_expr_argtype(fcinfo->flinfo, 0);
    int16_t typelen = get_typlen(typeoid);
    /* Validate size specifications: either positive (fixed-length) or -1 (varlena) or -2 (cstring). */
    if (typelen > 0 || typelen == -1 || typelen == -2) {
        switch (typelen) {
            case 1:
                return DirectFunctionCall1(hll_hash_1byte, inputDatum);
            case 2:
                return DirectFunctionCall1(hll_hash_2byte, inputDatum);
            case 4:
                return DirectFunctionCall1(hll_hash_4byte, inputDatum);
            case 8:
                return DirectFunctionCall1(hll_hash_8byte, inputDatum);
            case -1:
                return DirectFunctionCall1(hll_hash_varlena, inputDatum);
            default:
                /* for other fixed-length/cstring, we get a variable-length binary representation */
                Oid typeSendFunction = InvalidOid;
                bool typeIsVarlena = false;
                getTypeBinaryOutputInfo(typeoid, &typeSendFunction, &typeIsVarlena);
                Datum binaryDatum = OidFunctionCall1(typeSendFunction, inputDatum);
                return DirectFunctionCall1(hll_hash_varlena, binaryDatum);
        }
    } else { /* typelen only can be -1/-2 or positive ,so can never reach here */
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("invalid type internal size %d", typelen)));
        return 0; /* keep compiler quiet */
    }
}

/* hll hash function for any type, seed is set by input */
Datum hll_hash_anys(PG_FUNCTION_ARGS)
{
    Datum inputDatum = PG_GETARG_DATUM(0);
    int32_t seed = PG_GETARG_INT32(1);

    /* first we get the type */
    Oid typeoid = get_fn_expr_argtype(fcinfo->flinfo, 0);
    int16_t typelen = get_typlen(typeoid);
    /* Validate size specifications: either positive (fixed-length) or -1 (varlena) or -2 (cstring). */
    if (typelen > 0 || typelen == -1 || typelen == -2) {
        switch (typelen) {
            case 1:
                return DirectFunctionCall2(hll_hash_1bytes, inputDatum, Int32GetDatum(seed));
            case 2:
                return DirectFunctionCall2(hll_hash_2bytes, inputDatum, Int32GetDatum(seed));
            case 4:
                return DirectFunctionCall2(hll_hash_4bytes, inputDatum, Int32GetDatum(seed));
            case 8:
                return DirectFunctionCall2(hll_hash_8bytes, inputDatum, Int32GetDatum(seed));
            case -1:
                return DirectFunctionCall2(hll_hash_varlenas, inputDatum, Int32GetDatum(seed));
            default:
                /* for other fixed-length/cstring, we get a variable-length binary representation */
                Oid typeSendFunction = InvalidOid;
                bool typeIsVarlena = false;
                getTypeBinaryOutputInfo(typeoid, &typeSendFunction, &typeIsVarlena);
                Datum binaryDatum = OidFunctionCall1(typeSendFunction, inputDatum);
                return DirectFunctionCall2(hll_hash_varlenas, binaryDatum, Int32GetDatum(seed));
        }
    } else { /* typelen only can be -1/-2 or positive ,so can never reach here */
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("invalid type internal size %d", typelen)));
        return 0; /* keep compiler quiet */
    }
}

Datum hll_eq(PG_FUNCTION_ARGS)
{
    Datum flag = byteacmp(fcinfo);
    bool iseq = (DatumGetInt32(flag) == 0);
    PG_RETURN_BOOL(iseq);
}

Datum hll_ne(PG_FUNCTION_ARGS)
{
    Datum flag = byteacmp(fcinfo);
    bool isne = (DatumGetInt32(flag) != 0);
    PG_RETURN_BOOL(isne);
}

/* add a hashval to hll, demo: hll_add(hll, hashval) */
Datum hll_add(PG_FUNCTION_ARGS)
{
    /* first create hllObject */
    MemoryContext ctx = CurrentMemoryContext;
    Hll hll(ctx);
    hll.HllInit();

    bytea *byteval1 = PG_GETARG_BYTEA_P(0);
    size_t hllsize1 = BYTEVAL_LENGTH(byteval1);
    hll.HllObjectUnpack((uint8_t *)VARDATA(byteval1), hllsize1);

    /* second add hashvalue */
    uint64_t hashvalue = (uint64_t)PG_GETARG_INT64(1);
    hll.HllAdd(hashvalue);

    /* third create new hll byteval, hllsize should allocate at least 1 more byte */
    size_t hllsize2 = HLL_HEAD_SIZE + hll.HllDatalen() + 1;
    bytea *byteval2 = (bytea *)palloc0(VARHDRSZ + hllsize2);
    SET_VARSIZE(byteval2, VARHDRSZ + hllsize2);
    hll.HllObjectPack((uint8_t *)VARDATA(byteval2), hllsize2);

    hll.HllFree();

    PG_RETURN_BYTEA_P(byteval2);
}

/* add a hashval to hll, demo: hll_add_rev(hashval, hll) */
Datum hll_add_rev(PG_FUNCTION_ARGS)
{
    /* first create hllObject */
    MemoryContext ctx = CurrentMemoryContext;
    Hll hll(ctx);
    hll.HllInit();

    bytea *byteval1 = PG_GETARG_BYTEA_P(1);
    size_t hllsize1 = BYTEVAL_LENGTH(byteval1);
    hll.HllObjectUnpack((uint8_t *)VARDATA(byteval1), hllsize1);

    /* second add hashvalue */
    uint64_t hashvalue = (uint64_t)PG_GETARG_INT64(0);
    hll.HllAdd(hashvalue);

    /* third create new hll byteval, hllsize should allocate at least 1 more byte */
    size_t hllsize2 = HLL_HEAD_SIZE + hll.HllDatalen() + 1;
    bytea *byteval2 = (bytea *)palloc0(VARHDRSZ + hllsize2);
    SET_VARSIZE(byteval2, VARHDRSZ + hllsize2);
    hll.HllObjectPack((uint8_t *)VARDATA(byteval2), hllsize2);

    hll.HllFree();

    PG_RETURN_BYTEA_P(byteval2);
}

Datum hll_empty0(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(hll_emptyn(fcinfo));
}

Datum hll_empty1(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(hll_emptyn(fcinfo));
}

Datum hll_empty2(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(hll_emptyn(fcinfo));
}

Datum hll_empty3(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(hll_emptyn(fcinfo));
}

Datum hll_empty4(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(hll_emptyn(fcinfo));
}

Datum hll_emptyn(FunctionCallInfo fcinfo)
{
    /* first get hllpara */
    int32_t log2Registers = u_sess->attr.attr_sql.hll_default_log2m;
    int32_t log2Explicitsize = u_sess->attr.attr_sql.hll_default_log2explicit;
    int32_t log2Sparsesize = u_sess->attr.attr_sql.hll_default_log2sparse;
    int32_t duplicateCheck = u_sess->attr.attr_sql.hll_duplicate_check;
    switch (PG_NARGS()) {
        case HLL_PARAMETER4:
            if (PG_GETARG_INT32(3) != -1) {
                duplicateCheck = PG_GETARG_INT32(3);
            }
        case HLL_PARAMETER3:
            if (PG_GETARG_INT32(2) != -1) {
                log2Sparsesize = (int32_t)PG_GETARG_INT64(2);
            }
        case HLL_PARAMETER2:
            if (PG_GETARG_INT32(1) != -1) {
                log2Explicitsize = PG_GETARG_INT32(1);
            }
        case HLL_PARAMETER1:
            if (PG_GETARG_INT32(0) != -1) {
                log2Registers = PG_GETARG_INT32(0);
            }
        case HLL_PARAMETER0:
            break;
        default: /* input number only can be 0~4, can never reach here */
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("hll_empty requires 0~5 arguments")));
    }
    HllCheckPararange(log2Registers, log2Explicitsize, log2Sparsesize, duplicateCheck);

    HllPara hllpara;
    hllpara.log2Registers = (uint8_t)log2Registers;
    hllpara.log2Explicitsize = (uint8_t)log2Explicitsize;
    hllpara.log2Sparsesize = (uint8_t)log2Sparsesize;
    hllpara.duplicateCheck = (uint8_t)duplicateCheck;

    /* then create hllObject */
    MemoryContext ctx = CurrentMemoryContext;
    Hll hll(ctx);
    hll.HllInit();
    hll.HllEmpty();
    hll.HllSetPara(hllpara);

    /* third create hll byteval, hllsize should allocate at least 1 more byte */
    size_t hllsize = HLL_HEAD_SIZE + hll.HllDatalen() + 1;
    bytea *byteval = (bytea *)palloc0(VARHDRSZ + hllsize);
    SET_VARSIZE(byteval, VARHDRSZ + hllsize);
    hll.HllObjectPack((uint8_t *)VARDATA(byteval), hllsize);

    hll.HllFree();

    PG_RETURN_BYTEA_P(byteval);
}

/* union two hll, demo: hll_union(hll1, hll2) */
Datum hll_union(PG_FUNCTION_ARGS)
{
    MemoryContext ctx = CurrentMemoryContext;
    Hll hll1(ctx);
    hll1.HllInit();
    bytea *byteval1 = PG_GETARG_BYTEA_P(0);
    size_t hllsize1 = BYTEVAL_LENGTH(byteval1);
    hll1.HllObjectUnpack((uint8_t *)VARDATA(byteval1), hllsize1);

    Hll hll2(ctx);
    hll2.HllInit();
    bytea *byteval2 = PG_GETARG_BYTEA_P(1);
    size_t hllsize2 = BYTEVAL_LENGTH(byteval2);
    hll2.HllObjectUnpack((uint8_t *)VARDATA(byteval2), hllsize2);

    hll1.HllUnion(hll2);

    /* create hll byteval, hllsize should allocate at least 1 more byte */
    size_t hllsize3 = HLL_HEAD_SIZE + hll1.HllDatalen() + 1;
    bytea *byteval3 = (bytea *)palloc0(VARHDRSZ + hllsize3);
    SET_VARSIZE(byteval3, VARHDRSZ + hllsize3);
    hll1.HllObjectPack((uint8_t *)VARDATA(byteval3), hllsize3);

    hll1.HllFree();
    hll2.HllFree();

    PG_RETURN_BYTEA_P(byteval3);
}

/* calculate NDV of a hll */
Datum hll_cardinality(PG_FUNCTION_ARGS)
{
    MemoryContext ctx = CurrentMemoryContext;
    Hll hll(ctx);
    hll.HllInit();

    bytea *byteval = PG_GETARG_BYTEA_P(0);
    size_t hllsize = BYTEVAL_LENGTH(byteval);
    hll.HllObjectUnpack((uint8_t *)VARDATA(byteval), hllsize);

    double ndv = hll.HllCardinality();
    hll.HllFree();

    if (ndv == -1.0) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_FLOAT8(ndv);
    }
}

Datum hll_print(PG_FUNCTION_ARGS)
{
    MemoryContext ctx = CurrentMemoryContext;
    Hll hll(ctx);
    hll.HllInit();

    bytea *byteval = PG_GETARG_BYTEA_P(0);
    size_t hllsize = BYTEVAL_LENGTH(byteval);
    hll.HllObjectUnpack((uint8_t *)VARDATA(byteval), hllsize);

    Datum hllinfo = CStringGetDatum(hll.HllPrint());
    hll.HllFree();

    PG_RETURN_DATUM(hllinfo);
}

Datum hll_type(PG_FUNCTION_ARGS)
{
    MemoryContext ctx = CurrentMemoryContext;
    Hll hll(ctx);
    hll.HllInit();

    bytea *byteval = PG_GETARG_BYTEA_P(0);
    size_t hllsize = BYTEVAL_LENGTH(byteval);
    hll.HllObjectUnpack((uint8_t *)VARDATA(byteval), hllsize);
    HllType type = hll.HllGetType();
    hll.HllFree();

    PG_RETURN_INT32(type);
}

Datum hll_log2m(PG_FUNCTION_ARGS)
{
    MemoryContext ctx = CurrentMemoryContext;
    Hll hll(ctx);
    hll.HllInit();

    bytea *byteval = PG_GETARG_BYTEA_P(0);
    size_t hllsize = BYTEVAL_LENGTH(byteval);
    hll.HllObjectUnpack((uint8_t *)VARDATA(byteval), hllsize);
    uint8_t log2m = hll.HllRegisters();
    hll.HllFree();

    PG_RETURN_UINT8(log2m);
}

Datum hll_log2explicit(PG_FUNCTION_ARGS)
{
    MemoryContext ctx = CurrentMemoryContext;
    Hll hll(ctx);
    hll.HllInit();

    bytea *byteval = PG_GETARG_BYTEA_P(0);
    size_t hllsize = BYTEVAL_LENGTH(byteval);
    hll.HllObjectUnpack((uint8_t *)VARDATA(byteval), hllsize);
    uint8_t log2explicit = hll.HllExplicitsize();
    hll.HllFree();

    PG_RETURN_UINT8(log2explicit);
}

Datum hll_log2sparse(PG_FUNCTION_ARGS)
{
    MemoryContext ctx = CurrentMemoryContext;
    Hll hll(ctx);
    hll.HllInit();

    bytea *byteval = PG_GETARG_BYTEA_P(0);
    size_t hllsize = BYTEVAL_LENGTH(byteval);
    hll.HllObjectUnpack((uint8_t *)VARDATA(byteval), hllsize);
    uint8_t log2sparse = hll.HllSparsesize();
    hll.HllFree();

    PG_RETURN_UINT8(log2sparse);
}

Datum hll_duplicatecheck(PG_FUNCTION_ARGS)
{
    MemoryContext ctx = CurrentMemoryContext;
    Hll hll(ctx);
    hll.HllInit();

    bytea *byteval = PG_GETARG_BYTEA_P(0);
    size_t hllsize = BYTEVAL_LENGTH(byteval);
    hll.HllObjectUnpack((uint8_t *)VARDATA(byteval), hllsize);
    bool duplicatecheck = hll.HllDuplicateCheck();
    hll.HllFree();

    PG_RETURN_BOOL(duplicatecheck);
}

static int32_t *ArrayGetTypmods(ArrayType *arr, int *n)
{
    if (ARR_ELEMTYPE(arr) != CSTRINGOID) {
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("typmod array must be type cstring[]")));
    }
    if (ARR_NDIM(arr) != 1) {
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("typmod array must be one-dimensional")));
    }
    if (array_contains_nulls(arr)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("typmod array must not contain nulls")));
    }

    Datum *elem_values = NULL;
    /* hardwired knowledge about cstring's representation details here */
    deconstruct_array(arr, CSTRINGOID, -2, false, 'c', &elem_values, NULL, n);

    int32_t *result = NULL;
    result = (int32_t *)palloc0(sizeof(int32_t) * *n);
    for (int i = 0; i < *n; i++) {
        result[i] = pg_strtoint32(DatumGetCString(elem_values[i]));
    }

    pfree(elem_values);
    return result;
}

/* these functions are not supported again */
Datum hll_expthresh(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("function hll_expthresh is no longer supported, we use hll_log2explicit instead")));
    PG_RETURN_DATUM(0);
}

Datum hll_regwidth(PG_FUNCTION_ARGS)
{
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("function hll_regwidth is no longer supported, we set it as a "
        "const value, see hll_print for more hll information")));
    PG_RETURN_DATUM(0);
}

Datum hll_schema_version(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("function hll_schema_version is no longer supported, see hll_print for more hll information")));
    PG_RETURN_DATUM(0);
}

Datum hll_sparseon(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("function hll_sparseson is no longer supported, we use hll_log2sparse instead")));
    PG_RETURN_DATUM(0);
}
