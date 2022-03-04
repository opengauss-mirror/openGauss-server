/* -------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2002-2007, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * fmgr_comp.h
 * Definitions for the fmgr-compatible function
 *
 * src/include/fmgr/fmgr_comp.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef FMGRCOMP_H
#define FMGRCOMP_H

#include "fmgr/fmgr_core.h"

/* -------------------------------------------------------------------------
 *		Support macros to ease writing fmgr-compatible functions
 *
 * A C-coded fmgr-compatible function should be declared as
 *
 *		Datum
 *		function_name(PG_FUNCTION_ARGS)
 *		{
 *			...
 *		}
 *
 * It should access its arguments using appropriate PG_GETARG_xxx macros
 * and should return its result using PG_RETURN_xxx.
 *
 * -------------------------------------------------------------------------
 */

/* Standard parameter list for fmgr-compatible functions */
#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo

/*
 * Get collation function should use.
 */
#define PG_GET_COLLATION() (fcinfo->fncollation)

/*
 * Get number of arguments passed to function.
 */
#define PG_NARGS() (fcinfo->nargs)

/*
 * If function is not marked "proisstrict" in pg_proc, it must check for
 * null arguments using this macro.  Do not try to GETARG a null argument!
 */
#define PG_ARGISNULL(n) (fcinfo->argnull[n])

#ifndef FRONTEND_PARSER
/*
 * Support for fetching detoasted copies of toastable datatypes (all of
 * which are varlena types).  pg_detoast_datum() gives you either the input
 * datum (if not toasted) or a detoasted copy allocated with palloc().
 * pg_detoast_datum_copy() always gives you a palloc'd copy --- use it
 * if you need a modifiable copy of the input.	Caller is expected to have
 * checked for null inputs first, if necessary.
 *
 * pg_detoast_datum_packed() will return packed (1-byte header) datums
 * unmodified.	It will still expand an externally toasted or compressed datum.
 * The resulting datum can be accessed using VARSIZE_ANY() and VARDATA_ANY()
 * (beware of multiple evaluations in those macros!)
 *
 * WARNING: It is only safe to use pg_detoast_datum_packed() and
 * VARDATA_ANY() if you really don't care about the alignment. Either because
 * you're working with something like text where the alignment doesn't matter
 * or because you're not going to access its constituent parts and just use
 * things like memcpy on it anyways.
 *
 * Note: it'd be nice if these could be macros, but I see no way to do that
 * without evaluating the arguments multiple times, which is NOT acceptable.
 */
extern struct varlena* pg_detoast_datum(struct varlena* datum);
extern struct varlena* pg_detoast_datum_copy(struct varlena* datum);
extern struct varlena* pg_detoast_datum_slice(struct varlena* datum, int64 first, int32 count);
extern struct varlena* pg_detoast_datum_packed(struct varlena* datum);

#define PG_DETOAST_DATUM(datum) pg_detoast_datum((struct varlena*)DatumGetPointer(datum))
#define PG_DETOAST_DATUM_COPY(datum) pg_detoast_datum_copy((struct varlena*)DatumGetPointer(datum))
#define PG_DETOAST_DATUM_SLICE(datum, f, c) \
    pg_detoast_datum_slice((struct varlena*)DatumGetPointer(datum), (int64)(f), (int32)(c))
/* WARNING -- unaligned pointer */
#define PG_DETOAST_DATUM_PACKED(datum) pg_detoast_datum_packed((struct varlena*)DatumGetPointer(datum))

/*
 * Support for cleaning up detoasted copies of inputs.	This must only
 * be used for pass-by-ref datatypes, and normally would only be used
 * for toastable types.  If the given pointer is different from the
 * original argument, assume it's a palloc'd detoasted copy, and pfree it.
 * NOTE: most functions on toastable types do not have to worry about this,
 * but we currently require that support functions for indexes not leak
 * memory.
 */
#define PG_FREE_IF_COPY(ptr, n)                     \
    do {                                            \
        if ((Pointer)(ptr) != PG_GETARG_POINTER(n)) \
            pfree(ptr);                             \
    } while (0)

/* Macros for fetching arguments of standard types */

#define PG_GETARG_DATUM(n) (fcinfo->arg[n])
#define PG_GETARG_INT32(n) DatumGetInt32(PG_GETARG_DATUM(n))
#define PG_GETARG_UINT32(n) DatumGetUInt32(PG_GETARG_DATUM(n))
#define PG_GETARG_INT16(n) DatumGetInt16(PG_GETARG_DATUM(n))
#define PG_GETARG_UINT16(n) DatumGetUInt16(PG_GETARG_DATUM(n))
#define PG_GETARG_INT8(n) DatumGetInt8(PG_GETARG_DATUM(n))
#define PG_GETARG_UINT8(n) DatumGetUInt8(PG_GETARG_DATUM(n))
#define PG_GETARG_CHAR(n) DatumGetChar(PG_GETARG_DATUM(n))
#define PG_GETARG_BOOL(n) DatumGetBool(PG_GETARG_DATUM(n))
#define PG_GETARG_OID(n) DatumGetObjectId(PG_GETARG_DATUM(n))
#define PG_GETARG_POINTER(n) DatumGetPointer(PG_GETARG_DATUM(n))
#define PG_GETARG_CSTRING(n) DatumGetCString(PG_GETARG_DATUM(n))
#define PG_GETARG_NAME(n) DatumGetName(PG_GETARG_DATUM(n))
/* these macros hide the pass-by-reference-ness of the datatype: */
#define PG_GETARG_FLOAT4(n) DatumGetFloat4(PG_GETARG_DATUM(n))
#define PG_GETARG_FLOAT8(n) DatumGetFloat8(PG_GETARG_DATUM(n))
#define PG_GETARG_INT64(n) DatumGetInt64(PG_GETARG_DATUM(n))
#define PG_GETARG_INT128(n) DatumGetInt128(PG_GETARG_DATUM(n))
#define PG_GETARG_TRANSACTIONID(n) DatumGetTransactionId(PG_GETARG_DATUM(n))
#define PG_GETARG_SHORTTRANSACTIONID(n) DatumGetShortTransactionId(PG_GETARG_DATUM(n))
/* use this if you want the raw, possibly-toasted input datum: */
#define PG_GETARG_RAW_VARLENA_P(n) ((struct varlena*)PG_GETARG_POINTER(n))
/* use this if you want the input datum de-toasted: */
#define PG_GETARG_VARLENA_P(n) PG_DETOAST_DATUM(PG_GETARG_DATUM(n))
/* and this if you can handle 1-byte-header datums: */
#define PG_GETARG_VARLENA_PP(n) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(n))
/* DatumGetFoo macros for varlena types will typically look like this: */
#define DatumGetByteaP(X) ((bytea*)PG_DETOAST_DATUM(X))
#define DatumGetByteaPP(X) ((bytea*)PG_DETOAST_DATUM_PACKED(X))
#define DatumGetTextP(X) ((text*)PG_DETOAST_DATUM(X))
#define DatumGetTextPP(X) ((text*)PG_DETOAST_DATUM_PACKED(X))
#define DatumGetBpCharP(X) ((BpChar*)PG_DETOAST_DATUM(X))
#define DatumGetBpCharPP(X) ((BpChar*)PG_DETOAST_DATUM_PACKED(X))
#define DatumGetVarCharP(X) ((VarChar*)PG_DETOAST_DATUM(X))
#define DatumGetVarCharPP(X) ((VarChar*)PG_DETOAST_DATUM_PACKED(X))
#define DatumGetNVarChar2PP(X) ((NVarChar2*)PG_DETOAST_DATUM_PACKED(X))
#define DatumGetHeapTupleHeader(X) ((HeapTupleHeader)PG_DETOAST_DATUM(X))
#define DatumGetEncryptedcolP(X) ((byteawithoutorderwithequalcol *) PG_DETOAST_DATUM(X))
#define DatumGetEncryptedcolPP(X) ((byteawithoutorderwithequalcol *) PG_DETOAST_DATUM_PACKED(X))
/* And we also offer variants that return an OK-to-write copy */
#define DatumGetByteaPCopy(X) ((bytea*)PG_DETOAST_DATUM_COPY(X))
#define DatumGetEncryptedcolPCopy(X) ((byteawithoutorderwithequalcol *) PG_DETOAST_DATUM_COPY(X))
#define DatumGetTextPCopy(X) ((text*)PG_DETOAST_DATUM_COPY(X))
#define DatumGetBpCharPCopy(X) ((BpChar*)PG_DETOAST_DATUM_COPY(X))
#define DatumGetVarCharPCopy(X) ((VarChar*)PG_DETOAST_DATUM_COPY(X))
#define DatumGetHeapTupleHeaderCopy(X) ((HeapTupleHeader)PG_DETOAST_DATUM_COPY(X))
/* Variants which return n bytes starting at pos. m */
#define DatumGetByteaPSlice(X, m, n) ((bytea*)PG_DETOAST_DATUM_SLICE(X, m, n))
#define DatumGetTextPSlice(X, m, n) ((text*)PG_DETOAST_DATUM_SLICE(X, m, n))
#define DatumGetBpCharPSlice(X, m, n) ((BpChar*)PG_DETOAST_DATUM_SLICE(X, m, n))
#define DatumGetVarCharPSlice(X, m, n) ((VarChar*)PG_DETOAST_DATUM_SLICE(X, m, n))
#define DatumGetEncryptedcolPSlice(X, m, n) ((byteawithoutorderwithequalcol *)PG_DETOAST_DATUM_SLICE(X, m, n))
/* GETARG macros for varlena types will typically look like this: */
#define PG_GETARG_BYTEA_P(n) DatumGetByteaP(PG_GETARG_DATUM(n))
#define PG_GETARG_BYTEA_PP(n) DatumGetByteaPP(PG_GETARG_DATUM(n))
#define PG_GETARG_TEXT_P(n) DatumGetTextP(PG_GETARG_DATUM(n))
#define PG_GETARG_TEXT_PP(n) DatumGetTextPP(PG_GETARG_DATUM(n))
#define PG_GETARG_BPCHAR_P(n) DatumGetBpCharP(PG_GETARG_DATUM(n))
#define PG_GETARG_BPCHAR_PP(n) DatumGetBpCharPP(PG_GETARG_DATUM(n))
#define PG_GETARG_VARCHAR_P(n) DatumGetVarCharP(PG_GETARG_DATUM(n))
#define PG_GETARG_VARCHAR_PP(n) DatumGetVarCharPP(PG_GETARG_DATUM(n))
#define PG_GETARG_NVARCHAR2_PP(n) DatumGetNVarChar2PP(PG_GETARG_DATUM(n))
#define PG_GETARG_HEAPTUPLEHEADER(n) DatumGetHeapTupleHeader(PG_GETARG_DATUM(n))
#define PG_GETARG_ENCRYPTEDCOL_P(n) DatumGetEncryptedcolP(PG_GETARG_DATUM(n))
#define PG_GETARG_ENCRYPTEDCOL_PP(n) DatumGetEncryptedcolPP(PG_GETARG_DATUM(n))
/* And we also offer variants that return an OK-to-write copy */
#define PG_GETARG_BYTEA_P_COPY(n) DatumGetByteaPCopy(PG_GETARG_DATUM(n))
#define PG_GETARG_TEXT_P_COPY(n) DatumGetTextPCopy(PG_GETARG_DATUM(n))
#define PG_GETARG_BPCHAR_P_COPY(n) DatumGetBpCharPCopy(PG_GETARG_DATUM(n))
#define PG_GETARG_VARCHAR_P_COPY(n) DatumGetVarCharPCopy(PG_GETARG_DATUM(n))
#define PG_GETARG_HEAPTUPLEHEADER_COPY(n) DatumGetHeapTupleHeaderCopy(PG_GETARG_DATUM(n))
#define PG_GETARG_ENCRYPTEDCOL_P_COPY(n) DatumGetEncryptedcolPCopy(PG_GETARG_DATUM(n))
/* And a b-byte slice from position a -also OK to write */
#define PG_GETARG_BYTEA_P_SLICE(n, a, b) DatumGetByteaPSlice(PG_GETARG_DATUM(n), a, b)
#define PG_GETARG_ENCRYPTEDCOL_P_SLICE(n, a, b) DatumGetEncryptedcolPSlice(PG_GETARG_DATUM(n), a, b)
#define PG_GETARG_TEXT_P_SLICE(n, a, b) DatumGetTextPSlice(PG_GETARG_DATUM(n), a, b)
#define PG_GETARG_BPCHAR_P_SLICE(n, a, b) DatumGetBpCharPSlice(PG_GETARG_DATUM(n), a, b)
#define PG_GETARG_VARCHAR_P_SLICE(n, a, b) DatumGetVarCharPSlice(PG_GETARG_DATUM(n), a, b)

// Vectorized getters
//
#define PG_GETARG_VECTOR(n) ((ScalarVector*)(PG_GETARG_DATUM(n)))
#define PG_GETARG_SELECTION(n) ((bool*)(PG_GETARG_DATUM(n)))
#define PG_GETARG_VECVAL(n) ((ScalarValue*)(PG_GETARG_VECTOR(n)->m_vals))

/* To return a NULL do this: */
#define PG_RETURN_NULL()       \
    do {                       \
        fcinfo->isnull = true; \
        return (Datum)0;       \
    } while (0)

/* A few internal functions return void (which is not the same as NULL!) */
#define PG_RETURN_VOID() return (Datum)0

/* Macros for returning results of standard types */

#define PG_RETURN_DATUM(x) return (x)
#define PG_RETURN_INT32(x) return Int32GetDatum(x)
#define PG_RETURN_UINT32(x) return UInt32GetDatum(x)
#define PG_RETURN_INT16(x) return Int16GetDatum(x)
#define PG_RETURN_UINT16(x) return UInt16GetDatum(x)
#define PG_RETURN_UINT8(x) return UInt8GetDatum(x)
#define PG_RETURN_CHAR(x) return CharGetDatum(x)
#define PG_RETURN_BOOL(x) return BoolGetDatum(x)
#define PG_RETURN_OID(x) return ObjectIdGetDatum(x)
#define PG_RETURN_POINTER(x) return PointerGetDatum(x)
#define PG_RETURN_CSTRING(x) return CStringGetDatum(x)
#define PG_RETURN_NAME(x) return NameGetDatum(x)
/* these macros hide the pass-by-reference-ness of the datatype: */
#define PG_RETURN_FLOAT4(x) return Float4GetDatum(x)
#define PG_RETURN_FLOAT8(x) return Float8GetDatum(x)
#define PG_RETURN_INT64(x) return Int64GetDatum(x)
#define PG_RETURN_INT128(x) return Int128GetDatum(x)
#define PG_RETURN_TRANSACTIONID(x) return TransactionIdGetDatum(x)
#define PG_RETURN_SHORTTRANSACTIONID(x) return ShortTransactionIdGetDatum(x)
/* RETURN macros for other pass-by-ref types will typically look like this: */
#define PG_RETURN_BYTEA_P(x) PG_RETURN_POINTER(x)
#define PG_RETURN_ENCRYPTEDCOL_P(x) PG_RETURN_POINTER(x)
#define PG_RETURN_TEXT_P(x) PG_RETURN_POINTER(x)
#define PG_RETURN_BPCHAR_P(x) PG_RETURN_POINTER(x)
#define PG_RETURN_VARCHAR_P(x) PG_RETURN_POINTER(x)
#define PG_RETURN_HEAPTUPLEHEADER(x) PG_RETURN_POINTER(x)
#define PG_RETURN_NVARCHAR2_P(x) PG_RETURN_POINTER(x)

typedef enum COMM_ARG_IDX {
    ARG_0,
    ARG_1,
    ARG_2,
    ARG_3,
    ARG_4,
    ARG_5,
    ARG_6,
    ARG_7,
    ARG_8,
    ARG_9,
    ARG_10,
    ARG_11,
    ARG_12,
    ARG_13,
    ARG_14,
    ARG_15,
    ARG_16,
    ARG_17,
    ARG_18,
    ARG_19,
    ARG_20,
    ARG_21,
    ARG_22,
    ARG_END
} COMM_ARG_IDX;

typedef enum COMM_ARR_IDX {
    ARR_0,
    ARR_1,
    ARR_2,
    ARR_3,
    ARR_4,
    ARR_5,
    ARR_6,
    ARR_7,
    ARR_8,
    ARR_9,
    ARR_10,
    ARR_11,
    ARR_12,
    ARR_13,
    ARR_14,
    ARR_15,
    ARR_16,
    ARR_17,
    ARR_18,
    ARR_19,
    ARR_20,
    ARR_21,
    ARR_22,
    ARR_END
} COMM_ARR_IDX;

/*
 * !!! OLD INTERFACE !!!
 *
 * fmgr() is the only remaining vestige of the old-style caller support
 * functions.  It's no longer used anywhere in the openGauss distribution,
 * but we should leave it around for a release or two to ease the transition
 * for user-supplied C functions.  OidFunctionCallN() replaces it for new
 * code.
 */

/*
 * DEPRECATED, DO NOT USE IN NEW CODE
 */
extern char* fmgr(Oid procedureId, ...);

extern Datum DirectCall0(bool* isRetNull, PGFunction func, Oid collation);
extern Datum DirectCall1(bool* isRetNull, PGFunction func, Oid collation, Datum arg1);
extern Datum DirectCall2(bool* isRetNull, PGFunction func, Oid collation, Datum arg1, Datum arg2);
extern Datum DirectCall3(bool* isRetNull, PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3);
extern Datum interval_in(PG_FUNCTION_ARGS);

#define CHECK_RETNULL_RETURN_DATUM(x) \
    do {                              \
        _datum = (x);                 \
        if (_isRetNull)               \
            PG_RETURN_NULL();         \
        else                          \
            return _datum;            \
    } while (0)
#define CHECK_RETNULL_CALL0(func, collation) DirectCall0(&_isRetNull, func, collation)
#define CHECK_RETNULL_CALL1(func, collation, arg1) DirectCall1(&_isRetNull, func, collation, arg1)
#define CHECK_RETNULL_CALL2(func, collation, arg1, arg2) DirectCall2(&_isRetNull, func, collation, arg1, arg2)
#define CHECK_RETNULL_CALL3(func, collation, arg1, arg2, arg3) \
    DirectCall3(&_isRetNull, func, collation, arg1, arg2, arg3)
#define CHECK_RETNULL_INIT() \
    Datum _datum;            \
    bool _isRetNull = false

#endif /* !FRONTEND_PARSER */

#endif /* FMGRCOMP_H */
