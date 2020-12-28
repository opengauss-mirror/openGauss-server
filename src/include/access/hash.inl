/*-------------------------------------------------------------------------
 *
 * hash.inl
 *	  header file for hash template implementation
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/hash.inl
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef HASHTEMPLATE_INL
#define HASHTEMPLATE_INL

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"

#ifdef PGXC
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/date.h"
#include "utils/nabstime.h"
#endif

template<Oid type, char locator, bool ifCol>
Datum
computeHashT(Datum value)
{
	uint8	tmp8;
	int16	tmp16;
	int32	tmp32;
	int64	tmp64;
	Oid		tmpoid;
	char	tmpch;

	switch (type)
	{
		case INT8OID:
			/* This gives added advantage that
			 *	a = 8446744073709551359
			 * and	a = 8446744073709551359::int8 both work*/
			tmp64 = DatumGetInt64(value);
			if (locator == LOCATOR_TYPE_HASH)
				return DirectFunctionCall1(hashint8, value);
			return tmp64;
		case INT1OID:
			tmp8 = DatumGetUInt8(value);
			if (locator == LOCATOR_TYPE_HASH)
				return DirectFunctionCall1(hashint1, tmp8);
			return tmp8;
		case INT2OID:
			tmp16 = DatumGetInt16(value);
			if (locator == LOCATOR_TYPE_HASH)
				return DirectFunctionCall1(hashint2, tmp16);
			return tmp16;
		case OIDOID:
			tmpoid = DatumGetObjectId(value);
			if (locator == LOCATOR_TYPE_HASH)
				return DirectFunctionCall1(hashoid, tmpoid);
			return tmpoid;
		case INT4OID:
			tmp32 = DatumGetInt32(value);
			if (locator == LOCATOR_TYPE_HASH)
				return DirectFunctionCall1(hashint4, tmp32);
			return tmp32;
		case BOOLOID:
			tmpch = DatumGetBool(value);
			if (locator == LOCATOR_TYPE_HASH)
				return DirectFunctionCall1(hashchar, tmpch);
			return tmpch;

		case CHAROID:
			return DirectFunctionCall1(hashchar, value);
		case NAMEOID:
			return DirectFunctionCall1(hashname, value);
		case INT2VECTOROID:
			return DirectFunctionCall1(hashint2vector, value);

		case CLOBOID:
		case NVARCHAR2OID:
		case VARCHAROID:
		case TEXTOID:
			return DirectFunctionCall1(hashtext, value);

		case OIDVECTOROID:
			return DirectFunctionCall1(hashoidvector, value);
		case FLOAT4OID:
			return DirectFunctionCall1(hashfloat4, value);
		case FLOAT8OID:
			return DirectFunctionCall1(hashfloat8, value);

		case ABSTIMEOID:
			tmp32 = DatumGetAbsoluteTime(value);
			if (locator == LOCATOR_TYPE_HASH)
				return DirectFunctionCall1(hashint4, tmp32);
			return tmp32;
		case RELTIMEOID:
			tmp32 = DatumGetRelativeTime(value);
			if (locator == LOCATOR_TYPE_HASH)
				return DirectFunctionCall1(hashint4, tmp32);
			return tmp32;
		case CASHOID:
			return DirectFunctionCall1(hashint8, value);

		case BPCHAROID:
			return DirectFunctionCall1(hashbpchar, value);
		case BYTEAWITHOUTORDERWITHEQUALCOLOID:
		case BYTEAWITHOUTORDERCOLOID:
		case RAWOID:
		case BYTEAOID:
			return DirectFunctionCall1(hashvarlena, value);

		case DATEOID:
			tmp32 = DatumGetDateADT(value);
			if (locator == LOCATOR_TYPE_HASH)
				return DirectFunctionCall1(hashint4, tmp32);
			return tmp32;
		case TIMEOID:
			return DirectFunctionCall1(time_hash, value);
		case TIMESTAMPOID:
			return DirectFunctionCall1(timestamp_hash, value);
		case TIMESTAMPTZOID:
			return DirectFunctionCall1(timestamp_hash, value);
		case INTERVALOID:
			if (ifCol)
				value = PointerGetDatum((char*)value + VARHDRSZ_SHORT);
			return DirectFunctionCall1(interval_hash, value);
		case TIMETZOID:
			if (ifCol)
				value = PointerGetDatum((char*)value + VARHDRSZ_SHORT);
			return DirectFunctionCall1(timetz_hash, value);
		case SMALLDATETIMEOID:
			return DirectFunctionCall1(timestamp_hash, value);

		case NUMERICOID:
			return DirectFunctionCall1(hash_numeric, value);
		case UUIDOID:
			return DirectFunctionCall1(uuid_hash, value);
		default:
			ereport(ERROR,(errcode(ERRCODE_INDETERMINATE_DATATYPE),
					errmsg("Unhandled datatype for modulo or hash distribution\n")));
	}
	/* Control should not come here. */
	ereport(ERROR,(errcode(ERRCODE_INDETERMINATE_DATATYPE),
			errmsg("Unhandled datatype for modulo or hash distribution\n")));
	/* Keep compiler silent */
	return (Datum)0;
}

#endif   /* HASH_H */
