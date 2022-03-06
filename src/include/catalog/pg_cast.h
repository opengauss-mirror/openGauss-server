/* -------------------------------------------------------------------------
 *
 * pg_cast.h
 *	  definition of the system "type casts" relation (pg_cast)
 *	  along with the relation's initial contents.
 *
 * As of Postgres 8.0, pg_cast describes not only type coercion functions
 * but also length coercion functions.
 *
 *
 * Copyright (c) 2002-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/catalog/pg_cast.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_CAST_H
#define PG_CAST_H

#include "catalog/genbki.h"
#include "catalog/pg_authid.h"

/* ----------------
 *		pg_cast definition.  cpp turns this into
 *		typedef struct FormData_pg_cast
 * ----------------
 */
#define CastRelationId	2605
#define CastRelation_Rowtype_Id  11331
#define InvalidCastOwnerId ((Oid)(-1)) /* An invalid cast owner oid, for old version pg_cast */

CATALOG(pg_cast,2605) BKI_SCHEMA_MACRO
{
	Oid			castsource;		/* source datatype for cast */
	Oid			casttarget;		/* destination datatype for cast */
	Oid			castfunc;		/* cast function; 0 = binary coercible */
	char		castcontext;	/* contexts in which cast can be used */
	char		castmethod;		/* cast method */
    Oid			castowner;		/* cast owner */
} FormData_pg_cast;

typedef FormData_pg_cast *Form_pg_cast;

/*
 * The allowable values for pg_cast.castcontext are specified by this enum.
 * Since castcontext is stored as a "char", we use ASCII codes for human
 * convenience in reading the table.  Note that internally to the backend,
 * these values are converted to the CoercionContext enum (see primnodes.h),
 * which is defined to sort in a convenient order; the ASCII codes don't
 * have to sort in any special order.
 */

typedef enum CoercionCodes
{
	COERCION_CODE_IMPLICIT = 'i',		/* coercion in context of expression */
	COERCION_CODE_ASSIGNMENT = 'a',		/* coercion in context of assignment */
	COERCION_CODE_EXPLICIT = 'e'	/* explicit cast operation */
} CoercionCodes;

/*
 * The allowable values for pg_cast.castmethod are specified by this enum.
 * Since castmethod is stored as a "char", we use ASCII codes for human
 * convenience in reading the table.
 */
typedef enum CoercionMethod
{
	COERCION_METHOD_FUNCTION = 'f',		/* use a function */
	COERCION_METHOD_BINARY = 'b',		/* types are binary-compatible */
	COERCION_METHOD_INOUT = 'i' /* use input/output functions */
} CoercionMethod;


/* ----------------
 *		compiler constants for pg_cast
 * ----------------
 */
#define Natts_pg_cast				6
#define Anum_pg_cast_castsource		1
#define Anum_pg_cast_casttarget		2
#define Anum_pg_cast_castfunc		3
#define Anum_pg_cast_castcontext	4
#define Anum_pg_cast_castmethod		5
#define Anum_pg_cast_castowner		6

/* ----------------
 *		initial contents of pg_cast
 *
 * Note: this table has OIDs, but we don't bother to assign them manually,
 * since nothing needs to know the specific OID of any built-in cast.
 * ----------------
 */

/*
 * Numeric category: implicit casts are allowed in the direction
 * int2->int4->int8->int16->numeric->float4->float8, while casts in the
 * reverse direction are assignment-only.
 */
DATA(insert ( 23   5545 5526 i f _null_));
DATA(insert ( 5545 16   5533 i f _null_));
DATA(insert ( 16   5545 5534 i f _null_));
DATA(insert ( 5545 26   5525 i f _null_));
DATA(insert ( 5545 24   5525 i f _null_));
DATA(insert ( 5545 2202 5525 i f _null_));
DATA(insert ( 5545 2203 5525 i f _null_));
DATA(insert ( 5545 2204 5525 i f _null_));
DATA(insert ( 5545 2205 5525 i f _null_));
DATA(insert ( 5545 2206 5525 i f _null_));
DATA(insert ( 5545 3734 5525 i f _null_));
DATA(insert ( 5545 3769 5525 i f _null_));
DATA(insert ( 5545 21   5523 i f _null_));
DATA(insert ( 5545 23   5525 i f _null_));
DATA(insert ( 5545 20   5527 i f _null_));
DATA(insert ( 5545 1700 5521 i f _null_));
DATA(insert ( 5545 700  5529 i f _null_));
DATA(insert ( 5545 701  5531 i f _null_));
DATA(insert ( 20   5545 5528 a f _null_));
DATA(insert ( 21   5545 5524 i f _null_));
DATA(insert ( 700  5545 5530 i f _null_));
DATA(insert ( 701  5545 5532 i f _null_));
DATA(insert ( 1700 5545 5522 i f _null_));
DATA(insert ( 20   21   714  a f _null_));
DATA(insert ( 20   23   480  a f _null_));
DATA(insert ( 20   700  652  i f _null_));
DATA(insert ( 20   701  482  i f _null_));
DATA(insert ( 20   1700 1781 i f _null_));
DATA(insert ( 21   20   754  i f _null_));
DATA(insert ( 21   23   313  i f _null_));
DATA(insert ( 21   700  236  i f _null_));
DATA(insert ( 21   701  235  i f _null_));
DATA(insert ( 21   1700 1782 i f _null_));
DATA(insert ( 23   20   481  i f _null_));
DATA(insert ( 23   21   314  i f _null_));
DATA(insert ( 23   700  318  i f _null_));
DATA(insert ( 23   701  316  i f _null_));
DATA(insert ( 23   1700 1740 i f _null_));
DATA(insert ( 700  20   653  i f _null_));
DATA(insert ( 700  21   238  i f _null_));
DATA(insert ( 700  23   319  i f _null_));
DATA(insert ( 700  701  311  i f _null_));
DATA(insert ( 700  1700 1742 i f _null_));
DATA(insert ( 701  20   483  i f _null_));
DATA(insert ( 701  21   237  i f _null_));
DATA(insert ( 701  23   317  i f _null_));
DATA(insert ( 701  700  312  i f _null_));
DATA(insert ( 5545 34   6405 i f _null_));
DATA(insert ( 34   5545 6406 a f _null_));
DATA(insert ( 21   34   6407 i f _null_));
DATA(insert ( 34   21   6408 a f _null_));
DATA(insert ( 23   34   6409 i f _null_));
DATA(insert ( 34   23   6410 a f _null_));
DATA(insert ( 20   34   6411 i f _null_));
DATA(insert ( 34   20   6412 a f _null_));
DATA(insert ( 701  34   6413 i f _null_));
DATA(insert ( 34   701  6414 i f _null_));
DATA(insert ( 700  34   6415 i f _null_));
DATA(insert ( 34   700  6416 i f _null_));
DATA(insert ( 26   34   6417 i f _null_));
DATA(insert ( 34   26   6418 i f _null_));
DATA(insert ( 16   34   6419 i f _null_));
DATA(insert ( 34   16   6420 i f _null_));
DATA(insert ( 1700 34   6421 i f _null_));
DATA(insert ( 34   1700 6422 i f _null_));

/* 
 * convert float8 to numeric implicit(not assignment-only)
 * support mod(numeric, float8) and mod(float8, numeric)
 */
DATA(insert (  701 1700 1743 i f _null_));
DATA(insert ( 1700	 20 1779 i f _null_));
DATA(insert ( 1700	 21 1783 i f _null_));
DATA(insert ( 1700	 23 1744 i f _null_));
DATA(insert ( 1700	700 1745 i f _null_));
DATA(insert ( 1700	701 1746 i f _null_));
DATA(insert (  790 1700 3823 a f _null_));
DATA(insert ( 1700	790 3824 a f _null_));
DATA(insert ( 23	790 3811 a f _null_));
DATA(insert ( 20	790 3812 a f _null_));

/* Allow explicit coercions between int4 and bool */

/*Bool <-->INT4*/
DATA(insert (	23	16	2557 i f _null_));
DATA(insert (	16	23	2558 i f _null_));
/*Bool <-->INT2*/
DATA(insert (	21	16	3180 i f _null_));
DATA(insert (	16	21	3181 i f _null_));
/*Bool <-->INT8*/

DATA(insert (	20	16	3177 i f _null_));
DATA(insert (	16	20	3178 i f _null_));
/* Bool <--> Numeric*/
DATA(insert (	1700	16	6434 i f _null_));
DATA(insert (	16	1700	6433 i f _null_));


/* int4 ->bpchar */
DATA(insert (	23	1042 3192 i f _null_));

/* numeric ->interval */
DATA(insert (	1700	1186 3842 i f _null_));

/* float8-> interval */
DATA(insert (   701         1186 4229 i f _null_));

/* int1 ->interval */
DATA(insert (	5545	1186 3189 i f _null_));

/* int2 ->interval */
DATA(insert (	21	1186 3190 i f _null_));

/* int4 ->interval */
DATA(insert (	23	1186 3191 i f _null_));

/*
 * OID category: allow implicit conversion from any integral type (including
 * int8, to support OID literals > 2G) to OID, as well as assignment coercion
 * from OID to int4 or int8.  Similarly for each OID-alias type.  Also allow
 * implicit coercions between OID and each OID-alias type, as well as
 * regproc<->regprocedure and regoper<->regoperator.  (Other coercions
 * between alias types must pass through OID.)	Lastly, there are implicit
 * casts from text and varchar to regclass, which exist mainly to support
 * legacy forms of nextval() and related functions.
 */
DATA(insert (	20	 26 1287 i f _null_));
DATA(insert (	21	 26  313 i f _null_));
DATA(insert (	23	 26    0 i b _null_));
DATA(insert (	26	 20 1288 a f _null_));
DATA(insert (	26	 23    0 a b _null_));
DATA(insert (	26	 24    0 i b _null_));
DATA(insert (	24	 26    0 i b _null_));
DATA(insert (	20	 24 1287 i f _null_));
DATA(insert (	21	 24  313 i f _null_));
DATA(insert (	23	 24    0 i b _null_));
DATA(insert (	24	 20 1288 a f _null_));
DATA(insert (	24	 23    0 a b _null_));
DATA(insert (	24 2202    0 i b _null_));
DATA(insert ( 2202	 24    0 i b _null_));
DATA(insert (	26 2202    0 i b _null_));
DATA(insert ( 2202	 26    0 i b _null_));
DATA(insert (	20 2202 1287 i f _null_));
DATA(insert (	21 2202  313 i f _null_));
DATA(insert (	23 2202    0 i b _null_));
DATA(insert ( 2202	 20 1288 a f _null_));
DATA(insert ( 2202	 23    0 a b _null_));
DATA(insert (	26 2203    0 i b _null_));
DATA(insert ( 2203	 26    0 i b _null_));
DATA(insert (	20 2203 1287 i f _null_));
DATA(insert (	21 2203  313 i f _null_));
DATA(insert (	23 2203    0 i b _null_));
DATA(insert ( 2203	 20 1288 a f _null_));
DATA(insert ( 2203	 23    0 a b _null_));
DATA(insert ( 2203 2204    0 i b _null_));
DATA(insert ( 2204 2203    0 i b _null_));
DATA(insert (	26 2204    0 i b _null_));
DATA(insert ( 2204	 26    0 i b _null_));
DATA(insert (	20 2204 1287 i f _null_));
DATA(insert (	21 2204  313 i f _null_));
DATA(insert (	23 2204    0 i b _null_));
DATA(insert ( 2204	 20 1288 a f _null_));
DATA(insert ( 2204	 23    0 a b _null_));
DATA(insert (	26 2205    0 i b _null_));
DATA(insert ( 2205	 26    0 i b _null_));
DATA(insert (	20 2205 1287 i f _null_));
DATA(insert (	21 2205  313 i f _null_));
DATA(insert (	23 2205    0 i b _null_));
DATA(insert ( 2205	 20 1288 a f _null_));
DATA(insert ( 2205	 23    0 a b _null_));
DATA(insert (	26 2206    0 i b _null_));
DATA(insert ( 2206	 26    0 i b _null_));
DATA(insert (	20 2206 1287 i f _null_));
DATA(insert (	21 2206  313 i f _null_));
DATA(insert (	23 2206    0 i b _null_));
DATA(insert ( 2206	 20 1288 a f _null_));
DATA(insert ( 2206	 23    0 a b _null_));
DATA(insert (	26 3734    0 i b _null_));
DATA(insert ( 3734	 26    0 i b _null_));
DATA(insert (	20 3734 1287 i f _null_));
DATA(insert (	21 3734  313 i f _null_));
DATA(insert (	23 3734    0 i b _null_));
DATA(insert ( 3734	 20 1288 a f _null_));
DATA(insert ( 3734	 23    0 a b _null_));
DATA(insert (	26 3769    0 i b _null_));
DATA(insert ( 3769	 26    0 i b _null_));
DATA(insert (	20 3769 1287 i f _null_));
DATA(insert (	21 3769  313 i f _null_));
DATA(insert (	23 3769    0 i b _null_));
DATA(insert ( 3769	 20 1288 a f _null_));
DATA(insert ( 3769	 23    0 a b _null_));
DATA(insert (	25 2205 1079 i f _null_));
DATA(insert (	90 2205 1079 i f _null_));
DATA(insert ( 1043 2205 1079 i f _null_));
DATA(insert ( 3969 2205 1079 i f _null_));

/*
 * String category
 */
DATA(insert (	25 1042    0 i b _null_));
DATA(insert (	25 1043    0 i b _null_));
DATA(insert (	25 3969    0 i b _null_));
DATA(insert (	90 1042    0 i b _null_));
DATA(insert (	90 1043    0 i b _null_));
DATA(insert (	90 3969    0 i b _null_));
DATA(insert ( 1042	 25  401 i f _null_));
DATA(insert ( 1042	 90  401 i f _null_));
DATA(insert ( 1042 1043  401 i f _null_));
DATA(insert ( 1043	 25    0 i b _null_));
DATA(insert ( 1043	 90    0 i b _null_));
DATA(insert ( 1043 1042    0 i b _null_));
DATA(insert ( 1042 3969  401 i f _null_));
DATA(insert ( 3969	 25    0 i b _null_));
DATA(insert ( 3969	 90    0 i b _null_));
DATA(insert ( 3969 1042    0 i b _null_));
DATA(insert ( 3969 1043    0 i b _null_));
DATA(insert ( 1043 3969    0 i b _null_));
DATA(insert (	18	 25  946 i f _null_));
DATA(insert (	18	 90  946 i f _null_));
DATA(insert (	18 1042  860 a f _null_));
DATA(insert (	18 1043  946 a f _null_));
DATA(insert (	18 3969  946 a f _null_));
DATA(insert (	19	 25  406 i f _null_));
DATA(insert (	19	 90  406 i f _null_));
DATA(insert (	19 1042  408 a f _null_));
DATA(insert (	19 1043 1401 a f _null_));
DATA(insert (	19 3969 1401 a f _null_));
DATA(insert (	25	 18  944 a f _null_));
DATA(insert (	90	 18  944 a f _null_));
DATA(insert ( 1042	 18  944 a f _null_));
DATA(insert ( 1043	 18  944 a f _null_));
DATA(insert ( 3969	 18  944 a f _null_));
DATA(insert (	25	 19  407 i f _null_));
DATA(insert (	90	 19  407 i f _null_));
DATA(insert ( 1042	 19  409 i f _null_));
DATA(insert ( 1043	 19 1400 i f _null_));
DATA(insert ( 3969	 19 1400 i f _null_));



/* Allow explicit coercions between int4 and "char" */
DATA(insert (	18	 23   77 e f _null_));
DATA(insert (	23	 18   78 e f _null_));

/* pg_node_tree can be coerced to, but not from, text */
DATA(insert (  194	 25    0 i b _null_));
DATA(insert (  194	 90    0 i b _null_));

/*
 * Datetime category
 */
DATA(insert (  702 1082 1179 a f _null_));
DATA(insert (  702 1083 1364 a f _null_));
DATA(insert (  702 1114 2023 i f _null_));
DATA(insert (  702 1184 1173 i f _null_));
DATA(insert (  703 1186 1177 i f _null_));
DATA(insert ( 1082 1114 2024 i f _null_));
DATA(insert ( 1082 1184 1174 i f _null_));
DATA(insert ( 1083 1186 1370 i f _null_));
DATA(insert ( 1083 1266 2047 i f _null_));
DATA(insert ( 1114	702 2030 a f _null_));
DATA(insert ( 1114 1082 2029 a f _null_));
DATA(insert ( 1114 1083 1316 a f _null_));
DATA(insert ( 1114 1184 2028 i f _null_));
DATA(insert ( 1184	702 1180 a f _null_));
DATA(insert ( 1184 1082 1178 a f _null_));
DATA(insert ( 1184 1083 2019 a f _null_));
DATA(insert ( 1184 1114 2027 a f _null_));
DATA(insert ( 1184 1266 1388 a f _null_));
DATA(insert ( 1186	703 1194 a f _null_));
DATA(insert ( 1186 1083 1419 a f _null_));
DATA(insert ( 1266 1083 2046 a f _null_));
DATA(insert ( 25 1114 4073 i f _null_));
DATA(insert ( 90 1114 4073 i f _null_));

/* Cross-category casts between int4 and abstime, reltime */
DATA(insert (	23	702    0 e b _null_));
DATA(insert (  702	 23    0 e b _null_));
DATA(insert (	23	703    0 e b _null_));
DATA(insert (  703	 23    0 e b _null_));

/*
 * Geometric category
 */
DATA(insert (  601	600 1532 e f _null_));
DATA(insert (  602	600 1533 e f _null_));
DATA(insert (  602	604 1449 a f _null_));
DATA(insert (  603	600 1534 e f _null_));
DATA(insert (  603	601 1541 e f _null_));
DATA(insert (  603	604 1448 a f _null_));
DATA(insert (  603	718 1479 e f _null_));
DATA(insert (  604	600 1540 e f _null_));
DATA(insert (  604	602 1447 a f _null_));
DATA(insert (  604	603 1446 e f _null_));
DATA(insert (  604	718 1474 e f _null_));
DATA(insert (  718	600 1416 e f _null_));
DATA(insert (  718	603 1480 e f _null_));
DATA(insert (  718	604 1544 e f _null_));

/*
 * INET category
 */
DATA(insert (  650	869    0 i b _null_));
DATA(insert (  869	650 1715 a f _null_));

/*
 * BitString category
 */
DATA(insert ( 1560 1562    0 i b _null_));
DATA(insert ( 1562 1560    0 i b _null_));
/* Cross-category casts between bit and int4, int8 */
DATA(insert (	20 1560 2075 e f _null_));
DATA(insert (	23 1560 1683 e f _null_));
DATA(insert ( 1560	 20 2076 e f _null_));
DATA(insert ( 1560	 23 1684 e f _null_));

/*
 * Cross-category casts to and from TEXT
 *
 * We need entries here only for a few specialized cases where the behavior
 * of the cast function differs from the datatype's I/O functions.  Otherwise,
 * parse_coerce.c will generate CoerceViaIO operations without any prompting.
 *
 * Note that the castcontext values specified here should be no stronger than
 * parse_coerce.c's automatic casts ('a' to text, 'e' from text) else odd
 * behavior will ensue when the automatic cast is applied instead of the
 * pg_cast entry!
 */
DATA(insert (  650	 25  730 a f _null_));
DATA(insert (  869	 25  730 a f _null_));
DATA(insert (	16	 25 2971 a f _null_));
DATA(insert (  142	 25    0 a b _null_));
DATA(insert (	25	142 2896 e f _null_));

DATA(insert (  650	 90  730 a f _null_));
DATA(insert (  869	 90  730 a f _null_));
DATA(insert (	16	 90 2971 a f _null_));
DATA(insert (  142	 90    0 a b _null_));
DATA(insert (	90	142 2896 e f _null_));

/*
 * Cross-category casts to and from VARCHAR
 *
 * We support all the same casts as for TEXT.
 */
DATA(insert (  650 1043  730 a f _null_));
DATA(insert (  869 1043  730 a f _null_));
DATA(insert (	16 1043 2971 a f _null_));
DATA(insert (  142 1043    0 a b _null_));
DATA(insert ( 1043	142 2896 e f _null_));
DATA(insert (  650 3969  730 a f _null_));
DATA(insert (  869 3969  730 a f _null_));
DATA(insert (	16 3969 2971 a f _null_));
DATA(insert (  142 3969    0 a b _null_));
DATA(insert ( 3969	142 2896 e f _null_));

/*
 * Cross-category casts to and from BPCHAR
 *
 * We support all the same casts as for TEXT.
 */
DATA(insert (  650 1042  730 a f _null_));
DATA(insert (  869 1042  730 a f _null_));
DATA(insert (	16 1042 2971 a f _null_));
DATA(insert (  142 1042    0 a b _null_));
DATA(insert ( 1042	142 2896 e f _null_));
DATA(insert ( 1042	20 4195 i f _null_));


/*
 * Length-coercion functions
 */
DATA(insert ( 1042 1042  668 i f _null_));
DATA(insert ( 1043 1043  669 i f _null_));
DATA(insert ( 3969 3969  3961 i f _null_));
DATA(insert ( 1083 1083 1968 i f _null_));
DATA(insert ( 1114 1114 1961 i f _null_));
DATA(insert ( 1184 1184 1967 i f _null_));
DATA(insert ( 1186 1186 1200 i f _null_));
DATA(insert ( 1266 1266 1969 i f _null_));
DATA(insert ( 1560 1560 1685 i f _null_));
DATA(insert ( 1562 1562 1687 i f _null_));
DATA(insert ( 1700 1700 1703 i f _null_));

DATA(insert ( 1082 25 4159 i f _null_));
/*DATA(insert ( 1082 90 4159 i f _null_));*/
DATA(insert ( 1082 1042 4160 i f _null_));
DATA(insert ( 1082 1043 4161 i f _null_));
DATA(insert ( 1043 1082 4162 i f _null_));
DATA(insert ( 1042 1082 4163 i f _null_));
DATA(insert ( 25 1082 4164 i f _null_));
/*DATA(insert ( 90 1082 4164 i f _null_));*/

DATA(insert ( 5545 25 4165 i f _null_));
DATA(insert ( 21   25 4166 i f _null_));
DATA(insert ( 23   25 4167 i f _null_));
DATA(insert ( 20   25 4168 i f _null_));
DATA(insert ( 700  25 4169 i f _null_));
DATA(insert ( 701  25 4170 i f _null_));
DATA(insert ( 1700 25 4171 i f _null_));
DATA(insert ( 5545 90 4165 i f _null_));
DATA(insert ( 21   90 4166 i f _null_));
DATA(insert ( 23   90 4167 i f _null_));
DATA(insert ( 20   90 4168 i f _null_));
DATA(insert ( 700  90 4169 i f _null_));
DATA(insert ( 701  90 4170 i f _null_));
DATA(insert ( 1700 90 4171 i f _null_));
DATA(insert ( 1042 1700 4172 i f _null_));
DATA(insert ( 1043 1700 4173 i f _null_));
DATA(insert ( 1043 23 4174 i f _null_));
DATA(insert ( 1042 23 4175 i f _null_));
DATA(insert ( 1043 20 4176 i f _null_));
DATA(insert ( 1184 25 4177 i f _null_));
DATA(insert ( 1114 25 4178 i f _null_));
DATA(insert ( 1184 90 4177 i f _null_));
DATA(insert ( 1114 90 4178 i f _null_));
DATA(insert ( 1114 1043 4179 i f _null_));
DATA(insert ( 21 1043 4180 i f _null_));
DATA(insert ( 23 1043 4181 i f _null_));
DATA(insert ( 20 1043 4182 i f _null_));
DATA(insert ( 1700 1043 4183 i f _null_));
DATA(insert ( 700 1043 4184 i f _null_));
DATA(insert ( 701 1043 4185 i f _null_));
DATA(insert ( 1043 1114 4186 i f _null_));
DATA(insert ( 1042 1114 4187 i f _null_));

DATA(insert ( 25 5545 4188 i f _null_));
DATA(insert ( 25 21 4189 i f _null_));
DATA(insert ( 25 23 4190 i f _null_));
DATA(insert ( 25 20 4191 i f _null_));
DATA(insert ( 25 700 4192 i f _null_));
DATA(insert ( 25 701 4193 i f _null_));
DATA(insert ( 25 1700 4194 i f _null_));

DATA(insert ( 90 5545 4188 i f _null_));
DATA(insert ( 90 21 4189 i f _null_));
DATA(insert ( 90 23 4190 i f _null_));
DATA(insert ( 90 20 4191 i f _null_));
DATA(insert ( 90 700 4192 i f _null_));
DATA(insert ( 90 701 4193 i f _null_));
DATA(insert ( 90 1700 4194 i f _null_));

DATA(insert (5545 1043 4065 i f _null_));
DATA(insert (5545 3969 4066 i f _null_));
DATA(insert (5545 1042 4067 i f _null_));
DATA(insert (21 1042 4068 i f _null_));
DATA(insert (20 1042 4069 i f _null_));
DATA(insert (700 1042 4070 i f _null_));
DATA(insert (701 1042 4071 i f _null_));
DATA(insert (1700 1042 4072 i f _null_));

/* hll */
DATA(insert (4301 4301 4311 i f _null_));
DATA(insert (17    4301 0 e b _null_));
DATA(insert (20   4303 0 e b _null_));
DATA(insert (23   4303 4317 e f _null_));
DATA(insert (4402 4301 0 e b _null_));

/* json to/from jsonb */
DATA(insert ( 114 3802 0 e i _null_));
DATA(insert ( 3802 114 0 e i _null_));

#endif   /* PG_CAST_H */
