/* -------------------------------------------------------------------------
 *
 * pg_opclass.h
 *      definition of the system "opclass" relation (pg_opclass)
 *      along with the relation's initial contents.
 *
 * The primary key for this table is <opcmethod, opcname, opcnamespace> ---
 * that is, there is a row for each valid combination of opclass name and
 * index access method type.  This row specifies the expected input data type
 * for the opclass (the type of the heap column, or the expression output type
 * in the case of an index expression).  Note that types binary-coercible to
 * the specified type will be accepted too.
 *
 * For a given <opcmethod, opcintype> pair, there can be at most one row that
 * has opcdefault = true; this row is the default opclass for such data in
 * such an index.  (This is not currently enforced by an index, because we
 * don't support partial indexes on system catalogs.)
 *
 * Normally opckeytype = InvalidOid (zero), indicating that the data stored
 * in the index is the same as the data in the indexed column.    If opckeytype
 * is nonzero then it indicates that a conversion step is needed to produce
 * the stored index data, which will be of type opckeytype (which might be
 * the same or different from the input datatype).    Performing such a
 * conversion is the responsibility of the index access method --- not all
 * AMs support this.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_opclass.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_OPCLASS_H
#define PG_OPCLASS_H

#include "catalog/genbki.h"

/* ----------------
 *        pg_opclass definition.    cpp turns this into
 *        typedef struct FormData_pg_opclass
 * ----------------
 */
#define OperatorClassRelationId  2616
#define OperatorClassRelation_Rowtype_Id 10006

CATALOG(pg_opclass,2616) BKI_SCHEMA_MACRO
{
    Oid            opcmethod;        /* index access method opclass is for */
    NameData       opcname;          /* name of this opclass */
    Oid            opcnamespace;     /* namespace of this opclass */
    Oid            opcowner;         /* opclass owner */
    Oid            opcfamily;        /* containing operator family */
    Oid            opcintype;        /* type of data indexed by opclass */
    bool           opcdefault;       /* T if opclass is default for opcintype */
    Oid            opckeytype;       /* type of data in index, or InvalidOid */
} FormData_pg_opclass;

/* ----------------
 *        Form_pg_opclass corresponds to a pointer to a tuple with
 *        the format of pg_opclass relation.
 * ----------------
 */
typedef FormData_pg_opclass *Form_pg_opclass;

/* ----------------
 *        compiler constants for pg_opclass
 * ----------------
 */
#define Natts_pg_opclass                 8
#define Anum_pg_opclass_opcmethod        1
#define Anum_pg_opclass_opcname          2
#define Anum_pg_opclass_opcnamespace     3
#define Anum_pg_opclass_opcowner         4
#define Anum_pg_opclass_opcfamily        5
#define Anum_pg_opclass_opcintype        6
#define Anum_pg_opclass_opcdefault       7
#define Anum_pg_opclass_opckeytype       8

/* ----------------
 *        initial contents of pg_opclass
 *
 * Note: we hard-wire an OID only for a few entries that have to be explicitly
 * referenced in the C code or in built-in catalog entries.  The rest get OIDs
 * assigned on-the-fly during initdb.
 * ----------------
 */
DATA(insert ( 403 abstime_ops PGNSP PGUID  421  702 t 0 ));
DATA(insert ( 403 array_ops   PGNSP PGUID  397 2277 t 0 ));
DATA(insert ( 405 array_ops   PGNSP PGUID  627 2277 t 0 ));
DATA(insert ( 403 bit_ops     PGNSP PGUID  423 1560 t 0 ));
DATA(insert ( 403 bool_ops    PGNSP PGUID  424   16 t 0 ));
DATA(insert ( 403 bpchar_ops  PGNSP PGUID  426 1042 t 0 ));
DATA(insert ( 405 bpchar_ops  PGNSP PGUID  427 1042 t 0 ));
DATA(insert ( 403 bytea_ops   PGNSP PGUID  428   17 t 0 ));
DATA(insert ( 403 char_ops    PGNSP PGUID  429   18 t 0 ));
DATA(insert ( 405 char_ops    PGNSP PGUID  431   18 t 0 ));
DATA(insert ( 403 cidr_ops    PGNSP PGUID 1974  869 f 0 ));
DATA(insert ( 405 cidr_ops    PGNSP PGUID 1975  869 f 0 ));
DATA(insert OID = 3122 ( 403 date_ops   PGNSP PGUID  434 1082 t 0 ));
#define DATE_BTREE_OPS_OID 3122
DATA(insert ( 405 date_ops   PGNSP PGUID  435 1082 t 0 ));
DATA(insert ( 403 float4_ops PGNSP PGUID 1970  700 t 0 ));
DATA(insert ( 405 float4_ops PGNSP PGUID 1971  700 t 0 ));
DATA(insert OID = 3123 ( 403 float8_ops  PGNSP PGUID 1970  701 t 0 ));
#define FLOAT8_BTREE_OPS_OID 3123
DATA(insert ( 405 float8_ops PGNSP PGUID 1971  701 t 0 ));
DATA(insert ( 403 inet_ops   PGNSP PGUID 1974  869 t 0 ));
DATA(insert ( 405 inet_ops   PGNSP PGUID 1975  869 t 0 ));
DATA(insert OID = 1979 ( 403 int2_ops    PGNSP PGUID 1976   21 t 0 ));
#define INT2_BTREE_OPS_OID 1979
DATA(insert ( 405 int2_ops   PGNSP PGUID 1977   21 t 0 ));
DATA(insert OID = 1978 ( 403 int4_ops    PGNSP PGUID 1976   23 t 0 ));
#define INT4_BTREE_OPS_OID 1978
DATA(insert ( 405 int4_ops   PGNSP PGUID 1977   23 t 0 ));
DATA(insert OID = 3124 ( 403 int8_ops PGNSP PGUID 1976   20 t 0 ));
#define INT8_BTREE_OPS_OID 3124
DATA(insert ( 405 int8_ops     PGNSP PGUID 1977   20 t 0 ));
DATA(insert ( 403 interval_ops PGNSP PGUID 1982 1186 t 0 ));
DATA(insert ( 405 interval_ops PGNSP PGUID 1983 1186 t 0 ));
DATA(insert ( 403 macaddr_ops  PGNSP PGUID 1984  829 t 0 ));
DATA(insert ( 405 macaddr_ops  PGNSP PGUID 1985  829 t 0 ));
/*
 * Here's an ugly little hack to save space in the system catalog indexes.
 * btree doesn't ordinarily allow a storage type different from input type;
 * but cstring and name are the same thing except for trailing padding,
 * and we can safely omit that within an index entry.  So we declare the
 * btree opclass for name as using cstring storage type.
 */
DATA(insert ( 403 name_ops PGNSP PGUID 1986   19 t 2275 ));
DATA(insert ( 405 name_ops PGNSP PGUID 1987   19 t 0 ));
DATA(insert OID = 3125 ( 403    numeric_ops PGNSP PGUID 1988 1700 t 0 ));
#define NUMERIC_BTREE_OPS_OID 3125
DATA(insert ( 405        numeric_ops         PGNSP PGUID 1998 1700 t 0 ));
DATA(insert OID = 1981 ( 403    oid_ops         PGNSP PGUID 1989   26 t 0 ));
#define OID_BTREE_OPS_OID 1981
DATA(insert ( 405        oid_ops             PGNSP PGUID 1990   26 t 0 ));
DATA(insert ( 403        oidvector_ops       PGNSP PGUID 1991   30 t 0 ));
DATA(insert ( 405        oidvector_ops       PGNSP PGUID 1992   30 t 0 ));
DATA(insert ( 403        record_ops          PGNSP PGUID 2994 2249 t 0 ));
DATA(insert OID = 3126 ( 403    text_ops    PGNSP PGUID 1994   25 t 0 ));
#define TEXT_BTREE_OPS_OID 3126
DATA(insert ( 405        text_ops            PGNSP PGUID 1995   25 t 0 ));
DATA(insert ( 403        time_ops            PGNSP PGUID 1996 1083 t 0 ));
DATA(insert ( 405        time_ops            PGNSP PGUID 1997 1083 t 0 ));
DATA(insert OID = 3127 ( 403    timestamptz_ops PGNSP PGUID  434 1184 t 0 ));
#define TIMESTAMPTZ_BTREE_OPS_OID 3127
DATA(insert ( 405        timestamptz_ops     PGNSP PGUID 1999 1184 t 0 ));
DATA(insert ( 403        timetz_ops          PGNSP PGUID 2000 1266 t 0 ));
DATA(insert ( 405        timetz_ops          PGNSP PGUID 2001 1266 t 0 ));
DATA(insert ( 403        varbit_ops          PGNSP PGUID 2002 1562 t 0 ));
DATA(insert ( 403        varchar_ops         PGNSP PGUID 1994   25 f 0 ));
DATA(insert ( 405        varchar_ops         PGNSP PGUID 1995   25 f 0 ));
DATA(insert OID = 3128 ( 403    timestamp_ops   PGNSP PGUID  434 1114 t 0 ));
#define TIMESTAMP_BTREE_OPS_OID 3128
DATA(insert ( 405      timestamp_ops         PGNSP PGUID 2040 1114 t 0 ));
DATA(insert ( 403      text_pattern_ops      PGNSP PGUID 2095   25 f 0 ));
DATA(insert ( 403      varchar_pattern_ops   PGNSP PGUID 2095   25 f 0 ));
DATA(insert ( 403      bpchar_pattern_ops    PGNSP PGUID 2097 1042 f 0 ));
DATA(insert ( 403      money_ops             PGNSP PGUID 2099  790 t 0 ));
DATA(insert ( 405      bool_ops              PGNSP PGUID 2222   16 t 0 ));
DATA(insert ( 405      bytea_ops             PGNSP PGUID 2223   17 t 0 ));
DATA(insert ( 405      int2vector_ops        PGNSP PGUID 2224   22 t 0 ));
DATA(insert ( 403      tid_ops               PGNSP PGUID 2789   27 t 0 ));
DATA(insert ( 405      xid_ops               PGNSP PGUID 2225   28 t 0 ));
DATA(insert ( 405      xid32_ops             PGNSP PGUID 2232   31 t 0 ));
DATA(insert ( 405      cid_ops               PGNSP PGUID 2226   29 t 0 ));
DATA(insert ( 405      abstime_ops           PGNSP PGUID 2227  702 t 0 ));
DATA(insert ( 405      reltime_ops           PGNSP PGUID 2228  703 t 0 ));
DATA(insert ( 405      text_pattern_ops      PGNSP PGUID 2229   25 f 0 ));
DATA(insert ( 405      varchar_pattern_ops   PGNSP PGUID 2229   25 f 0 ));
DATA(insert ( 405      bpchar_pattern_ops    PGNSP PGUID 2231 1042 f 0 ));
DATA(insert ( 403      reltime_ops           PGNSP PGUID 2233  703 t 0 ));
DATA(insert ( 403      tinterval_ops         PGNSP PGUID 2234  704 t 0 ));
DATA(insert ( 405      aclitem_ops           PGNSP PGUID 2235 1033 t 0 ));
DATA(insert ( 783      box_ops               PGNSP PGUID 2593  603 t 0 ));
DATA(insert ( 783      point_ops             PGNSP PGUID 1029  600 t 603 ));
DATA(insert ( 783      poly_ops              PGNSP PGUID 2594  604 t 603 ));
DATA(insert ( 783      circle_ops            PGNSP PGUID 2595  718 t 603 ));
DATA(insert ( 2742    _int4_ops              PGNSP PGUID 2745  1007 t 23 ));
DATA(insert ( 2742    _text_ops              PGNSP PGUID 2745  1009 t 25 ));
DATA(insert ( 2742    _abstime_ops           PGNSP PGUID 2745  1023 t 702 ));
DATA(insert ( 2742    _bit_ops               PGNSP PGUID 2745  1561 t 1560 ));
DATA(insert ( 2742    _bool_ops              PGNSP PGUID 2745  1000 t 16 ));
DATA(insert ( 2742    _bpchar_ops            PGNSP PGUID 2745  1014 t 1042 ));
DATA(insert ( 2742    _bytea_ops             PGNSP PGUID 2745  1001 t 17 ));
DATA(insert ( 2742    _char_ops              PGNSP PGUID 2745  1002 t 18 ));
DATA(insert ( 2742    _cidr_ops              PGNSP PGUID 2745  651 t 650 ));
DATA(insert ( 2742    _date_ops              PGNSP PGUID 2745  1182 t 1082 ));
DATA(insert ( 2742    _float4_ops            PGNSP PGUID 2745  1021 t 700 ));
DATA(insert ( 2742    _float8_ops            PGNSP PGUID 2745  1022 t 701 ));
DATA(insert ( 2742    _inet_ops              PGNSP PGUID 2745  1041 t 869 ));
DATA(insert ( 2742    _int2_ops              PGNSP PGUID 2745  1005 t 21 ));
DATA(insert ( 2742    _int8_ops              PGNSP PGUID 2745  1016 t 20 ));
DATA(insert ( 2742    _interval_ops          PGNSP PGUID 2745  1187 t 1186 ));
DATA(insert ( 2742    _macaddr_ops           PGNSP PGUID 2745  1040 t 829 ));
DATA(insert ( 2742    _name_ops              PGNSP PGUID 2745  1003 t 19 ));
DATA(insert ( 2742    _numeric_ops           PGNSP PGUID 2745  1231 t 1700 ));
DATA(insert ( 2742    _oid_ops               PGNSP PGUID 2745  1028 t 26 ));
DATA(insert ( 2742    _oidvector_ops         PGNSP PGUID 2745  1013 t 30 ));
DATA(insert ( 2742    _time_ops              PGNSP PGUID 2745  1183 t 1083 ));
DATA(insert ( 2742    _timestamptz_ops       PGNSP PGUID 2745  1185 t 1184 ));
DATA(insert ( 2742    _timetz_ops            PGNSP PGUID 2745  1270 t 1266 ));
DATA(insert ( 2742    _varbit_ops            PGNSP PGUID 2745  1563 t 1562 ));
DATA(insert ( 2742    _varchar_ops           PGNSP PGUID 2745  1015 t 1043 ));
DATA(insert ( 2742    _timestamp_ops         PGNSP PGUID 2745  1115 t 1114 ));
DATA(insert ( 2742    _money_ops             PGNSP PGUID 2745  791 t 790 ));
DATA(insert ( 2742    _reltime_ops           PGNSP PGUID 2745  1024 t 703 ));
DATA(insert ( 2742    _tinterval_ops         PGNSP PGUID 2745  1025 t 704 ));
/*
DATA(insert ( 4444    _int4_ops            PGNSP PGUID 4445  1007 t 23 ));
DATA(insert ( 4444    _text_ops            PGNSP PGUID 4445  1009 t 25 ));
DATA(insert ( 4444    _abstime_ops         PGNSP PGUID 4445  1023 t 702 ));
DATA(insert ( 4444    _bit_ops             PGNSP PGUID 4445  1561 t 1560 ));
DATA(insert ( 4444    _bool_ops            PGNSP PGUID 4445  1000 t 16 ));
DATA(insert ( 4444    _bpchar_ops          PGNSP PGUID 4445  1014 t 1042 ));
DATA(insert ( 4444    _bytea_ops           PGNSP PGUID 4445  1001 t 17 ));
DATA(insert ( 4444    _char_ops            PGNSP PGUID 4445  1002 t 18 ));
DATA(insert ( 4444    _cidr_ops            PGNSP PGUID 4445  651 t 650 ));
DATA(insert ( 4444    _date_ops            PGNSP PGUID 4445  1182 t 1082 ));
DATA(insert ( 4444    _float4_ops          PGNSP PGUID 4445  1021 t 700 ));
DATA(insert ( 4444    _float8_ops          PGNSP PGUID 4445  1022 t 701 ));
DATA(insert ( 4444    _inet_ops            PGNSP PGUID 4445  1041 t 869 ));
DATA(insert ( 4444    _int2_ops            PGNSP PGUID 4445  1005 t 21 ));
DATA(insert ( 4444    _int8_ops            PGNSP PGUID 4445  1016 t 20 ));
DATA(insert ( 4444    _interval_ops        PGNSP PGUID 4445  1187 t 1186 ));
DATA(insert ( 4444    _macaddr_ops         PGNSP PGUID 4445  1040 t 829 ));
DATA(insert ( 4444    _name_ops            PGNSP PGUID 4445  1003 t 19 ));
DATA(insert ( 4444    _numeric_ops         PGNSP PGUID 4445  1231 t 1700 ));
DATA(insert ( 4444    _oid_ops             PGNSP PGUID 4445  1028 t 26 ));
DATA(insert ( 4444    _oidvector_ops       PGNSP PGUID 4445  1013 t 30 ));
DATA(insert ( 4444    _time_ops            PGNSP PGUID 4445  1183 t 1083 ));
DATA(insert ( 4444    _timestamptz_ops     PGNSP PGUID 4445  1185 t 1184 ));
DATA(insert ( 4444    _timetz_ops          PGNSP PGUID 4445  1270 t 1266 ));
DATA(insert ( 4444    _varbit_ops          PGNSP PGUID 4445  1563 t 1562 ));
DATA(insert ( 4444    _varchar_ops         PGNSP PGUID 4445  1015 t 1043 ));
DATA(insert ( 4444    _timestamp_ops       PGNSP PGUID 4445  1115 t 1114 ));
DATA(insert ( 4444    _money_ops           PGNSP PGUID 4445  791 t 790 ));
DATA(insert ( 4444    _reltime_ops         PGNSP PGUID 4445  1024 t 703 ));
DATA(insert ( 4444    _tinterval_ops       PGNSP PGUID 4445  1025 t 704 ));
*/
DATA(insert ( 403        uuid_ops            PGNSP PGUID 2968  2950 t 0 ));
DATA(insert ( 405        uuid_ops            PGNSP PGUID 2969  2950 t 0 ));
DATA(insert ( 403        enum_ops            PGNSP PGUID 3522  3500 t 0 ));
DATA(insert ( 405        enum_ops            PGNSP PGUID 3523  3500 t 0 ));
DATA(insert ( 403        tsvector_ops        PGNSP PGUID 3626  3614 t 0 ));
DATA(insert ( 783        tsvector_ops        PGNSP PGUID 3655  3614 t 3642 ));
DATA(insert ( 2742       tsvector_ops        PGNSP PGUID 3659  3614 t 25 ));
DATA(insert ( 4444       tsvector_ops        PGNSP PGUID 4446  3614 t 25 ));
DATA(insert ( 403        tsquery_ops         PGNSP PGUID 3683  3615 t 0 ));
DATA(insert ( 783        tsquery_ops         PGNSP PGUID 3702  3615 t 20 ));
DATA(insert ( 403        range_ops           PGNSP PGUID 3901  3831 t 0 ));
DATA(insert ( 405        range_ops           PGNSP PGUID 3903  3831 t 0 ));
DATA(insert ( 783        range_ops           PGNSP PGUID 3919  3831 t 0 ));
DATA(insert ( 4000    quad_point_ops         PGNSP PGUID 4015  600 t 0 ));
DATA(insert ( 4000    kd_point_ops           PGNSP PGUID 4016  600 f 0 ));
DATA(insert ( 4000    text_ops               PGNSP PGUID 4017  25 t 0 ));

DATA(insert ( 403        raw_ops             PGNSP PGUID 3806  86 t 0 ));
DATA(insert ( 405        raw_ops             PGNSP PGUID 3807  86 t 0 ));
DATA(insert ( 403        int1_ops            PGNSP PGUID 5535  5545 t 0 ));
DATA(insert ( 405        int1_ops            PGNSP PGUID 5536  5545 t 0 ));
DATA(insert ( 403        smalldatetime_ops   PGNSP PGUID 5570  9003 t 0 ));
DATA(insert ( 405        smalldatetime_ops   PGNSP PGUID 5571  9003 t 0 ));

/* psort index, fake data just make index work */
DATA(insert ( 4039    int4_ops         PGNSP    PGUID  4050    23    t    0));
DATA(insert ( 4039    int2_ops         PGNSP    PGUID  4050    21    t    0));
DATA(insert ( 4039    int8_ops         PGNSP    PGUID  4050    20    t    0));
DATA(insert ( 4039    oid_ops          PGNSP    PGUID  4051    26    t    0));
DATA(insert ( 4039    date_ops         PGNSP    PGUID  4052  1082    t    0));
DATA(insert ( 4039    timestamp_ops    PGNSP    PGUID  4052  1114    t    0));
DATA(insert ( 4039    timestamptz_ops  PGNSP    PGUID  4052  1184    t    0));
DATA(insert ( 4039    float4_ops       PGNSP    PGUID  4053   700    t    0));
DATA(insert ( 4039    float8_ops       PGNSP    PGUID  4053   701    t    0));
DATA(insert ( 4039    numeric_ops      PGNSP    PGUID  4054  1700    t    0));
DATA(insert ( 4039    text_ops         PGNSP    PGUID  4055    25    t    0));
DATA(insert ( 4039    bpchar_ops       PGNSP    PGUID  4056  1042    t    0));
DATA(insert ( 4039    time_ops         PGNSP    PGUID  4057  1083    t    0));
DATA(insert ( 4039    timetz_ops       PGNSP    PGUID  4058  1266    t    0));
DATA(insert ( 4039    money_ops        PGNSP    PGUID  4059   790    t    0));
DATA(insert ( 4039    interval_ops     PGNSP    PGUID  4060  1186    t    0));
DATA(insert ( 4039    tinterval_ops    PGNSP    PGUID  4061   704    t    0));
DATA(insert ( 4039    int1_ops         PGNSP    PGUID  4062  5545    t    0));
DATA(insert ( 4039    bool_ops         PGNSP    PGUID  4063    16    t    0));
DATA(insert ( 4039    smalldatetime_ops  PGNSP  PGUID  4064  9003    t    0));

/* cbtree index, fake data just make index work */
DATA(insert ( 4239    int4_ops         PGNSP    PGUID  4250    23    t    0));
DATA(insert ( 4239    int2_ops         PGNSP    PGUID  4250    21    t    0));
DATA(insert ( 4239    int8_ops         PGNSP    PGUID  4250    20    t    0));
DATA(insert ( 4239    oid_ops          PGNSP    PGUID  4251    26    t    0));
DATA(insert ( 4239    date_ops         PGNSP    PGUID  4252  1082    t    0));
DATA(insert ( 4239    timestamp_ops    PGNSP    PGUID  4252  1114    t    0));
DATA(insert ( 4239    timestamptz_ops  PGNSP    PGUID  4252  1184    t    0));
DATA(insert ( 4239    float4_ops       PGNSP    PGUID  4253   700    t    0));
DATA(insert ( 4239    float8_ops       PGNSP    PGUID  4253   701    t    0));
DATA(insert ( 4239    numeric_ops      PGNSP    PGUID  4254  1700    t    0));
DATA(insert ( 4239    text_ops         PGNSP    PGUID  4255    25    t    0));
DATA(insert ( 4239    bpchar_ops       PGNSP    PGUID  4256  1042    t    0));
DATA(insert ( 4239    time_ops         PGNSP    PGUID  4257  1083    t    0));
DATA(insert ( 4239    timetz_ops       PGNSP    PGUID  4258  1266    t    0));
DATA(insert ( 4239    money_ops        PGNSP    PGUID  4259   790    t    0));
DATA(insert ( 4239    interval_ops     PGNSP    PGUID  4260  1186    t    0));
DATA(insert ( 4239    tinterval_ops    PGNSP    PGUID  4261   704    t    0));
DATA(insert ( 4239    int1_ops         PGNSP    PGUID  4262  5545    t    0));
DATA(insert ( 4239    bool_ops         PGNSP    PGUID  4263    16    t    0));
DATA(insert ( 4239    smalldatetime_ops  PGNSP  PGUID  4264  9003    t    0));

/* encrypted column operators */
DATA(insert ( 403     byteawithoutorderwithequalcol_ops PGNSP PGUID  436  4402 t 0 ));
DATA(insert ( 405     byteawithoutorderwithequalcol_ops PGNSP PGUID 4470 4402 t 0 ));

/* ubtree index */
DATA(insert ( 4439 abstime_ops PGNSP PGUID 5421  702 t 0 ));
DATA(insert ( 4439 array_ops   PGNSP PGUID 5397 2277 t 0 ));
DATA(insert ( 4439 bit_ops     PGNSP PGUID 5423 1560 t 0 ));
DATA(insert ( 4439 bool_ops    PGNSP PGUID 5424   16 t 0 ));
DATA(insert ( 4439 bpchar_ops  PGNSP PGUID 5426 1042 t 0 ));
DATA(insert ( 4439 bytea_ops   PGNSP PGUID 5428   17 t 0 ));
DATA(insert ( 4439 char_ops    PGNSP PGUID 5429   18 t 0 ));
DATA(insert ( 4439 cidr_ops    PGNSP PGUID 6974  869 f 0 ));
DATA(insert ( 4439 date_ops   PGNSP PGUID 5434 1082 t 0 ));
DATA(insert ( 4439 float4_ops PGNSP PGUID 6970  700 t 0 ));
DATA(insert ( 4439 float8_ops  PGNSP PGUID 6970  701 t 0 ));
DATA(insert ( 4439 inet_ops   PGNSP PGUID 6974  869 t 0 ));
DATA(insert ( 4439 int2_ops    PGNSP PGUID 6976   21 t 0 ));
DATA(insert ( 4439 int4_ops    PGNSP PGUID 6976   23 t 0 ));
DATA(insert ( 4439 int8_ops PGNSP PGUID 6976   20 t 0 ));
DATA(insert ( 4439 interval_ops PGNSP PGUID 6982 1186 t 0 ));
DATA(insert ( 4439 macaddr_ops  PGNSP PGUID 6984  829 t 0 ));
DATA(insert ( 4439 name_ops PGNSP PGUID 6986   19 t 2275 ));
DATA(insert ( 4439 numeric_ops PGNSP PGUID 6988 1700 t 0 ));
DATA(insert ( 4439 oid_ops         PGNSP PGUID 6989   26 t 0 ));
DATA(insert ( 4439 oidvector_ops       PGNSP PGUID 6991   30 t 0 ));
DATA(insert ( 4439 record_ops          PGNSP PGUID 7994 2249 t 0 ));
DATA(insert ( 4439 text_ops    PGNSP PGUID 6994   25 t 0 ));
DATA(insert ( 4439 time_ops            PGNSP PGUID 6996 1083 t 0 ));
DATA(insert ( 4439 timestamptz_ops PGNSP PGUID 5434 1184 t 0 ));
DATA(insert ( 4439 timetz_ops          PGNSP PGUID 7000 1266 t 0 ));
DATA(insert ( 4439 varbit_ops          PGNSP PGUID 7002 1562 t 0 ));
DATA(insert ( 4439 varchar_ops         PGNSP PGUID 6994   25 f 0 ));
DATA(insert ( 4439 timestamp_ops   PGNSP PGUID 5434 1114 t 0 ));
DATA(insert ( 4439 text_pattern_ops      PGNSP PGUID 7095   25 f 0 ));
DATA(insert ( 4439 varchar_pattern_ops   PGNSP PGUID 7095   25 f 0 ));
DATA(insert ( 4439 bpchar_pattern_ops    PGNSP PGUID 7097 1042 f 0 ));
DATA(insert ( 4439 money_ops             PGNSP PGUID 7099  790 t 0 ));
DATA(insert ( 4439 tid_ops               PGNSP PGUID 7789   27 t 0 ));
DATA(insert ( 4439 reltime_ops           PGNSP PGUID 7233  703 t 0 ));
DATA(insert ( 4439 tinterval_ops         PGNSP PGUID 7234  704 t 0 ));
DATA(insert ( 4439 uuid_ops            PGNSP PGUID 7968  2950 t 0 ));
DATA(insert ( 4439 enum_ops            PGNSP PGUID 8522  3500 t 0 ));
DATA(insert ( 4439 tsvector_ops        PGNSP PGUID 8626  3614 t 0 ));
DATA(insert ( 4439 tsquery_ops         PGNSP PGUID 8683  3615 t 0 ));
DATA(insert ( 4439 range_ops           PGNSP PGUID 8901  3831 t 0 ));
DATA(insert ( 4439 raw_ops             PGNSP PGUID 8806  86 t 0 ));
DATA(insert ( 4439 int1_ops            PGNSP PGUID 9535  5545 t 0 ));
DATA(insert ( 4439 smalldatetime_ops   PGNSP PGUID 9570  9003 t 0 ));
DATA(insert ( 4439 byteawithoutorderwithequalcol_ops PGNSP PGUID 5436  4402 t 0 ));

/* jsonb */
DATA(insert ( 403     jsonb_ops        PGNSP    PGUID  4033  3802 t 0 ));
DATA(insert ( 405     jsonb_ops        PGNSP    PGUID  4034  3802 t 0 ));
DATA(insert ( 2742    jsonb_ops        PGNSP    PGUID  4036  3802 t 25 ));
DATA(insert ( 2742    jsonb_hash_ops   PGNSP    PGUID  4037  3802 f 23 ));
/* set */
DATA(insert ( 403  setasint_ops PGNSP PGUID 1976  3272 t 0 ));
DATA(insert ( 405  setasint_ops PGNSP PGUID 1977  3272 f 0 ));
DATA(insert ( 405  settext_ops  PGNSP PGUID 1995  3272 f 0 ));
DATA(insert ( 4439 setasint_ops PGNSP PGUID 6976  3272 t 0 ));
DATA(insert ( 405  set_ops      PGNSP PGUID 8646  3272 t 0 ));

DATA(insert OID = 8900 (8300  vector_l2_ops PGNSP PGUID 8371 8305 f 0));
DATA(insert OID = 8999 (8300  vector_ip_ops PGNSP PGUID 8372 8305 f 0));
DATA(insert OID = 8902 (8300  vector_cosine_ops PGNSP PGUID 8373 8305 f 0));
DATA(insert OID = 8903 (8300  vector_l1_ops PGNSP PGUID 8374 8305 f 0));

DATA(insert OID = 8908 (8300  bit_jaccard_ops PGNSP PGUID 8379 1560 f 0));
DATA(insert OID = 8909 (8300  bit_hamming_ops PGNSP PGUID 8380 1560 f 0));

DATA(insert OID = 8910 (8300  sparsevec_l2_ops PGNSP PGUID 8381 8307 f 0));
DATA(insert OID = 8911 (8300  sparsevec_ip_ops PGNSP PGUID 8382 8307 f 0));
DATA(insert OID = 8912 (8300  sparsevec_cosine_ops PGNSP PGUID 8383 8307 f 0));
DATA(insert OID = 8913 (8300  sparsevec_l1_ops PGNSP PGUID 8384 8307 f 0));

DATA(insert OID = 8914 (8301  vector_l2_ops PGNSP PGUID 8385 8305 t 0));
DATA(insert OID = 8915 (8301  vector_ip_ops PGNSP PGUID 8386 8305 f 0));
DATA(insert OID = 8916 (8301  vector_cosine_ops PGNSP PGUID 8387 8305 f 0));

DATA(insert OID = 8923 (8301  bit_hamming_ops PGNSP PGUID 8394 1560 f 0));

DATA(insert OID = 8977 (403  vector_ops PGNSP PGUID 8392 8305 t 0));
DATA(insert OID = 8979 (403  sparsevec_ops PGNSP PGUID 8397 8307 t 0));

DATA(insert OID = 8951 (4439  vector_ops PGNSP PGUID 8375 8305 t 0));
DATA(insert OID = 8952 (4439  sparsevec_ops PGNSP PGUID 8376 8307 t 0));
#endif   /* PG_OPCLASS_H */

