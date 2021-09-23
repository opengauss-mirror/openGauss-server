/* -------------------------------------------------------------------------
 *
 * pg_opfamily.h
 *      definition of the system "opfamily" relation (pg_opfamily)
 *      along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_opfamily.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_OPFAMILY_H
#define PG_OPFAMILY_H

#include "catalog/genbki.h"

/* ----------------
 *        pg_opfamily definition. cpp turns this into
 *        typedef struct FormData_pg_opfamily
 * ----------------
 */
#define OperatorFamilyRelationId  2753
#define OperatorFamilyRelation_Rowtype_Id 10005

CATALOG(pg_opfamily,2753) BKI_SCHEMA_MACRO
{
    Oid         opfmethod;        /* index access method opfamily is for */
    NameData    opfname;         /* name of this opfamily */
    Oid         opfnamespace;    /* namespace of this opfamily */
    Oid         opfowner;        /* opfamily owner */
} FormData_pg_opfamily;

/* ----------------
 *        Form_pg_opfamily corresponds to a pointer to a tuple with
 *        the format of pg_opfamily relation.
 * ----------------
 */
typedef FormData_pg_opfamily *Form_pg_opfamily;

/* ----------------
 *        compiler constants for pg_opfamily
 * ----------------
 */
#define Natts_pg_opfamily                4
#define Anum_pg_opfamily_opfmethod       1
#define Anum_pg_opfamily_opfname         2
#define Anum_pg_opfamily_opfnamespace    3
#define Anum_pg_opfamily_opfowner        4

/* ----------------
 *        initial contents of pg_opfamily
 * ----------------
 */
DATA(insert OID =  421 (403        abstime_ops        PGNSP PGUID));
DATA(insert OID =  397 (403        array_ops        PGNSP PGUID));
DATA(insert OID =  627 (405        array_ops        PGNSP PGUID));
DATA(insert OID =  423 (403        bit_ops            PGNSP PGUID));
DATA(insert OID =  424 (403        bool_ops        PGNSP PGUID));
#define BOOL_BTREE_FAM_OID 424
DATA(insert OID =  426 (403        bpchar_ops        PGNSP PGUID));
#define BPCHAR_BTREE_FAM_OID 426
DATA(insert OID =  427 (405        bpchar_ops        PGNSP PGUID));
DATA(insert OID =  428 (403        bytea_ops        PGNSP PGUID));
#define BYTEA_BTREE_FAM_OID 428
DATA(insert OID =  436 (403        byteawithoutorderwithequalcol_ops PGNSP PGUID));
#define ENCRYPTEDCOL_BTREE_FAM_OID 436
DATA(insert OID =  429 (403        char_ops        PGNSP PGUID));
DATA(insert OID =  431 (405        char_ops        PGNSP PGUID));
DATA(insert OID =  434 (403        datetime_ops    PGNSP PGUID));
DATA(insert OID =  435 (405        date_ops        PGNSP PGUID));
DATA(insert OID = 1970 (403        float_ops        PGNSP PGUID));
DATA(insert OID = 1971 (405        float_ops        PGNSP PGUID));
DATA(insert OID = 1974 (403        network_ops        PGNSP PGUID));
#define NETWORK_BTREE_FAM_OID 1974
DATA(insert OID = 1975 (405        network_ops        PGNSP PGUID));
DATA(insert OID = 1976 (403        integer_ops        PGNSP PGUID));
#define INTEGER_BTREE_FAM_OID 1976
DATA(insert OID = 1977 (405        integer_ops        PGNSP PGUID));
DATA(insert OID = 1982 (403        interval_ops    PGNSP PGUID));
DATA(insert OID = 1983 (405        interval_ops    PGNSP PGUID));
DATA(insert OID = 1984 (403        macaddr_ops        PGNSP PGUID));
DATA(insert OID = 1985 (405        macaddr_ops        PGNSP PGUID));
DATA(insert OID = 1986 (403        name_ops        PGNSP PGUID));
#define NAME_BTREE_FAM_OID 1986
DATA(insert OID = 1987 (405        name_ops        PGNSP PGUID));
DATA(insert OID = 1988 (403        numeric_ops        PGNSP PGUID));
DATA(insert OID = 1998 (405        numeric_ops        PGNSP PGUID));
DATA(insert OID = 1989 (403        oid_ops            PGNSP PGUID));
#define OID_BTREE_FAM_OID 1989
DATA(insert OID = 1990 (405        oid_ops            PGNSP PGUID));
DATA(insert OID = 1991 (403        oidvector_ops    PGNSP PGUID));
DATA(insert OID = 1992 (405        oidvector_ops    PGNSP PGUID));
DATA(insert OID = 2994 (403        record_ops        PGNSP PGUID));
DATA(insert OID = 1994 (403        text_ops        PGNSP PGUID));
#define TEXT_BTREE_FAM_OID 1994
DATA(insert OID = 1995 (405        text_ops        PGNSP PGUID));
DATA(insert OID = 1996 (403        time_ops        PGNSP PGUID));
DATA(insert OID = 1997 (405        time_ops        PGNSP PGUID));
DATA(insert OID = 1999 (405        timestamptz_ops PGNSP PGUID));
DATA(insert OID = 2000 (403        timetz_ops        PGNSP PGUID));
DATA(insert OID = 2001 (405        timetz_ops        PGNSP PGUID));
DATA(insert OID = 2002 (403        varbit_ops        PGNSP PGUID));
DATA(insert OID = 2040 (405        timestamp_ops    PGNSP PGUID));
DATA(insert OID = 2095 (403        text_pattern_ops    PGNSP PGUID));
#define TEXT_PATTERN_BTREE_FAM_OID 2095
DATA(insert OID = 2097 (403        bpchar_pattern_ops    PGNSP PGUID));
#define BPCHAR_PATTERN_BTREE_FAM_OID 2097
DATA(insert OID = 2099 (403        money_ops        PGNSP PGUID));
DATA(insert OID = 2222 (405        bool_ops        PGNSP PGUID));
#define BOOL_HASH_FAM_OID 2222
DATA(insert OID = 2223 (405        bytea_ops        PGNSP PGUID));
DATA(insert OID = 4470 (405        byteawithoutorderwithequalcol_ops        PGNSP PGUID ));
DATA(insert OID = 2224 (405        int2vector_ops    PGNSP PGUID));
DATA(insert OID = 2789 (403        tid_ops            PGNSP PGUID));
DATA(insert OID = 2225 (405        xid_ops            PGNSP PGUID));
DATA(insert OID = 2232 (405         xid32_ops               PGNSP PGUID));
DATA(insert OID = 2226 (405        cid_ops            PGNSP PGUID));
DATA(insert OID = 2227 (405        abstime_ops        PGNSP PGUID));
DATA(insert OID = 2228 (405        reltime_ops        PGNSP PGUID));
DATA(insert OID = 2229 (405        text_pattern_ops    PGNSP PGUID));
DATA(insert OID = 2231 (405        bpchar_pattern_ops    PGNSP PGUID));
DATA(insert OID = 2233 (403        reltime_ops        PGNSP PGUID));
DATA(insert OID = 2234 (403        tinterval_ops    PGNSP PGUID));
DATA(insert OID = 2235 (405        aclitem_ops        PGNSP PGUID));
DATA(insert OID = 2593 (783        box_ops            PGNSP PGUID));
DATA(insert OID = 2594 (783        poly_ops        PGNSP PGUID));
DATA(insert OID = 2595 (783        circle_ops        PGNSP PGUID));
DATA(insert OID = 1029 (783        point_ops        PGNSP PGUID));
DATA(insert OID = 2745 (2742    array_ops        PGNSP PGUID));
DATA(insert OID = 2968 (403        uuid_ops        PGNSP PGUID));
DATA(insert OID = 2969 (405        uuid_ops        PGNSP PGUID));
DATA(insert OID = 3522 (403        enum_ops        PGNSP PGUID));
DATA(insert OID = 3523 (405        enum_ops        PGNSP PGUID));
DATA(insert OID = 3626 (403        tsvector_ops    PGNSP PGUID));
DATA(insert OID = 3655 (783        tsvector_ops    PGNSP PGUID));
DATA(insert OID = 3659 (2742    tsvector_ops    PGNSP PGUID));
DATA(insert OID = 4446 (4444    tsvector_ops    PGNSP PGUID));
DATA(insert OID = 3683 (403        tsquery_ops        PGNSP PGUID));
DATA(insert OID = 3702 (783        tsquery_ops        PGNSP PGUID));
DATA(insert OID = 3901 (403        range_ops        PGNSP PGUID));
DATA(insert OID = 3903 (405        range_ops        PGNSP PGUID));
DATA(insert OID = 3919 (783        range_ops        PGNSP PGUID));
DATA(insert OID = 4015 (4000    quad_point_ops    PGNSP PGUID));
DATA(insert OID = 4016 (4000    kd_point_ops    PGNSP PGUID));
DATA(insert OID = 4017 (4000    text_ops        PGNSP PGUID));
DATA(insert OID = 4033 (403     jsonb_ops       PGNSP PGUID ));
DATA(insert OID = 4034 (405     jsonb_ops       PGNSP PGUID ));
DATA(insert OID = 4035 (783     jsonb_ops       PGNSP PGUID ));
DATA(insert OID = 4036 (2742    jsonb_ops       PGNSP PGUID ));
DATA(insert OID = 4037 (2742    jsonb_hash_ops  PGNSP PGUID ));
#define TEXT_SPGIST_FAM_OID 4017

DATA(insert OID = 3806 (403        raw_ops         PGNSP PGUID));
DATA(insert OID = 3807 (405        raw_ops         PGNSP PGUID));
DATA(insert OID = 5535 (403        int1_ops         PGNSP PGUID));
DATA(insert OID = 5536 (405        int1_ops         PGNSP PGUID));
DATA(insert OID = 5570 (403        smalldatetime_ops         PGNSP PGUID));
DATA(insert OID = 5571 (405        smalldatetime_ops         PGNSP PGUID));

/* psort index, fake data just make index work */
DATA(insert OID = 4050 (4039    integer_ops      PGNSP    PGUID));
DATA(insert OID = 4051 (4039    oid_ops          PGNSP    PGUID));
DATA(insert OID = 4052 (4039    datetime_ops     PGNSP    PGUID));
DATA(insert OID = 4053 (4039    float_ops        PGNSP    PGUID));
DATA(insert OID = 4054 (4039    numeric_ops      PGNSP    PGUID));
DATA(insert OID = 4055 (4039    text_ops         PGNSP    PGUID));
DATA(insert OID = 4056 (4039    bpchar_ops       PGNSP    PGUID));
DATA(insert OID = 4057 (4039    time_ops         PGNSP    PGUID));
DATA(insert OID = 4058 (4039    timetz_ops       PGNSP    PGUID));
DATA(insert OID = 4059 (4039    money_ops        PGNSP    PGUID));
DATA(insert OID = 4060 (4039    interval_ops     PGNSP    PGUID));
DATA(insert OID = 4061 (4039    tinterval_ops    PGNSP    PGUID));
DATA(insert OID = 4062 (4039    int1_ops         PGNSP    PGUID));
DATA(insert OID = 4063 (4039    bool_ops         PGNSP    PGUID));
DATA(insert OID = 4064 (4039    smalldatetime_ops         PGNSP    PGUID));

/* cbtree index, fake data just make index work */
DATA(insert OID = 4250 (4239    integer_ops      PGNSP    PGUID));
DATA(insert OID = 4251 (4239    oid_ops          PGNSP    PGUID));
DATA(insert OID = 4252 (4239    datetime_ops     PGNSP    PGUID));
DATA(insert OID = 4253 (4239    float_ops        PGNSP    PGUID));
DATA(insert OID = 4254 (4239    numeric_ops      PGNSP    PGUID));
DATA(insert OID = 4255 (4239    text_ops         PGNSP    PGUID));
DATA(insert OID = 4256 (4239    bpchar_ops       PGNSP    PGUID));
DATA(insert OID = 4257 (4239    time_ops         PGNSP    PGUID));
DATA(insert OID = 4258 (4239    timetz_ops       PGNSP    PGUID));
DATA(insert OID = 4259 (4239    money_ops        PGNSP    PGUID));
DATA(insert OID = 4260 (4239    interval_ops     PGNSP    PGUID));
DATA(insert OID = 4261 (4239    tinterval_ops    PGNSP    PGUID));
DATA(insert OID = 4262 (4239    int1_ops         PGNSP    PGUID));
DATA(insert OID = 4263 (4239    bool_ops         PGNSP    PGUID));
DATA(insert OID = 4264 (4239    smalldatetime_ops  PGNSP  PGUID));

/* ubtree index */
#define BTREE_UBTREE_FAM_OID_DIFF 5000
#define BTREE_UBTREE_FAM_OID_SPECIAL_DIFF 4000
#define UBTREE_FAM_START_OID 5000

static const int specialListLen = 4;
static const Oid specialList[specialListLen] = {5535, 5536, 5570, 5571};

static inline bool OpFamilyIsUBTreeFam(Oid oid)
{
    if (oid < UBTREE_FAM_START_OID) {
        return false;
    }
    for (int i = 0; i < specialListLen; i++) {
        if (oid == specialList[i]) {
            return false;
        }
    }
    return true;
}

static inline Oid OpFamilyToBtree(Oid oid)
{
    for (int i = 0; i < specialListLen; i++) {
        if (oid == specialList[i] + BTREE_UBTREE_FAM_OID_SPECIAL_DIFF) {
            return specialList[i];
        }
    }
    return oid - BTREE_UBTREE_FAM_OID_DIFF;
}

inline bool OpFamilyEquals(Oid oid1, Oid oid2)
{
    if (OpFamilyIsUBTreeFam(oid1)) {
        oid1 = OpFamilyToBtree(oid1);
    }
    if (OpFamilyIsUBTreeFam(oid2)) {
        oid2 = OpFamilyToBtree(oid2);
    }
    return oid1 == oid2;
}

DATA(insert OID = 5421 (4439        abstime_ops        PGNSP PGUID));
DATA(insert OID = 5397 (4439        array_ops        PGNSP PGUID));
DATA(insert OID = 5423 (4439       bit_ops            PGNSP PGUID));
DATA(insert OID = 5424 (4439       bool_ops        PGNSP PGUID));
#define BOOL_UBTREE_FAM_OID 5424
DATA(insert OID = 5426 (4439       bpchar_ops        PGNSP PGUID));
#define BPCHAR_UBTREE_FAM_OID 5426
DATA(insert OID = 5428 (4439       bytea_ops        PGNSP PGUID));
#define BYTEA_UBTREE_FAM_OID 5428
DATA(insert OID = 5436 (4439       byteawithoutorderwithequalcol_ops PGNSP PGUID));
#define ENCRYPTEDCOL_UBTREE_FAM_OID 5436
DATA(insert OID = 5429 (4439       char_ops        PGNSP PGUID));
DATA(insert OID = 5434 (4439       datetime_ops    PGNSP PGUID));
DATA(insert OID = 6970 (4439       float_ops        PGNSP PGUID));
DATA(insert OID = 6974 (4439       network_ops        PGNSP PGUID));
#define NETWORK_UBTREE_FAM_OID 6974
DATA(insert OID = 6976 (4439       integer_ops        PGNSP PGUID));
#define INTEGER_UBTREE_FAM_OID 6976
DATA(insert OID = 6982 (4439       interval_ops    PGNSP PGUID));
DATA(insert OID = 6984 (4439       macaddr_ops        PGNSP PGUID));
DATA(insert OID = 6986 (4439       name_ops        PGNSP PGUID));
#define NAME_UBTREE_FAM_OID 6986
DATA(insert OID = 6988 (4439       numeric_ops        PGNSP PGUID));
DATA(insert OID = 6989 (4439       oid_ops            PGNSP PGUID));
#define OID_UBTREE_FAM_OID 6989
DATA(insert OID = 6991 (4439       oidvector_ops    PGNSP PGUID));
DATA(insert OID = 7994 (4439       record_ops        PGNSP PGUID));
DATA(insert OID = 6994 (4439       text_ops        PGNSP PGUID));
#define TEXT_UBTREE_FAM_OID 6994
DATA(insert OID = 6996 (4439       time_ops        PGNSP PGUID));
DATA(insert OID = 7000 (4439       timetz_ops        PGNSP PGUID));
DATA(insert OID = 7002 (4439       varbit_ops        PGNSP PGUID));
DATA(insert OID = 7095 (4439       text_pattern_ops    PGNSP PGUID));
#define TEXT_PATTERN_UBTREE_FAM_OID 7095
DATA(insert OID = 7097 (4439       bpchar_pattern_ops    PGNSP PGUID));
#define BPCHAR_PATTERN_UBTREE_FAM_OID 7097
DATA(insert OID = 7099 (4439       money_ops        PGNSP PGUID));
DATA(insert OID = 7789 (4439       tid_ops            PGNSP PGUID));
DATA(insert OID = 7233 (4439       reltime_ops        PGNSP PGUID));
DATA(insert OID = 7234 (4439       tinterval_ops    PGNSP PGUID));
DATA(insert OID = 7968 (4439       uuid_ops        PGNSP PGUID));
DATA(insert OID = 8522 (4439       enum_ops        PGNSP PGUID));
DATA(insert OID = 8626 (4439       tsvector_ops    PGNSP PGUID));
DATA(insert OID = 8683 (4439       tsquery_ops        PGNSP PGUID));
DATA(insert OID = 8901 (4439       range_ops        PGNSP PGUID));
DATA(insert OID = 8806 (4439       raw_ops         PGNSP PGUID));
DATA(insert OID = 9535 (4439       int1_ops         PGNSP PGUID));
DATA(insert OID = 9570 (4439       smalldatetime_ops         PGNSP PGUID));

#endif   /* PG_OPFAMILY_H */

