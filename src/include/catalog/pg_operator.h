/* -------------------------------------------------------------------------
 *
 * pg_operator.h
 *      definition of the system "operator" relation (pg_operator)
 *      along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_operator.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 *      XXX do NOT break up DATA() statements into multiple lines!
 *          the scripts are not as smart as you might think...
 *      if you need change the CATALOG, check pg_operator.data too pls.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_OPERATOR_H
#define PG_OPERATOR_H

#include "catalog/genbki.h"
#include "nodes/pg_list.h"

/* ----------------
 *        pg_operator definition.  cpp turns this into
 *        typedef struct FormData_pg_operator
 * ----------------
 */
#define OperatorRelationId    2617
#define OperatorRelation_Rowtype_Id 10004

CATALOG(pg_operator,2617) BKI_SCHEMA_MACRO
{
    NameData    oprname;        /* name of operator */
    Oid         oprnamespace;   /* OID of namespace containing this oper */
    Oid         oprowner;       /* operator owner */
    char        oprkind;        /* 'l', 'r', or 'b' */
    bool        oprcanmerge;    /* can be used in merge join? */
    bool        oprcanhash;     /* can be used in hash join? */
    Oid         oprleft;        /* left arg type, or 0 if 'l' oprkind */
    Oid         oprright;       /* right arg type, or 0 if 'r' oprkind */
    Oid         oprresult;      /* result datatype */
    Oid         oprcom;         /* OID of commutator oper, or 0 if none */
    Oid         oprnegate;      /* OID of negator oper, or 0 if none */
    regproc     oprcode;        /* OID of underlying function */
    regproc     oprrest;        /* OID of restriction estimator, or 0 */
    regproc     oprjoin;        /* OID of join estimator, or 0 */
} FormData_pg_operator;

/* ----------------
 *        Form_pg_operator corresponds to a pointer to a tuple with
 *        the format of pg_operator relation.
 * ----------------
 */
typedef FormData_pg_operator *Form_pg_operator;

/* ----------------
 *        compiler constants for pg_operator
 * ----------------
 */
#define Natts_pg_operator               14
#define Anum_pg_operator_oprname        1
#define Anum_pg_operator_oprnamespace   2
#define Anum_pg_operator_oprowner       3
#define Anum_pg_operator_oprkind        4
#define Anum_pg_operator_oprcanmerge    5
#define Anum_pg_operator_oprcanhash     6
#define Anum_pg_operator_oprleft        7
#define Anum_pg_operator_oprright       8
#define Anum_pg_operator_oprresult      9
#define Anum_pg_operator_oprcom         10
#define Anum_pg_operator_oprnegate      11
#define Anum_pg_operator_oprcode        12
#define Anum_pg_operator_oprrest        13
#define Anum_pg_operator_oprjoin        14

/* ----------------
 *        initial contents of pg_operator
 * ----------------
 */

/*
 * Note: every entry in pg_operator.h is expected to have a DESCR() comment.
 * If the operator is a deprecated equivalent of some other entry, be sure
 * to comment it as such so that initdb doesn't think it's a preferred name
 * for the underlying function.
 */

#define INT48EQOID   15
#define INT48NEOID   36
#define INT48LTOID   37
#define INT48GTOID   76
#define INT48LEOID   80
#define INT48GEOID   82
#define BooleanNotEqualOperator   85
#define BooleanEqualOperator   91
#define CHAREQOID 92
#define INT2EQOID    94
#define INT2LTOID    95
#define INT4EQOID    96
#define INT4LTOID    97
#define TEXTEQOID 98
#define TIDEqualOperator   387
#define TIDLessOperator    2799
#define INT8EQOID    410
#define INT8NEOID    411
#define INT8LTOID    412
#define INT8GTOID    413
#define INT8LEOID    414
#define INT8GEOID    415
#define INT84EQOID    416
#define INT84NEOID    417
#define INT84LTOID    418
#define INT84GTOID    419
#define INT84LEOID    420
#define INT84GEOID    430
#define INT4MULOID 514
#define INT4NEOID    518
#define INT2NEOID    519
#define INT2GTOID    520
#define INT4GTOID    521
#define INT2LEOID    522
#define INT4LEOID    523
#define INT2GEOID    524
#define INT4GEOID    525
#define INT2MULOID 526
#define INT2DIVOID 527
#define INT4DIVOID 528
#define INT24EQOID    532
#define INT42EQOID    533
#define INT24LTOID    534
#define INT42LTOID    535
#define INT24GTOID    536
#define INT42GTOID    537
#define INT24NEOID    538
#define INT42NEOID    539
#define INT24LEOID    540
#define INT42LEOID    541
#define INT24GEOID    542
#define INT42GEOID    543
#define INT24MULOID 544
#define INT42MULOID 545
#define INT24DIVOID 546
#define INT42DIVOID 547
#define INT2PLOID 550
#define INT4PLOID 551
#define INT24PLOID 552
#define INT42PLOID 553
#define INT2MIOID 554
#define INT4MIOID 555
#define INT24MIOID 556
#define INT42MIOID 557
#define FLOAT4EQOID    620
#define FLOAT4NEOID    621
#define FLOAT4LTOID    622
#define FLOAT4GTOID    623
#define FLOAT4LEOID    624
#define FLOAT4GEOID    625
#define INT1EQOID 5513
#define OID_NAME_REGEXEQ_OP        639
#define OID_TEXT_REGEXEQ_OP        641
#define TEXTLTOID 664
#define TEXTGTOID 666
#define FLOAT8EQOID    670
#define FLOAT8NEOID    671
#define FLOAT8LTOID    672
#define FLOAT8LEOID    673
#define FLOAT8GTOID    674
#define FLOAT8GEOID    675
#define INT8PLOID 684
#define INT8MIOID 685
#define INT8MULOID 686
#define INT8DIVOID 687
#define INT84PLOID 688
#define INT84MIOID 689
#define INT84MULOID 690
#define INT84DIVOID 691
#define INT48PLOID 692
#define INT48MIOID 693
#define INT48MULOID 694
#define INT48DIVOID 695
#define INT82PLOID 818
#define INT82MIOID 819
#define INT82MULOID 820
#define INT82DIVOID 821
#define INT28PLOID 822
#define INT28MIOID 823
#define INT28MULOID 824
#define INT28DIVOID 825
#define BPCHAREQOID 1054
#define OID_BPCHAR_REGEXEQ_OP        1055
#define BPCHARNEOID 1057
#define BPCHARLTOID 1058
#define BPCHARGTOID 1060
#define ARRAY_EQ_OP 1070
#define ARRAY_LT_OP 1072
#define ARRAY_GT_OP 1073
#define DATEEQOID 1093
#define DATENEOID 1094
#define DATELTOID 1095
#define DATELEOID 1096
#define DATEGTOID 1097
#define DATEGEOID 1098
#define TIMETZEQOID    1550
#define FLOAT48EQOID 1120
#define FLOAT48NEOID 1121
#define FLOAT48LTOID 1122
#define FLOAT48GTOID 1123
#define FLOAT48LEOID 1124
#define FLOAT48GEOID 1125
#define FLOAT84EQOID 1130
#define FLOAT84NEOID 1131
#define FLOAT84LTOID 1132
#define FLOAT84GTOID 1133
#define FLOAT84LEOID 1134
#define FLOAT84GEOID 1135
#define OID_NAME_LIKE_OP        1207
#define OID_NAME_NOT_LIKE_OP        1208
#define OID_TEXT_LIKE_OP        1209
#define TEXTNOTLIKEOID 1210
#define OID_BPCHAR_LIKE_OP        1211
#define OID_BPCHAR_NOT_LIKE_OP  1212
#define OID_NAME_ICREGEXEQ_OP        1226
#define OID_TEXT_ICREGEXEQ_OP        1228
#define OID_BPCHAR_ICREGEXEQ_OP        1234
#define TIMESTAMPTZLTOID  1322
#define TIMESTAMPTZLEOID  1323
#define TIMESTAMPTZGTOID  1324
#define TIMESTAMPTZGEOID  1325
#define INTERVALEQOID    1330
#define OID_INET_SUB_OP                  931
#define OID_INET_SUBEQ_OP                932
#define OID_INET_SUP_OP                  933
#define OID_INET_SUPEQ_OP                934
#define OID_NAME_ICLIKE_OP        1625
#define OID_TEXT_ICLIKE_OP        1627
#define OID_BPCHAR_ICLIKE_OP    1629
#define NUMEQOID 1752
#define NUMERICEQOID 1752
#define NUMERICNEOID 1753
#define NUMERICLTOID 1754
#define NUMERICLEOID 1755
#define NUMERICGTOID 1756
#define NUMERICGEOID 1757
#define NUMERICADDOID 1758
#define NUMERICSUBOID 1759
#define NUMERICMULOID 1760
#define NUMERICDIVOID 1761
#define NUMERICMODOID 1762
#define INT28EQOID    1862
#define INT28NEOID    1863
#define INT28LTOID    1864
#define INT28GTOID    1865
#define INT28LEOID    1866
#define INT28GEOID    1867
#define INT82EQOID    1868
#define INT82NEOID    1869
#define INT82LTOID    1870
#define INT82GTOID    1871
#define INT82LEOID    1872
#define INT82GEOID    1873
#define OID_BYTEA_LIKE_OP        2016
#define OID_RAW_LIKE_OP        3804
#define TIMESTAMPEQOID 2060
#define TIMESTAMPNEOID 2061
#define TIMESTAMPLTOID 2062
#define TIMESTAMPLEOID 2063
#define TIMESTAMPGTOID 2064
#define TIMESTAMPGEOID 2065
#define OID_ARRAY_OVERLAP_OP    2750
#define OID_ARRAY_CONTAINS_OP    2751
#define OID_ARRAY_CONTAINED_OP    2752
#define RECORD_EQ_OP 2988
#define RECORD_LT_OP 2990
#define RECORD_GT_OP 2991
extern void OperatorCreate(const char *operatorName, Oid operatorNamespace, Oid leftTypeId, Oid rightTypeId, Oid procedureId,
    List *commutatorName, List *negatorName, Oid restrictionId, Oid joinId, bool canMerge, bool canHash);

extern Oid OperatorGet(const char *operatorName, Oid operatorNamespace, Oid leftObjectId, Oid rightObjectId, bool *defined);

#endif   /* PG_OPERATOR_H */

