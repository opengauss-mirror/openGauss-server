/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * --------------------------------------------------------------------------------------
 *
 * pg_statistic_history.h
 *      definition of the system "statistic" relation (pg_statistic_history)
 *      along with the relation's initial contents.
 *
 * src/include/catalog/pg_statistic_history.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_STATISTIC_HISTORY_H
#define PG_STATISTIC_HISTORY_H

#include "catalog/genbki.h"
#include "commands/vacuum.h"

#define timestamptz Datum

/* ----------------
 *        pg_statistic_history definition.  cpp turns this into
 *        typedef struct FormData_pg_statistic_history
 * ----------------
 */
#define StatisticHistoryRelationId  4885
#define StatisticHistoryRelation_Rowtype_Id 4886

CATALOG(pg_statistic_history,4885) BKI_WITHOUT_OIDS BKI_ROWTYPE_OID(4886) BKI_SCHEMA_MACRO
{
    /* These fields form the unique key for the entry: */
    Oid         namespaceid;
    Oid         starelid;           /* relation containing attribute */
    Oid         partid;
    char        statype;            /* 't': statistic of tables
                                     * 'p': statistic of partitions
                                     * 'c': statistic of attributes and indexes
                                     * */
    timestamptz last_analyzetime;
    timestamptz current_analyzetime;
    char        starelkind;
    int2        staattnum;
    bool        stainherit;
    float4      stanullfrac;
    int4        stawidth;
    float4      stadistinct;
    float8      reltuples;
    float8      relpages;
    char        stalocktype;

    int2        stakind1;
    int2        stakind2;
    int2        stakind3;
    int2        stakind4;
    int2        stakind5;

    Oid         staop1;
    Oid         staop2;
    Oid         staop3;
    Oid         staop4;
    Oid         staop5;
#ifdef CATALOG_VARLEN               /* variable-length fields start here */
    float4      stanumbers1[1];
    float4      stanumbers2[1];
    float4      stanumbers3[1];
    float4      stanumbers4[1];
    float4      stanumbers5[1];

    /*
     * Values in these arrays are values of the column's data type, or of some
     * related type such as an array element type.    We presently have to cheat
     * quite a bit to allow polymorphic arrays of this kind, but perhaps
     * someday it'll be a less bogus facility.
     */
    anyarray    stavalues1;
    anyarray    stavalues2;
    anyarray    stavalues3;
    anyarray    stavalues4;
    anyarray    stavalues5;
    float4      stadndistinct;
    text        statextinfo;
#endif
} FormData_pg_statistic_history;

#undef timestamptz

/* ----------------
 *        Form_pg_statistic corresponds to a pointer to a tuple with
 *        the format of pg_statistic_history relation.
 * ----------------
 */
typedef FormData_pg_statistic_history *Form_pg_statistic_history;

/* ----------------
 *        compiler constants for pg_statistic_history
 * ----------------
 */
#define Natts_pg_statistic_history                      37
#define Anum_pg_statistic_history_namespaceid           1
#define Anum_pg_statistic_history_starelid              2
#define Anum_pg_statistic_history_partid                3
#define Anum_pg_statistic_history_statype               4
#define Anum_pg_statistic_history_last_analyzetime      5
#define Anum_pg_statistic_history_current_analyzetime   6
#define Anum_pg_statistic_history_starelkind            7
#define Anum_pg_statistic_history_staattnum              8
#define Anum_pg_statistic_history_stainherit            9
#define Anum_pg_statistic_history_stanullfrac           10
#define Anum_pg_statistic_history_stawidth              11
#define Anum_pg_statistic_history_stadistinct           12
#define Anum_pg_statistic_history_reltuples             13
#define Anum_pg_statistic_history_relpages              14
#define Anum_pg_statistic_history_stalocktype           15
#define Anum_pg_statistic_history_stakind1              16
#define Anum_pg_statistic_history_stakind2              17
#define Anum_pg_statistic_history_stakind3              18
#define Anum_pg_statistic_history_stakind4              19
#define Anum_pg_statistic_history_stakind5              20
#define Anum_pg_statistic_history_staop1                21
#define Anum_pg_statistic_history_staop2                22
#define Anum_pg_statistic_history_staop3                23
#define Anum_pg_statistic_history_staop4                24
#define Anum_pg_statistic_history_staop5                25
#define Anum_pg_statistic_history_stanumbers1           26
#define Anum_pg_statistic_history_stanumbers2           27  
#define Anum_pg_statistic_history_stanumbers3           28  
#define Anum_pg_statistic_history_stanumbers4           29  
#define Anum_pg_statistic_history_stanumbers5           30  
#define Anum_pg_statistic_history_stavalues1            31
#define Anum_pg_statistic_history_stavalues2            32
#define Anum_pg_statistic_history_stavalues3            33
#define Anum_pg_statistic_history_stavalues4            34
#define Anum_pg_statistic_history_stavalues5            35
#define Anum_pg_statistic_history_stadndistinct         36
#define Anum_pg_statistic_history_staextinfo           37

/*
 * user statistic table columns
 */
#define Natts_user_table              34
#define Anum_user_table_namespaceid    1
#define Anum_user_table_starelid       2
#define Anum_user_table_partid         3
#define Anum_user_table_statype        4
#define Anum_user_table_starelkind     5
#define Anum_user_table_staattnum      6
#define Anum_user_table_stainherit     7
#define Anum_user_table_stanullfrac    8
#define Anum_user_table_stawidth       9
#define Anum_user_table_stadistinct    10
#define Anum_user_table_reltuples      11
#define Anum_user_table_relpages       12
#define Anum_user_table_stakind1       13
#define Anum_user_table_stakind2       14
#define Anum_user_table_stakind3       15
#define Anum_user_table_stakind4       16
#define Anum_user_table_stakind5       17
#define Anum_user_table_staop1         18
#define Anum_user_table_staop2         19
#define Anum_user_table_staop3         20
#define Anum_user_table_staop4         21
#define Anum_user_table_staop5         22
#define Anum_user_table_stanumbers1    23
#define Anum_user_table_stanumbers2    24
#define Anum_user_table_stanumbers3    25
#define Anum_user_table_stanumbers4    26
#define Anum_user_table_stanumbers5    27
#define Anum_user_table_stavalues1     28
#define Anum_user_table_stavalues2     29
#define Anum_user_table_stavalues3     30
#define Anum_user_table_stavalues4     31
#define Anum_user_table_stavalues5     32
#define Anum_user_table_stadndistinct  33
#define Anum_user_table_staextinfo     34

#define STATYPE_RELATION    't'
#define STATYPE_PARTITION   'p'
#define STATYPE_COLUMN      'c'

#define STALOCKTYPE_RELATION    't'
#define STALOCKTYPE_SCHEMA      's'
#define STALOCKTYPE_PARTITION   'p'

extern TimestampTz GetRecentAnalyzeTime(TimestampTz asOfTime, Oid relid, char statype);
extern void InsertColumnStatisticHistory(Oid relid, char relkind, bool inh, VacAttrStats* stats);
extern void ImportColumnStatisticHistory(HeapTuple tuple, TupleDesc tupdesc, Oid namespaceid, Oid relid,
                                        char relkind, int16 attnum, TimestampTz currentAnalyzetime);
extern void InsertClassStatisHistory(Oid relid, double numpages, double numtuples);
extern void InsertPartitionStatisticHistory(Oid relid, Oid partid, double numpages, double numtuples);
extern void RemoveStatisticHistory(Oid relid, AttrNumber attnum);

#endif   /* PG_STATISTIC_HISTORY_H */