/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * pg_recyclebin.h
 *        define the recyclebin for time capsule manager.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_recyclebin.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PG_RECYCLEBIN_H
#define PG_RECYCLEBIN_H

#include "catalog/genbki.h"

#define int8 int64
#define timestamptz Datum

/* ----------------
 *    pg_recyclebin definition.  cpp turns this into
 *    typedef struct FormData_pg_recyclebin
 * ----------------
 */

#define RecyclebinRelationId  8643
#define RecyclebinRelation_Rowtype_Id 8644

CATALOG(gs_recyclebin,8643) BKI_SCHEMA_MACRO
{
    /* base rb item id, refer to pg_recyclebin.oid */
    Oid rcybaseid;

    /* database id */
    Oid rcydbid;

    /* relation id */
    Oid rcyrelid;

    /* unique recycle object name */
    NameData rcyname;

    /* origin name before truncate or drop */
    NameData rcyoriginname;

    /* operation type: 't' truncate, 'd' drop */
    char rcyoperation;

    /* object type: 0 table, 1 index, 2 toast table, 3 toast index */
    int4 rcytype;

    /* drop/truncate csn */
    int8 rcyrecyclecsn;

    /* drop/truncate timestamp */
    timestamptz rcyrecycletime;

    /* create csn */
    int8 rcycreatecsn;

    /* latest ddl csn */
    int8 rcychangecsn;

    /* OID of namespace containing this class */
    Oid rcynamespace;

    /* object owner */
    Oid rcyowner;

    /* identifier of table space for object */
    Oid rcytablespace;

    /* identifier of physical storage file */
    Oid rcyrelfilenode;

    /* whether object can be restore */
    bool rcycanrestore;

    /* whether object can be purged */
    bool rcycanpurge;

    /* all Xids < this are frozen in this rel */
    ShortTransactionId rcyfrozenxid;

#ifdef CATALOG_VARLEN            /* variable-length fields start here */
    /* all Xids < this are frozen in this rel */
    TransactionId rcyfrozenxid64;
#endif
} FormData_pg_recyclebin;

/* ----------------
 *        Form_pg_recyclebin corresponds to a pointer to a tuple with
 *        the format of pg_recyclebin relation.
 * ----------------
 */
typedef FormData_pg_recyclebin *Form_pg_recyclebin;

/* ----------------
 *        compiler constants for pg_recyclebin
 * ----------------
 */
#define Natts_pg_recyclebin                    19
#define Anum_pg_recyclebin_rcybaseid           1
#define Anum_pg_recyclebin_rcydbid             2
#define Anum_pg_recyclebin_rcyrelid            3
#define Anum_pg_recyclebin_rcyname             4
#define Anum_pg_recyclebin_rcyoriginname       5
#define Anum_pg_recyclebin_rcyoperation        6
#define Anum_pg_recyclebin_rcytype             7
#define Anum_pg_recyclebin_rcyrecyclecsn       8
#define Anum_pg_recyclebin_rcyrecycletime      9
#define Anum_pg_recyclebin_rcycreatecsn        10
#define Anum_pg_recyclebin_rcychangecsn        11
#define Anum_pg_recyclebin_rcynamespace        12
#define Anum_pg_recyclebin_rcyowner            13
#define Anum_pg_recyclebin_rcytablespace       14
#define Anum_pg_recyclebin_rcyrelfilenode      15
#define Anum_pg_recyclebin_rcycanrestore       16
#define Anum_pg_recyclebin_rcycanpurge         17
#define Anum_pg_recyclebin_rcyfrozenxid        18
#define Anum_pg_recyclebin_rcyfrozenxid64      19

/* Size of fixed part of pg_recyclebin tuples, not counting var-length fields */
#define RECYCLEBIN_TUPLE_SIZE \
    (offsetof(FormData_pg_recyclebin, rcyfrozenxid) + sizeof(ShortTransactionId))

/* rcyoperation: drop table, truncate table */
#define RCYOPERATION_DROP 'd'
#define RCYOPERATION_TRUNCATE 't'

#undef int8
#undef timestamptz

#endif /* PG_RECYCLEBIN_H */

