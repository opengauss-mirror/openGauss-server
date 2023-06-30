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
 * pg_set.h
 *      definition of the system "set" relation (pg_set)
 *      along with the relation's initial contents.
 *
 *
 *
 * src/include/catalog/pg_set.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 *      XXX do NOT break up DATA() statements into multiple lines!
 *          the scripts are not as smart as you might think...
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SET_H
#define PG_SET_H

#include "catalog/genbki.h"
#include "nodes/pg_list.h"

/* ----------------
 *        pg_set definition.  cpp turns this into
 *        typedef struct FormData_pg_set
 * ----------------
 */
#define SetRelationId    3516
#define SetRelation_Rowtype_Id 11654

CATALOG(pg_set,3516) BKI_SCHEMA_MACRO
{
    Oid     settypid;        /* OID of set type */
    int1    setnum;          /* number of set value, not include null string */
    int1    setsortorder;    /* sort position of this set value */
    text    setlabel;        /* text representation of set value */
} FormData_pg_set;

/* ----------------
 *        Form_pg_set corresponds to a pointer to a tuple with
 *        the format of pg_set relation.
 * ----------------
 */
typedef FormData_pg_set *Form_pg_set;

/* ----------------
 *        compiler constants for pg_set
 * ----------------
 */
#define Natts_pg_set                    4
#define Anum_pg_set_settypid            1
#define Anum_pg_set_setnum              2
#define Anum_pg_set_setsortorder        3
#define Anum_pg_set_setlabel            4

/* ----------------
 *        pg_set has no initial contents
 * ----------------
 */
 
/*
 * prototypes for functions in pg_set.cpp
 */
#define SETLABELNUM (64)
#define SETNAMELEN  (255)
#define SETLABELDELIMIT ","

extern void SetValuesCreate(Oid setTypeOid, List *vals, Oid collation);
extern void SetValuesDelete(Oid setTypeOid);
extern Datum GetSetDefineStr(Oid settypid);

#endif   /* PG_SET_H */
