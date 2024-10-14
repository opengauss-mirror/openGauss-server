/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * pg_object_type.h
 *        definition of the system "object type" relation (pg_object_type).
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_object_type.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_OBJECT_TYPE_H
#define PG_OBJECT_TYPE_H

#include "catalog/genbki.h"

/*-------------------------------------------------------------------------
 *        pg_object_type definition.  cpp turns this into
 *        typedef struct FormData_pg_object_type
 *-------------------------------------------------------------------------
 */
#define ObjectTypeRelationId 9815
#define ObjectTypeRelation_Rowtype_Id 2989

CATALOG(pg_object_type,9815) BKI_BOOTSTRAP BKI_ROWTYPE_OID(2989) BKI_SCHEMA_MACRO
{
    Oid           typoid;               /* type oid. */
    Oid           supertypeoid;         /* type oid of parent type. */
    bool          isfinal;              /* final means not allow to create subtype. */
    bool          isinstantiable;       /* Is instantiable or not? always true for now */
    bool          ispersistable;        /* Is persistable or not? always true for now */
    bool          isbodydefined;        /* Is type body built? */
    Oid           mapmethod;            /* map method oid. */
    Oid           ordermethod;          /* order method oid. */
    Oid           objectrelid;          /* oid for object table. */
    int4          objectoptions;        /* preseverd parameter for future development, always 0 for now, use your own mask if necessary */
#ifdef CATALOG_VARLEN                   /* variable-length fields start here */
    text          typespecsrc;          /* package specification */
    text          typebodydeclsrc;      /* package declare */
    text          objectextensions[1];  /* text array for future development, always null for now */
#endif
} FormData_pg_object_type;

/*-------------------------------------------------------------------------
 *        Form_pg_object_type corresponds to a pointer to a tuple with
 *        the format of pg_object relation.
 *-------------------------------------------------------------------------
 */
typedef FormData_pg_object_type* Form_pg_object_type;

/*-------------------------------------------------------------------------
 *        compiler constants for pg_object
 *-------------------------------------------------------------------------
 */
#define Natts_pg_object_type                          13
#define Anum_pg_object_type_typoid                    1
#define Anum_pg_object_type_supertypeoid              2
#define Anum_pg_object_type_isfinal                   3
#define Anum_pg_object_type_isinstantiable            4
#define Anum_pg_object_type_ispersistable             5
#define Anum_pg_object_type_isbodydefined             6
#define Anum_pg_object_type_mapmethod                 7
#define Anum_pg_object_type_ordermethod               8
#define Anum_pg_object_type_objectrelid               9
#define Anum_pg_object_type_objectoptions             10
#define Anum_pg_object_type_typespecsrc               11
#define Anum_pg_object_type_typebodydeclsrc           12
#define Anum_pg_object_type_objectextensions          13


extern bool isNeedObjectCmp(Oid typid, Oid *mapid, Oid *orderid);
extern int ObjectIntanceCmp(Datum arg1, Datum arg2, Oid mapid, Oid orderid);

Datum object_table_value(FunctionCallInfo fcinfo);
extern bool isObjectTypeAttributes(Oid objectypeid, char* attr);
#endif /* PG_OBJECT_TYPE_H */
