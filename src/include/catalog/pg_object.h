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
 * pg_object.h
 *        definition of the system "object" relation (pg_object).
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_object.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_OBJECT_H
#define PG_OBJECT_H

#include "catalog/genbki.h"
#include "fmgr.h"
#include "nodes/parsenodes.h"
#include "catalog/objectaddress.h"
#include "postgres.h"


#ifndef timestamptz
#ifdef HAVE_INT64_TIMESTAMP
#define timestamptz int64
#else
#define timestamptz double
#endif
#define new_timestamptz
#endif

/*-------------------------------------------------------------------------
 *        pg_object definition.  cpp turns this into
 *        typedef struct FormData_pg_object
 *-------------------------------------------------------------------------
 */
#define PgObjectRelationId 9025
#define PgObjectRelationId_Rowtype_Id 11661

CATALOG(pg_object,9025)   BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
    Oid            object_oid;         /* Object oid. */
    char           object_type;        /* Object type. */
    Oid            creator;            /* Who create the object. */
    timestamptz    ctime;              /* When create the object. */
    timestamptz    mtime;              /* When modify the object. */
    int8           createcsn;          /* When create relation */
    int8           changecsn;          /* When modify the table structure or store properties */
    bool           valid;              /* Is valid? */
} FormData_pg_object;

#ifdef new_timestamptz
#undef new_timestamptz
#undef timestamptz
#endif

/*-------------------------------------------------------------------------
 *        Form_pg_object corresponds to a pointer to a tuple with
 *        the format of pg_object relation.
 *-------------------------------------------------------------------------
 */
typedef FormData_pg_object* Form_pg_object;

/*-------------------------------------------------------------------------
 *        compiler constants for pg_object
 *-------------------------------------------------------------------------
 */
#define Natts_pg_object                          8
#define Anum_pg_object_oid                       1
#define Anum_pg_object_type                      2
#define Anum_pg_object_creator                   3
#define Anum_pg_object_ctime                     4
#define Anum_pg_object_mtime                     5
#define Anum_pg_object_createcsn                 6
#define Anum_pg_object_changecsn                 7
#define Anum_pg_object_valid                     8

#define PgObjectType char

/* Define the type of object which maybe different with object is pg_class. */
#define OBJECT_TYPE_INVALID '\0'
#define OBJECT_TYPE_RELATION 'r'
#define OBJECT_TYPE_FOREIGN_TABLE 'f'
#define OBJECT_TYPE_INDEX 'i'
#define OBJECT_TYPE_SEQUENCE 's'
#define OBJECT_TYPE_LARGE_SEQUENCE 'l'
#define OBJECT_TYPE_VIEW 'v'
#define OBJECT_TYPE_CONTQUERY 'o'
#define OBJECT_TYPE_PROC 'P'
#define OBJECT_TYPE_STREAM 'e'
#define OBJECT_TYPE_PKGSPEC 'S'
#define OBJECT_TYPE_PKGBODY 'B'
#define OBJECT_TYPE_MATVIEW 'm'

extern bool GetPgObjectValid(Oid oid, PgObjectType objectType);
extern bool SetPgObjectValid(Oid oid, PgObjectType objectType, bool valid);
extern bool GetCurrCompilePgObjStatus();
extern void SetCurrCompilePgObjStatus(bool status);
extern void UpdateCurrCompilePgObjStatus(bool status);
extern void InvalidateCurrCompilePgObj();
extern void CreatePgObject(Oid objectOid, PgObjectType objectType, Oid creator, const PgObjectOption objectOpt, bool isValid = true);
extern void DeletePgObject(Oid objectOid, PgObjectType objectType);
extern void GetObjectCSN(Oid objectOid, Relation userRel, PgObjectType objectType, ObjectCSN * const csnInfo);
void UpdatePgObjectMtime(Oid objectOid, PgObjectType objectType);
void UpdatePgObjectChangecsn(Oid objectOid, PgObjectType objectType);
extern PgObjectType GetPgObjectTypePgClass(char relkind);
extern void recordCommentObjectTime(ObjectAddress addr, Relation rel, ObjectType objType);
extern void recordRelationMTime(Oid relOid, char relkind);

#endif /* PG_OBJECT_H */
