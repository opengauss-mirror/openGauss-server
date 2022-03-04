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
 * pg_synonym.h
 *        definition of the system "synonym" relation (pg_synonym)
 *        along with the relation's initial contents.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_synonym.h
 *
 * NOTES
 *    the genbki.sh script reads this file and generates .bki
 *    information from the DATA() statements.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PG_SYNONYM_H
#define PG_SYNONYM_H

#include "catalog/genbki.h"
#include "nodes/parsenodes.h"

/* ----------------------------------------------------------------
 *  pg_synonym definition.
 *
 *  synname             name of the synonym
 *  synnamespace        name space which the synonym belongs to
 *  synowner            owner of the synonym
 *  synobjschema        schema name which the referenced object belongs to
 *  synobjname          referenced object name
 * ----------------------------------------------------------------
 */
#define PgSynonymRelationId 3546
#define PgSynonymRelationId_Rowtype_Id 11662

#define SYNONYM_VERSION_NUM 92059

CATALOG(pg_synonym,3546) BKI_SCHEMA_MACRO
{
    NameData    synname;
    Oid         synnamespace;
    Oid         synowner;
    NameData    synobjschema;
    NameData    synobjname;
} FormData_pg_synonym;

/* ----------------
 *      Form_pg_synonym corresponds to a pointer to a tuple with
 *      the format of pg_synonym relation.
 * ----------------
 */
typedef FormData_pg_synonym *Form_pg_synonym;

/* ----------------
 *      compiler constants for pg_synonym
 * ----------------
 */
#define Natts_pg_synonym                5
#define Anum_pg_synonym_synname         1
#define Anum_pg_synonym_synnamespace    2
#define Anum_pg_synonym_synowner        3
#define Anum_pg_synonym_synobjschema    4
#define Anum_pg_synonym_synobjname      5

/* ----------------
 * initial contents of pg_synonym
 * ---------------
 */

/*
 * function prototypes
 */
extern void CreateSynonym(CreateSynonymStmt *stmt);
extern void DropSynonym(DropSynonymStmt *stmt);
extern void AlterSynonymOwner(List *name, Oid newOwnerId);
extern void RemoveSynonymById(Oid synonymOid);
extern RangeVar* SearchReferencedObject(const char *synName, Oid synNamespace);
extern RangeVar* ReplaceReferencedObject(const RangeVar *relation);
extern char* CheckReferencedObject(Oid relOid, RangeVar *objVar, const char *synName);
extern Oid GetSynonymOid(const char *synName, Oid synNamespace, bool missing);
extern char* GetQualifiedSynonymName(Oid synOid, bool qualified = true);
extern void GetSynonymAndSchemaName(Oid synOid, char **synName_p, char **synSchema_p);
extern void AlterSynonymOwnerByOid(Oid synonymOid, Oid newOwnerId);

#endif   /* PG_SYNONYM_H */


