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
 * pg_directory.h
 *        definition of the system "pg_directory" relation (pg_directory)
 *        along with the relation's initial contents.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_directory.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_DIRECTORY_H
#define PG_DIRECTORY_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/genbki.h"
#include "fmgr.h"

/* -------------------------------------------------------------------------
 *        pg_directories definition.  cpp turns this into
 *        typedef struct FormData_pg_directory
 * -------------------------------------------------------------------------
 */
#define PgDirectoryRelationId 4347
#define PgDirectoryRelation_Rowtype_Id 11662

CATALOG(pg_directory,4347) BKI_SCHEMA_MACRO
{
    NameData    dirname;    /* directory name */
    Oid         owner;      /* owner of directory */
    text        dirpath;    /* real directory path */
#ifdef CATALOG_VARLEN       /* variable-length fields start here*/
    aclitem     diracl[1];  /* access permissions */
#endif
} FormData_pg_directory;


/* -------------------------------------------------------------------------
 *        Form_pg_directory corresponds to a pointer to a tuple with
 *        the format of pg_directory relation.
 * -------------------------------------------------------------------------
 */
typedef FormData_pg_directory* Form_pg_directory;

/* -------------------------------------------------------------------------
 *        compiler constants for pg_directory
 * -------------------------------------------------------------------------
 */
#define Natts_pg_directory                          4
#define Anum_pg_directory_directory_name            1
#define Anum_pg_directory_owner                     2
#define Anum_pg_directory_directory_path            3
#define Anum_pg_directory_directory_acl             4

#endif /* PG_DIRECTORY_H */

