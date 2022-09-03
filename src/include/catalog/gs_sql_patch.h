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
 * -------------------------------------------------------------------------
 *
 * gs_sql_patch.h
 *
 * IDENTIFICATION
 *    src/include/catalog/gs_sql_patch.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_SQL_PATCH_H
#define GS_SQL_PATCH_H

#include "postgres.h"
#include "catalog/genbki.h"

/* to make data type compatible for genbki */
#define int8 int64

#define GsSqlPatchRelationId  9050
#define GsSqlPatchRelationId_Rowtype_Id 9051

CATALOG(gs_sql_patch,9050) BKI_ROWTYPE_OID(9051) BKI_SCHEMA_MACRO BKI_WITHOUT_OIDS
{
    NameData        patch_name;
    int8            unique_sql_id;
    Oid             owner; /* owner of sql patch */
    bool            enable;
    char            status; /* reserved attribute */
    bool            abort;
#ifdef CATALOG_VARLEN           /* variable-length fields start here */
    text            hint_string;
    pg_node_tree    hint_node;    
    text            original_query; /* reserved attribute */
    pg_node_tree    original_query_tree; /* reserved attribute */
    text            patched_query;  /* reserved attribute */
    pg_node_tree    patched_query_tree; /* reserved attribute */
    text            description;
#endif
} FormData_gs_sql_patch;

#define STATUS_DEFAULT 'd'

typedef FormData_gs_sql_patch *Form_gs_sql_patch;

#define Natts_gs_sql_patch                      13

#define Anum_gs_sql_patch_patch_name            1
#define Anum_gs_sql_patch_unique_sql_id         2
#define Anum_gs_sql_patch_owner                 3
#define Anum_gs_sql_patch_enable                4
#define Anum_gs_sql_patch_status                5
#define Anum_gs_sql_patch_abort                 6
#define Anum_gs_sql_patch_hint_string           7
#define Anum_gs_sql_patch_hint_node             8
#define Anum_gs_sql_patch_original_query        9
#define Anum_gs_sql_patch_original_query_tree   10
#define Anum_gs_sql_patch_patched_query         11
#define Anum_gs_sql_patch_patched_query_tree    12
#define Anum_gs_sql_patch_description           13

#endif   /* GS_SQL_PATCH_H */