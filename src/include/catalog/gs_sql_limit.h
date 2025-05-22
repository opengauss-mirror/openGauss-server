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
 * gs_sql_limit.h
 *
 * IDENTIFICATION
 *    src/include/catalog/gs_sql_limit.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_SQL_LIMIT_H
#define GS_SQL_LIMIT_H

#include "postgres.h"
#include "catalog/genbki.h"

/* to make data type compatible for genbki */
#define int8 int64

#define GsSqlLimitRelationId  3240
#define GsSqlLimitRelationId_Rowtype_Id 3241

CATALOG(gs_sql_limit,3240) BKI_SHARED_RELATION BKI_ROWTYPE_OID(3241) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
    int8            limit_id;
    NameData        limit_name;
    bool            is_valid;
    int1            work_node;
    int8            max_concurrency;
#ifdef CATALOG_VARLEN           /* variable-length fields start here */
    timestamptz     start_time;
    timestamptz     end_time;
    text            limit_type;
    NameData        databases[1];
    NameData        users[1];
    text            limit_opt[1];
#endif
} FormData_gs_sql_limit;

#undef int8

typedef FormData_gs_sql_limit *Form_gs_sql_limit;

#define Natts_gs_sql_limit                      11

#define Anum_gs_sql_limit_limit_id              1
#define Anum_gs_sql_limit_limit_name            2
#define Anum_gs_sql_limit_is_valid              3
#define Anum_gs_sql_limit_work_node             4
#define Anum_gs_sql_limit_max_concurrency       5
#define Anum_gs_sql_limit_start_time            6
#define Anum_gs_sql_limit_end_time              7
#define Anum_gs_sql_limit_limit_type            8
#define Anum_gs_sql_limit_databases             9
#define Anum_gs_sql_limit_users                 10
#define Anum_gs_sql_limit_limit_opt             11

#endif   /* GS_SQL_LIMIT_H */