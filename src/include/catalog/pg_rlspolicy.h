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
 * pg_rlspolicy.h
 *        definition of the row level security policy system catalog (pg_rlspolicy)
 *        the genbki.pl script reads this file and generates postgres.bki information.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_rlspolicy.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_RLSPOLICY_H
#define PG_RLSPOLICY_H

#include "catalog/genbki.h"

#define RlsPolicyRelationId 3254
#define RlsPolicyRelation_Rowtype_Id 11652

CATALOG(pg_rlspolicy,3254) BKI_SCHEMA_MACRO
{
    NameData    polname;         /* Rlspolicy name */
    Oid         polrelid;        /* Oid of the relation with rlspolicy */
    char        polcmd;          /* Keep same with ACL_*_CHR, support 'a','r','w','d','*'(all) */
    bool        polpermissive;   /* Permissive or Restrictive rlspolicy */
#ifdef CATALOG_VARLEN            /* variable-length fields start here */
    Oid         polroles[1];     /* Roles associated with rlspolicy */
    pg_node_tree polqual;        /* Using quals */
#endif
} FormData_pg_rlspolicy;

/* ----------------
 *      Form_pg_rlspolicy corresponds to a pointer to a row with
 *      the format of pg_rlspolicy relation.
 * ----------------
 */
typedef FormData_pg_rlspolicy *Form_pg_rlspolicy;

/* ----------------
 *      compiler constants for pg_rlspolicy
 * ----------------
 */
#define Natts_pg_rlspolicy                6
#define Anum_pg_rlspolicy_polname         1
#define Anum_pg_rlspolicy_polrelid        2
#define Anum_pg_rlspolicy_polcmd          3
#define Anum_pg_rlspolicy_polpermissive   4
#define Anum_pg_rlspolicy_polroles        5
#define Anum_pg_rlspolicy_polqual         6

#endif   /* PG_RLSPOLICY_H */

