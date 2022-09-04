/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * gs_db_privilege.h
 *     definition of relation for database level privileges
 *
 * IDENTIFICATION
 *	  src/include/catalog/gs_db_privilege.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_DB_PRIVILEGE_H
#define GS_DB_PRIVILEGE_H

#include "catalog/genbki.h"
#include "nodes/parsenodes.h"

#define DbPrivilegeId  5566
#define DbPrivilege_Rowtype_Id  5567

CATALOG(gs_db_privilege,5566) BKI_SCHEMA_MACRO
{
    Oid    roleid;
#ifdef CATALOG_VARLEN
    text    privilege_type;
#endif
    bool    admin_option;
} FormData_gs_db_privilege;

typedef FormData_gs_db_privilege *Form_gs_db_privilege;

#define Natts_gs_db_privilege                      3
#define Anum_gs_db_privilege_roleid                1
#define Anum_gs_db_privilege_privilege_type        2
#define Anum_gs_db_privilege_admin_option          3

extern void ExecuteGrantDbStmt(GrantDbStmt* stmt);
extern bool HasSpecAnyPriv(Oid userId, const char* priv, bool isAdminOption);
extern bool HasOneOfAnyPriv(Oid roleId);
extern Datum has_any_privilege(PG_FUNCTION_ARGS);
extern void DropDbPrivByOid(Oid roleID);

#define CREATE_ANY_TABLE     "create any table"
#define ALTER_ANY_TABLE      "alter any table"
#define DROP_ANY_TABLE       "drop any table"
#define SELECT_ANY_TABLE     "select any table"
#define INSERT_ANY_TABLE     "insert any table"
#define UPDATE_ANY_TABLE     "update any table"
#define DELETE_ANY_TABLE     "delete any table"
#define CREATE_ANY_FUNCTION  "create any function"
#define EXECUTE_ANY_FUNCTION "execute any function"
#define CREATE_ANY_PACKAGE   "create any package"
#define EXECUTE_ANY_PACKAGE  "execute any package"
#define CREATE_ANY_TYPE      "create any type"
#define CREATE_ANY_SEQUENCE  "create any sequence"
#define CREATE_ANY_INDEX     "create any index"
#define ALTER_ANY_TYPE       "alter any type"
#define DROP_ANY_TYPE        "drop any type"
#define ALTER_ANY_SEQUENCE   "alter any sequence"
#define DROP_ANY_SEQUENCE    "drop any sequence"
#define SELECT_ANY_SEQUENCE  "select any sequence"
#define ALTER_ANY_INDEX      "alter any index"
#define DROP_ANY_INDEX       "drop any index"
#define CREATE_ANY_SYNONYM   "create any synonym"
#define DROP_ANY_SYNONYM     "drop any synonym"
#define CREATE_ANY_TRIGGER   "create any trigger"
#define ALTER_ANY_TRIGGER    "alter any trigger"
#define DROP_ANY_TRIGGER     "drop any trigger"


#endif /* GS_DB_PRIVILEGE_H */
