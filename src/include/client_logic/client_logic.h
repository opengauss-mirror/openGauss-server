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
 * client_logic.h
 *
 * IDENTIFICATION
 *	  src\include\client_logic\client_logic.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once
#include "postgres.h"
#include "catalog/heap.h"
#include "catalog/dependency.h"
#include "catalog/gs_client_global_keys.h"
#include "catalog/gs_client_global_keys_args.h"
#include "catalog/gs_column_keys.h"
#include "catalog/gs_column_keys_args.h"
#include "catalog/gs_encrypted_columns.h"
#include "lib/stringinfo.h"

typedef struct {
    Oid key_oid;
    NameData key_name;
} KeyOidNameMap;

void delete_client_master_keys(Oid roleid);
void delete_column_keys(Oid roleid);
int process_encrypted_columns(const ColumnDef * const def, CeHeapInfo *ce_heap_info);
int process_global_settings(CreateClientLogicGlobal *parsetree);
int process_column_settings(CreateClientLogicColumn *parsetree);
int drop_global_settings(DropStmt *stmt);
int drop_column_settings(DropStmt *stmt);
void remove_cmk_by_id(Oid id);
void remove_cek_by_id(Oid id);
void remove_encrypted_col_by_id(Oid id);
void remove_cmk_args_by_id(Oid id);
void remove_cek_args_by_id(Oid id);
void insert_gs_sec_encrypted_column_tuple(CeHeapInfo *ce_heap_info, Relation rel, const Oid rel_id,
    CatalogIndexState indstate);
bool is_exist_encrypted_column(const ObjectAddresses *targetObjects);
bool is_enc_type(Oid type_oid);
bool is_enc_type(const char *type_name);
bool is_full_encrypted_rel(Relation rel);
ClientLogicColumnRef *get_column_enc_def(Oid rel_oid, const char *col_name);
bool IsFullEncryptedRel(char* objSchema, char* objName);
bool IsFuncProcOnEncryptedRel(char* objSchema, char* objName);
/* Get description functions */
void get_global_setting_description(StringInfo buffer, const ObjectAddress* object);
void get_column_setting_description(StringInfo buffer, const ObjectAddress* object);
void get_cached_column_description(StringInfo buffer, const ObjectAddress* object);
void get_global_setting_args_description(StringInfo buffer, const ObjectAddress* object);
void get_column_setting_args_description(StringInfo buffer, const ObjectAddress* object);
const char *get_typename_by_id(Oid typeOid);
const char *get_encryption_type_name(EncryptionType algorithm_type);
extern Datum get_client_info(PG_FUNCTION_ARGS);