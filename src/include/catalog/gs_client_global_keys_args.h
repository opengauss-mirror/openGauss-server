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
 * gs_client_global_keys_args.h
 *
 * IDENTIFICATION
 *	  src/include/catalog/gs_client_global_keys_args.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_CLIENT_GLOBAL_KEYS_ARGS_H
#define GS_CLIENT_GLOBAL_KEYS_ARGS_H
#include "catalog/genbki.h"

#define ClientLogicGlobalSettingsArgsId  9730
#define ClientLogicGlobalSettingsArgsId_Rowtype_Id  9730
CATALOG(gs_client_global_keys_args,9730) BKI_SCHEMA_MACRO
{
    /* ID of the master client key that was used to encrypt the columd encryption key. (foreign key) */
    Oid         global_key_id; 
    NameData    function_name;
    NameData    key;
    bytea       value;
} FormData_gs_client_global_keys_args;

typedef FormData_gs_client_global_keys_args *Form_gs_client_global_keys_args;

#define Natts_gs_client_global_keys_args                        4
#define Anum_gs_client_global_keys_args_global_key_id    1 
#define Anum_gs_client_global_keys_args_function_name           2 
#define Anum_gs_client_global_keys_args_key                     3
#define Anum_gs_client_global_keys_args_value                   4

#endif /* GS_CLIENT_GLOBAL_KEYS_ARGS_H */
