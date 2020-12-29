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
 * gs_column_keys_args.h
 *
 * IDENTIFICATION
 *	  src/include/catalog/gs_column_keys_args.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_COLUMN_KEYS_ARGS_H
#define GS_COLUMN_KEYS_ARGS_H


#include "catalog/genbki.h"

#define ClientLogicColumnSettingsArgsId  9740
#define ClientLogicColumnSettingsArgsId_Rowtype_Id  9743
CATALOG(gs_column_keys_args,9740) BKI_SCHEMA_MACRO
{
    Oid         column_key_id;
    NameData    function_name;
    NameData    key;
    bytea       value;
} FormData_gs_column_keys_args;

typedef FormData_gs_column_keys_args *Form_gs_column_keys_args;

#define Natts_gs_column_keys_args                        4
#define Anum_gs_column_keys_args_column_key_id    1 
#define Anum_gs_column_keys_args_function_name           2 
#define Anum_gs_column_keys_args_key                     3
#define Anum_gs_column_keys_args_value                   4

#endif   /* GS_COLUMN_KEYS_ARGS_H */
