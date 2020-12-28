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
 * gs_encrypted_columns.h
 *
 * IDENTIFICATION
 *	  src/include/catalog/gs_encrypted_columns.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_ENCRYPTED_COLUMNS_H
#define GS_ENCRYPTED_COLUMNS_H

#include "catalog/genbki.h"

#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

#define ClientLogicCachedColumnsId  9700
#define ClientLogicCachedColumnsId_Rowtype_Id  9703
CATALOG(gs_encrypted_columns,9700) BKI_SCHEMA_MACRO
{
    Oid rel_id;
    NameData column_name;
    Oid    column_key_id;
    int1        encryption_type;
    Oid    data_type_original_oid;
    int4    data_type_original_mod;
    timestamp create_date;
} FormData_gs_encrypted_columns;

typedef FormData_gs_encrypted_columns *Form_gs_encrypted_columns;

#define Natts_gs_encrypted_columns                        7

#define Anum_gs_encrypted_columns_rel_id       1
#define Anum_gs_encrypted_columns_column_name                     2
#define Anum_gs_encrypted_columns_column_key_id             3
#define Anum_gs_sec_encrypted_columns_encryption_type         4
#define Anum_gs_encrypted_columns_data_type_original_oid           5
#define Anum_gs_encrypted_columns_data_type_original_mod        6
#define Anum_gs_encrypted_columns_create_date       7

#endif   /* GS_ENCRYPTED_COLUMNS_H */

