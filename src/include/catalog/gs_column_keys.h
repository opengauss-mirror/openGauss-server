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
 * gs_column_keys.h
 *
 * IDENTIFICATION
 *	  src/include/catalog/gs_column_keys.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_COLUMN_KEYS_H
#define GS_COLUMN_KEYS_H

#include "catalog/genbki.h"
#include "utils/timestamp.h"


#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

#define ClientLogicColumnSettingsId  9720
#define ClientLogicColumnSettingsId_Rowtype_Id 9724

CATALOG(gs_column_keys,9720) BKI_SCHEMA_MACRO
{
    NameData    column_key_name; /* An alias for column encrypyion key */
    Oid         column_key_distributed_id; /* id created from hasing fqdn */
    /* ID of the client encryption key that was used to encrypt the columd encryption key. (foreign key) */
    Oid         global_key_id; 
    Oid         key_namespace;
    Oid         key_owner;
    timestamp 	create_date;
#ifdef CATALOG_VARLEN
    aclitem     key_acl[1];      /* access permissions */
#endif
} FormData_gs_column_keys;

typedef FormData_gs_column_keys *Form_gs_column_keys;

#define Natts_gs_column_keys                         7

#define Anum_gs_column_keys_column_key_name                          1
#define Anum_gs_column_keys_column_key_distributed_id                2
#define Anum_gs_column_keys_global_key_id                            3
#define Anum_gs_column_keys_key_namespace                                4 
#define Anum_gs_column_keys_key_owner                                    5
#define Anum_gs_column_keys_create_date                                  6
#define Anum_gs_column_keys_key_acl                                      7

#endif   /* GS_COLUMN_KEYS_H */
