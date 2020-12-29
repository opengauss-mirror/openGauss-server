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
 * gs_client_global_keys.h
 *
 * IDENTIFICATION
 *	  src/include/catalog/gs_client_global_keys.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_CLIENT_GLOBAL_KEYS_H
#define GS_CLIENT_GLOBAL_KEYS_H

#include "catalog/genbki.h"

#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

#define ClientLogicGlobalSettingsId  9710
#define ClientLogicGlobalSettingsId_Rowtype_Id  9713
CATALOG(gs_client_global_keys,9710) BKI_SCHEMA_MACRO
{
    NameData   global_key_name; /* An alias for client encrypyion key */
    Oid         key_namespace;
    Oid         key_owner;
#ifdef CATALOG_VARLEN
    aclitem     key_acl[1];
#endif
    timestamp   create_date;
} FormData_gs_client_global_keys;

typedef FormData_gs_client_global_keys *Form_gs_client_global_keys;

#define Natts_gs_client_global_keys                         5
#define Anum_gs_client_global_keys_global_key_name   1 
#define Anum_gs_client_global_keys_key_namespace            2 
#define Anum_gs_client_global_keys_key_owner                3
#define Anum_gs_client_global_keys_key_acl                  4
#define Anum_gs_client_global_keys_create_date              5

#endif /* GS_CLIENT_GLOBAL_KEYS_H */
