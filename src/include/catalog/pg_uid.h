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
 * ---------------------------------------------------------------------------------------
 *
 * pg_uid.h
 *
 *
 * IDENTIFICATION
 *        src/include/catalog/pg_uid.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef PG_UID_H
#define PG_UID_H
#include "catalog/genbki.h"

#define UidRelationId 8666
#define UidRelationId_Rowtype_Id (8667)

#define int8 int64

CATALOG(gs_uid,8666) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
    Oid   relid;
    int8  uid_backup;
} FormData_gs_uid;

#undef int8

typedef FormData_gs_uid* Form_gs_uid;

#define Natts_gs_uid           2
#define Anum_gs_uid_relid      1
#define Anum_gs_uid_backup     2
#endif /* PG_UID_H */

