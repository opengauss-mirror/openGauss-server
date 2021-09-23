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
 * pg_hashbucket.h
 *
 *
 * IDENTIFICATION
 *        src/include/catalog/pg_hashbucket.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_HASHBUCKET_H
#define PG_HASHBUCKET_H
#include "catalog/genbki.h"

#define HashBucketRelationId 9027
#define HashBucketRelationId_Rowtype_Id 9108

CATALOG(pg_hashbucket,9027) BKI_ROWTYPE_OID(9108) BKI_SCHEMA_MACRO
{
    Oid   bucketid;
    int4  bucketcnt;
    int4  bucketmapsize;
    int4  bucketref;
#ifdef CATALOG_VARLEN
    oidvector_extend bucketvector;
#endif
} FormData_pg_hashbucket;

#define Natts_pg_hashbucket              5
#define Anum_pg_hashbucket_bucketid      1
#define Anum_pg_hashbucket_bucketcnts    2
#define Anum_pg_hashbucket_bucketmapsize 3
#define Anum_pg_hashbucket_bucketref     4
#define Anum_pg_hashbucket_bucketvector  5
#endif /* PG_HASHBUCKET_H */

