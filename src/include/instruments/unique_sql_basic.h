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
 * unique_sql_basic.h
 *
 *
 * IDENTIFICATION
 *       src/include/instruments/unique_sql_basic.h
 *
 * ---------------------------------------------------------------------------------------
 */
 
#ifndef UNIQUE_SQL_BASIC_H
#define UNIQUE_SQL_BASIC_H

typedef struct {
    uint32 cn_id;         /* SQL is run on which CN node,
                           * same with node_id in PGXC_NODE */
    Oid user_id;          /* user id */
    uint64 unique_sql_id; /* unique sql id */
} UniqueSQLKey;

typedef struct {
    pg_atomic_uint64 soft_parse; /* reuse plan counter */
    pg_atomic_uint64 hard_parse; /* new generated plan counter */
} UniqueSQLParse;

typedef struct {
    pg_atomic_uint64 returned_rows; /* select SQL returned rows */

    pg_atomic_uint64 tuples_fetched;  /* randowm IO */
    pg_atomic_uint64 tuples_returned; /* sequence IO */

    pg_atomic_uint64 tuples_inserted; /* inserted tuples counter */
    pg_atomic_uint64 tuples_updated;  /* updated tuples counter */
    pg_atomic_uint64 tuples_deleted;  /* deleted tuples counter */
} UniqueSQLRowActivity;

typedef struct {
    pg_atomic_uint64 blocks_fetched; /* the blocks fetched times */
    pg_atomic_uint64 blocks_hit;     /* the blocks hit times in buffer */
} UniqueSQLCacheIO;

#endif

