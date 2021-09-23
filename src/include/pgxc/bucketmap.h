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
 * bucketmap.h
 *
 * IDENTIFICATION
 *	  src/include/pgxc/bucketmap.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef BUCKETMAP_H
#define BUCKETMAP_H

#include "c.h"

#include "access/htup.h"
#include "nodes/parsenodes.h"
#include "storage/item/itemptr.h"
#include "utils/relcache.h"

/*
 * The element data structure of bucketmap cache, where store the bucketmap that
 * palloc-ed form t_thrd.top_mem_cxt, it is created on its first being used
 */
typedef struct BucketMapCache {
    /* Search key of bucketmap cache */
    Oid groupoid;
    ItemPointerData ctid;
    char* groupname;

    /* bucketmap content, palloc()-ed form top memory context */
    uint2* bucketmap;
    int    bucketcnt;
} NodeGroupBucketMap;

#define BUCKET_MAP_SIZE 32
#define BUCKETMAP_MODE_DEFAULT 0
#define BUCKETMAP_MODE_REMAP 1

extern void GenerateConsistentHashBucketmap(CreateGroupStmt* stmt, oidvector* nodes_array,
     Relation pgxc_group_rel, HeapTuple tuple, uint2* bucket_ptr, int bucketmap_mode, int bucketCnt);
extern void BucketMapCacheRemoveEntry(Oid groupoid);
extern char* GetBucketString(uint2* bucket_ptr, int bucketCnt);
extern void PgxcCopyBucketsFromNew(const char* group_name, const char* src_group_name);

#endif
