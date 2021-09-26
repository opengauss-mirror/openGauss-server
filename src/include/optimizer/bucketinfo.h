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
 * bucketinfo.h
 *    the basic node defination for hashbucket
 *    we keep BucketInfo in a seperate file
 *
 *
 * IDENTIFICATION
 *     src/include/optimizer/bucketinfo.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef BUCKETINFO_H
#define BUCKETINFO_H

#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "lib/stringinfo.h"

typedef struct BucketInfo {
    NodeTag type;
    List* buckets; /* bukect ids to be scanned, NULL means all buckets */
} BucketInfo;

extern char* bucketInfoToString(BucketInfo* bucket_info);
extern bool hasValidBuckets(RangeVar* r, int bucketmapsize);
extern List* RangeVarGetBucketList(RangeVar* rangeVar);
#endif
