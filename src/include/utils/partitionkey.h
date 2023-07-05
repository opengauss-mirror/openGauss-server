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
 * partitionkey.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/partitionkey.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PARTITIONKEY_H
#define PARTITIONKEY_H

#include "nodes/pg_list.h"

/*
 * @@GaussDB@@
 * Brief
 * Description	: 	transform the list of maxvalue
 *				  	(partition's boundary, Const node form) into TEXT array
 * input		: 	List of Const
 * return value	: 	TEXT array
 */
extern Datum transformPartitionBoundary(List* maxValueList, const bool* isTimestamptz);

extern Datum transformListBoundary(List* maxValueList, const bool* isTimestamptz);

/*
 * @@GaussDB@@
 * Brief
 * Description	: transform TEXT array into a list of Value
 * input		: TEXT Array
 * return value	: List of Value
 * Note			:
 */
extern List* untransformPartitionBoundary(Datum options);

/*
 * @@GaussDB@@
 * Brief
 * Description	: Check if given attnum pos is satisfied with value partitioned policy
 * input		: atts array and position(attno) of partitioned columns
 * return value	: None-
 * Note			:
 */
extern void CheckValuePartitionKeyType(FormData_pg_attribute* attrs, List* pos);

extern bool GetPartitionOidForRTE(RangeTblEntry *rte, RangeVar *relation, ParseState *pstate, Relation rel);
extern bool GetSubPartitionOidForRTE(RangeTblEntry *rte, RangeVar *relation, ParseState *pstate, Relation rel);
extern void GetPartitionOidListForRTE(RangeTblEntry *rte, RangeVar *relation);

/* the 2nd parameter must be partition boundary */
#define partitonKeyCompareForRouting(partkey_value, partkey_bound, len, compare)                                 \
    do {                                                                                                         \
        uint32 i = 0;                                                                                            \
        Const *kv = NULL;                                                                                        \
        Const *bv = NULL;                                                                                        \
        for (; i < (len); i++) {                                                                                 \
            kv = *((partkey_value) + i);                                                                                \
            bv = *((partkey_bound) + i);                                                                                \
            if (kv == NULL || bv == NULL) {                                                                      \
                if (kv == NULL && bv == NULL) {                                                                  \
                    ereport(ERROR,                                                                               \
                            (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("NULL can not be compared with NULL"))); \
                } else if (kv == NULL) {                                                                         \
                    compare = -1;                                                                                \
                } else {                                                                                         \
                    compare = 1;                                                                                 \
                }                                                                                                \
                break;                                                                                           \
            }                                                                                                    \
            if (constIsMaxValue(kv) || constIsMaxValue(bv)) {                                                    \
                if (constIsMaxValue(kv) && constIsMaxValue(bv)) {                                                \
                    compare = 0;                                                                                 \
                    continue;                                                                                    \
                } else if (constIsMaxValue(kv)) {                                                                \
                    compare = 1;                                                                                 \
                } else {                                                                                         \
                    compare = -1;                                                                                \
                }                                                                                                \
                break;                                                                                           \
            }                                                                                                    \
            if (kv->constisnull || bv->constisnull) {                                                            \
                if (kv->constisnull && bv->constisnull) {                                                        \
                    ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),                                      \
                                    errmsg("null value can not be compared with null value.")));                 \
                } else if (kv->constisnull) {                                                                    \
                    compare = 1;                                                                                 \
                } else {                                                                                         \
                    compare = -1;                                                                                \
                }                                                                                                \
                break;                                                                                           \
            }                                                                                                    \
            constCompare(kv, bv, bv->constcollid, compare);                                                      \
            if ((compare) != 0) {                                                                                \
                break;                                                                                           \
            }                                                                                                    \
        }                                                                                                        \
    } while (0)

#endif

#define constIsNull(x) ((x)->constisnull)
#define constIsMaxValue(x) ((x)->ismaxvalue)

int ConstCompareWithNull(Const *c1, Const *c2, Oid collation);
int ListPartKeyCompare(PartitionKey* k1, PartitionKey* k2);