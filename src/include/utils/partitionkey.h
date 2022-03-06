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
extern void CheckValuePartitionKeyType(Form_pg_attribute* attrs, List* pos);

extern Oid getPartitionOidForRTE(RangeTblEntry *rte, RangeVar *relation, ParseState *pstate, Relation rel);
extern Oid GetSubPartitionOidForRTE(RangeTblEntry *rte, RangeVar *relation, ParseState *pstate, Relation rel,
                                    Oid *partOid);

#define partitonKeyCompareForRouting(value1, value2, len, compare)                                               \
    do {                                                                                                         \
        uint32 i = 0;                                                                                            \
        Const *v1 = NULL;                                                                                        \
        Const *v2 = NULL;                                                                                        \
        for (; i < (len); i++) {                                                                                 \
            v1 = *((value1) + i);                                                                                \
            v2 = *((value2) + i);                                                                                \
            if (v1 == NULL || v2 == NULL) {                                                                      \
                if (v1 == NULL && v2 == NULL) {                                                                  \
                    ereport(ERROR,                                                                               \
                            (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("NULL can not be compared with NULL"))); \
                } else if (v1 == NULL) {                                                                         \
                    compare = -1;                                                                                \
                } else {                                                                                         \
                    compare = 1;                                                                                 \
                }                                                                                                \
                break;                                                                                           \
            }                                                                                                    \
            if (constIsMaxValue(v1) || constIsMaxValue(v2)) {                                                    \
                if (constIsMaxValue(v1) && constIsMaxValue(v2)) {                                                \
                    compare = 0;                                                                                 \
                    continue;                                                                                    \
                } else if (constIsMaxValue(v1)) {                                                                \
                    compare = 1;                                                                                 \
                } else {                                                                                         \
                    compare = -1;                                                                                \
                }                                                                                                \
                break;                                                                                           \
            }                                                                                                    \
            if (v1->constisnull || v2->constisnull) {                                                            \
                if (v1->constisnull && v2->constisnull) {                                                        \
                    ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),                                      \
                                    errmsg("null value can not be compared with null value.")));                 \
                } else if (v1->constisnull) {                                                                    \
                    compare = 1;                                                                                 \
                } else {                                                                                         \
                    compare = -1;                                                                                \
                }                                                                                                \
                break;                                                                                           \
            }                                                                                                    \
            constCompare(v1, v2, compare);                                                                       \
            if ((compare) != 0) {                                                                                \
                break;                                                                                           \
            }                                                                                                    \
        }                                                                                                        \
    } while (0)

#endif
