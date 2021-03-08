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
#endif
