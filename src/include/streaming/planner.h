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
 * planner.h
 *        Head file for the entance of stream to modify the plan tree.
 *
 *
 * IDENTIFICATION
 *        src/include/streaming/planner.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef STREAMING_PLANNER_H
#define STREAMING_PLANNER_H

#include <optimizer/planner.h>

bool is_insert_stream_query(Query *query);
bool is_streaming_hash_group_func(const char* funcname, Oid funcnamespace);
bool is_streaming_invisible_obj(Oid oid);

#endif
