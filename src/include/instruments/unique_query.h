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
 * unique_query.h
 *	  definitions for unique query
 * 
 * 
 * IDENTIFICATION
 *        src/include/instruments/unique_query.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef _QUERYID_
#define _QUERYID_

#include "nodes/parsenodes.h"
#include "c.h"

extern uint32 generate_unique_queryid(Query* query, const char* query_string);
extern bool normalized_unique_querystring(Query* query, const char* query_string, char* unique_string, int len,
    uint32 multi_sql_offset);
extern void init_builtin_unique_sql();

#endif
