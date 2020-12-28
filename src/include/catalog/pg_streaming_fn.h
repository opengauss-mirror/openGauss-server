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
 *---------------------------------------------------------------------------------------
 *
 * pg_streaming_fn.h
 *      streaming functions
 *
 *
 * IDENTIFICATION
 *        src/include/catalog/pg_streaming_fn.h
 *
 *---------------------------------------------------------------------------------------
 */
#ifndef PG_STREAMING_FN_H
#define PG_STREAMING_FN_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"

#define STREAMING_SERVER "streaming"

void lookup_pg_stream_cont_query();

#endif
