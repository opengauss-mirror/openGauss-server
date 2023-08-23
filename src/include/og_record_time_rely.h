/*
 * Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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
 * IDENTIFICATION
 *	  src/include/og_record_time_rely.h
 *
 * NOTES
 *	  Declare some openGauss system func or types.
 *
 * -------------------------------------------------------------------------
 */

#ifndef PG_RECORD_TIME_TMP_H
#define PG_RECORD_TIME_TMP_H
#include "postgres.h"
#include "c.h"
#include "datatype/timestamp.h"
extern TimestampTz GetCurrentTimestamp();
void DestroyStringInfo(StringInfo str);
void ResetMemory(void* dest, size_t size);
#endif