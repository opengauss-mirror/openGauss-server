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
 * time_bucket.h
 *      time_window function, like date_trunc, but more relax
 *
 * IDENTIFICATION
 *        src/include/tsdb/time_bucket.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CONTRIB_TSDB_TIME_BUCKET_H
#define CONTRIB_TSDB_TIME_BUCKET_H

#include "fmgr.h"
#include "utils/timestamp.h"

extern "C" Datum time_window(PG_FUNCTION_ARGS);
#ifdef ENABLE_UT
extern "C" int64 get_interval_period_timestamp_units(Interval* interval);
extern "C" TimestampTz time_bucket_ts(int64 period, TimestampTz timestamp, TimestampTz shift);
#endif

#endif /* CONTRIB_TSDB_TIME_BUCKET_H */

