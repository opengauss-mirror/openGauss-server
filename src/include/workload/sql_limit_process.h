/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * sql_limit_process.h
 *
 * IDENTIFICATION
 *	  src/include/workload/sql_limit_process.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SQL_LIMIT_PROCESS_H
#define SQL_LIMIT_PROCESS_H

#include "postgres.h"
#include "commands/dbcommands.h"
#include "utils/timestamp.h"
#include "workload/sql_limit_base.h"

const int MAX_SQL_LIMIT_COUNT = 100;
const int TIME_INTERVAL_SEC = 5;
const int SQL_LIMIT_INIT_HASH_SIZE = 100;

SqlLimit* SearchSqlLimitCache(uint64 limitId);
bool DeleteSqlLimitCache(uint64 limitId);
void CleanSqlLimitCache();
void RemoveInvalidSqlLimitCache();
void UpdateSqlLimitCache();
void LimitCurrentQuery(const char* commandTag, const char* queryString);
void UnlimitCurrentQuery();
void InitSqlLimitCache();
SqlType GetSqlLimitType(const char* sqlType);
bool ValidateAndExtractOption(SqlType sqlType, Datum* values, uint64* uniqueSqlid);
void CreateSqlLimit(Datum* values, bool* nulls, uint64* uniqueSqlid);
bool UpdateSqlLimit(Datum* values, bool* nulls, uint64* uniqueSqlid);

#endif