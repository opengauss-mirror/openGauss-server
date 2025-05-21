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
 * sql_limit_base.h
 *
 * The file is used to provide the base structure of sql limit.
 *
 * IDENTIFICATION
 *	  src/include/workload/sql_limit_base.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SQL_LIMIT_BASE_H
#define SQL_LIMIT_BASE_H

#include "postgres.h"
#include "utils/timestamp.h"
#include "nodes/pg_list.h"
#include "commands/dbcommands.h"

#define SQLID_TYPE "sqlId"
#define SELECT_TYPE "select"
#define UPDATE_TYPE "update"
#define INSERT_TYPE "insert"
#define DELETE_TYPE "delete"

typedef enum {
    SQL_TYPE_UNIQUE_SQLID = -1,
    SQL_TYPE_SELECT = 0,
    SQL_TYPE_INSERT,
    SQL_TYPE_UPDATE,
    SQL_TYPE_DELETE,
    SQL_TYPE_OTHER
} SqlType;

typedef struct {
    TimestampTz startTime;  /* start time, 0 means no start limit */
    TimestampTz endTime;    /* end time, 0 means no end limit */
} TimeWindow;


/* limit statistics structure */
typedef struct {
    volatile uint64 hitCount;
    volatile uint64 rejectCount;
    volatile uint64 currConcurrency;
} LimitStats;

/* SQL limit common structure */
typedef struct SqlLimit {
    uint64 limitId;
    SqlType sqlType;
    List *databases;
    List *users;
    uint64 maxConcurrency;
    int workNode;
    bool isValid;
    TransactionId xmin;

    TimeWindow timeWindow;
    LimitStats stats;

    union {
        struct {
            uint64 uniqueSqlId;
        } uniqueSql;

        struct {
            List *keywords;
        } keyword;
    } typeData;
} SqlLimit;

typedef struct KeywordsLimitNode {
    dlist_node node;
    SqlLimit* limit;
} KeywordsLimitNode;

typedef struct SqlLimitHashEntry {
    uint64 limitId;
    SqlType sqlType;
    SqlLimit *limit;
    dlist_node *keywordsNode;
} SqlLimitHashEntry;

typedef struct UniqueSqlIdHashEntry {
    uint64 uniqueSqlId;
    SqlLimit *limit;
} UniqueSqlIdHashEntry;


void TimeWindowInit(TimeWindow *window);
void TimeWindowSet(TimeWindow *window, TimestampTz start, TimestampTz end);
bool TimeWindowContainsTime(const TimeWindow *window);
bool TimeWindowContainsTimestamp(const TimeWindow *window, TimestampTz ts);

void LimitStatsInit(LimitStats *stats);
void LimitStatsUpdateHit(LimitStats *stats);
void LimitStatsUpdateReject(LimitStats *stats);
void LimitStatsUpdateConcurrency(LimitStats *stats, bool increase);
bool LimitStatsTryIncreaseConcurrency(LimitStats *stats, uint64 maxConcurrency);
void LimitStatsDecreaseConcurrency(LimitStats *stats);
void LimitStatsReset(LimitStats *stats);

SqlLimit *SqlLimitCreate(uint64 limitId, SqlType sqlType);
void SqlLimitDestroy(SqlLimit *limit);
void SqlLimitSetDatabases(SqlLimit *limit, List *databases);
void SqlLimitSetUsers(SqlLimit *limit, List *users);
bool SqlLimitIsValidTime(const SqlLimit *limit);
bool SqlLimitIsValidNode(const SqlLimit *limit);
bool SqlLimitIsValidDatabases(const SqlLimit *limit);
bool SqlLimitIsValidUsers(const SqlLimit *limit);
bool SqlLimitIsHit(const SqlLimit *limit, const char *queryString, uint64 queryId);
bool SqlLimitIsExceedMaxConcurrency(const SqlLimit *limit);
void SqlLimitClear(SqlLimit *limit);
bool IsKeywordsLimit(SqlType sqlType);

NON_EXEC_STATIC void SqlLimitMain();

#endif /* SQL_LIMIT_BASE_H */