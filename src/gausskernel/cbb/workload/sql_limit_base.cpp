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
 * sql_limit_base.cpp
 *
 * The file is used to provide the interfaces of sql limit management.
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/workload/sql_limit_base.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "workload/sql_limit_base.h"
#include "catalog/gs_sql_limit.h"
#include "catalog/indexing.h"
#include "utils/atomic.h"
#include "utils/hsearch.h"
#include "utils/palloc.h"
#include "utils/snapmgr.h"
#include "utils/array.h"
#include "utils/mem_snapshot.h"
#include "utils/fmgroids.h"
#include "knl/knl_variable.h"
#include "utils/builtins.h"
#include "nodes/pg_list.h"
#include "access/xact.h"

void TimeWindowInit(TimeWindow *window)
{
    if (window == NULL) {
        return;
    }

    window->startTime = 0;
    window->endTime = 0;
}

void TimeWindowSet(TimeWindow *window, TimestampTz start, TimestampTz end)
{
    if (window == NULL) {
        return;
    }

    window->startTime = start;
    window->endTime = end;
}

bool TimeWindowContainsTime(const TimeWindow *window)
{
    return TimeWindowContainsTimestamp(window, GetCurrentTimestamp());
}

bool TimeWindowContainsTimestamp(const TimeWindow *window, TimestampTz ts)
{
    if (window == NULL) {
        return false;
    }

    if (window->startTime == 0 || ts >= window->startTime) {
        if (window->endTime == 0 || ts <= window->endTime) {
            return true;
        }
    }

    return false;
}

void LimitStatsInit(LimitStats *stats)
{
    if (stats == NULL) {
        return;
    }

    stats->hitCount = 0;
    stats->rejectCount = 0;
    stats->currConcurrency = 0;
}

void LimitStatsUpdateHit(LimitStats *stats)
{
    if (stats == NULL) {
        return;
    }

    pg_atomic_fetch_add_u64((pg_atomic_uint64 *)&stats->hitCount, 1);
}

void LimitStatsUpdateReject(LimitStats *stats)
{
    if (stats == NULL) {
        return;
    }

    pg_atomic_fetch_add_u64((pg_atomic_uint64 *)&stats->rejectCount, 1);
}

void LimitStatsUpdateConcurrency(LimitStats *stats, bool increase)
{
    if (stats == NULL) {
        return;
    }

    if (increase) {
        pg_atomic_fetch_add_u64((pg_atomic_uint64 *)&stats->currConcurrency, 1);
        return;
    }

    uint64 oldValue;
    uint64 newValue;
    do {
        oldValue = stats->currConcurrency;
        if (oldValue <= 0) {
            break;
        }
        newValue = oldValue - 1;
    } while (!pg_atomic_compare_exchange_u64((pg_atomic_uint64 *)&stats->currConcurrency, &oldValue, newValue));
}

bool LimitStatsTryIncreaseConcurrency(LimitStats *stats, uint64 maxConcurrency)
{
    if (stats == NULL) {
        return false;
    }

    uint64 oldValue;
    uint64 newValue;
    do {
        oldValue = stats->currConcurrency;
        if (oldValue >= maxConcurrency) {
            return false;
        }
        newValue = oldValue + 1;
    } while (!pg_atomic_compare_exchange_u64((pg_atomic_uint64 *)&stats->currConcurrency, &oldValue, newValue));
    return true;
}

void LimitStatsDecreaseConcurrency(LimitStats *stats)
{
    if (stats == NULL) {
        return;
    }

    uint64 oldValue;
    uint64 newValue;
    do {
        oldValue = stats->currConcurrency;
        if (oldValue <= 0) {
            break;
        }
        newValue = oldValue - 1;
    } while (!pg_atomic_compare_exchange_u64((pg_atomic_uint64 *)&stats->currConcurrency, &oldValue, newValue));
}

void LimitStatsReset(LimitStats *stats)
{
    if (stats == NULL) {
        return;
    }

    stats->hitCount = 0;
    stats->rejectCount = 0;
    stats->currConcurrency = 0;
}


SqlLimit *SqlLimitCreate(uint64 limitId, SqlType sqlType)
{
    SqlLimit *limit = (SqlLimit *)palloc0(sizeof(SqlLimit));

    limit->limitId = limitId;
    limit->sqlType = sqlType;
    limit->databases = NIL;
    limit->users = NIL;
    limit->maxConcurrency = 0;
    limit->workNode = 0;
    limit->isValid = true;
    limit->xmin = InvalidTransactionId;
    TimeWindowInit(&limit->timeWindow);
    LimitStatsInit(&limit->stats);
    limit->typeData.keyword.keywords = NIL;
    limit->typeData.uniqueSql.uniqueSqlId = 0;

    return limit;
}

void SqlLimitClear(SqlLimit *limit)
{
    if (limit == NULL) {
        return;
    }

    SqlType sqlType = limit->sqlType;

    limit->sqlType = SQL_TYPE_OTHER;
    limit->maxConcurrency = 0;
    limit->isValid = false;
    limit->workNode = 0;
    TimeWindowInit(&limit->timeWindow);
    LimitStatsInit(&limit->stats);

    list_free_ext(limit->databases);
    list_free_ext(limit->users);

    if (IsKeywordsLimit(sqlType)) {
        list_free_deep(limit->typeData.keyword.keywords);
    } else if (sqlType == SQL_TYPE_UNIQUE_SQLID) {
        limit->typeData.uniqueSql.uniqueSqlId = 0;
    }
}

void SqlLimitDestroy(SqlLimit *limit)
{
    if (limit == NULL) {
        return;
    }

    if (limit->databases != NIL) {
        list_free_ext(limit->databases);
    }

    if (limit->users != NIL) {
        list_free_ext(limit->users);
    }

    if (IsKeywordsLimit(limit->sqlType)) {
        if (limit->typeData.keyword.keywords != NIL) {
            list_free_deep(limit->typeData.keyword.keywords);
        }
    } else if (limit->sqlType == SQL_TYPE_UNIQUE_SQLID) {
        limit->typeData.uniqueSql.uniqueSqlId = 0;
    }

    pfree(limit);
}

void SqlLimitSetDatabases(SqlLimit *limit, List *databases)
{
    if (limit == NULL) {
        return;
    }

    if (limit->databases != NIL) {
        list_free_ext(limit->databases);
    }

    limit->databases = databases;
}

void SqlLimitSetUsers(SqlLimit *limit, List *users)
{
    if (limit == NULL) {
        return;
    }

    if (limit->users != NIL) {
        list_free_ext(limit->users);
    }

    limit->users = users;
}

bool SqlLimitIsValidTime(const SqlLimit *limit)
{
    if (limit == NULL) {
        return false;
    }

    return TimeWindowContainsTime(&limit->timeWindow);
}

bool SqlLimitIsValidNode(const SqlLimit *limit)
{
    if (limit == NULL) {
        return false;
    }

    if (limit->workNode == 0) {
        return true;
    } else if (limit->workNode == 1) {
        return (!RecoveryInProgress()); // master node
    } else {
        return (RecoveryInProgress()); // standby node
    }
}

bool SqlLimitIsValidDatabases(const SqlLimit *limit)
{
    if (limit == NULL) {
        return false;
    }

    if (list_length(limit->databases) == 0) {
        return true;
    }

    foreach_cell(cell, limit->databases) {
        Oid dbOid = lfirst_oid(cell);
        if (u_sess->proc_cxt.MyDatabaseId == dbOid) {
            return true;
        }
    }

    return false;
}

bool SqlLimitIsValidUsers(const SqlLimit *limit)
{
    if (limit == NULL) {
        return false;
    }

    // if no users, means all users need to be limited.
    if (list_length(limit->users) == 0) {
        return true;
    }

    // otherwise, limit the user in the list.
    foreach_cell(cell, limit->users) {
        Oid userOid = lfirst_oid(cell);
        if (GetCurrentUserId() == userOid) {
            return true;
        }
    }
    return false;
}

bool SqlLimitIsExceedMaxConcurrency(const SqlLimit *limit)
{
    if (limit == NULL) {
        return false;
    }

    return limit->stats.currConcurrency >= limit->maxConcurrency;
}

bool SqlLimitIsHit(const SqlLimit *limit, const char *queryString, uint64 queryId)
{
    if (limit == NULL || !limit->isValid) {
        return false;
    }

    if (!SqlLimitIsValidTime(limit) || !SqlLimitIsValidNode(limit) || !SqlLimitIsValidUsers(limit)) {
        return false;
    }

    switch (limit->sqlType) {
        case SQL_TYPE_UNIQUE_SQLID:
            return (queryId == limit->typeData.uniqueSql.uniqueSqlId);

        case SQL_TYPE_SELECT:
        case SQL_TYPE_INSERT:
        case SQL_TYPE_UPDATE:
        case SQL_TYPE_DELETE: {
            if (queryString == NULL || limit->typeData.keyword.keywords == NIL) {
                return false;
            }

            if (!SqlLimitIsValidDatabases(limit)) {
                return false;
            }

            const char* currentPos = queryString;
            foreach_cell(cell, limit->typeData.keyword.keywords) {
                const char* keyword = (const char*)lfirst(cell);
                // find keyword in current position and after
                const char* foundPos = strcasestr(currentPos, keyword);
                if (foundPos == NULL) {
                    return false;
                }

                // update current position to found keyword position
                currentPos = foundPos + strlen(keyword);
            }
            return true;
        }
        case SQL_TYPE_OTHER:
            return false;
        default:
            return false;
    }
}
