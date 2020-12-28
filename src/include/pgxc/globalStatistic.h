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
 * -------------------------------------------------------------------------
 *
 * globalStatistic.h
 *
 * IDENTIFICATION
 *	 src/include/pgxc/globalStatistic.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GLOBAL_STAISTTIC_H
#define GLOBAL_STAISTTIC_H

typedef enum StatisticKind {
    StatisticNone,
    StatisticPageAndTuple,
    StatisticHistogram,
    StatisticMultiHistogram,
    StatisticPartitionPageAndTuple
} StatisticKind;

extern char* construct_fetch_statistics_query(const char* schemaname, const char* relname, List* va_cols,
    StatisticKind kind, VacuumStmt* stmt, Oid relid, RangeVar* parentRel);
extern void FetchGlobalStatisticsFromCN(int cn_conn_count, PGXCNodeHandle** pgxc_connections,
    RemoteQueryState* remotestate, StatisticKind kind, VacuumStmt* stmt, Oid relid, PGFDWTableAnalyze* info);

#endif
