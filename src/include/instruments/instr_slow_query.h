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
 * instr_slow_query.h
 *
 * IDENTIFICATION
 *    src/include/instruments/instr_slow_query.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef INSTR_SLOW_QUERY_H
#define INSTR_SLOW_QUERY_H
#include "nodes/parsenodes.h"
#include "pgstat.h"
#include "c.h"

void ReportQueryStatus(void);
void WLMSetSessionSlowInfo(WLMStmtDetail* pDetail);
void exec_auto_explain(QueryDesc  *queryDesc);
void exec_explain_plan(QueryDesc *queryDesc);
void print_duration(const QueryDesc *queryDesc);
void explain_querydesc(ExplainState *es, QueryDesc *queryDesc);

#endif
