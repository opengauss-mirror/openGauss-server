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

#ifdef ENABLE_MULTIPLE_NODES
#define is_valid_query(queryDesc)                                                                                \
    ((queryDesc != NULL) &&                                                                                      \
     (((queryDesc->sourceText != NULL && strcmp(queryDesc->sourceText, "DUMMY") != 0 && IS_PGXC_COORDINATOR)) || \
      (IS_PGXC_DATANODE && !StreamThreadAmI() && !StreamTopConsumerAmI())))
#else
#define is_valid_query(queryDesc) \
    (queryDesc != NULL && queryDesc->sourceText != NULL && strcmp(queryDesc->sourceText, "DUMMY") != 0)
#endif

#define auto_explain_enabled() \
    (u_sess->attr.attr_storage.log_min_duration_statement >= 0 && u_sess->exec_cxt.nesting_level == 0)
#define auto_explain_plan() (!u_sess->attr.attr_sql.under_explain && u_sess->attr.attr_resource.enable_auto_explain)

void ReportQueryStatus(void);
void WLMSetSessionSlowInfo(WLMStmtDetail *pDetail);
void exec_auto_explain(QueryDesc *queryDesc);
void exec_explain_plan(QueryDesc *queryDesc);
void exec_end_explain_plan(QueryDesc *queryDesc);
void print_duration();
void explain_querydesc(ExplainState *es, QueryDesc *queryDesc);
void exec_do_explain(QueryDesc *queryDesc, bool running);

#endif
