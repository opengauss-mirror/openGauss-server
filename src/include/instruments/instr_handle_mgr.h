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
 * instr_handle_mgr.h
 *        definitions for handle manager used in full/slow sql
 *
 *
 * IDENTIFICATION
 *        src/include/instruments/instr_handle_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef INSTR_HANDLE_MGR_H
#define INSTR_HANDLE_MGR_H
#include "pgstat.h"

#define CURRENT_STMT_METRIC_HANDLE ((StatementStatContext*)(u_sess->statement_cxt.curStatementMetrics))
#define CHECK_STMT_HANDLE() \
{                                                                                               \
    if (CURRENT_STMT_METRIC_HANDLE == NULL || u_sess->statement_cxt.stmt_stat_cxt == NULL) {    \
        return;                                                                                 \
    }                                                                                           \
}

void statement_init_metric_context();
void statement_commit_metirc_context();
void release_statement_context(PgBackendStatus* beentry, const char* func, int line);
void* bind_statement_context();

#endif
