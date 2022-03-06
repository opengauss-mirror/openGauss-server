/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 *   explain_model.h
 *
 * IDENTIFICATION
 *   src/include/db4ai/explain_model.h
 *
 *---------------------------------------------------------------------------------------
 */

#ifndef DB4AI_EXPLAIN_MODEL_H
#define DB4AI_EXPLAIN_MODEL_H
#include "postgres.h"
#include "fmgr/fmgr_comp.h"

extern char* str_tolower(const char* buff, size_t nbytes, Oid collid);
text* ExecExplainModel(char* model_name);
Datum db4ai_explain_model(PG_FUNCTION_ARGS);
#endif
