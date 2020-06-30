/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * encoding.h
 *
 * IDENTIFICATION
 * src/include/optimizer/encoding.h
 *
 * DESCRIPTION
 * Declaration of externel APIs of Code/src/backend/utils/learn/encoding.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef ENCODING_H
#define ENCODING_H

#include "postgres.h"

#include "nodes/execnodes.h"
#include "nodes/plannodes.h"

extern void PHGetPlanNodeText(Plan* plan, char** pname, bool* isRow, char** strategy, char** optionData);
extern void SaveDataToFile(const char* filename);
extern OperatorPlanInfo* ExtractOperatorPlanInfo(PlanState* result_plan, PlannedStmt* pstmt);

#endif /* ENCODING_H */