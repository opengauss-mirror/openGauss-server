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
 * ml_model.h
 *
 * IDENTIFICATION
 * src/include/optimizer/ml_model.h
 *
 * DESCRIPTION
 * Declaration of externel APIs of Code/src/backend/utils/learn/ml_model.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef ML_MODEL_H
#define ML_MODEL_H

#include "postgres.h"

#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "optimizer/learn.h"

extern Form_gs_opt_model CheckModelTargets(const char* templateName, const char* modelName, char** labels, int* nLabel);
extern void ModelPredictForExplain(PlanState* root, char* fileName, const char* modelName);
extern char* PreModelPredict(PlanState* resultPlan, PlannedStmt* pstmt);
extern void SetNullPrediction(PlanState* resultPlan);
extern bool PredictorIsValid(const char* modelName);

#endif /* ML_MODEL_H */