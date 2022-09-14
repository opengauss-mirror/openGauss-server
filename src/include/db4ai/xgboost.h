/**
Copyright (c) 2021 Huawei Technologies Co.,Ltd. 
openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

  http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
---------------------------------------------------------------------------------------

xgboost.h
        Public xgboost interface

IDENTIFICATION
    src/include/db4ai/xgboost.h

---------------------------------------------------------------------------------------
**/

#ifndef DB4AI_XGBOOST_H
#define DB4AI_XGBOOST_H

#include "db4ai/db4ai_api.h"

#define XG_TARGET_COLUMN    0
/*
 * the XGBoostnode (re-packaged)
 */
typedef struct XGBoost {
    AlgorithmAPI algo;
} XGBoost;

extern XGBoost xg_reg_logistic;
extern XGBoost xg_bin_logistic;
extern XGBoost xg_reg_sqe;
extern XGBoost xg_reg_gamma;
#endif 