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
 * dynsmp_single.cpp
 *	  functions related to dynamic smp.
 *
 * IDENTIFICATION
 *     src/gausskernel/optimizer/plan/dynsmp_single.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "optimizer/streamplan.h"
void InitDynamicSmp()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void ChooseStartQueryDop(int hashTableCount)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void OptimizePlanDop(PlannedStmt* plannedStmt)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

bool IsDynamicSmpEnabled()
{
    return IS_STREAM_PLAN && u_sess->opt_cxt.max_query_dop >= 0 && !u_sess->attr.attr_common.IsInplaceUpgrade &&
           !IsInitdb;
}
