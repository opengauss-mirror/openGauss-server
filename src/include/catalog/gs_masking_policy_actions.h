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
 * gs_masking_policy_actions.h
 *
 * IDENTIFICATION
 *    src/include/catalog/gs_masking_policy_actions.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_MASKING_POLICY_ACTIONS_H
#define GS_MASKING_POLICY_ACTIONS_H

#include "catalog/genbki.h"
#include "utils/timestamp.h"
#include "utils/date.h"

#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

#define GsMaskingPolicyActionsId    9650
#define GsMaskingPolicyActionsId_Rowtype_Id 9654

CATALOG(gs_masking_policy_actions,9650) BKI_SCHEMA_MACRO
{
    NameData    actiontype;
    NameData    actparams;
    NameData    actlabelname;
    Oid         policyoid;
    timestamp   actmodifydate;
} FormData_gs_masking_policy_actions;

typedef FormData_gs_masking_policy_actions *Form_gs_masking_policy_actions;

#define Natts_gs_masking_policy_actions             5

#define Anum_gs_masking_policy_act_action_type          1
#define Anum_gs_masking_policy_act_action_params        2
#define Anum_gs_masking_policy_act_label_name           3
#define Anum_gs_masking_policy_act_policy_oid           4
#define Anum_gs_masking_policy_act_modify_date          5


#endif   /* GS_MASKING_POLICY_ACTIONS_H */

