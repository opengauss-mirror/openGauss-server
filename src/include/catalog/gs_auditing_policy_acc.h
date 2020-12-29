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
 * gs_auditing_policy_acc.h
 *     Definition about access catalog in auditing policy
 * 
 * 
 * IDENTIFICATION
 *     src/include/catalog/gs_auditing_policy_acc.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GS_AUDITING_POLICY_ACC_H
#define GS_AUDITING_POLICY_ACC_H

#include "catalog/genbki.h"
#include "utils/timestamp.h"
#include "utils/date.h"

#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

#define GsAuditingPolicyAccessRelationId    9520
#define GsAuditingPolicyAccessRelationId_Rowtype_Id 9523

CATALOG(gs_auditing_policy_access,9520) BKI_SCHEMA_MACRO
{
    NameData    accesstype;
    NameData    labelname;
    Oid         policyoid;
    timestamp   modifydate;
} FormData_gs_auditing_policy_access;

typedef FormData_gs_auditing_policy_access *Form_gs_auditing_policy_access;

#define Natts_gs_auditing_policy_acc                 4

#define Anum_gs_auditing_policy_acc_type             1
#define Anum_gs_auditing_policy_acc_label_name       2
#define Anum_gs_auditing_policy_acc_policy_oid       3
#define Anum_gs_auditing_policy_acc_modify_date      4

#endif   /* GS_AUDITING_POLICY_ACC_H */

