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
 * gs_auditing_policy_priv.h
 *     Definition about filter catalog in auditing policy
 * 
 * 
 * IDENTIFICATION
 *     src/include/catalog/gs_auditing_policy_filter.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GS_AUDITING_POLICY_FILTER_H
#define GS_AUDITING_POLICY_FILTER_H

#include "catalog/genbki.h"
#include "utils/timestamp.h"
#include "utils/date.h"

#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

#define GsAuditingPolicyFiltersRelationId   9540
#define GsAuditingPolicyFiltersRelationId_Rowtype_Id 9543

CATALOG(gs_auditing_policy_filters,9540) BKI_SCHEMA_MACRO
{
    NameData    filtertype;
    NameData    labelname;
    Oid         policyoid;
    timestamp   modifydate;
    text        logicaloperator;
} FormData_gs_auditing_policy_filters;

typedef FormData_gs_auditing_policy_filters *Form_gs_auditing_policy_filters;

#define Natts_gs_auditing_policy_filters             5

#define Anum_gs_auditing_policy_fltr_filter_type         1
#define Anum_gs_auditing_policy_fltr_label_name          2
#define Anum_gs_auditing_policy_fltr_policy_oid          3
#define Anum_gs_auditing_policy_fltr_modify_date         4
#define Anum_gs_auditing_policy_fltr_logical_operator    5

#endif   /* GS_AUDITING_POLICY_FILTER_H */

