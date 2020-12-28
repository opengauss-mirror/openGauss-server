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
 * gs_masking_policy_filter.h
 *
 * IDENTIFICATION
 *    src/include/catalog/gs_masking_policy_filter.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_MASKING_POLICY_FILTER_H
#define GS_MASKING_POLICY_FILTER_H

#include "catalog/genbki.h"
#include "utils/timestamp.h"
#include "utils/date.h"

#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

#define GsMaskingPolicyFiltersId    9640
#define GsMaskingPolicyFiltersId_Rowtype_Id 9643

CATALOG(gs_masking_policy_filters,9640) BKI_SCHEMA_MACRO
{
    NameData    filtertype;
    NameData    filterlabelname;
    Oid         policyoid;
    timestamp   modifydate;
#ifdef CATALOG_VARLEN
    text        logicaloperator;
#endif
} FormData_gs_masking_policy_filters;

typedef FormData_gs_masking_policy_filters *Form_gs_masking_policy_filters;

#define Natts_gs_masking_policy_filters             5

#define Anum_gs_masking_policy_fltr_filter_type         1
#define Anum_gs_masking_policy_fltr_label_name          2
#define Anum_gs_masking_policy_fltr_policy_oid          3
#define Anum_gs_masking_policy_fltr_modify_date         4
#define Anum_gs_masking_policy_fltr_logical_operator    5


#endif   /* GS_MASKING_POLICY_FILTER_H */

