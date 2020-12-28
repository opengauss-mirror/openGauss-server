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
 * gs_masking_policy.h
 *
 * IDENTIFICATION
 *    src/include/catalog/gs_masking_policy.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_MASKING_POLICY_H
#define GS_MASKING_POLICY_H

#include "catalog/genbki.h"
#include "utils/timestamp.h"
#include "utils/date.h"

#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

#define GsMaskingPolicyRelationId  9610
#define GsMaskingPolicyRelationId_Rowtype_Id 9613

CATALOG(gs_masking_policy,9610) BKI_SCHEMA_MACRO
{
	NameData	polname;
    NameData    polcomments;
    timestamp   modifydate;
    bool        polenabled;
} FormData_gs_masking_policy;

typedef FormData_gs_masking_policy *Form_gs_masking_policy;

#define Natts_gs_masking_policy                         4

#define Anum_gs_masking_policy_pol_name                 1
#define Anum_gs_masking_policy_pol_comments             2 
#define Anum_gs_masking_policy_pol_modify_date          3
#define Anum_gs_masking_policy_pol_enabled              4


#endif   /* GS_MASKING_POLICY_H */

